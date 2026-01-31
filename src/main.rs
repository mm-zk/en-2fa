use anyhow::{anyhow, Context, Result};
use clap::Parser;
use dotenvy::dotenv;
use ethers::abi::{ParamType, Token};
use ethers::prelude::*;
use ethers::types::{Address, Bytes, H256, U256};
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

mod db;
use db::{BatchDb, PostgresBatchDb};

const CONTRACT_ADDR: &str = "0xE222D6354b49eaF8a7099fC4E7F9C0B4FE72d1E7";

abigen!(
    ExecutionMultisigValidator,
    r#"[
        function approveHash(bytes32 _hash)
        function individualApprovals(address signer, bytes32 hash) view returns (bool)
        function executionMultisigMember(address signer) view returns (bool)
        function totalApprovals(bytes32 hash) view returns (uint256)
        function threshold() view returns (uint256)
    ]"#
);

#[derive(Parser, Debug)]
#[command(name="en-approvehash", about="Auto-approve zkSync batch execution hashes from EN DB")]
struct Args {
    /// External Node JSON-RPC URL
    #[arg(long, env = "EN_RPC_URL", default_value = "http://127.0.0.1:3060")]
    en_rpc_url: String,

    /// Postgres connection string
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Private key hex string (0x...)
    #[arg(long, env = "PK")]
    pk: String,

    /// Poll interval seconds
    #[arg(long, env = "POLL_INTERVAL_SECS", default_value_t = 3)]
    poll_interval_secs: u64,

    /// If true, do not send transactions
    #[arg(long, env = "DRY_RUN", default_value_t = 0)]
    dry_run: u8,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();

    // --- Provider + wallet ---
    let provider = Provider::<Http>::try_from(args.en_rpc_url.as_str())
        .context("Failed to create provider (EN_RPC_URL)")?
        .interval(Duration::from_millis(10000));

    let chain_id = provider
        .get_chainid()
        .await
        .context("Failed to fetch chain id")?
        .as_u64();

    let wallet: LocalWallet = args
        .pk
        .parse::<LocalWallet>()
        .context("Failed to parse PK as a local wallet")?
        .with_chain_id(chain_id);

    let signer_addr = wallet.address();
    info!(%chain_id, %signer_addr, "Signer ready");

    let client = Arc::new(SignerMiddleware::new(provider.clone(), wallet));
    let contract_addr = Address::from_str(CONTRACT_ADDR).context("Bad CONTRACT_ADDR")?;
    let contract = ExecutionMultisigValidator::new(contract_addr, client.clone());

    // --- Basic contract sanity checks ---
    let is_member = contract
        .execution_multisig_member(signer_addr)
        .call()
        .await
        .context("Failed to read executionMultisigMember")?;

    if !is_member {
        return Err(anyhow!(
            "Signer {} is not an executionMultisigMember; approveHash would revert NotSigner()",
            signer_addr
        ));
    }

    let threshold = contract.threshold().call().await.unwrap_or_default();
    info!(%threshold, "Contract threshold read");

    // --- DB ---
    let pool = sqlx::PgPool::connect(&args.database_url)
        .await
        .context("Failed to connect to Postgres (DATABASE_URL)")?;
    let db = PostgresBatchDb::new(pool);

    let mut last_seen_batch: i64 = 0;

    loop {
        match db.fetch_next_ready_execute_call(last_seen_batch).await? {
            None => {
                sleep(Duration::from_secs(args.poll_interval_secs)).await;
                continue;
            }
            Some(ready) => {
                info!(batch=%ready.l1_batch_number, "Found batch with prepared execute calldata");

                // Decode execute calldata => (chainAddress, from, to, batchData)
                let (chain_addr, from_batch, to_batch, batch_data) =
                    decode_execute_batches_shared_bridge(&ready.execute_calldata)
                        .context("Failed to decode execute calldata (adjust signature/decoder if needed)")?;

                // keccak256(abi.encode(chainAddress, from, to, batchData))
                let approved_hash = solidity_abi_encode_and_keccak(chain_addr, from_batch, to_batch, &batch_data);

                // check already signed
                let already = contract
                    .individual_approvals(signer_addr, approved_hash.into())
                    .call()
                    .await
                    .context("Failed to read individualApprovals")?;

                if already {
                    info!(batch=%ready.l1_batch_number, hash=%approved_hash, "Already approved; skipping");
                    last_seen_batch = ready.l1_batch_number;
                    continue;
                }

                info!(
                    batch=%ready.l1_batch_number,
                    chain=%chain_addr,
                    from=%from_batch,
                    to=%to_batch,
                    hash=%approved_hash,
                    data_len=batch_data.0.len(),
                    "Approving hash"
                );

                if args.dry_run == 1 {
                    warn!("DRY_RUN=1; not sending tx");
                    last_seen_batch = ready.l1_batch_number;
                    continue;
                }

                let call = contract.approve_hash(approved_hash.into());
                let pending = call
                    .send()
                    .await
                    .context("Failed to send approveHash tx")?;

                let receipt = pending
                    .await
                    .context("Failed while awaiting receipt")?
                    .ok_or_else(|| anyhow!("Tx dropped from mempool / no receipt"))?;

                info!(
                    tx=%receipt.transaction_hash,
                    status=?receipt.status,
                    batch=%ready.l1_batch_number,
                    "approveHash mined"
                );

                last_seen_batch = ready.l1_batch_number;
            }
        }

        sleep(Duration::from_secs(args.poll_interval_secs)).await;
    }
}

/// Decode calldata for: executeBatchesSharedBridge(address,uint256,uint256,bytes)
///
/// Layout:
/// 4 bytes selector + ABI-encoded args
fn decode_execute_batches_shared_bridge(calldata: &[u8]) -> Result<(Address, U256, U256, Bytes)> {
    if calldata.len() < 4 {
        return Err(anyhow!("calldata too short"));
    }

    // Function selector is first 4 bytes; we don't strictly require it to match, but you can add a check.
    let args = &calldata[4..];

    // Decode (address,uint256,uint256,bytes)
    let tokens = ethers::abi::decode(
        &[
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Bytes,
        ],
        args,
    )
    .context("ABI decode failed")?;

    let chain = match &tokens[0] {
        Token::Address(a) => *a,
        _ => return Err(anyhow!("bad token[0]")),
    };
    let from = match &tokens[1] {
        Token::Uint(u) => *u,
        _ => return Err(anyhow!("bad token[1]")),
    };
    let to = match &tokens[2] {
        Token::Uint(u) => *u,
        _ => return Err(anyhow!("bad token[2]")),
    };
    let data = match &tokens[3] {
        Token::Bytes(b) => Bytes::from(b.clone()),
        _ => return Err(anyhow!("bad token[3]")),
    };

    Ok((chain, from, to, data))
}

/// Mimic Solidity: keccak256(abi.encode(address,uint256,uint256,bytes))
fn solidity_abi_encode_and_keccak(chain: Address, from: U256, to: U256, data: &Bytes) -> H256 {
    use ethers::abi::encode;

    let encoded = encode(&[
        Token::Address(chain),
        Token::Uint(from),
        Token::Uint(to),
        Token::Bytes(data.to_vec()),
    ]);

    H256::from(ethers::utils::keccak256(encoded))
}
