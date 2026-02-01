use anyhow::{anyhow, Context, Result};
use clap::Parser;
use dotenvy::dotenv;
use ethers::abi::{ParamType, Token};
use ethers::prelude::*;
use ethers::types::{Address, Bytes, H256, U256};
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

mod db;
use db::{BatchDb, PostgresBatchDb};
use sqlx::{PgPool, Row};

use crate::utils::get_priority_op_merkle_path;
mod utils;

const CONTRACT_ADDR: &str = "0xE222D6354b49eaF8a7099fC4E7F9C0B4FE72d1E7";

// With 6
//const SAMPLE_TX: &str = "0xb4c1ea93f17e77cdbf6c47868e7727c18917eff489e626b8f8c4ab121a8438d6";

// With 2
const SAMPLE_TX: &str = "0x08dc9dfe4d8a6fc97a16c6b4eb028f3f5dcd9b4368f4a8f0ce11b42dedba8a83";

const PRIORITY_TREE_START_INDEX: usize = 3270719;

abigen!(
    ExecutionMultisigValidator,
    r#"[
        function approveHash(bytes32 _hash)
        function individualApprovals(address signer, bytes32 hash) view returns (bool)
        function executionMultisigMember(address signer) view returns (bool)
        function totalApprovals(bytes32 hash) view returns (uint256)
        function threshold() view returns (uint256)
        function executeBatchesSharedBridge(address _chainAddress, uint256 _processBatchFrom, uint256 _processBatchTo, bytes calldata _batchData) 
    ]"#
);

#[derive(Parser, Debug)]
#[command(
    name = "en-approvehash",
    about = "Auto-approve zkSync batch execution hashes from EN DB (tx sent on Ethereum mainnet)"
)]
struct Args {
    /// Ethereum mainnet JSON-RPC URL (for contract calls + sending tx)
    #[arg(long, env = "ETH_RPC_URL")]
    eth_rpc_url: String,

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

    /// L1 chain address used as `_chainAddress` in executeBatchesSharedBridge calldata
    #[arg(long, env = "CHAIN_ADDRESS")]
    chain_address: String,

    /// If not provided, default to the internal protocol version from the first batch.
    #[arg(long, env = "CHAIN_PROTOCOL_VERSION")]
    chain_protocol_version: Option<u16>,

    /// If set, run only one batch with this L1 batch number and exit.
    #[arg(long)]
    run_one_batch: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?),
        )
        .init();

    let args = Args::parse();

    // --- Ethereum mainnet provider + wallet (ALL contract calls go here) ---
    let eth_provider = Provider::<Http>::try_from(args.eth_rpc_url.as_str())
        .context("Failed to create provider (ETH_RPC_URL)")?
        .interval(Duration::from_millis(200));

    // Get the calldata from SAMPLE_TX, and parse it as ExecuteBatches

    let chain_id = eth_provider
        .get_chainid()
        .await
        .context("Failed to fetch Ethereum chain id")?
        .as_u64();

    if chain_id != 1 {
        return Err(anyhow!(
            "ETH_RPC_URL is not Ethereum mainnet (chain_id={})",
            chain_id
        ));
    }

    let wallet: LocalWallet = args
        .pk
        .parse::<LocalWallet>()
        .context("Failed to parse PK as a local wallet")?
        .with_chain_id(chain_id);

    let signer_addr = wallet.address();
    info!(%chain_id, %signer_addr, "Ethereum signer ready");
    println!("here!");

    let client = Arc::new(SignerMiddleware::new(eth_provider.clone(), wallet));
    let contract_addr = Address::from_str(CONTRACT_ADDR).context("Bad CONTRACT_ADDR")?;
    let contract = ExecutionMultisigValidator::new(contract_addr, client.clone());

    // --- Basic contract sanity checks (on Ethereum mainnet) ---
    let is_member = contract
        .execution_multisig_member(signer_addr)
        .call()
        .await
        .context("Failed to read executionMultisigMember")?;

    if !is_member {
        warn!(
            "Signer {} is NOT an executionMultisigMember; approveHash would revert NotSigner()",
            signer_addr
        );
        // TODO: restore after testing

        //     return Err(anyhow!(
        //         "Signer {} is not an executionMultisigMember; approveHash would revert NotSigner()",
        //         signer_addr
        //     ));
    }

    let threshold = contract.threshold().call().await.unwrap_or_default();
    info!(%threshold, "Contract threshold read");

    let chain_address = parse_address(&args.chain_address)?;

    // --- DB (External Node Postgres) ---
    let pool = PgPool::connect(&args.database_url)
        .await
        .context("Failed to connect to Postgres (DATABASE_URL)")?;
    let db = PostgresBatchDb::new(pool.clone());

    let (batch_number, proof) = get_priority_op_merkle_path(args.eth_rpc_url.as_str(), SAMPLE_TX)
        .await
        .unwrap();

    let first_priority_op_id = get_batch_first_priority_op_id(&pool.clone(), batch_number.as_u32())
        .await
        .context("get_batch_first_priority_op_id")?
        .unwrap();

    let initial_mini_merkle_tree = MiniMerkleTree::from_start_index_and_proof(
        //first_priority_op_id.checked_sub(1).unwrap(),
        first_priority_op_id - PRIORITY_TREE_START_INDEX,
        proof,
    );

    println!(
        "Mini merkle tree initialized to start index {}",
        initial_mini_merkle_tree.start_index()
    );
    {
        let first_priority_op_id =
            get_batch_first_priority_op_id(&pool.clone(), batch_number.as_u32())
                .await
                .context("get_batch_first_priority_op_id")?
                .unwrap();
        let next_batch = get_batch_first_priority_op_id(&pool.clone(), batch_number.as_u32() + 1)
            .await
            .context("get_batch_first_priority_op_id")?
            .unwrap();

        println!(
            "First priority op id in batch {} is {}, next batch starts at {}",
            batch_number, first_priority_op_id, next_batch
        );
        let priority_op_hashes = get_l1_transactions_hashes(&pool, first_priority_op_id)
            .await
            .context("get_l1_transactions_hashes")?;
        let my_hashes = priority_op_hashes.iter().take(2);

        let mut my_merkle = initial_mini_merkle_tree.clone();
        for h in my_hashes {
            println!("Priority op hash: {:#x}", h);
            my_merkle.push_hash(*h);
        }
        let (a, b, c) = my_merkle.merkle_root_and_paths_for_range(..2);
        println!("Merkle root after adding 6 hashes: {:#x}", a);
        println!("Left path: {:#?}", b);
        println!("Right path: {:#?}", c);
    }

    panic!("stop here");

    // Running just a single batch manually (mostly for testing).
    if let Some(run_one_batch) = args.run_one_batch {
        info!(batch=%run_one_batch, "Running single batch as requested; exiting after");
        run_single_batch(
            pool.clone(),
            run_one_batch as i64,
            chain_address,
            &args,
            contract.clone(),
            signer_addr,
            initial_mini_merkle_tree,
        )
        .await
        .with_context(|| format!("run_single_batch for L1 batch {}", run_one_batch))?;
        return Ok(());
    }

    // Automatic looping mode.
    let mut last_seen_batch: i64 = 0;

    loop {
        match db.fetch_next_ready_execute_call(last_seen_batch).await? {
            None => {
                sleep(Duration::from_secs(args.poll_interval_secs)).await;
                continue;
            }
            Some(ready) => {
                info!(batch=%ready.l1_batch_number, "Found batch ready for execute; building calldata");

                run_single_batch(
                    pool.clone(),
                    ready.l1_batch_number,
                    chain_address,
                    &args,
                    contract.clone(),
                    signer_addr,
                    initial_mini_merkle_tree.clone(),
                )
                .await
                .with_context(|| {
                    format!("run_single_batch for L1 batch {}", ready.l1_batch_number)
                })?;

                last_seen_batch = ready.l1_batch_number;
            }
        }

        sleep(Duration::from_secs(args.poll_interval_secs)).await;
    }
}

async fn run_single_batch(
    pool: PgPool,
    l1_batch_number: i64,
    chain_address: Address,
    args: &Args,
    contract: ExecutionMultisigValidator<SignerMiddleware<Provider<Http>, LocalWallet>>,
    signer_addr: Address,
    initial_mini_merkle_tree: MiniMerkleTree,
) -> Result<()> {
    let (from_batch, to_batch, batch_data) = build_execute_batches_data(
        pool,
        l1_batch_number as u32,
        initial_mini_merkle_tree.clone(),
        args.chain_protocol_version,
    )
    .await
    .context("Failed to build executeBatchesSharedBridge data")?;

    // Print batch data as hex for debugging.
    println!(
        "Batch data (len={}): 0x{}",
        batch_data.0.len(),
        hex::encode(&batch_data.0)
    );

    let _calldata = build_execute_shared_bridge_calldata(
        chain_address,
        from_batch.as_u64(),
        to_batch.as_u64(),
        batch_data.clone(),
    );

    // keccak256(abi.encode(chainAddress, from, to, batchData))
    let approved_hash =
        solidity_abi_encode_and_keccak(chain_address, from_batch, to_batch, &batch_data);

    // check already signed (on Ethereum mainnet)
    let already = contract
        .individual_approvals(signer_addr, approved_hash.into())
        .call()
        .await
        .context("Failed to read individualApprovals")?;

    if already {
        info!(batch=%l1_batch_number, hash=%approved_hash, "Already approved; skipping");
        return Ok(());
    }

    info!(
        batch=%l1_batch_number,
        chain=%chain_address,
        from=%from_batch,
        to=%to_batch,
        hash=%approved_hash,
        data_len=batch_data.0.len(),
        "Approving hash (tx on Ethereum mainnet)"
    );

    if args.dry_run == 1 {
        warn!("DRY_RUN=1; not sending tx");
        return Ok(());
    }

    // avoid temporary-lifetime issue: bind call first
    let call = contract.approve_hash(approved_hash.into());
    let pending = call.send().await.context("Failed to send approveHash tx")?;

    let receipt = pending
        .await
        .context("Failed while awaiting receipt")?
        .ok_or_else(|| anyhow!("Tx dropped from mempool / no receipt"))?;

    info!(
        tx=%receipt.transaction_hash,
        status=?receipt.status,
        batch=%l1_batch_number,
        "approveHash mined"
    );
    Ok(())
}

fn parse_address(s: &str) -> Result<Address> {
    let s = s.trim();
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).context("bad hex address")?;
    if bytes.len() != 20 {
        return Err(anyhow!("address must be 20 bytes"));
    }
    Ok(Address::from_slice(&bytes))
}

/// Build calldata for executeBatchesSharedBridge(address,uint256,uint256,bytes)
fn build_execute_shared_bridge_calldata(
    chain: Address,
    from: u64,
    to: u64,
    batch_data: Bytes,
) -> Vec<u8> {
    let selector =
        &ethers::utils::keccak256(b"executeBatchesSharedBridge(address,uint256,uint256,bytes)")
            [0..4];
    let encoded_args = ethers::abi::encode(&[
        Token::Address(chain),
        Token::Uint(U256::from(from)),
        Token::Uint(U256::from(to)),
        Token::Bytes(batch_data.to_vec()),
    ]);
    [selector.to_vec(), encoded_args].concat()
}

/// Mimic Solidity: keccak256(abi.encode(address,uint256,uint256,bytes))
fn solidity_abi_encode_and_keccak(chain: Address, from: U256, to: U256, data: &Bytes) -> H256 {
    let encoded = ethers::abi::encode(&[
        Token::Address(chain),
        Token::Uint(from),
        Token::Uint(to),
        Token::Bytes(data.to_vec()),
    ]);
    H256::from(ethers::utils::keccak256(encoded))
}

async fn build_execute_batches_data(
    pool: PgPool,
    batch_number: u32,
    initial_mini_merkle_tree: MiniMerkleTree,
    chain_protocol_version: Option<u16>,
) -> Result<(U256, U256, Bytes)> {
    let batch = load_l1_batch_with_metadata(&pool, batch_number)
        .await
        .context("load_l1_batch_with_metadata")?;

    let l1_batches = vec![batch];

    let internal_pv = l1_batches[0].header.protocol_version.unwrap_or(0);
    let mut dependency_roots: Vec<Vec<InteropRoot>> = Vec::with_capacity(l1_batches.len());
    if is_pre_interop_fast_blocks(internal_pv) {
        dependency_roots.push(Vec::new());
    } else {
        for b in &l1_batches {
            let roots = get_interop_roots_batch(&pool, b.header.number)
                .await
                .context("get_interop_roots_batch")?;
            dependency_roots.push(roots);
        }
    }

    let priority_ops_proofs =
        build_priority_ops_proofs(&pool, &l1_batches, initial_mini_merkle_tree).await?;

    println!("Priority ops proofs: {:#?}", priority_ops_proofs);

    let execute = ExecuteBatches {
        l1_batches,
        priority_ops_proofs,
        dependency_roots,
    };

    let internal_pv = execute.l1_batches[0].header.protocol_version.unwrap_or(0);
    let chain_pv = chain_protocol_version.unwrap_or(internal_pv);

    let tokens = execute.encode_for_eth_tx(chain_pv);

    let (from_batch, to_batch, batch_data) = match tokens.as_slice() {
        [Token::Uint(f), Token::Uint(t), Token::Bytes(b)] => (*f, *t, Bytes::from(b.clone())),
        _ => {
            let batch_data = ethers::abi::encode(&tokens);
            let from = execute.l1_batches[0].header.number as u64;
            let to = execute.l1_batches.last().unwrap().header.number as u64;
            (U256::from(from), U256::from(to), Bytes::from(batch_data))
        }
    };

    Ok((from_batch, to_batch, batch_data))
}

async fn build_priority_ops_proofs(
    pool: &PgPool,
    l1_batches: &[L1BatchWithMetadata],
    initial_mini_merkle_tree: MiniMerkleTree,
) -> Result<Vec<PriorityOpsMerkleProof>> {
    let mut mini_merkle_tree = initial_mini_merkle_tree.clone();
    // This is slow (as we are re-loading all L1 tx hashes from the DB), but simpler to implement.
    let priority_op_hashes = get_l1_transactions_hashes(pool, mini_merkle_tree.start_index)
        .await
        .context("get_l1_transactions_hashes")?;

    println!(
        "Got {} priority op hashes newer than {}",
        priority_op_hashes.len(),
        mini_merkle_tree.start_index()
    );
    for h in &priority_op_hashes {
        mini_merkle_tree.push_hash(*h);
    }

    let mut priority_ops_proofs = Vec::with_capacity(l1_batches.len());
    for batch in l1_batches {
        println!("Building proof for batch {}", batch.header.number);
        let first_priority_op_id_option = get_batch_first_priority_op_id(pool, batch.header.number)
            .await
            .context("get_batch_first_priority_op_id")?;
        println!("First priority op id: {:#?}", first_priority_op_id_option);

        let count = batch.header.l1_tx_count as usize;
        // TODO: what to do if count == 0?
        if count == 0 || first_priority_op_id_option.is_none() {
            priority_ops_proofs.push(PriorityOpsMerkleProof::default());
            continue;
        }

        let first_priority_op_id_in_batch = first_priority_op_id_option.unwrap();

        mini_merkle_tree.trim_start(first_priority_op_id_in_batch - mini_merkle_tree.start_index());

        let (_root, left, right) = mini_merkle_tree.merkle_root_and_paths_for_range(..count);
        let left_path: Vec<H256> = left.into_iter().map(Option::unwrap_or_default).collect();
        let right_path: Vec<H256> = right.into_iter().map(Option::unwrap_or_default).collect();
        let hashes = mini_merkle_tree.hashes_prefix(count);

        priority_ops_proofs.push(PriorityOpsMerkleProof {
            left_path,
            right_path,
            hashes,
        });
    }

    Ok(priority_ops_proofs)
}

#[derive(Debug, Clone)]
struct L1BatchWithMetadata {
    header: L1BatchHeader,
    metadata: L1BatchMetadata,
}

#[derive(Debug, Clone)]
struct L1BatchHeader {
    number: u32,
    timestamp: u64,
    l1_tx_count: u16,
    priority_ops_onchain_data: Vec<H256>,
    system_logs: Vec<Vec<u8>>,
    protocol_version: Option<u16>,
}

impl L1BatchHeader {
    fn priority_ops_onchain_data_hash(&self) -> H256 {
        let mut rolling = H256::from(ethers::utils::keccak256(&[]));
        for onchain_hash in &self.priority_ops_onchain_data {
            let mut preimage = Vec::with_capacity(64);
            preimage.extend_from_slice(rolling.as_bytes());
            preimage.extend_from_slice(onchain_hash.as_bytes());
            rolling = H256::from(ethers::utils::keccak256(preimage));
        }
        rolling
    }
}

#[derive(Debug, Clone)]
struct L1BatchMetadata {
    root_hash: H256,
    rollup_last_leaf_index: u64,
    l2_l1_merkle_root: H256,
    commitment: H256,
}

#[derive(Debug, Clone)]
struct InteropRoot {
    chain_id: u64,
    block_number: u32,
    sides: Vec<H256>,
}

impl InteropRoot {
    fn into_token(self) -> Token {
        Token::Tuple(vec![
            Token::Uint(self.chain_id.into()),
            Token::Uint(self.block_number.into()),
            Token::Array(
                self.sides
                    .iter()
                    .map(|hash| Token::FixedBytes(hash.as_bytes().to_vec()))
                    .collect(),
            ),
        ])
    }
}

#[derive(Debug, Clone, Default)]
struct PriorityOpsMerkleProof {
    left_path: Vec<H256>,
    right_path: Vec<H256>,
    hashes: Vec<H256>,
}

impl PriorityOpsMerkleProof {
    fn into_token(&self) -> Token {
        let array_into_token = |array: &[H256]| {
            Token::Array(
                array
                    .iter()
                    .map(|hash| Token::FixedBytes(hash.as_bytes().to_vec()))
                    .collect(),
            )
        };
        Token::Tuple(vec![
            array_into_token(&self.left_path),
            array_into_token(&self.right_path),
            array_into_token(&self.hashes),
        ])
    }
}

#[derive(Debug, Clone)]
struct StoredBatchInfo {
    batch_number: u64,
    batch_hash: H256,
    index_repeated_storage_changes: u64,
    number_of_layer1_txs: U256,
    priority_operations_hash: H256,
    dependency_roots_rolling_hash: H256,
    l2_logs_tree_root: H256,
    timestamp: U256,
    commitment: H256,
}

impl StoredBatchInfo {
    fn into_token_with_protocol_version(self, protocol_version: u16) -> Token {
        if is_pre_interop_fast_blocks(protocol_version) {
            Token::Tuple(vec![
                Token::Uint(self.batch_number.into()),
                Token::FixedBytes(self.batch_hash.as_bytes().to_vec()),
                Token::Uint(self.index_repeated_storage_changes.into()),
                Token::Uint(self.number_of_layer1_txs),
                Token::FixedBytes(self.priority_operations_hash.as_bytes().to_vec()),
                Token::FixedBytes(self.l2_logs_tree_root.as_bytes().to_vec()),
                Token::Uint(self.timestamp),
                Token::FixedBytes(self.commitment.as_bytes().to_vec()),
            ])
        } else {
            Token::Tuple(vec![
                Token::Uint(self.batch_number.into()),
                Token::FixedBytes(self.batch_hash.as_bytes().to_vec()),
                Token::Uint(self.index_repeated_storage_changes.into()),
                Token::Uint(self.number_of_layer1_txs),
                Token::FixedBytes(self.priority_operations_hash.as_bytes().to_vec()),
                Token::FixedBytes(self.dependency_roots_rolling_hash.as_bytes().to_vec()),
                Token::FixedBytes(self.l2_logs_tree_root.as_bytes().to_vec()),
                Token::Uint(self.timestamp),
                Token::FixedBytes(self.commitment.as_bytes().to_vec()),
            ])
        }
    }
}

#[derive(Debug, Clone)]
struct ExecuteBatches {
    l1_batches: Vec<L1BatchWithMetadata>,
    priority_ops_proofs: Vec<PriorityOpsMerkleProof>,
    dependency_roots: Vec<Vec<InteropRoot>>,
}

impl ExecuteBatches {
    fn encode_for_eth_tx(&self, chain_protocol_version: u16) -> Vec<Token> {
        let internal_protocol_version = self.l1_batches[0].header.protocol_version.unwrap_or(0);

        if is_pre_gateway(internal_protocol_version) && is_pre_gateway(chain_protocol_version) {
            vec![Token::Array(
                self.l1_batches
                    .iter()
                    .map(|batch| {
                        StoredBatchInfo::from(batch)
                            .into_token_with_protocol_version(internal_protocol_version)
                    })
                    .collect(),
            )]
        } else if is_pre_interop_fast_blocks(internal_protocol_version)
            && is_pre_interop_fast_blocks(chain_protocol_version)
        {
            let encoded_data = ethers::abi::encode(&[
                Token::Array(
                    self.l1_batches
                        .iter()
                        .map(|batch| {
                            StoredBatchInfo::from(batch)
                                .into_token_with_protocol_version(internal_protocol_version)
                        })
                        .collect(),
                ),
                Token::Array(
                    self.priority_ops_proofs
                        .iter()
                        .map(|proof| proof.into_token())
                        .collect(),
                ),
            ]);
            let execute_data = [
                [get_encoding_version(internal_protocol_version)].to_vec(),
                encoded_data,
            ]
            .concat()
            .to_vec();

            vec![
                Token::Uint(self.l1_batches[0].header.number.into()),
                Token::Uint(self.l1_batches.last().unwrap().header.number.into()),
                Token::Bytes(execute_data),
            ]
        } else {
            let encoded_data = ethers::abi::encode(&[
                Token::Array(
                    self.l1_batches
                        .iter()
                        .map(|batch| {
                            StoredBatchInfo::from(batch)
                                .into_token_with_protocol_version(internal_protocol_version)
                        })
                        .collect(),
                ),
                Token::Array(
                    self.priority_ops_proofs
                        .iter()
                        .map(|proof| proof.into_token())
                        .collect(),
                ),
                Token::Array(
                    self.dependency_roots
                        .iter()
                        .map(|batch_roots| {
                            Token::Array(
                                batch_roots
                                    .iter()
                                    .cloned()
                                    .map(InteropRoot::into_token)
                                    .collect(),
                            )
                        })
                        .collect(),
                ),
            ]);
            let execute_data = [
                [get_encoding_version(internal_protocol_version)].to_vec(),
                encoded_data,
            ]
            .concat()
            .to_vec();
            vec![
                Token::Uint(self.l1_batches[0].header.number.into()),
                Token::Uint(self.l1_batches.last().unwrap().header.number.into()),
                Token::Bytes(execute_data),
            ]
        }
    }
}

impl From<&L1BatchWithMetadata> for StoredBatchInfo {
    fn from(x: &L1BatchWithMetadata) -> Self {
        let pv = x.header.protocol_version.unwrap_or(0);
        let dependency_roots_rolling_hash = if is_pre_interop_fast_blocks(pv) {
            H256::zero()
        } else {
            extract_dependency_roots_rolling_hash(&x.header.system_logs).unwrap_or_else(H256::zero)
        };
        Self {
            batch_number: x.header.number as u64,
            batch_hash: x.metadata.root_hash,
            index_repeated_storage_changes: x.metadata.rollup_last_leaf_index,
            number_of_layer1_txs: x.header.l1_tx_count.into(),
            priority_operations_hash: x.header.priority_ops_onchain_data_hash(),
            dependency_roots_rolling_hash,
            l2_logs_tree_root: x.metadata.l2_l1_merkle_root,
            timestamp: x.header.timestamp.into(),
            commitment: x.metadata.commitment,
        }
    }
}

fn get_encoding_version(protocol_version: u16) -> u8 {
    if is_pre_interop_fast_blocks(protocol_version) {
        0
    } else {
        1
    }
}

fn is_pre_gateway(protocol_version: u16) -> bool {
    protocol_version < 26
}

fn is_pre_interop_fast_blocks(protocol_version: u16) -> bool {
    protocol_version < 29
}

const MESSAGE_ROOT_ROLLING_HASH_KEY: [u8; 32] = [
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
];

fn extract_dependency_roots_rolling_hash(system_logs: &[Vec<u8>]) -> Option<H256> {
    for raw_log in system_logs {
        if raw_log.len() != 88 {
            continue;
        }
        let key = &raw_log[24..56];
        if key == MESSAGE_ROOT_ROLLING_HASH_KEY.as_slice() {
            let value = &raw_log[56..88];
            return Some(H256::from_slice(value));
        }
    }
    None
}

async fn load_l1_batch_with_metadata(
    pool: &PgPool,
    batch_number: u32,
) -> Result<L1BatchWithMetadata> {
    let row = sqlx::query(
        r#"
        SELECT
            number,
            timestamp,
            l1_tx_count,
            priority_ops_onchain_data,
            hash,
            rollup_last_leaf_index,
            l2_l1_merkle_root,
            commitment,
            system_logs,
            protocol_version
        FROM l1_batches
        WHERE number = $1
        "#,
    )
    .bind(batch_number as i64)
    .fetch_optional(pool)
    .await
    .context("load l1_batches row")?;

    let Some(row) = row else {
        return Err(anyhow!("L1 batch {} not found in DB", batch_number));
    };

    let number: i64 = row.try_get("number")?;
    let timestamp: i64 = row.try_get("timestamp")?;
    let l1_tx_count: i32 = row.try_get("l1_tx_count")?;
    let priority_ops_onchain_data: Vec<Vec<u8>> = row.try_get("priority_ops_onchain_data")?;
    let hash: Option<Vec<u8>> = row.try_get("hash")?;
    let rollup_last_leaf_index: Option<i64> = row.try_get("rollup_last_leaf_index")?;
    let l2_l1_merkle_root: Option<Vec<u8>> = row.try_get("l2_l1_merkle_root")?;
    let commitment: Option<Vec<u8>> = row.try_get("commitment")?;
    let system_logs: Vec<Vec<u8>> = row.try_get("system_logs")?;
    let protocol_version: Option<i32> = row.try_get("protocol_version")?;

    let mut priority_ops_hashes = Vec::with_capacity(priority_ops_onchain_data.len());
    for data in priority_ops_onchain_data {
        if data.len() != 64 {
            return Err(anyhow!(
                "priority_ops_onchain_data entry has bad length {}",
                data.len()
            ));
        }
        priority_ops_hashes.push(H256::from_slice(&data[32..64]));
    }

    let header = L1BatchHeader {
        number: number as u32,
        timestamp: timestamp as u64,
        l1_tx_count: l1_tx_count as u16,
        priority_ops_onchain_data: priority_ops_hashes,
        system_logs,
        protocol_version: protocol_version.map(|v| v as u16),
    };

    let metadata = L1BatchMetadata {
        root_hash: H256::from_slice(&hash.ok_or_else(|| anyhow!("missing batch hash"))?),
        rollup_last_leaf_index: rollup_last_leaf_index
            .ok_or_else(|| anyhow!("missing rollup_last_leaf_index"))?
            as u64,
        l2_l1_merkle_root: H256::from_slice(
            &l2_l1_merkle_root.ok_or_else(|| anyhow!("missing l2_l1_merkle_root"))?,
        ),
        commitment: H256::from_slice(&commitment.ok_or_else(|| anyhow!("missing commitment"))?),
    };

    Ok(L1BatchWithMetadata { header, metadata })
}

async fn get_interop_roots_batch(pool: &PgPool, batch_number: u32) -> Result<Vec<InteropRoot>> {
    let rows = sqlx::query(
        r#"
        SELECT
            interop_roots.chain_id,
            interop_roots.dependency_block_number,
            interop_roots.interop_root_sides
        FROM interop_roots
        JOIN miniblocks
            ON interop_roots.processed_block_number = miniblocks.number
        WHERE l1_batch_number = $1
        ORDER BY chain_id, processed_block_number, dependency_block_number DESC
        "#,
    )
    .bind(batch_number as i64)
    .fetch_all(pool)
    .await
    .context("get interop_roots batch")?;

    let mut roots = Vec::with_capacity(rows.len());
    for row in rows {
        let chain_id: i64 = row.try_get("chain_id")?;
        let dependency_block_number: i64 = row.try_get("dependency_block_number")?;
        let sides_raw: Vec<Vec<u8>> = row.try_get("interop_root_sides")?;
        let sides = sides_raw
            .iter()
            .map(|side| H256::from_slice(side))
            .collect::<Vec<_>>();
        roots.push(InteropRoot {
            chain_id: chain_id as u64,
            block_number: dependency_block_number as u32,
            sides,
        });
    }
    Ok(roots)
}

// This will NOT work
async fn get_l1_transactions_hashes(pool: &PgPool, start_id: usize) -> Result<Vec<H256>> {
    let rows = sqlx::query(
        r#"
        SELECT hash
        FROM transactions
        WHERE priority_op_id >= $1
            AND is_priority = TRUE
        ORDER BY priority_op_id
        "#,
    )
    .bind(start_id as i64)
    .fetch_all(pool)
    .await
    .context("get_l1_transactions_hashes query")?;

    Ok(rows
        .into_iter()
        .map(|row| H256::from_slice(row.get::<Vec<u8>, _>("hash").as_slice()))
        .collect())
}

async fn get_batch_first_priority_op_id(pool: &PgPool, batch_number: u32) -> Result<Option<usize>> {
    let row = sqlx::query(
        r#"
        SELECT
            MIN(miniblocks.number) AS min_block,
            MAX(miniblocks.number) AS max_block
        FROM miniblocks
        WHERE l1_batch_number = $1
        "#,
    )
    .bind(batch_number as i64)
    .fetch_one(pool)
    .await
    .context("get l2 block range for l1 batch")?;

    let min_block: Option<i64> = row.try_get("min_block")?;
    let max_block: Option<i64> = row.try_get("max_block")?;

    let (Some(min_block), Some(max_block)) = (min_block, max_block) else {
        return Ok(None);
    };

    let row = sqlx::query(
        r#"
        SELECT MIN(priority_op_id) AS id
        FROM transactions
        WHERE miniblock_number BETWEEN $1 AND $2
            AND is_priority = TRUE
        "#,
    )
    .bind(min_block)
    .bind(max_block)
    .fetch_one(pool)
    .await
    .context("get_batch_first_priority_op_id")?;

    let id: Option<i64> = row.try_get("id")?;
    Ok(id.map(|v| v as usize))
}

#[derive(Debug, Clone)]
struct MiniMerkleTree {
    hashes: VecDeque<H256>,
    binary_tree_size: usize,
    start_index: usize,
    cache: Vec<Option<H256>>,
}

impl MiniMerkleTree {
    fn from_start_index_and_proof(start_index: usize, proof: Vec<H256>) -> Self {
        // Check if not off by one.
        let binary_tree_size = 1 << proof.len();
        println!(
            "Initializing MiniMerkleTree with start_index {} and binary_tree_size {}",
            start_index, binary_tree_size
        );
        let depth = tree_depth_by_size(binary_tree_size);
        assert_eq!(proof.len(), depth);
        Self {
            hashes: VecDeque::new(),
            binary_tree_size,
            start_index,
            cache: proof.into_iter().map(Some).collect(),
        }
    }

    fn from_hashes(hashes: Vec<H256>) -> Self {
        let hashes: VecDeque<_> = hashes.into_iter().collect();
        let mut binary_tree_size = hashes.len().next_power_of_two();
        if binary_tree_size == 0 {
            binary_tree_size = 1;
        }
        let depth = tree_depth_by_size(binary_tree_size);
        Self {
            hashes,
            binary_tree_size,
            start_index: 0,
            cache: vec![None; depth],
        }
    }

    fn length(&self) -> usize {
        self.start_index + self.hashes.len()
    }

    fn start_index(&self) -> usize {
        self.start_index
    }

    fn push_hash(&mut self, leaf_hash: H256) {
        self.hashes.push_back(leaf_hash);
        if self.start_index + self.hashes.len() > self.binary_tree_size {
            self.binary_tree_size *= 2;
            if self.cache.len() < tree_depth_by_size(self.binary_tree_size) {
                self.cache.push(None);
            }
        }
    }

    fn hashes_prefix(&self, length: usize) -> Vec<H256> {
        self.hashes.iter().take(length).copied().collect()
    }

    fn trim_start(&mut self, count: usize) {
        let mut new_cache = vec![];
        let root = self.compute_merkle_root_and_path(count, Some(&mut new_cache), Some(Side::Left));
        self.hashes.drain(..count);
        self.start_index += count;
        if self.start_index == self.binary_tree_size {
            new_cache.push(Some(root));
        }
        self.cache = new_cache;
    }

    fn merkle_root_and_paths_for_range(
        &self,
        range: std::ops::RangeTo<usize>,
    ) -> (H256, Vec<Option<H256>>, Vec<Option<H256>>) {
        let mut right_path = vec![];
        let root_hash = self.compute_merkle_root_and_path(
            range.end - 1,
            Some(&mut right_path),
            Some(Side::Right),
        );
        (root_hash, self.cache.clone(), right_path)
    }

    fn compute_merkle_root_and_path(
        &self,
        mut index: usize,
        mut path: Option<&mut Vec<Option<H256>>>,
        side: Option<Side>,
    ) -> H256 {
        let depth = tree_depth_by_size(self.binary_tree_size);
        println!(
            "Computing merkle root at depth {} binary tree size: {}",
            depth, self.binary_tree_size
        );
        if let Some(path) = path.as_deref_mut() {
            path.reserve(depth);
        }

        let mut hashes = self.hashes.clone();
        let mut absolute_start_index = self.start_index;

        for level in 0..depth {
            println!(
                "On level {}, index {} absolute_start index {}",
                level, index, absolute_start_index
            );
            if absolute_start_index % 2 == 1 {
                hashes.push_front(self.cache[level].expect("cache is invalid"));
                index += 1;
            }
            if hashes.len() % 2 == 1 {
                hashes.push_back(compute_empty_tree_hashes(empty_leaf_hash())[level]);
            }

            if let Some(path) = path.as_deref_mut() {
                let hash = match side {
                    Some(Side::Left) if index % 2 == 0 => None,
                    Some(Side::Right) if index % 2 == 1 => None,
                    _ => hashes.get(index ^ 1).copied(),
                };
                path.push(hash);
            }

            let level_len = hashes.len() / 2;
            println!("Hashes before compression: {:?}", hashes);
            for i in 0..level_len {
                hashes[i] = compress_hashes(&hashes[2 * i], &hashes[2 * i + 1]);
            }
            hashes.truncate(level_len);
            println!("Hashes after compression: {:?}", hashes);
            index /= 2;
            absolute_start_index /= 2;
        }

        hashes[0]
    }
}

#[derive(Debug, Clone, Copy)]
enum Side {
    Left,
    Right,
}

fn compress_hashes(left: &H256, right: &H256) -> H256 {
    let mut data = [0u8; 64];
    data[..32].copy_from_slice(left.as_bytes());
    data[32..].copy_from_slice(right.as_bytes());
    H256::from(ethers::utils::keccak256(data))
}

fn empty_leaf_hash() -> H256 {
    H256::from(ethers::utils::keccak256(&[]))
}

fn compute_empty_tree_hashes(empty_leaf_hash: H256) -> Vec<H256> {
    let mut hashes = Vec::with_capacity(33);
    let mut cur = empty_leaf_hash;
    for _ in 0..=32 {
        hashes.push(cur);
        cur = compress_hashes(&cur, &cur);
    }
    hashes
}

fn tree_depth_by_size(tree_size: usize) -> usize {
    tree_size.trailing_zeros() as usize
}
