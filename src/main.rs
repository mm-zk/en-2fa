use anyhow::{anyhow, Context, Result};
use clap::Parser;
use tiny_keccak::{Hasher, Keccak};

use zksync_dal::ConnectionPool;
use zksync_types::{
    commitment::{L1BatchWithMetadata, PriorityOpsMerkleProof},
    ethabi::{encode, Token},
    Address, InteropRoot, ProtocolVersionId, U256,
};

// -----------------------------
// You provided this snippet; it depends on these helpers.
// In your repo, these are in some crate/module that exists in the zksync-era workspace.
// In a standalone program, you have two choices:
//   A) re-implement those helpers here (not recommended; easy to get wrong)
//   B) depend on the crate that defines them and import the same paths.
//
// Below imports are placeholders; you MUST point them to where they live in your pinned tag.
// Use `rg "get_encoding_version" -n` in your zksync-era checkout to find the right path.
use crate::i_executor::structures::{get_encoding_version, StoredBatchInfo};
// -----------------------------

/// Input required to encode `executeBatches` call.
#[derive(Debug, Clone)]
pub struct ExecuteBatches {
    pub l1_batches: Vec<L1BatchWithMetadata>,
    pub priority_ops_proofs: Vec<PriorityOpsMerkleProof>,
    pub dependency_roots: Vec<Vec<InteropRoot>>,
}

impl ExecuteBatches {
    pub fn encode_for_eth_tx(&self, chain_protocol_version: ProtocolVersionId) -> Vec<Token> {
        let internal_protocol_version = self.l1_batches[0].header.protocol_version.unwrap();

        if internal_protocol_version.is_pre_gateway() && chain_protocol_version.is_pre_gateway() {
            vec![Token::Array(
                self.l1_batches
                    .iter()
                    .map(|batch| {
                        StoredBatchInfo::from(batch)
                            .into_token_with_protocol_version(internal_protocol_version)
                    })
                    .collect(),
            )]
        } else if internal_protocol_version.is_pre_interop_fast_blocks()
            && chain_protocol_version.is_pre_interop_fast_blocks()
        {
            let encoded_data = encode(&[
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
                Token::Uint(self.l1_batches[0].header.number.0.into()),
                Token::Uint(self.l1_batches.last().unwrap().header.number.0.into()),
                Token::Bytes(execute_data),
            ]
        } else {
            let encoded_data = encode(&[
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
                                    .map(|root| root.clone().into_token())
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
                Token::Uint(self.l1_batches[0].header.number.0.into()),
                Token::Uint(self.l1_batches.last().unwrap().header.number.0.into()),
                Token::Bytes(execute_data),
            ]
        }
    }
}

#[derive(Parser, Debug)]
struct Args {
    /// Postgres connection string to the EN DB
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// L1 chain address to use as `_chainAddress` in hashing + calldata
    /// (If you truly want DB-only, you can store this in env/compose and pass it here.)
    #[arg(long)]
    chain_address: String,

    /// First L1 batch number in the execution range
    #[arg(long)]
    from: u64,

    /// Last L1 batch number in the execution range
    #[arg(long)]
    to: u64,

    /// If not provided, we default to the internal protocol version from the first batch.
    #[arg(long)]
    chain_protocol_version: Option<u16>,
}

/// Keccak256 helper
fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    let mut k = Keccak::v256();
    k.update(data);
    k.finalize(&mut out);
    out
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
fn build_execute_shared_bridge_calldata(chain: Address, from: u64, to: u64, batch_data: Vec<u8>) -> Vec<u8> {
    let selector = &keccak256(b"executeBatchesSharedBridge(address,uint256,uint256,bytes)")[0..4];
    let encoded_args = encode(&[
        Token::Address(chain),
        Token::Uint(U256::from(from)),
        Token::Uint(U256::from(to)),
        Token::Bytes(batch_data),
    ]);
    [selector.to_vec(), encoded_args].concat()
}

/// approvedHash = keccak256(abi.encode(chainAddress, from, to, batchData))
fn approved_hash(chain: Address, from: u64, to: u64, batch_data: Vec<u8>) -> [u8; 32] {
    let encoded = encode(&[
        Token::Address(chain),
        Token::Uint(U256::from(from)),
        Token::Uint(U256::from(to)),
        Token::Bytes(batch_data),
    ]);
    keccak256(&encoded)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let chain_address = parse_address(&args.chain_address)?;

    // Connect to DB using the same DAL the node uses (stable vs raw SQL).
    let pool = ConnectionPool::builder(args.database_url.clone())
        .build()
        .await
        .context("failed to build ConnectionPool")?;

    let mut conn = pool.connection().await.context("failed to get DB connection")?;

    // -----------------------------
    // Fetch execution inputs from DB
    //
    // These DAL method names can differ across tags.
    // If you get a compile error "method not found", search in your pinned tag:
    //   rg "L1BatchWithMetadata" core/lib/dal -n
    //   rg "PriorityOpsMerkleProof" core/lib/dal -n
    //   rg "InteropRoot" core/lib/dal -n
    // and update the calls accordingly.
    // -----------------------------

    let mut l1_batches: Vec<L1BatchWithMetadata> = Vec::new();
    let mut proofs: Vec<PriorityOpsMerkleProof> = Vec::new();
    let mut dependency_roots: Vec<Vec<InteropRoot>> = Vec::new();

    for number in args.from..=args.to {
        // 1) batch + metadata
        let batch: L1BatchWithMetadata = conn
            .blocks_dal()
            .get_l1_batch_with_metadata(number)
            .await
            .with_context(|| format!("get_l1_batch_with_metadata({number})"))?;
        l1_batches.push(batch);

        // 2) priority ops merkle proof
        let proof: PriorityOpsMerkleProof = conn
            .blocks_dal()
            .get_priority_ops_merkle_proof(number)
            .await
            .with_context(|| format!("get_priority_ops_merkle_proof({number})"))?;
        proofs.push(proof);

        // 3) dependency roots (may be empty for older protocol versions)
        let roots: Vec<InteropRoot> = conn
            .blocks_dal()
            .get_interop_roots(number)
            .await
            .unwrap_or_default();
        dependency_roots.push(roots);
    }

    if l1_batches.is_empty() {
        return Err(anyhow!("no batches loaded"));
    }

    // Protocol version selection:
    // - internal: comes from batch header (used inside encode_for_eth_tx)
    // - chain_protocol_version: if not provided, default to internal (works in most setups)
    let internal_pv = l1_batches[0]
        .header
        .protocol_version
        .ok_or_else(|| anyhow!("first batch has no protocol_version in header"))?;

    let chain_pv = if let Some(v) = args.chain_protocol_version {
        ProtocolVersionId::try_from(v as u16)
            .map_err(|_| anyhow!("invalid chain_protocol_version={v}"))?
    } else {
        internal_pv
    };

    let execute = ExecuteBatches {
        l1_batches,
        priority_ops_proofs: proofs,
        dependency_roots,
    };

    // Your snippet returns (from,to,bytes) in most modern branches; we take the bytes as `_batchData`.
    let tokens = execute.encode_for_eth_tx(chain_pv);

    let (process_from, process_to, batch_data) = match tokens.as_slice() {
        [Token::Uint(f), Token::Uint(t), Token::Bytes(b)] => (f.as_u64(), t.as_u64(), b.clone()),
        // very old encoding variant returns just an array; in that case batchData is the ABI-encoding of that token list
        _ => {
            let batch_data = encode(&tokens);
            (args.from, args.to, batch_data)
        }
    };

    let hash = approved_hash(chain_address, process_from, process_to, batch_data.clone());
    let calldata = build_execute_shared_bridge_calldata(chain_address, process_from, process_to, batch_data.clone());

    println!("processBatchFrom : {}", process_from);
    println!("processBatchTo   : {}", process_to);
    println!("batchData        : 0x{}", hex::encode(&batch_data));
    println!("approvedHash     : 0x{}", hex::encode(hash));
    println!("execute calldata : 0x{}", hex::encode(calldata));

    Ok(())
}