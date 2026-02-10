use std::{str::FromStr, time::Duration};

use alloy::sol_types::SolValue;
use anyhow::Context;
use ethers::{
    abi::ParamType,
    providers::{Http, Middleware, Provider},
    types::{H256, U256},
};

use crate::{ExecutePayload, InteropRootSol, StoredBatchInfoSol};

/// Takes a given "trusted" transaction (that should be an existing "ExecuteBatches" on-chain tx),
/// and extracts the Merkle path for the first priority operation in the first batch included in that
/// transaction.
pub async fn get_priority_op_merkle_path(
    eth_rpc_url: &str,
    tx: &str,
) -> anyhow::Result<(U256, Vec<H256>, Vec<H256>)> {
    let eth_provider = Provider::<Http>::try_from(eth_rpc_url)
        .context("Failed to create provider (ETH_RPC_URL)")?
        .interval(Duration::from_millis(200));

    // Get the calldata from SAMPLE_TX, and parse it as ExecuteBatches

    let transaction = eth_provider
        .get_transaction(H256::from_str(tx).unwrap())
        .await
        .unwrap()
        .unwrap();
    let header = transaction.input.0[..4].to_vec();
    // Compare header to executeBatchesSharedBridge selector
    let expected_header =
        &ethers::utils::keccak256(b"executeBatchesSharedBridge(address,uint256,uint256,bytes)")
            [0..4];
    assert_eq!(header.as_slice(), expected_header);
    // TODO: check address
    let decoded_execute_batches = ethers::abi::decode(
        &[
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Bytes,
        ],
        &transaction.input.0[4..],
    )
    .unwrap();

    println!(
        "Address is {:?}",
        decoded_execute_batches[0].clone().into_address().unwrap()
    );

    let first_batch = decoded_execute_batches[1].clone().into_uint().unwrap();
    println!("First batch number: {}", first_batch);

    let payload = decoded_execute_batches[3].clone().into_bytes().unwrap();
    // First element is a version.
    assert_eq!(payload[0], 1u8);

    let execute_payload = ExecutePayload::abi_decode_sequence(&payload[1..]).unwrap();

    let priority_ops = &execute_payload.priorityOpsData[0];

    println!("Priority ops: {:?}", priority_ops);

    let priority_ops_left = &priority_ops.leftPath;
    let priority_ops_right = &priority_ops.rightPath;

    Ok((
        first_batch,
        priority_ops_left
            .iter()
            .map(|h| H256::from_slice(h.as_slice()))
            .collect(),
        priority_ops_right
            .iter()
            .map(|h| H256::from_slice(h.as_slice()))
            .collect(),
    ))
}

fn tokens_to_h256_vec(tokens: Vec<ethers::abi::Token>) -> Vec<H256> {
    tokens
        .into_iter()
        .map(|t| H256::from_slice(&t.into_fixed_bytes().unwrap()))
        .collect()
}
