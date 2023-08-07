use std::collections::BTreeMap;
use std::mem;

use arrow2::array::{MutableArray, MutableBinaryArray, MutableBooleanArray, UInt64Vec, UInt8Vec};
use arrow2::datatypes::{DataType, Field, Schema, SchemaRef};

use skar_ingest::BatchData;

use crate::state::ArrowChunk;

fn hash_dt() -> DataType {
    DataType::Binary
}

fn hash_builder() -> MutableBinaryArray<i32> {
    MutableBinaryArray::new()
}

fn addr_dt() -> DataType {
    DataType::Binary
}

fn addr_builder() -> MutableBinaryArray<i32> {
    MutableBinaryArray::new()
}

fn quantity_dt() -> DataType {
    DataType::Binary
}

fn quantity_builder() -> MutableBinaryArray<i32> {
    MutableBinaryArray::new()
}

pub fn block_header() -> SchemaRef {
    Schema::from(vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("hash", hash_dt(), false),
        Field::new("nonce", DataType::Binary, false),
        Field::new("sha3_uncles", hash_dt(), false),
        Field::new("logs_bloom", DataType::Binary, false),
        Field::new("transactions_root", hash_dt(), false),
        Field::new("state_root", hash_dt(), false),
        Field::new("receipts_root", hash_dt(), false),
        Field::new("miner", addr_dt(), false),
        Field::new("difficulty", quantity_dt(), false),
        Field::new("total_difficulty", quantity_dt(), false),
        Field::new("extra_data", DataType::Binary, false),
        Field::new("size", quantity_dt(), false),
        Field::new("gas_limit", quantity_dt(), false),
        Field::new("gas_used", quantity_dt(), false),
        Field::new("timestamp", quantity_dt(), false),
        Field::new("uncles", DataType::Binary, false),
    ])
    .into()
}

pub fn transaction() -> SchemaRef {
    Schema::from(vec![
        Field::new("block_hash", hash_dt(), false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("from", addr_dt(), false),
        Field::new("gas", quantity_dt(), false),
        Field::new("gas_price", quantity_dt(), false),
        Field::new("hash", hash_dt(), false),
        Field::new("input", DataType::Binary, false),
        Field::new("nonce", quantity_dt(), false),
        Field::new("to", addr_dt(), true),
        Field::new("transaction_index", DataType::UInt64, false),
        Field::new("value", quantity_dt(), false),
        Field::new("v", quantity_dt(), false),
        Field::new("r", quantity_dt(), false),
        Field::new("s", quantity_dt(), false),
        Field::new("cumulative_gas_used", quantity_dt(), false),
        Field::new("effective_gas_price", quantity_dt(), false),
        Field::new("gas_used", quantity_dt(), false),
        Field::new("contract_address", addr_dt(), true),
        Field::new("logs_bloom", DataType::Binary, false),
        Field::new("type", DataType::UInt8, false),
        Field::new("root", hash_dt(), true),
        Field::new("status", DataType::UInt8, true),
        Field::new("sighash", DataType::Binary, true),
    ])
    .into()
}

pub fn log() -> SchemaRef {
    Schema::from(vec![
        Field::new("removed", DataType::Boolean, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt64, false),
        Field::new("transaction_hash", hash_dt(), false),
        Field::new("block_hash", hash_dt(), false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("address", addr_dt(), false),
        Field::new("data", DataType::Binary, false),
        Field::new("topic0", DataType::Binary, true),
        Field::new("topic1", DataType::Binary, true),
        Field::new("topic2", DataType::Binary, true),
        Field::new("topic3", DataType::Binary, true),
    ])
    .into()
}

pub struct Batches {
    pub blocks: ArrowChunk,
    pub transactions: ArrowChunk,
    pub logs: ArrowChunk,
}

pub fn data_to_batches(mut data: BatchData) -> Batches {
    let mut b_number = UInt64Vec::new();
    let mut b_hash = hash_builder();
    let mut b_nonce = MutableBinaryArray::<i32>::new();
    let mut b_sha3_uncles = hash_builder();
    let mut b_logs_bloom = MutableBinaryArray::<i32>::new();
    let mut b_transactions_root = hash_builder();
    let mut b_state_root = hash_builder();
    let mut b_receipts_root = hash_builder();
    let mut b_miner = addr_builder();
    let mut b_difficulty = quantity_builder();
    let mut b_total_difficulty = quantity_builder();
    let mut b_extra_data = MutableBinaryArray::<i32>::new();
    let mut b_size = quantity_builder();
    let mut b_gas_limit = quantity_builder();
    let mut b_gas_used = quantity_builder();
    let mut b_timestamp = quantity_builder();
    let mut b_uncles = MutableBinaryArray::<i32>::new();

    let mut tx_block_hash = hash_builder();
    let mut tx_block_number = UInt64Vec::new();
    let mut tx_from = addr_builder();
    let mut tx_gas = MutableBinaryArray::<i32>::new();
    let mut tx_gas_price = MutableBinaryArray::<i32>::new();
    let mut tx_hash = hash_builder();
    let mut tx_input = MutableBinaryArray::<i32>::new();
    let mut tx_nonce = MutableBinaryArray::<i32>::new();
    let mut tx_to = addr_builder();
    let mut tx_transaction_index = UInt64Vec::new();
    let mut tx_value = quantity_builder();
    let mut tx_v = quantity_builder();
    let mut tx_r = quantity_builder();
    let mut tx_s = quantity_builder();
    let mut tx_cumulative_gas_used = quantity_builder();
    let mut tx_effective_gas_price = quantity_builder();
    let mut tx_gas_used = quantity_builder();
    let mut tx_contract_address = addr_builder();
    let mut tx_logs_bloom = MutableBinaryArray::<i32>::new();
    let mut tx_type = UInt8Vec::new();
    let mut tx_root = hash_builder();
    let mut tx_status = UInt8Vec::new();
    let mut tx_sighash = MutableBinaryArray::<i32>::new();

    let mut log_removed = MutableBooleanArray::new();
    let mut log_log_index = UInt64Vec::new();
    let mut log_transaction_index = UInt64Vec::new();
    let mut log_transaction_hash = hash_builder();
    let mut log_block_hash = hash_builder();
    let mut log_block_number = UInt64Vec::new();
    let mut log_address = addr_builder();
    let mut log_data = MutableBinaryArray::<i32>::new();
    let mut log_topic0 = MutableBinaryArray::<i32>::new();
    let mut log_topic1 = MutableBinaryArray::<i32>::new();
    let mut log_topic2 = MutableBinaryArray::<i32>::new();
    let mut log_topic3 = MutableBinaryArray::<i32>::new();

    let mut tx_map = data
        .blocks
        .iter_mut()
        .flat_map(|block| {
            let tx = mem::take(&mut block.transactions);
            tx.into_vec().into_iter()
        })
        .map(|tx| (tx.hash.to_vec(), tx))
        .collect::<BTreeMap<_, _>>();

    for receipt in data.receipts.into_iter().flat_map(|r| r.into_iter()) {
        let tx = tx_map.remove(receipt.transaction_hash.as_slice()).unwrap();
        assert_eq!(tx.hash, receipt.transaction_hash);

        tx_block_hash.push(Some(tx.block_hash.as_ref()));
        tx_block_number.push(Some(tx.block_number.into()));
        tx_from.push(Some(tx.from.as_ref()));
        tx_gas.push(Some(&*tx.gas));
        tx_gas_price.push(Some(&*tx.gas_price));
        tx_hash.push(Some(tx.hash.as_ref()));
        tx_input.push(Some(tx.input.as_ref()));
        tx_nonce.push(Some(&*tx.nonce));
        tx_to.push(tx.to.as_ref().map(|s| s.as_slice()));
        tx_transaction_index.push(Some(tx.transaction_index.into()));
        tx_value.push(Some(tx.value.as_ref()));
        tx_v.push(Some(&*tx.v));
        tx_r.push(Some(&*tx.r));
        tx_s.push(Some(&*tx.s));
        tx_cumulative_gas_used.push(Some(&*receipt.cumulative_gas_used));
        tx_effective_gas_price.push(Some(&*receipt.effective_gas_price));
        tx_gas_used.push(Some(&*receipt.gas_used));
        tx_contract_address.push(receipt.contract_address.as_ref().map(|s| s.as_slice()));
        tx_logs_bloom.push(Some(receipt.logs_bloom.as_ref()));
        tx_type.push(Some(receipt.kind.to_u8()));
        tx_root.push(receipt.root.as_ref().map(|s| s.as_slice()));
        tx_status.push(receipt.status.map(|s| s.to_u8()));
        tx_sighash.push(tx.input.get(0..4));

        for log in receipt.logs.iter() {
            log_removed.push(Some(log.removed));
            log_log_index.push(Some(log.log_index.into()));
            log_transaction_index.push(Some(log.transaction_index.into()));
            log_transaction_hash.push(Some(log.transaction_hash.as_ref()));
            log_block_hash.push(Some(log.block_hash.as_ref()));
            log_block_number.push(Some(log.block_number.into()));
            log_address.push(Some(log.address.as_ref()));
            log_data.push(Some(log.data.as_ref()));
            log_topic0.push(log.topics.get(0).map(|s| s.as_slice()));
            log_topic1.push(log.topics.get(1).map(|s| s.as_slice()));
            log_topic2.push(log.topics.get(2).map(|s| s.as_slice()));
            log_topic3.push(log.topics.get(3).map(|s| s.as_slice()));
        }
    }

    assert!(tx_map.is_empty());

    for block in data.blocks.into_iter() {
        b_number.push(Some(block.header.number.into()));
        b_hash.push(Some(block.header.hash.as_slice()));
        b_nonce.push(Some(block.header.nonce.as_slice()));
        b_sha3_uncles.push(Some(block.header.sha3_uncles.as_slice()));
        b_logs_bloom.push(Some(block.header.logs_bloom.as_slice()));
        b_transactions_root.push(Some(block.header.transactions_root.as_slice()));
        b_state_root.push(Some(block.header.state_root.as_slice()));
        b_receipts_root.push(Some(block.header.receipts_root.as_slice()));
        b_miner.push(Some(block.header.miner.as_slice()));
        b_difficulty.push(Some(&*block.header.difficulty));
        b_total_difficulty.push(Some(&*block.header.total_difficulty));
        b_extra_data.push(Some(&*block.header.extra_data));
        b_size.push(Some(&*block.header.size));
        b_gas_limit.push(Some(&*block.header.gas_limit));
        b_gas_used.push(Some(&*block.header.gas_used));
        b_timestamp.push(Some(&*block.header.timestamp));
        b_uncles.push(Some(block.header.uncles.iter().fold(
            Vec::new(),
            |mut v, b| {
                v.extend_from_slice(b.as_slice());
                v
            },
        )));
    }

    let blocks = ArrowChunk::try_new(vec![
        b_number.as_box(),
        b_hash.as_box(),
        b_nonce.as_box(),
        b_sha3_uncles.as_box(),
        b_logs_bloom.as_box(),
        b_transactions_root.as_box(),
        b_state_root.as_box(),
        b_receipts_root.as_box(),
        b_miner.as_box(),
        b_difficulty.as_box(),
        b_total_difficulty.as_box(),
        b_extra_data.as_box(),
        b_size.as_box(),
        b_gas_limit.as_box(),
        b_gas_used.as_box(),
        b_timestamp.as_box(),
        b_uncles.as_box(),
    ])
    .unwrap();

    let transactions = ArrowChunk::try_new(vec![
        tx_block_hash.as_box(),
        tx_block_number.as_box(),
        tx_from.as_box(),
        tx_gas.as_box(),
        tx_gas_price.as_box(),
        tx_hash.as_box(),
        tx_input.as_box(),
        tx_nonce.as_box(),
        tx_to.as_box(),
        tx_transaction_index.as_box(),
        tx_value.as_box(),
        tx_v.as_box(),
        tx_r.as_box(),
        tx_s.as_box(),
        tx_cumulative_gas_used.as_box(),
        tx_effective_gas_price.as_box(),
        tx_gas_used.as_box(),
        tx_contract_address.as_box(),
        tx_logs_bloom.as_box(),
        tx_type.as_box(),
        tx_root.as_box(),
        tx_status.as_box(),
        tx_sighash.as_box(),
    ])
    .unwrap();

    let logs = ArrowChunk::try_new(vec![
        log_removed.as_box(),
        log_log_index.as_box(),
        log_transaction_index.as_box(),
        log_transaction_hash.as_box(),
        log_block_hash.as_box(),
        log_block_number.as_box(),
        log_address.as_box(),
        log_data.as_box(),
        log_topic0.as_box(),
        log_topic1.as_box(),
        log_topic2.as_box(),
        log_topic3.as_box(),
    ])
    .unwrap();

    Batches {
        logs,
        blocks,
        transactions,
    }
}
