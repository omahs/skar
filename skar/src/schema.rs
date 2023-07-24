use std::collections::BTreeMap;
use std::mem;
use std::sync::Arc;

use arrow::array::BooleanBuilder;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::{
    array::{BinaryBuilder, FixedSizeBinaryBuilder, UInt64Builder, UInt8Builder},
    record_batch::RecordBatch,
};
use skar_ingest::BatchData;

fn hash_dt() -> DataType {
    DataType::FixedSizeBinary(32)
}

fn hash_builder() -> FixedSizeBinaryBuilder {
    FixedSizeBinaryBuilder::new(32)
}

fn addr_dt() -> DataType {
    DataType::FixedSizeBinary(20)
}

fn addr_builder() -> FixedSizeBinaryBuilder {
    FixedSizeBinaryBuilder::new(20)
}

fn quantity_dt() -> DataType {
    DataType::Binary
}

fn quantity_builder() -> BinaryBuilder {
    BinaryBuilder::new()
}

pub fn block_header() -> SchemaRef {
    Schema::new(vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("hash", hash_dt(), false),
        Field::new("nonce", DataType::FixedSizeBinary(8), false),
        Field::new("sha3_uncles", hash_dt(), false),
        Field::new("logs_bloom", DataType::FixedSizeBinary(256), false),
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
    Schema::new(vec![
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
        Field::new("logs_bloom", DataType::FixedSizeBinary(256), false),
        Field::new("type", DataType::UInt8, false),
        Field::new("root", hash_dt(), true),
        Field::new("status", DataType::UInt8, true),
        Field::new("sighash", DataType::FixedSizeBinary(4), true),
        Field::new("tx_id", DataType::FixedSizeBinary(16), false),
    ])
    .into()
}

pub fn log() -> SchemaRef {
    Schema::new(vec![
        Field::new("removed", DataType::Boolean, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt64, false),
        Field::new("transaction_hash", hash_dt(), false),
        Field::new("block_hash", hash_dt(), false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("address", addr_dt(), false),
        Field::new("data", DataType::Binary, false),
        Field::new("topic0", DataType::FixedSizeBinary(32), true),
        Field::new("topic1", DataType::FixedSizeBinary(32), true),
        Field::new("topic2", DataType::FixedSizeBinary(32), true),
        Field::new("topic3", DataType::FixedSizeBinary(32), true),
    ])
    .into()
}

pub fn data_to_batches(mut data: BatchData) -> (RecordBatch, RecordBatch, RecordBatch) {
    let mut b_number = UInt64Builder::new();
    let mut b_hash = hash_builder();
    let mut b_nonce = FixedSizeBinaryBuilder::new(8);
    let mut b_sha3_uncles = hash_builder();
    let mut b_logs_bloom = FixedSizeBinaryBuilder::new(256);
    let mut b_transactions_root = hash_builder();
    let mut b_state_root = hash_builder();
    let mut b_receipts_root = hash_builder();
    let mut b_miner = addr_builder();
    let mut b_difficulty = quantity_builder();
    let mut b_total_difficulty = quantity_builder();
    let mut b_extra_data = BinaryBuilder::new();
    let mut b_size = quantity_builder();
    let mut b_gas_limit = quantity_builder();
    let mut b_gas_used = quantity_builder();
    let mut b_timestamp = quantity_builder();
    let mut b_uncles = BinaryBuilder::new();

    let mut tx_block_hash = hash_builder();
    let mut tx_block_number = UInt64Builder::new();
    let mut tx_from = addr_builder();
    let mut tx_gas = BinaryBuilder::new();
    let mut tx_gas_price = BinaryBuilder::new();
    let mut tx_hash = hash_builder();
    let mut tx_input = BinaryBuilder::new();
    let mut tx_nonce = BinaryBuilder::new();
    let mut tx_to = addr_builder();
    let mut tx_transaction_index = UInt64Builder::new();
    let mut tx_value = quantity_builder();
    let mut tx_v = quantity_builder();
    let mut tx_r = quantity_builder();
    let mut tx_s = quantity_builder();
    let mut tx_cumulative_gas_used = quantity_builder();
    let mut tx_effective_gas_price = quantity_builder();
    let mut tx_gas_used = quantity_builder();
    let mut tx_contract_address = addr_builder();
    let mut tx_logs_bloom = FixedSizeBinaryBuilder::new(256);
    let mut tx_type = UInt8Builder::new();
    let mut tx_root = hash_builder();
    let mut tx_status = UInt8Builder::new();
    let mut tx_sighash = FixedSizeBinaryBuilder::new(4);
    let mut tx_tx_id = FixedSizeBinaryBuilder::new(16);

    let mut log_removed = BooleanBuilder::new();
    let mut log_log_index = UInt64Builder::new();
    let mut log_transaction_index = UInt64Builder::new();
    let mut log_transaction_hash = hash_builder();
    let mut log_block_hash = hash_builder();
    let mut log_block_number = UInt64Builder::new();
    let mut log_address = addr_builder();
    let mut log_data = BinaryBuilder::new();
    let mut log_topic0 = FixedSizeBinaryBuilder::new(32);
    let mut log_topic1 = FixedSizeBinaryBuilder::new(32);
    let mut log_topic2 = FixedSizeBinaryBuilder::new(32);
    let mut log_topic3 = FixedSizeBinaryBuilder::new(32);

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

        tx_block_hash.append_value(tx.block_hash.as_ref()).unwrap();
        tx_block_number.append_value(tx.block_number.into());
        tx_from.append_value(tx.from.as_ref()).unwrap();
        tx_gas.append_value(&*tx.gas);
        tx_gas_price.append_value(&*tx.gas_price);
        tx_hash.append_value(tx.hash.as_ref()).unwrap();
        tx_input.append_value(tx.input.as_ref());
        tx_nonce.append_value(&*tx.nonce);
        match tx.to {
            Some(to) => tx_to.append_value(to.as_ref()).unwrap(),
            None => tx_to.append_null(),
        }
        tx_transaction_index.append_value(tx.transaction_index.into());
        tx_value.append_value(tx.value.as_ref());
        tx_v.append_value(&*tx.v);
        tx_r.append_value(&*tx.r);
        tx_s.append_value(&*tx.s);
        tx_cumulative_gas_used.append_value(&*receipt.cumulative_gas_used);
        tx_effective_gas_price.append_value(&*receipt.effective_gas_price);
        tx_gas_used.append_value(&*receipt.gas_used);
        match receipt.contract_address {
            Some(addr) => tx_contract_address.append_value(addr.as_ref()).unwrap(),
            None => tx_contract_address.append_null(),
        }
        tx_logs_bloom
            .append_value(receipt.logs_bloom.as_ref())
            .unwrap();
        tx_type.append_value(receipt.kind.to_u8());
        match receipt.root {
            Some(root) => tx_root.append_value(root.as_ref()).unwrap(),
            None => tx_root.append_null(),
        }
        match receipt.status {
            Some(status) => tx_status.append_value(status.to_u8()),
            None => tx_status.append_null(),
        }
        if let Some(sighash) = tx.input.get(..4) {
            tx_sighash.append_value(sighash).unwrap();
        } else {
            tx_sighash.append_null();
        }
        tx_tx_id
            .append_value(concat_u64(
                tx.block_number.into(),
                tx.transaction_index.into(),
            ))
            .unwrap();

        for log in receipt.logs.iter() {
            log_removed.append_value(log.removed);
            log_log_index.append_value(log.log_index.into());
            log_transaction_index.append_value(log.transaction_index.into());
            log_transaction_hash
                .append_value(log.transaction_hash.as_ref())
                .unwrap();
            log_block_hash
                .append_value(log.block_hash.as_ref())
                .unwrap();
            log_block_number.append_value(log.block_number.into());
            log_address.append_value(log.address.as_ref()).unwrap();
            log_data.append_value(log.data.as_ref());
            match log.topics.get(0) {
                Some(topic) => log_topic0.append_value(topic.as_ref()).unwrap(),
                None => log_topic0.append_null(),
            }
            match log.topics.get(1) {
                Some(topic) => log_topic1.append_value(topic.as_ref()).unwrap(),
                None => log_topic1.append_null(),
            }
            match log.topics.get(2) {
                Some(topic) => log_topic2.append_value(topic.as_ref()).unwrap(),
                None => log_topic2.append_null(),
            }
            match log.topics.get(3) {
                Some(topic) => log_topic3.append_value(topic.as_ref()).unwrap(),
                None => log_topic3.append_null(),
            }
        }
    }

    assert!(tx_map.is_empty());

    for block in data.blocks.into_iter() {
        b_number.append_value(block.header.number.into());
        b_hash.append_value(block.header.hash.as_slice()).unwrap();
        b_nonce.append_value(block.header.nonce.as_slice()).unwrap();
        b_sha3_uncles
            .append_value(block.header.sha3_uncles.as_slice())
            .unwrap();
        b_logs_bloom
            .append_value(block.header.logs_bloom.as_slice())
            .unwrap();
        b_transactions_root
            .append_value(block.header.transactions_root.as_slice())
            .unwrap();
        b_state_root
            .append_value(block.header.state_root.as_slice())
            .unwrap();
        b_receipts_root
            .append_value(block.header.receipts_root.as_slice())
            .unwrap();
        b_miner.append_value(block.header.miner.as_slice()).unwrap();
        b_difficulty.append_value(&*block.header.difficulty);
        b_total_difficulty.append_value(&*block.header.total_difficulty);
        b_extra_data.append_value(&*block.header.extra_data);
        b_size.append_value(&*block.header.size);
        b_gas_limit.append_value(&*block.header.gas_limit);
        b_gas_used.append_value(&*block.header.gas_used);
        b_timestamp.append_value(&*block.header.timestamp);
        b_uncles.append_value(block.header.uncles.iter().fold(Vec::new(), |mut v, b| {
            v.extend_from_slice(b.as_slice());
            v
        }));
    }

    let blocks = RecordBatch::try_new(
        block_header(),
        vec![
            Arc::new(b_number.finish()),
            Arc::new(b_hash.finish()),
            Arc::new(b_nonce.finish()),
            Arc::new(b_sha3_uncles.finish()),
            Arc::new(b_logs_bloom.finish()),
            Arc::new(b_transactions_root.finish()),
            Arc::new(b_state_root.finish()),
            Arc::new(b_receipts_root.finish()),
            Arc::new(b_miner.finish()),
            Arc::new(b_difficulty.finish()),
            Arc::new(b_total_difficulty.finish()),
            Arc::new(b_extra_data.finish()),
            Arc::new(b_size.finish()),
            Arc::new(b_gas_limit.finish()),
            Arc::new(b_gas_used.finish()),
            Arc::new(b_timestamp.finish()),
            Arc::new(b_uncles.finish()),
        ],
    )
    .unwrap();

    let transactions = RecordBatch::try_new(
        transaction(),
        vec![
            Arc::new(tx_block_hash.finish()),
            Arc::new(tx_block_number.finish()),
            Arc::new(tx_from.finish()),
            Arc::new(tx_gas.finish()),
            Arc::new(tx_gas_price.finish()),
            Arc::new(tx_hash.finish()),
            Arc::new(tx_input.finish()),
            Arc::new(tx_nonce.finish()),
            Arc::new(tx_to.finish()),
            Arc::new(tx_transaction_index.finish()),
            Arc::new(tx_value.finish()),
            Arc::new(tx_v.finish()),
            Arc::new(tx_r.finish()),
            Arc::new(tx_s.finish()),
            Arc::new(tx_cumulative_gas_used.finish()),
            Arc::new(tx_effective_gas_price.finish()),
            Arc::new(tx_gas_used.finish()),
            Arc::new(tx_contract_address.finish()),
            Arc::new(tx_logs_bloom.finish()),
            Arc::new(tx_type.finish()),
            Arc::new(tx_root.finish()),
            Arc::new(tx_status.finish()),
            Arc::new(tx_sighash.finish()),
            Arc::new(tx_tx_id.finish()),
        ],
    )
    .unwrap();

    let logs = RecordBatch::try_new(
        log(),
        vec![
            Arc::new(log_removed.finish()),
            Arc::new(log_log_index.finish()),
            Arc::new(log_transaction_index.finish()),
            Arc::new(log_transaction_hash.finish()),
            Arc::new(log_block_hash.finish()),
            Arc::new(log_block_number.finish()),
            Arc::new(log_address.finish()),
            Arc::new(log_data.finish()),
            Arc::new(log_topic0.finish()),
            Arc::new(log_topic1.finish()),
            Arc::new(log_topic2.finish()),
            Arc::new(log_topic3.finish()),
        ],
    )
    .unwrap();

    (blocks, transactions, logs)
}

pub fn concat_u64(a: u64, b: u64) -> [u8; 16] {
    let mut buf = [0; 16];

    buf[..8].copy_from_slice(a.to_be_bytes().as_slice());
    buf[8..].copy_from_slice(b.to_be_bytes().as_slice());

    buf
}
