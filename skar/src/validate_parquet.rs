use anyhow::{anyhow, Context, Result};
use arrow2::{
    array::{Array, BinaryArray, UInt64Array},
    datatypes::Schema,
    io::parquet,
};
use ethbloom::{Bloom, Input};
use skar_format::{Address, LogArgument};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::Path,
};

use crate::state::ArrowChunk;

pub fn validate_parquet_folder_data(path: &Path) -> Result<()> {
    let mut path = path.to_owned();

    path.push("blocks.parquet");
    let blocks = load_block_data(&path).context("load block numbers")?;
    path.pop();

    path.push("transactions.parquet");
    let transactions = load_tx_identities(&path).context("load transaction identities")?;
    path.pop();

    path.push("logs.parquet");
    let logs = load_log_data(&path).context("load log data")?;

    for (block_number, logs) in logs.iter() {
        let mut logs_bloom = Bloom::default();

        for (_log_idx, log) in logs.iter() {
            if blocks.get(block_number).is_none() {
                return Err(anyhow!("block {} not found in parquet file", block_number));
            }

            if !transactions.contains(&(*block_number, log.transaction_index)) {
                return Err(anyhow!(
                    "transaction with block_num={};tx_index={}; not found in parquet file",
                    block_number,
                    log.transaction_index
                ));
            }

            logs_bloom.accrue(Input::Raw(log.address.as_slice()));
            for topic in log.topics.iter() {
                logs_bloom.accrue(Input::Raw(topic.as_slice()));
            }
        }

        let block_logs_bloom = blocks
            .get(block_number)
            .ok_or_else(|| anyhow!("data for block {} not found in parquet file", block_number))?;

        if logs_bloom.as_fixed_bytes() != &**block_logs_bloom {
            return Err(anyhow!(
                "logs_bloom for block {} doesn't match",
                block_number
            ));
        }
    }

    Ok(())
}

fn get_column<'a, T: 'static>(chunk: &'a ArrowChunk, schema: &Schema, name: &str) -> Result<&'a T> {
    let col = schema
        .fields
        .iter()
        .enumerate()
        .find(|(_, f)| f.name == name)
        .context("field not found")?;
    let col = chunk
        .columns()
        .get(col.0)
        .context("column not found")?
        .as_any()
        .downcast_ref::<T>()
        .context(format!("{name} is not the expected type"))?;

    Ok(col)
}

fn load_tx_identities(path: &Path) -> Result<BTreeSet<(u64, u64)>> {
    let mut tx_ident = BTreeSet::new();

    let mut reader = File::open(path).context("open parquet file")?;
    let metadata = parquet::read::read_metadata(&mut reader).context("read metadata")?;
    let schema = parquet::read::infer_schema(&metadata).context("infer schema")?;
    let chunks = parquet::read::FileReader::new(
        reader,
        metadata.row_groups,
        schema.clone(),
        None,
        None,
        None,
    );

    for chunk in chunks {
        let chunk = chunk.context("read chunk from parquet")?;

        let tx_index = get_column::<UInt64Array>(&chunk, &schema, "transaction_index")?;
        let block_num = get_column::<UInt64Array>(&chunk, &schema, "block_number")?;

        for (b, t) in block_num.iter().zip(tx_index) {
            let ident = (*b.unwrap(), *t.unwrap());
            tx_ident.insert(ident);
        }
    }

    Ok(tx_ident)
}

fn load_block_data(path: &Path) -> Result<BTreeMap<u64, Vec<u8>>> {
    let mut block_data = BTreeMap::new();

    let mut reader = File::open(path).context("open parquet file")?;
    let metadata = parquet::read::read_metadata(&mut reader).context("read metadata")?;
    let schema = parquet::read::infer_schema(&metadata).context("infer schema")?;
    let chunks = parquet::read::FileReader::new(
        reader,
        metadata.row_groups,
        schema.clone(),
        None,
        None,
        None,
    );

    for chunk in chunks {
        let chunk = chunk.context("read chunk from parquet")?;

        let logs_bloom = get_column::<BinaryArray<i32>>(&chunk, &schema, "logs_bloom")?;
        let block_num = get_column::<UInt64Array>(&chunk, &schema, "number")?;

        for (b, lb) in block_num.iter().zip(logs_bloom.iter()) {
            let data = (b.unwrap(), lb.unwrap());
            block_data.insert(*data.0, data.1.to_vec());
        }
    }

    Ok(block_data)
}

fn load_log_data(path: &Path) -> Result<BTreeMap<u64, BTreeMap<u64, LogData>>> {
    let mut res_data = BTreeMap::new();

    let mut reader = File::open(path).context("open parquet file")?;

    let metadata = parquet::read::read_metadata(&mut reader).context("read metadata")?;
    let schema = parquet::read::infer_schema(&metadata).context("infer schema")?;
    let chunks = parquet::read::FileReader::new(
        reader,
        metadata.row_groups,
        schema.clone(),
        None,
        None,
        None,
    );

    for chunk in chunks {
        let chunk = chunk.context("read chunk from parquet")?;

        let log_idx = get_column::<UInt64Array>(&chunk, &schema, "log_index")?;
        let block_num = get_column::<UInt64Array>(&chunk, &schema, "block_number")?;
        let tx_idx = get_column::<UInt64Array>(&chunk, &schema, "transaction_index")?;
        let address = get_column::<BinaryArray<i32>>(&chunk, &schema, "address")?;

        let topics = (0..4)
            .map(|i| {
                let col_name = format!("topic{i}");
                get_column::<BinaryArray<i32>>(&chunk, &schema, &col_name)
            })
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(topics.len(), 4);

        for i in 0..chunk.len() {
            let blk_num = block_num.value(i);
            let blk = res_data.entry(blk_num).or_insert(BTreeMap::new());

            let mut log_topics = Vec::new();
            for topic_ref in topics.iter() {
                if topic_ref.is_valid(i) {
                    log_topics.push(topic_ref.value(i).try_into().unwrap());
                } else {
                    break;
                }
            }

            blk.insert(
                log_idx.value(i),
                LogData {
                    transaction_index: tx_idx.value(i),
                    address: address.value(i).try_into().unwrap(),
                    topics: log_topics,
                },
            );
        }
    }

    Ok(res_data)
}

struct LogData {
    transaction_index: u64,
    address: Address,
    topics: Vec<LogArgument>,
}
