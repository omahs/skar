use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, FixedSizeBinaryArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use ethbloom::{Bloom, Input};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use skar_format::{Address, LogArgument};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::Path,
};

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

fn get_column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    let col = batch
        .column_by_name(name)
        .context(format!("get {name} column"))?
        .as_any()
        .downcast_ref::<T>()
        .context(format!("{name} is not the expected type"))?;

    Ok(col)
}

fn load_tx_identities(path: &Path) -> Result<BTreeSet<(u64, u64)>> {
    let mut tx_ident = BTreeSet::new();

    let file = File::open(path).context("open parquet file")?;

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).context("create parquet reader")?;
    let reader = builder.build().context("start parquet reader")?;

    for batch in reader {
        let batch = batch.context("read batch from parquet")?;

        let tx_index = get_column::<UInt64Array>(&batch, "transaction_index")?;
        let block_num = get_column::<UInt64Array>(&batch, "block_number")?;

        for (b, t) in block_num.iter().zip(tx_index) {
            let ident = (b.unwrap(), t.unwrap());
            tx_ident.insert(ident);
        }
    }

    Ok(tx_ident)
}

fn load_block_data(path: &Path) -> Result<BTreeMap<u64, Vec<u8>>> {
    let mut block_data = BTreeMap::new();

    let file = File::open(path).context("open parquet file")?;

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).context("create parquet reader")?;
    let reader = builder.build().context("start parquet reader")?;

    for batch in reader {
        let batch = batch.context("read batch from parquet")?;

        let logs_bloom = get_column::<FixedSizeBinaryArray>(&batch, "logs_bloom")?;
        let block_num = get_column::<UInt64Array>(&batch, "number")?;

        for (b, lb) in block_num.iter().zip(logs_bloom.iter()) {
            let data = (b.unwrap(), lb.unwrap());
            block_data.insert(data.0, data.1.to_vec());
        }
    }

    Ok(block_data)
}

fn load_log_data(path: &Path) -> Result<BTreeMap<u64, BTreeMap<u64, LogData>>> {
    let mut res_data = BTreeMap::new();

    let file = File::open(path).context("open parquet file")?;

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).context("create parquet reader")?;
    let reader = builder.build().context("start parquet reader")?;

    for batch in reader {
        let batch = batch.context("read batch from parquet")?;

        let log_idx = get_column::<UInt64Array>(&batch, "log_index")?;
        let block_num = get_column::<UInt64Array>(&batch, "block_number")?;
        let tx_idx = get_column::<UInt64Array>(&batch, "block_number")?;
        let address = get_column::<FixedSizeBinaryArray>(&batch, "address")?;

        let topics = (0..4)
            .map(|i| {
                let col_name = format!("topic{i}");
                get_column::<FixedSizeBinaryArray>(&batch, &col_name)
            })
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(topics.len(), 4);

        for i in 0..batch.num_rows() {
            let blk_num = block_num.value(i);
            let blk = res_data.entry(blk_num).or_insert(BTreeMap::new());

            let mut log_topics = Vec::new();
            for topic_ref in topics.iter() {
                if !topic_ref.is_null(i) {
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
