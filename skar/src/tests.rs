use std::env::temp_dir;

use serde::de::DeserializeOwned;
use skar_ingest::BatchData;

use crate::{
    config::{CompressionConfig, ParquetConfig, TableConfig},
    schema::data_to_batches,
    state::{InMemory, InMemoryTable},
    validate_parquet::validate_parquet_folder_data,
    write_parquet::write_folder,
};

fn read_json<T: DeserializeOwned>(name: &str) -> T {
    let data = std::fs::read_to_string(format!(
        "{}/test-data/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();

    serde_json::from_str(&data).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_write_validate() {
    let block_data = read_json("block_data");
    let receipt_data = read_json("receipt_data");

    let data = BatchData {
        blocks: vec![block_data],
        receipts: vec![receipt_data],
        from_block: 12911679,
        to_block: 12911680,
    };

    let batches = data_to_batches(data);

    let in_mem = InMemory {
        blocks: InMemoryTable {
            num_rows: batches.blocks.len(),
            data: vec![batches.blocks.into()],
        },
        transactions: InMemoryTable {
            num_rows: batches.transactions.len(),
            data: vec![batches.transactions.into()],
        },
        logs: InMemoryTable {
            num_rows: batches.logs.len(),
            data: vec![batches.logs.into()],
        },
        from_block: 12911679,
        to_block: 12911680,
    };

    let mut tmp = temp_dir();
    tmp.push(format!("{}", uuid::Uuid::new_v4()));

    tokio::fs::create_dir_all(&tmp).await.unwrap();

    write_folder(
        &in_mem,
        &tmp,
        &ParquetConfig {
            path: "".into(),
            compression: CompressionConfig::Zstd,
            blocks: TableConfig {
                max_file_size: 69,
                max_row_group_size: 69,
            },
            transactions: TableConfig {
                max_file_size: 69,
                max_row_group_size: 69,
            },
            logs: TableConfig {
                max_file_size: 69,
                max_row_group_size: 69,
            },
        },
    )
    .await
    .unwrap();

    validate_parquet_folder_data(&tmp).unwrap();
}
