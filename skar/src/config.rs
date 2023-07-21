use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use skar_ingest::IngestConfig;

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub ingest: IngestConfig,
    pub parquet: ParquetConfig,
}

#[derive(Serialize, Deserialize)]
pub struct ParquetConfig {
    pub path: PathBuf,
    pub blocks: TableConfig,
    pub transactions: TableConfig,
    pub logs: TableConfig,
}

#[derive(Serialize, Deserialize)]
pub struct TableConfig {
    /// Maximum number of rows per file.
    ///
    /// This is implemented as best effort, so
    /// the actual files might contain more records.
    pub max_file_size: usize,
    /// Maximum number of rows per row group.
    ///
    /// This is implemented as best effort, so
    /// the actual row groups might contain more records.
    pub max_row_group_size: usize,
    /// Maximum page size limit in terms of bytes.
    ///
    /// This is implemented as best effort, so
    /// actual pages might be bigger.
    pub data_page_size_limit: usize,
}
