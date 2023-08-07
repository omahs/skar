use std::{net::SocketAddr, path::PathBuf};

use serde::{Deserialize, Serialize};
use skar_ingest::IngestConfig;

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub ingest: IngestConfig,
    pub parquet: ParquetConfig,
    pub db: DbConfig,
    pub http_server: HttpServerConfig,
    pub query: QueryConfig,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct QueryConfig {
    pub time_limit_ms: u64,
    pub parquet_folder_batch_size: usize,
    pub max_concurrent_queries: usize,
}

#[derive(Serialize, Deserialize)]
pub struct HttpServerConfig {
    pub addr: SocketAddr,
    pub response_size_limit_mb: usize,
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
}

#[derive(Serialize, Deserialize)]
pub struct DbConfig {
    pub path: PathBuf,
}
