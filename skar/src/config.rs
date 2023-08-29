use std::{net::SocketAddr, path::PathBuf};

use serde::{Deserialize, Serialize};
use skar_ingest::IngestConfig;

#[derive(Serialize, Deserialize)]
pub struct Config {
    /// Maximum rollback depth of the indexed chain.
    pub maximum_rollback_depth: u64,
    /// Ingestion config
    pub ingest: IngestConfig,
    /// Config for parquet files
    pub parquet: ParquetConfig,
    /// Config for the embedded database
    pub db: DbConfig,
    /// Config for the http server
    pub http_server: HttpServerConfig,
    /// Config for query handler
    pub query: QueryConfig,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct QueryConfig {
    /// Time limit for handling a single query.
    ///
    /// If this time limit is hit, the query will stop,
    /// and the data will be returned to the user.
    pub time_limit_ms: u64,
}

#[derive(Serialize, Deserialize)]
pub struct HttpServerConfig {
    /// Socket address to serve the http server from
    pub addr: SocketAddr,
    /// Limit for amount of logs in a single response.
    ///
    /// If reponse payload reaches this limit, the query will stop and
    /// the payload will be returned to client.
    pub response_num_logs_limit: usize,
    /// Limit for amount of transactions in a single response.
    pub response_num_transactions_limit: usize,
    /// Limit for amount of blocks in a single response.
    pub response_num_blocks_limit: usize,
}

#[derive(Serialize, Deserialize)]
pub struct ParquetConfig {
    /// Disable parquet file validation
    pub disable_validation: bool,
    /// Compression option for parquet files
    pub compression: CompressionConfig,
    /// path to the directory of parquet files
    pub path: PathBuf,
    /// config for block parquet files
    pub blocks: TableConfig,
    /// config for transaction parquet files
    pub transactions: TableConfig,
    /// config for log parquet files
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
    /// Path to the database directory
    pub path: PathBuf,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum CompressionConfig {
    Zstd,
    Lz4,
}
