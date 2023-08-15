use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};
use skar_rpc_client::RpcClientConfig;

#[derive(Serialize, Deserialize)]
pub struct IngestConfig {
    /// Configuration for the Ethereum RPC client
    pub rpc_client: RpcClientConfig,
    #[serde(flatten)]
    pub inner: InnerConfig,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct InnerConfig {
    /// The block number to start the sync from
    pub from_block: u64,
    /// Limit to concurrent http requests
    pub concurrency_limit: NonZeroUsize,
    /// Batch size for Ethereum RPC requests
    pub batch_size: NonZeroUsize,
    /// Offset from the blockchain tip.
    /// Should be configured so ingestion stays behind the possible rollback range of the indexed chain
    /// A sensible value for eth mainnet is 10
    pub tip_offset: u64,
}
