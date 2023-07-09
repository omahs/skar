use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};
use skar_rpc_client::RpcClientConfig;

#[derive(Serialize, Deserialize)]
pub struct IngestConfig {
    pub rpc_client: RpcClientConfig,
    #[serde(flatten)]
    pub inner: InnerConfig,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct InnerConfig {
    pub from_block: u64,
    pub to_block: u64,
    pub concurrency_limit: NonZeroUsize,
    pub batch_size: NonZeroUsize,
}
