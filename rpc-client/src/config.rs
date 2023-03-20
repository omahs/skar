use serde::{Deserialize, Serialize};
use std::num::{NonZeroU128, NonZeroU64, NonZeroUsize};

#[derive(Serialize, Deserialize)]
pub struct RpcClientConfig {
    pub http_req_timeout_millis: NonZeroU64,
    pub endpoints: Vec<EndpointConfig>,
}

#[derive(Serialize, Deserialize)]
pub struct EndpointConfig {
    pub url: surf::Url,
    pub status_refresh_interval_secs: NonZeroU64,
    #[serde(flatten)]
    pub limit: LimitConfig,
}

#[derive(Serialize, Deserialize)]
pub struct LimitConfig {
    pub req_limit: NonZeroUsize,
    pub req_limit_window_ms: NonZeroU128,
    pub get_logs_range_limit: NonZeroU64,
    pub batch_size_limit: NonZeroUsize,
}
