use serde::{Deserialize, Serialize};
use std::num::{NonZeroU64, NonZeroUsize};
use url::Url;

#[derive(Serialize, Deserialize, Clone)]
pub struct RpcClientConfig {
    pub http_req_timeout_millis: NonZeroU64,
    pub endpoints: Vec<EndpointConfig>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct EndpointConfig {
    pub url: Url,
    pub bearer_token: Option<String>,
    pub status_refresh_interval_secs: NonZeroU64,
    #[serde(flatten)]
    pub limit: LimitConfig,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LimitConfig {
    pub req_limit: NonZeroUsize,
    pub req_limit_window_ms: NonZeroU64,
    pub get_logs_range_limit: NonZeroU64,
    pub batch_size_limit: NonZeroUsize,
}
