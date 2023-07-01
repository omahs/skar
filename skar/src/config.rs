use serde::{Deserialize, Serialize};
use skar_rpc_client::RpcClientConfig;

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub rpc_client: RpcClientConfig,
}
