use crate::{endpoint::Endpoint, Error, Result, RpcClientConfig, RpcRequest, RpcResponse};
use std::sync::Arc;
use std::time::Duration;

pub struct RpcClient {
    endpoints: Vec<Endpoint>,
}

impl RpcClient {
    pub fn new(config: RpcClientConfig) -> Self {
        let http_client = reqwest::Client::builder()
            .gzip(true)
            .timeout(Duration::from_millis(config.http_req_timeout_millis.get()))
            .tcp_keepalive(Duration::from_secs(5))
            .build()
            .unwrap();

        let endpoints = config
            .endpoints
            .into_iter()
            .map(|cfg| Endpoint::new(http_client.clone(), cfg))
            .collect::<Vec<_>>();

        Self { endpoints }
    }

    pub fn endpoints(&self) -> &[Endpoint] {
        &self.endpoints
    }

    pub async fn send(&self, req: RpcRequest) -> Result<RpcResponse> {
        let req = Arc::new(req);
        let mut errs = Vec::new();
        for endpoint in self.endpoints.iter() {
            match endpoint.send(req.clone()).await {
                Ok(resp) => return Ok(resp),
                Err(e) => errs.push(e),
            }
        }

        Err(Error::NoHealthyEndpoints(errs))
    }
}
