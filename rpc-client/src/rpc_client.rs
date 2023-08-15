use crate::{endpoint::Endpoint, Error, Result, RpcClientConfig, RpcRequest, RpcResponse};
use std::cmp;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

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

    pub async fn last_block(&self) -> u64 {
        let mut last_block = 0;

        for e in self.endpoints.iter() {
            if let Some(lb) = e.last_block().await {
                last_block = cmp::max(last_block, *lb);
            }
        }

        last_block
    }

    pub fn endpoints(&self) -> &[Endpoint] {
        &self.endpoints
    }

    /// Executes the given rpc request without retries
    pub async fn send_once(&self, req: RpcRequest) -> Result<RpcResponse> {
        let req = Arc::new(req);
        let mut errs = Vec::new();
        for endpoint in self.endpoints.iter() {
            match endpoint.send(req.clone()).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    log::debug!(
                        "failed make request to endpoint {}.\nCaused by: {}",
                        endpoint.url(),
                        e
                    );
                    errs.push(e);
                }
            }
        }

        Err(Error::NoHealthyEndpoints(errs))
    }

    /// Executes the given rpc request, retries using exponential backoff until it succeds.
    pub async fn send(&self, req: RpcRequest) -> Result<RpcResponse> {
        let mut base = 1;

        loop {
            match self.send_once(req.clone()).await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    log::debug!("failed to execute request: {}", e);
                }
            }

            let secs = Duration::from_secs(base);
            let millis = Duration::from_millis(fastrange_rs::fastrange_64(rand::random(), 1000));

            sleep(secs + millis).await;

            base = cmp::min(base + 1, 5);
        }
    }
}
