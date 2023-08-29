use crate::config::InnerConfig;
use crate::{validate_batch_data, BatchData, IngestConfig};
use anyhow::{Context, Error, Result};
use futures::{stream, StreamExt};
use skar_format::{Block, Transaction, TransactionReceipt};
use skar_rpc_client::{GetBlockByNumber, GetBlockReceipts, RpcClient, RpcRequest};
use std::cmp;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub struct Ingest {
    data_rx: mpsc::Receiver<BatchData>,
}

impl Ingest {
    pub fn spawn(config: IngestConfig, max_rollback_depth: u64) -> Self {
        let (data_tx, data_rx) = mpsc::channel(4);

        let client = RpcClient::new(config.rpc_client).into();

        tokio::spawn(async move {
            let e = Ingester {
                client,
                data_tx,
                config: config.inner,
                max_rollback_depth,
            }
            .ingest()
            .await;

            if let Err(e) = e {
                log::error!("failed to run ingester: {:?}", e);
            }
        });

        Self { data_rx }
    }

    pub async fn recv(&mut self) -> Result<BatchData> {
        self.data_rx.recv().await.context("receive batch data")
    }
}

struct Ingester {
    client: Arc<RpcClient>,
    data_tx: mpsc::Sender<BatchData>,
    config: InnerConfig,
    max_rollback_depth: u64,
}

impl Ingester {
    async fn ingest(self) -> Result<()> {
        while self.client.last_block().await == 0 {
            log::info!("waiting for endpoints to be ready to start ingesting...");
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        self.initial_sync(self.max_rollback_depth)
            .await
            .context("run initial sync")?;

        Ok(())
    }

    async fn initial_sync(&self, max_rollback_depth: u64) -> Result<u64> {
        let to_block = self
            .client
            .last_block()
            .await
            .saturating_sub(max_rollback_depth);

        let step: u64 = self.config.batch_size.get().try_into().unwrap();

        log::info!(
            "starting initial sync from {} to {}",
            self.config.from_block,
            to_block
        );

        let futs = (self.config.from_block..to_block)
            .step_by(self.config.batch_size.get())
            .map(|start_block| {
                let client = self.client.clone();

                async move {
                    let end_block = cmp::min(to_block, start_block + step);

                    log::debug!("ingesting {}-{}", start_block, end_block);

                    let req: RpcRequest = (start_block..end_block)
                        .map(|block_num| GetBlockByNumber(block_num.into()))
                        .collect::<Vec<_>>()
                        .into();

                    let resp = client.send(req).await.context("get block headers")?;

                    let blocks: Vec<Block<Transaction>> = resp.try_into().unwrap();

                    let req: RpcRequest = (start_block..end_block)
                        .map(|block_num| GetBlockReceipts(block_num.into()))
                        .collect::<Vec<_>>()
                        .into();

                    let resp = client.send(req).await.context("get block receipts")?;

                    let receipts: Vec<Vec<TransactionReceipt>> = resp.try_into().unwrap();

                    Ok::<_, Error>(BatchData {
                        blocks,
                        receipts,
                        from_block: start_block,
                        to_block: end_block,
                    })
                }
            });

        let mut data_stream = stream::iter(futs).buffered(self.config.concurrency_limit.get());
        while let Some(data) = data_stream.next().await {
            let data = data?;

            if !self.config.disable_validation {
                validate_batch_data(&data).context("validate batch data")?;
            }

            if self.data_tx.send(data).await.is_err() {
                log::warn!("no one is listening so quitting ingest loop.");
                break;
            }
        }

        log::info!("finished initial sync");

        Ok(cmp::max(to_block, self.config.from_block))
    }
}
