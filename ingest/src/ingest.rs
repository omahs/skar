use crate::config::InnerConfig;
use crate::{BatchData, IngestConfig};
use anyhow::{Context, Result};
use futures::{stream, StreamExt};
use skar_format::{Block, Transaction, TransactionReceipt};
use skar_rpc_client::{GetBlockByNumber, GetTransactionReceipt, RpcClient, RpcRequest};
use std::cmp;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct Ingest {
    data_rx: mpsc::Receiver<BatchData>,
}

impl Ingest {
    pub fn spawn(config: IngestConfig) -> Self {
        let (data_tx, data_rx) = mpsc::channel(4);

        let client = RpcClient::new(config.rpc_client).into();

        tokio::spawn(async move {
            let e = Ingester {
                client,
                data_tx,
                config: config.inner,
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
}

impl Ingester {
    async fn ingest(self) -> Result<()> {
        let step: u64 = self.config.batch_size.get().try_into().unwrap();
        let config = self.config;

        for start_block in (config.from_block..config.to_block).step_by(config.batch_size.get()) {
            let end_block = cmp::min(config.to_block, start_block + step);

            let req: RpcRequest = (start_block..end_block)
                .map(|block_num| GetBlockByNumber(block_num.into()))
                .collect::<Vec<_>>()
                .into();

            let resp = self.client.send(req).await.context("get block headers")?;

            let blocks: Vec<Block<Transaction>> = resp.try_into().unwrap();

            let mut reqs = Vec::new();

            for block in blocks.iter() {
                for tx in block.transactions.iter() {
                    reqs.push(GetTransactionReceipt(block.header.number, tx.hash.clone()));
                }
            }

            let futs = reqs
                .chunks(config.batch_size.get())
                .map(|chunk| {
                    let req: RpcRequest = chunk.to_vec().into();
                    let client = self.client.clone();
                    async move { client.clone().send(req).await }
                })
                .collect::<Vec<_>>();

            let mut receipts_stream =
                stream::iter(futs).buffer_unordered(config.concurrency_limit.get());

            let mut res_receipts = Vec::new();

            while let Some(receipts) = receipts_stream.next().await {
                let receipts: Vec<TransactionReceipt> = receipts
                    .context("execute receipts request")?
                    .try_into()
                    .unwrap();
                res_receipts.extend_from_slice(&receipts);
            }

            if self
                .data_tx
                .send(BatchData {
                    blocks,
                    receipts: res_receipts,
                })
                .await
                .is_err()
            {
                log::warn!("no one is listening so quitting ingest loop.");
                break;
            }
        }

        Ok(())
    }
}