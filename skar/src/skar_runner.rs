use std::sync::Arc;

use crate::{config::Config, query, schema::data_to_batches, Args};
use anyhow::{Context, Result};
use arc_swap::{access::DynAccess, ArcSwap};
use arrow::record_batch::RecordBatch;
use skar_ingest::Ingest;

pub struct SkarRunner;

#[derive(Default)]
pub(crate) struct State {
    pub(crate) blocks: Vec<RecordBatch>,
    pub(crate) transactions: Vec<RecordBatch>,
    pub(crate) logs: Vec<RecordBatch>,
}

impl SkarRunner {
    pub async fn run(args: Args) -> Result<()> {
        let cfg = tokio::fs::read_to_string(&args.config_path)
            .await
            .context("read config file")?;
        let cfg: Config = toml::de::from_str(&cfg).context("parse config")?;

        let ingest = Ingest::spawn(cfg.ingest);

        let state = Arc::new(ArcSwap::new(State::default().into()));

        tokio::spawn({
            let write = Write {
                state: state.clone(),
                ingest,
            };

            async move {
                if let Err(e) = write.spawn().await {
                    log::error!("failed to run write task: {:?}", e);
                }
            }
        });

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

        loop {
            interval.tick().await;
            query::query(&state.load())
                .await
                .context("run datafusion query")?;
        }
    }
}

struct Write {
    state: Arc<ArcSwap<State>>,
    ingest: Ingest,
}

impl Write {
    async fn spawn(mut self) -> Result<()> {
        while let Ok(data) = self.ingest.recv().await {
            println!("GOT DATA");

            let (blocks, transactions, logs) = data_to_batches(data);

            let state: &State = &self.state.load();

            self.state.store(
                State {
                    blocks: state
                        .blocks
                        .iter()
                        .cloned()
                        .chain(std::iter::once(blocks))
                        .collect(),
                    transactions: state
                        .blocks
                        .iter()
                        .cloned()
                        .chain(std::iter::once(transactions))
                        .collect(),
                    logs: state
                        .logs
                        .iter()
                        .cloned()
                        .chain(std::iter::once(logs))
                        .collect(),
                }
                .into(),
            );
        }

        Ok(())
    }
}
