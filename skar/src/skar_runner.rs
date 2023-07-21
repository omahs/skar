use std::{cmp, sync::Arc};

use crate::{
    config::{Config, ParquetConfig},
    query::{self, write_folder},
    schema::data_to_batches,
    Args,
};
use anyhow::{Context, Result};
use arc_swap::{access::DynAccess, ArcSwap};
use arrow::record_batch::RecordBatch;
use skar_ingest::Ingest;
use tokio::fs;

pub struct SkarRunner;

#[derive(Default, Clone)]
pub(crate) struct State {
    pub(crate) in_mem: InMemory,
}

#[derive(Clone)]
pub struct InMemory {
    pub(crate) blocks: InMemoryTable,
    pub(crate) transactions: InMemoryTable,
    pub(crate) logs: InMemoryTable,
    pub(crate) from_block: u64,
    pub(crate) to_block: u64,
}

impl Default for InMemory {
    fn default() -> Self {
        Self {
            from_block: u64::MAX,
            to_block: u64::MIN,
            blocks: Default::default(),
            transactions: Default::default(),
            logs: Default::default(),
        }
    }
}

#[derive(Default, Clone)]
pub struct InMemoryTable {
    pub(crate) data: Vec<RecordBatch>,
    pub(crate) num_rows: usize,
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
                parquet_config: cfg.parquet,
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
    parquet_config: ParquetConfig,
}

impl Write {
    async fn spawn(mut self) -> Result<()> {
        while let Ok(data) = self.ingest.recv().await {
            println!("GOT DATA");

            let mut state: Arc<State> = self.state.load_full();

            if state.in_mem.blocks.num_rows >= self.parquet_config.blocks.max_file_size
                || state.in_mem.transactions.num_rows
                    >= self.parquet_config.transactions.max_file_size
                || state.in_mem.logs.num_rows >= self.parquet_config.logs.max_file_size
            {
                let mut path = self.parquet_config.path.clone();
                path.push(format!(
                    "{}-{}",
                    state.in_mem.from_block, state.in_mem.to_block
                ));
                fs::create_dir_all(&path)
                    .await
                    .context("create parquet directory")?;

                write_folder(&state, path, &self.parquet_config)
                    .await
                    .context("write parquet folder")?;

                state = Arc::new(State::default());
            }

            let from_block = cmp::min(data.from_block, state.in_mem.from_block);
            let to_block = cmp::max(data.to_block, state.in_mem.to_block);

            let (blocks, transactions, logs) = data_to_batches(data);

            let blocks = InMemoryTable {
                num_rows: state.in_mem.blocks.num_rows + blocks.num_rows(),
                data: state
                    .in_mem
                    .blocks
                    .data
                    .iter()
                    .cloned()
                    .chain(std::iter::once(blocks))
                    .collect(),
            };

            let transactions = InMemoryTable {
                num_rows: state.in_mem.transactions.num_rows + transactions.num_rows(),
                data: state
                    .in_mem
                    .transactions
                    .data
                    .iter()
                    .cloned()
                    .chain(std::iter::once(transactions))
                    .collect(),
            };

            let logs = InMemoryTable {
                num_rows: state.in_mem.logs.num_rows + logs.num_rows(),
                data: state
                    .in_mem
                    .logs
                    .data
                    .iter()
                    .cloned()
                    .chain(std::iter::once(logs))
                    .collect(),
            };

            self.state.store(
                State {
                    in_mem: InMemory {
                        blocks,
                        transactions,
                        logs,
                        from_block,
                        to_block,
                    },
                }
                .into(),
            );
        }

        Ok(())
    }
}
