use std::{cmp, collections::BTreeSet, sync::Arc};

use crate::{
    config::{Config, ParquetConfig},
    db::Db,
    query,
    schema::data_to_batches,
    write_parquet::write_folder,
    Args,
};
use anyhow::{Context, Result};
use arc_swap::{access::DynAccess, ArcSwap};
use arrow::{array::FixedSizeBinaryArray, record_batch::RecordBatch};
use sbbf_rs_safe::Filter as Sbbf;
use skar_ingest::Ingest;

pub struct SkarRunner;

pub(crate) struct State {
    pub(crate) in_mem: InMemory,
    pub(crate) db: Arc<Db>,
}

impl State {
    fn new(db: Arc<Db>) -> Self {
        Self {
            in_mem: Default::default(),
            db,
        }
    }
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

        tokio::fs::create_dir_all(&cfg.db.path)
            .await
            .context("create db directory if not exists")?;

        let db = Db::new(&cfg.db.path).context("open db")?;
        let db = Arc::new(db);

        let db_next_block_num = db
            .get_next_block_num()
            .context("get next block num from db")?;

        let mut ingest_cfg = cfg.ingest;
        ingest_cfg.inner.from_block = ingest_cfg.inner.from_block.max(db_next_block_num);
        let ingest = Ingest::spawn(ingest_cfg);

        let state = ArcSwap::new(State::new(db).into());
        let state = Arc::new(state);

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

pub fn build_addr_set(in_mem: &InMemory) -> BTreeSet<Vec<u8>> {
    let mut addrs = BTreeSet::new();

    for batch in in_mem.transactions.data.iter() {
        for col_name in ["from", "to"] {
            let col = batch
                .column_by_name(col_name)
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();

            for addr in col.iter().flatten() {
                addrs.insert(addr.to_vec());
            }
        }
    }

    for batch in in_mem.logs.data.iter() {
        let col = batch
            .column_by_name("address")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        for addr in col.iter().flatten() {
            addrs.insert(addr.to_vec());
        }
    }

    addrs
}

fn build_filter(addrs: &BTreeSet<Vec<u8>>) -> Sbbf {
    let mut filter = Sbbf::new(8, cmp::min(addrs.len() * 2, 16 * 1024));

    for addr in addrs.iter() {
        filter.insert_hash(wyhash::wyhash(addr, 0));
    }

    filter
}

struct Write {
    state: Arc<ArcSwap<State>>,
    ingest: Ingest,
    parquet_config: ParquetConfig,
}

impl Write {
    async fn spawn(mut self) -> Result<()> {
        while let Ok(data) = self.ingest.recv().await {
            let mut state: Arc<State> = self.state.load_full();

            if state.in_mem.blocks.num_rows >= self.parquet_config.blocks.max_file_size
                || state.in_mem.transactions.num_rows
                    >= self.parquet_config.transactions.max_file_size
                || state.in_mem.logs.num_rows >= self.parquet_config.logs.max_file_size
            {
                let to_block = state.in_mem.to_block;
                let from_block = state.in_mem.from_block;
                let mut path = self.parquet_config.path.clone();
                path.push(format!("{}-{}", from_block, to_block,));

                tokio::fs::create_dir_all(&path)
                    .await
                    .context("create parquet directory")?;

                let addr_set = build_addr_set(&state.in_mem);

                write_folder(
                    &state,
                    path,
                    &self.parquet_config,
                    addr_set.len().try_into().unwrap(),
                )
                .await
                .context("write parquet folder")?;

                state
                    .db
                    .insert_folder_record(from_block, to_block, build_filter(&addr_set).as_bytes())
                    .context("insert folder record")?;

                state = Arc::new(State::new(state.db.clone()));
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
                    db: state.db.clone(),
                }
                .into(),
            );
        }

        Ok(())
    }
}
