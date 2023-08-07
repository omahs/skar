use std::{cmp, sync::Arc};

use crate::{
    build_parquet_idx::build_parquet_indices,
    config::{Config, ParquetConfig},
    db::{BlockRange, Db},
    query::Handler,
    schema::data_to_batches,
    server,
    state::{InMemory, State},
    validate_parquet::validate_parquet_folder_data,
    write_parquet::write_folder,
    Args,
};
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use skar_ingest::Ingest;

pub struct SkarRunner;

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
            .next_block_num()
            .await
            .context("get next block num from db")?;

        let mut ingest_cfg = cfg.ingest;
        ingest_cfg.inner.from_block = ingest_cfg.inner.from_block.max(db_next_block_num);
        let ingest = Ingest::spawn(ingest_cfg);

        let state = State {
            db: db.clone(),
            in_mem: ArcSwap::new(InMemory::default().into()),
        };
        let state = Arc::new(state);

        let handler = Handler::new(cfg.query, state.clone(), &cfg.parquet.path);
        let handler = Arc::new(handler);

        let write = Write {
            state: state.clone(),
            ingest,
            parquet_config: cfg.parquet,
        };

        tokio::task::spawn(async move {
            if let Err(e) = write.ingest().await {
                log::error!("failed to run write task: {:?}", e);
            }
        });

        server::run(cfg.http_server, handler)
            .await
            .context("run http server")
    }
}

struct Write {
    state: Arc<State>,
    ingest: Ingest,
    parquet_config: ParquetConfig,
}

impl Write {
    async fn ingest(mut self) -> Result<()> {
        while let Ok(data) = self.ingest.recv().await {
            let mut in_mem = InMemory::clone(&self.state.in_mem.load());

            if in_mem.blocks.num_rows >= self.parquet_config.blocks.max_file_size
                || in_mem.transactions.num_rows >= self.parquet_config.transactions.max_file_size
                || in_mem.logs.num_rows >= self.parquet_config.logs.max_file_size
            {
                let to_block = in_mem.to_block;
                let from_block = in_mem.from_block;
                let mut temp_path = self.parquet_config.path.clone();
                temp_path.push(format!("{}-{}temp", from_block, to_block,));

                tokio::fs::create_dir_all(&temp_path)
                    .await
                    .context("create parquet directory")?;

                write_folder(&self.state, &temp_path, &self.parquet_config)
                    .await
                    .context("write temp parquet folder")?;

                validate_parquet_folder_data(&temp_path)
                    .context("validate parquet folder after writing")?;

                let mut final_path = self.parquet_config.path.clone();
                final_path.push(format!("{}-{}", from_block, to_block,));

                tokio::fs::remove_dir_all(&final_path).await.ok();

                tokio::fs::rename(&temp_path, &final_path)
                    .await
                    .context("rename parquet dir")?;

                let (folder_index, rg_index) =
                    build_parquet_indices(&final_path).context("build parquet indices")?;
                assert_eq!(BlockRange(from_block, to_block), folder_index.block_range);

                self.state
                    .db
                    .insert_folder_index(folder_index, rg_index)
                    .await
                    .context("insert parquet idx to db")?;

                in_mem = InMemory::default();
            }

            in_mem.from_block = cmp::min(data.from_block, in_mem.from_block);
            in_mem.to_block = cmp::max(data.to_block, in_mem.to_block);

            let batches = data_to_batches(data);

            in_mem.blocks.extend(batches.blocks.into());
            in_mem.transactions.extend(batches.transactions.into());
            in_mem.logs.extend(batches.logs.into());

            self.state.in_mem.store(in_mem.into());
        }

        Ok(())
    }
}
