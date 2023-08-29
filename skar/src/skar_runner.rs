use std::{cmp, sync::Arc, time::Duration};

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
use skar_format::{Block, Hash, Transaction};
use skar_ingest::{BatchData, Ingest};
use skar_rpc_client::{GetBlockByNumber, GetBlockReceipts, RpcClient};

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

        let rpc_client = Arc::new(RpcClient::new(cfg.ingest.rpc_client.clone()));

        let mut ingest_cfg = cfg.ingest;
        ingest_cfg.inner.from_block = ingest_cfg.inner.from_block.max(db_next_block_num);
        let ingest = Ingest::spawn(ingest_cfg, cfg.maximum_rollback_depth);

        let state = State {
            db: db.clone(),
            in_mem: ArcSwap::new(
                InMemory {
                    from_block: u64::MAX,
                    to_block: 0,
                    blocks: Default::default(),
                    transactions: Default::default(),
                    logs: Default::default(),
                }
                .into(),
            ),
        };
        let state = Arc::new(state);

        let handler = Handler::new(cfg.query, state.clone(), &cfg.parquet.path);
        let handler = Arc::new(handler);

        let write = Write {
            state: state.clone(),
            ingest,
            parquet_config: cfg.parquet,
            maximum_rollback_depth: cfg.maximum_rollback_depth,
            rpc_client,
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
    maximum_rollback_depth: u64,
    rpc_client: Arc<RpcClient>,
}

impl Write {
    async fn ingest(mut self) -> Result<()> {
        self.initial_sync()
            .await
            .context("run initial sync and write")?;

        self.sync_with_rollbacks()
            .await
            .context("run sync with rollbacks")?;

        Ok(())
    }

    async fn sync_with_rollbacks(&mut self) -> Result<()> {
        let mut next_block = self.state.in_mem.load().to_block;
        let mut current_block_hash = self
            .get_block_hash(next_block.checked_sub(1).unwrap())
            .await
            .context("get last block hash")?;

        loop {
            while self.rpc_client.last_block().await < next_block {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            let data = self
                .get_block_data(next_block)
                .await
                .context("get block data")?;

            if current_block_hash != data.blocks[0].header.parent_hash {
                log::info!(
                    "rollback detected at parent of block: {:?}",
                    data.blocks[0].header.number
                );
                current_block_hash = self.execute_rollback().await.context("execute rollback")?;
            } else {
                current_block_hash = data.blocks[0].header.hash.clone();
                self.process_data(data)
                    .await
                    .context("process downloaded data")?;
            }

            next_block += 1;
        }
    }

    async fn execute_rollback(&mut self) -> Result<Hash> {
        let mut in_mem = InMemory::clone(&self.state.in_mem.load());

        let target_block_num = in_mem.to_block + 1;

        'rollback: loop {
            let blocks_to_delete = self
                .find_rollback_depth(&in_mem)
                .await
                .context("find the depth of rollback")?;
            in_mem.split_off_num_blocks(blocks_to_delete);

            let mut last_hash = self
                .get_block_hash(in_mem.to_block.checked_sub(1).unwrap())
                .await
                .context("get block hash")?;

            for block_num in in_mem.to_block..target_block_num {
                loop {
                    let data = self
                        .get_block_data(block_num)
                        .await
                        .context("get block data")?;

                    if last_hash.as_slice() != data.blocks[0].header.parent_hash.as_slice() {
                        continue 'rollback;
                    }
                    last_hash = data.blocks[0].header.hash.clone();

                    in_mem.from_block = cmp::min(data.from_block, in_mem.from_block);
                    in_mem.to_block = cmp::max(data.to_block, in_mem.to_block);

                    let batches = match data_to_batches(data) {
                        Ok(batches) => batches,
                        Err(e) => {
                            log::info!("failed to construct data from batches: {}", e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            log::info!("retrying to get data");
                            continue;
                        }
                    };

                    in_mem.blocks.extend(batches.blocks.into());
                    in_mem.transactions.extend(batches.transactions.into());
                    in_mem.logs.extend(batches.logs.into());

                    break;
                }
            }

            self.state.in_mem.store(Arc::new(in_mem));
            return Ok(last_hash);
        }
    }

    async fn find_rollback_depth(&self, in_mem: &InMemory) -> Result<u64> {
        let mut rollback_depth = 1;

        for (number, hash) in in_mem.iter_block_hashes() {
            let real_hash = self
                .get_block_hash(*number)
                .await
                .context("get block hash")?;

            if real_hash.as_slice() == hash {
                break;
            } else {
                rollback_depth += 1;
            }
        }

        Ok(rollback_depth)
    }

    async fn get_block_data(&self, block_num: u64) -> Result<BatchData> {
        let block = self
            .rpc_client
            .send(GetBlockByNumber(block_num.into()).into());

        let receipts = self
            .rpc_client
            .send(GetBlockReceipts(block_num.into()).into());

        let (block, receipts) = futures::join!(block, receipts);

        let block = block.context("get block data")?.try_into_single().unwrap();
        let receipts = receipts
            .context("get block receipts")?
            .try_into_single()
            .unwrap();

        log::trace!("downloaded data for block {}", block_num);

        let data = BatchData {
            blocks: vec![block],
            receipts: vec![receipts],
            from_block: block_num,
            to_block: block_num + 1,
        };

        Ok(data)
    }

    async fn get_block_hash(&self, block_num: u64) -> Result<Hash> {
        let block: Block<Transaction> = self
            .rpc_client
            .send(GetBlockByNumber(block_num.into()).into())
            .await
            .context("get block data")?
            .try_into_single()
            .unwrap();
        Ok(block.header.hash)
    }

    async fn initial_sync(&mut self) -> Result<()> {
        while let Ok(data) = self.ingest.recv().await {
            self.process_data(data)
                .await
                .context("process initial sync data")?;
        }

        Ok(())
    }

    async fn process_data(&mut self, data: BatchData) -> Result<()> {
        let mut in_mem = InMemory::clone(&self.state.in_mem.load());

        let mut new_in_mem = in_mem.split_off_num_blocks(self.maximum_rollback_depth);

        let flush_threshold_reached = in_mem.blocks.num_rows
            >= self.parquet_config.blocks.max_file_size
            || in_mem.transactions.num_rows >= self.parquet_config.transactions.max_file_size
            || in_mem.logs.num_rows >= self.parquet_config.logs.max_file_size;

        if flush_threshold_reached {
            let to_block = in_mem.to_block;
            let from_block = in_mem.from_block;
            let mut temp_path = self.parquet_config.path.clone();
            temp_path.push(format!("{}-{}temp", from_block, to_block,));

            tokio::fs::create_dir_all(&temp_path)
                .await
                .context("create parquet directory")?;

            write_folder(&in_mem, &temp_path, &self.parquet_config)
                .await
                .context("write temp parquet folder")?;

            if !self.parquet_config.disable_validation {
                validate_parquet_folder_data(&temp_path)
                    .context("validate parquet folder after writing")?;
            }

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
        } else if new_in_mem.blocks.num_rows > 0 {
            in_mem
                .blocks
                .data
                .extend_from_slice(&new_in_mem.blocks.data);
            in_mem.blocks.num_rows += new_in_mem.blocks.num_rows;

            in_mem
                .transactions
                .data
                .extend_from_slice(&new_in_mem.transactions.data);
            in_mem.transactions.num_rows += new_in_mem.transactions.num_rows;

            in_mem.logs.data.extend_from_slice(&new_in_mem.logs.data);
            in_mem.logs.num_rows += new_in_mem.logs.num_rows;

            in_mem.to_block = new_in_mem.to_block;

            new_in_mem = in_mem;
        } else {
            new_in_mem = in_mem;
        }

        let mut in_mem = new_in_mem;

        log::debug!("in_mem.from_block: {}", in_mem.from_block);
        log::debug!("in_mem.to_block: {}", in_mem.to_block);

        in_mem.from_block = cmp::min(data.from_block, in_mem.from_block);
        in_mem.to_block = cmp::max(data.to_block, in_mem.to_block);

        let batches = data_to_batches(data).context("construct data from batches")?;

        in_mem.blocks.extend(batches.blocks.into());
        in_mem.transactions.extend(batches.transactions.into());
        in_mem.logs.extend(batches.logs.into());

        self.state.in_mem.store(in_mem.into());

        Ok(())
    }
}
