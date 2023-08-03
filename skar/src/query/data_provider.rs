use std::path::Path;

use anyhow::{Context, Result};
use datafusion::prelude::{DataFrame, ParquetReadOptions, SessionContext};

use crate::skar_runner::InMemory;

#[async_trait::async_trait]
pub trait DataProvider: Send + Sync {
    async fn load_logs(&self) -> Result<DataFrame>;
    async fn load_transactions(&self) -> Result<DataFrame>;
    async fn load_blocks(&self) -> Result<DataFrame>;
}

pub struct InMemDataProvider<'in_mem, 'ctx> {
    pub ctx: &'ctx SessionContext,
    pub in_mem: &'in_mem InMemory,
}

#[async_trait::async_trait]
impl<'in_mem, 'ctx> DataProvider for InMemDataProvider<'in_mem, 'ctx> {
    async fn load_logs(&self) -> Result<DataFrame> {
        self.ctx
            .read_batch(self.in_mem.logs.clone())
            .context("read logs batch from memory")
    }
    async fn load_transactions(&self) -> Result<DataFrame> {
        self.ctx
            .read_batch(self.in_mem.transactions.clone())
            .context("read transactions batch from memory")
    }
    async fn load_blocks(&self) -> Result<DataFrame> {
        self.ctx
            .read_batch(self.in_mem.blocks.clone())
            .context("read blocks batch from memory")
    }
}

pub struct ParquetDataProvider<'path, 'ctx> {
    pub ctx: &'ctx SessionContext,
    pub path: &'path Path,
}

impl<'path, 'ctx> ParquetDataProvider<'path, 'ctx> {
    async fn load_table(&self, name: &str) -> Result<DataFrame> {
        let mut path = self.path.to_owned();
        path.push(format!("{name}.parquet"));
        let path = path.to_str().context("invalid path")?;
        self.ctx
            .read_parquet(path, parquet_read_options())
            .await
            .context(format!("read {name} parquet file"))
    }
}

#[async_trait::async_trait]
impl<'path, 'ctx> DataProvider for ParquetDataProvider<'path, 'ctx> {
    async fn load_logs(&self) -> Result<DataFrame> {
        self.load_table("logs").await
    }

    async fn load_transactions(&self) -> Result<DataFrame> {
        self.load_table("transactions").await
    }

    async fn load_blocks(&self) -> Result<DataFrame> {
        self.load_table("blocks").await
    }
}

fn parquet_read_options() -> ParquetReadOptions<'static> {
    ParquetReadOptions::default()
        .parquet_pruning(true)
        .skip_metadata(true)
}
