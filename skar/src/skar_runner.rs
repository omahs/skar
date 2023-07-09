use crate::{config::Config, Args};
use anyhow::{Context, Result};
use skar_ingest::Ingest;

pub struct SkarRunner {}

impl SkarRunner {
    pub async fn run(args: Args) -> Result<()> {
        let cfg = tokio::fs::read_to_string(&args.config_path)
            .await
            .context("read config file")?;
        let cfg: Config = toml::de::from_str(&cfg).context("parse config")?;

        let mut ingest = Ingest::spawn(cfg.ingest);

        while let Ok(data) = ingest.recv().await {
            for block in data.blocks {
                let block_num: u64 = block.header.number.into();
                dbg!(block_num);
            }
        }

        Ok(())
    }
}
