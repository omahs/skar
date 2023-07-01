use crate::{Args, config::Config};
use anyhow::{Result, Context};
use skar_format::BlockNumber;
use skar_rpc_client::{RpcClient, GetBlockNumber};
use tokio::fs::read_to_string;

pub struct Skar {}

impl Skar {
	pub async fn run(args: Args) -> Result<()> {
		let cfg = read_to_string(&args.config_path).await.context("read config file")?;
		let cfg: Config = toml::de::from_str(&cfg).context("parse config")?;

		let rpc_client = RpcClient::new(cfg.rpc_client);

		let block_number: BlockNumber = rpc_client.send(GetBlockNumber.into()).await.context("get block number")?.try_into_single().unwrap();
		let block_number: u64 = block_number.into();

		println!("block_number = {}", block_number);

		Ok(())
	}
}
