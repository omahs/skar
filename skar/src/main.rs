use skar::{Args, Skar};
use anyhow::{Context, Result};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    Skar::run(args).await.context("run skar")?;

    Ok(())
}
