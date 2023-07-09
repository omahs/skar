use anyhow::{Context, Result};
use skar::{Args, SkarRunner};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    SkarRunner::run(args).await.context("run skar")?;

    Ok(())
}
