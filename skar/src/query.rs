use std::sync::Arc;

use crate::{schema, skar_runner::State};
use anyhow::{Context, Result};
use datafusion::{datasource::MemTable, prelude::SessionContext};

pub(crate) async fn query(state: &State) -> Result<()> {
    let ctx = SessionContext::new();

    let blocks: Arc<_> = MemTable::try_new(schema::block_header(), vec![state.blocks.clone()])
        .context("create blocks table")?
        .into();
    ctx.register_table("blocks", blocks)
        .context("register blocks table")?;

    let transactions: Arc<_> =
        MemTable::try_new(schema::transaction(), vec![state.transactions.clone()])
            .context("create transactions table")?
            .into();
    ctx.register_table("transactions", transactions)
        .context("register transactions table")?;

    let logs: Arc<_> = MemTable::try_new(schema::log(), vec![state.logs.clone()])
        .context("create logs table")?
        .into();
    ctx.register_table("logs", logs)
        .context("register logs table")?;

    ctx.sql(
        "
		SELECT COUNT(*) as block_count from blocks; 
	",
    )
    .await
    .context("execute sql query")?
    .show()
    .await
    .context("show block data frame")?;

    ctx.sql(
        "
        SELECT COUNT(*) as tx_count from transactions; 
    ",
    )
    .await
    .context("execute sql query")?
    .show()
    .await
    .context("show tx dataframe")?;

    ctx.sql(
        "
        SELECT COUNT(*) as log_count from logs; 
    ",
    )
    .await
    .context("execute sql query")?
    .show()
    .await
    .context("show log dataframe")?;

    Ok(())
}
