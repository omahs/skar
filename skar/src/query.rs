use std::{path::PathBuf, sync::Arc};

use crate::{
    config::{ParquetConfig, TableConfig},
    schema,
    skar_runner::State,
};
use anyhow::{Context, Result};
use datafusion::{datasource::MemTable, prelude::SessionContext};
use parquet::{
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
    format::SortingColumn,
};

fn create_ctx(state: &State) -> Result<SessionContext> {
    let ctx = SessionContext::new();

    let blocks: Arc<_> = MemTable::try_new(
        schema::block_header(),
        vec![state.in_mem.blocks.data.clone()],
    )
    .context("create blocks table")?
    .into();
    ctx.register_table("blocks", blocks)
        .context("register blocks table")?;

    let transactions: Arc<_> = MemTable::try_new(
        schema::transaction(),
        vec![state.in_mem.transactions.data.clone()],
    )
    .context("create transactions table")?
    .into();
    ctx.register_table("transactions", transactions)
        .context("register transactions table")?;

    let logs: Arc<_> = MemTable::try_new(schema::log(), vec![state.in_mem.logs.data.clone()])
        .context("create logs table")?
        .into();
    ctx.register_table("logs", logs)
        .context("register logs table")?;

    Ok(ctx)
}

fn blocks_properties(cfg: &TableConfig) -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_data_page_size_limit(cfg.data_page_size_limit)
        .set_compression(Compression::LZ4_RAW)
        .set_max_row_group_size(cfg.max_row_group_size)
        .set_sorting_columns(Some(vec![SortingColumn {
            column_idx: 0,
            descending: false,
            nulls_first: false,
        }]))
        .set_column_statistics_enabled("number".into(), EnabledStatistics::Page)
        .build()
}

fn transactions_properties(cfg: &TableConfig) -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_data_page_size_limit(cfg.data_page_size_limit)
        .set_compression(Compression::LZ4_RAW)
        .set_max_row_group_size(cfg.max_row_group_size)
        .set_sorting_columns(Some(vec![
            SortingColumn {
                column_idx: 1,
                descending: false,
                nulls_first: false,
            },
            SortingColumn {
                column_idx: 9,
                descending: false,
                nulls_first: false,
            },
        ]))
        .set_column_statistics_enabled("block_number".into(), EnabledStatistics::Page)
        .set_column_statistics_enabled("transaction_index".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("from".into(), true)
        .set_column_bloom_filter_enabled("to".into(), true)
        .set_column_bloom_filter_enabled("hash".into(), true)
        .build()
}

fn logs_properties(cfg: &TableConfig) -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_data_page_size_limit(cfg.data_page_size_limit)
        .set_compression(Compression::LZ4_RAW)
        .set_max_row_group_size(cfg.max_row_group_size)
        .set_sorting_columns(Some(vec![
            SortingColumn {
                column_idx: 5,
                descending: false,
                nulls_first: false,
            },
            SortingColumn {
                column_idx: 1,
                descending: false,
                nulls_first: false,
            },
        ]))
        .set_column_statistics_enabled("block_number".into(), EnabledStatistics::Page)
        .set_column_statistics_enabled("log_index".into(), EnabledStatistics::Page)
        .set_column_statistics_enabled("transaction_index".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("address".into(), true)
        .set_column_bloom_filter_enabled("topic0".into(), true)
        .set_column_bloom_filter_enabled("topic1".into(), true)
        .set_column_bloom_filter_enabled("topic2".into(), true)
        .set_column_bloom_filter_enabled("topic3".into(), true)
        .build()
}

pub(crate) async fn write_folder(
    state: &State,
    mut path: PathBuf,
    cfg: &ParquetConfig,
) -> Result<()> {
    let ctx = create_ctx(state).context("create datafusion context")?;

    path.push("blocks");
    ctx.sql("SELECT * FROM blocks;")
        .await
        .context("load blocks")?
        .write_parquet(path.to_str().unwrap(), Some(blocks_properties(&cfg.blocks)))
        .await
        .context("write blocks parquet file")?;
    path.pop();

    path.push("transactions");
    ctx.sql("SELECT * FROM transactions;")
        .await
        .context("load transactions")?
        .write_parquet(
            path.to_str().unwrap(),
            Some(transactions_properties(&cfg.transactions)),
        )
        .await
        .context("write transactions parquet file")?;
    path.pop();

    path.push("logs");
    ctx.sql("SELECT * FROM logs;")
        .await
        .context("load logs")?
        .write_parquet(path.to_str().unwrap(), Some(logs_properties(&cfg.logs)))
        .await
        .context("write logs parquet file")?;
    path.pop();

    Ok(())
}

pub(crate) async fn query(state: &State) -> Result<()> {
    let ctx = create_ctx(state).context("create datafusion context")?;

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

    ctx.sql(
        "
        SELECT MAX(number) as max_block_num from blocks; 
    ",
    )
    .await
    .context("execute sql query")?
    .show()
    .await
    .context("show max_block_num dataframe")?;

    Ok(())
}
