use std::{collections::BTreeSet, path::Path, sync::Arc};

use crate::{
    schema::{self, concat_u64},
    skar_runner::State,
    types::{LogSelection, Query, QueryResult, TransactionSelection},
};
use anyhow::{anyhow, Context, Result};
use arrow::{
    array::UInt64Array,
    datatypes::{DataType, SchemaRef},
};
use datafusion::{
    datasource::MemTable,
    logical_expr::Literal,
    prelude::{col, encode, DataFrame, Expr, ParquetReadOptions, SessionContext},
};
use itertools::Itertools;

fn create_ctx_from_state(state: &State) -> Result<SessionContext> {
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

async fn create_ctx_from_folder(path: &Path) -> Result<SessionContext> {
    let ctx = SessionContext::new();

    let mut path = path.to_owned();

    for table_name in ["blocks", "transactions", "logs"] {
        path.push(format!("{table_name}.parquet"));
        let path_str = path.to_str().context("convert path to str")?;
        ctx.register_parquet(
            table_name,
            path_str,
            ParquetReadOptions {
                parquet_pruning: Some(true),
                ..Default::default()
            },
        )
        .await
        .context("register {table_name} parquet into df context")?;
        path.pop();
    }

    Ok(ctx)
}

#[allow(dead_code)]
pub(crate) async fn query_mem(state: &State, query: &Query) -> Result<QueryResult> {
    let ctx = create_ctx_from_state(state).context("create context")?;
    execute_query(&ctx, query).await
}

#[allow(dead_code)]
pub(crate) async fn query_folder(path: &Path, query: &Query) -> Result<QueryResult> {
    let ctx = create_ctx_from_folder(path)
        .await
        .context("create context")?;
    execute_query(&ctx, query).await
}

async fn execute_query(ctx: &SessionContext, query: &Query) -> Result<QueryResult> {
    let mut blk_set = BTreeSet::<u64>::new();
    let mut tx_set = BTreeSet::<(u64, u64)>::new();

    let mut logs_df = query_logs(ctx, query)
        .await
        .context("query logs")?
        .collect()
        .await
        .context("collect logs")?;

    for batch in logs_df.iter_mut() {
        let blk_num = batch
            .column_by_name("blk_num")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let tx_idx = batch
            .column_by_name("tx_idx")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        for (b, t) in blk_num.iter().zip(tx_idx.iter()) {
            let (b, t) = (b.unwrap(), t.unwrap());
            tx_set.insert((b, t));
            blk_set.insert(b);
        }

        // remove fields that were added for joining
        let indices = batch
            .schema()
            .fields
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| {
                if field.name() != "blk_num" && field.name() != "tx_idx" {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        *batch = batch.project(&indices).context("project log columns")?;
    }

    let mut transactions_df = query_transactions(ctx, query, tx_set)
        .await
        .context("query transactions")?
        .collect()
        .await
        .context("collect transactions")?;

    for batch in transactions_df.iter_mut() {
        let blk_num = batch
            .column_by_name("blk_num")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        for b in blk_num.iter() {
            blk_set.insert(b.unwrap());
        }

        // remove fields that were added for joining
        let indices = batch
            .schema()
            .fields
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| {
                if field.name() != "blk_num" {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        *batch = batch.project(&indices).context("project tx columns")?;
    }

    let blocks_df = query_blocks(ctx, query, blk_set)
        .await
        .context("query blocks")?
        .collect()
        .await
        .context("collect blocks")?;

    Ok(QueryResult {
        logs: logs_df,
        transactions: transactions_df,
        blocks: blocks_df,
    })
}

fn build_select(schema: SchemaRef, field_selection: &BTreeSet<String>) -> Result<Vec<Expr>> {
    let mut select = Vec::new();
    for col_name in field_selection.iter() {
        let (_, field) = match schema.fields().find(col_name) {
            Some(field) => field,
            None => {
                let available_fields = schema.fields.iter().map(|f| f.name()).join(",");
                return Err(anyhow!("Column '{col_name}' isn't found in the schema. Available fields are: {available_fields}"));
            }
        };

        let expr = match field.data_type() {
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                encode(col(field.name()), "hex".lit())
            }
            _ => col(field.name()),
        };

        select.push(expr);
    }

    Ok(select)
}

async fn query_blocks(
    ctx: &SessionContext,
    query: &Query,
    blk_set: BTreeSet<u64>,
) -> Result<DataFrame> {
    let select = build_select(schema::block_header(), &query.field_selection.block)
        .context("build select statement")?;

    let mut range_filter = col("number").gt_eq(query.from_block.lit());
    if let Some(to_block) = query.to_block {
        range_filter = range_filter.and(col("number").lt(to_block.lit()));
    }

    let num_list = blk_set.into_iter().map(|num| num.lit()).collect();
    let list_filter = query
        .include_all_blocks
        .lit()
        .or(col("number").in_list(num_list, false));
    let filter = range_filter.and(list_filter);

    ctx.table("blocks")
        .await
        .context("get table")?
        .filter(filter)
        .context("filter")?
        .select(select)
        .context("select columns")
}

async fn query_transactions(
    ctx: &SessionContext,
    query: &Query,
    tx_set: BTreeSet<(u64, u64)>,
) -> Result<DataFrame> {
    let mut select = build_select(schema::transaction(), &query.field_selection.transaction)
        .context("build select statement")?;

    // add aliases that are used for joining other tables
    select.push(col("block_number").alias("blk_num"));

    let tx_selection = query
        .transactions
        .iter()
        .fold(false.lit(), |ex, selection| {
            ex.or(tx_selection_to_expr(selection))
        });

    let mut range_filter = col("block_number").gt_eq(query.from_block.lit());
    if let Some(to_block) = query.to_block {
        range_filter = range_filter.and(col("block_number").lt(to_block.lit()));
    }

    let id_list = tx_set
        .into_iter()
        .map(|(b_num, tx_idx)| concat_u64(b_num, tx_idx).as_slice().lit())
        .collect();
    let filter = range_filter
        .and(tx_selection)
        .or(col("tx_id").in_list(id_list, false));

    ctx.table("transactions")
        .await
        .context("get table")?
        .filter(filter)
        .context("filter")?
        .select(select)
        .context("select columns")
}

fn tx_selection_to_expr(s: &TransactionSelection) -> Expr {
    let mut expr: Expr = true.lit();

    if !s.from.is_empty() {
        let list = s.from.iter().map(|addr| addr.as_slice().lit()).collect();
        expr = expr.and(col("from").in_list(list, false));
    }

    if !s.to.is_empty() {
        let list = s.to.iter().map(|addr| addr.as_slice().lit()).collect();
        expr = expr.and(col("to").in_list(list, false))
    }

    if !s.sighash.is_empty() {
        let list = s.sighash.iter().map(|sig| sig.as_slice().lit()).collect();
        expr = expr.and(col("sighash").in_list(list, false));
    }

    if let Some(status) = s.status {
        expr = expr.and(col("status").eq(status.lit()));
    }

    expr
}

async fn query_logs(ctx: &SessionContext, query: &Query) -> Result<DataFrame> {
    let mut select = build_select(schema::log(), &query.field_selection.log)
        .context("build select statement")?;

    // add aliases that are used for joining other tables
    select.push(col("transaction_index").alias("tx_idx"));
    select.push(col("block_number").alias("blk_num"));

    let log_selection = query.logs.iter().fold(false.lit(), |ex, selection| {
        ex.or(log_selection_to_expr(selection))
    });

    let mut range_filter = col("block_number").gt_eq(query.from_block.lit());
    if let Some(to_block) = query.to_block {
        range_filter = range_filter.and(col("block_number").lt(to_block.lit()));
    }

    ctx.table("logs")
        .await
        .context("get table")?
        .filter(range_filter.and(log_selection))
        .context("filter")?
        .select(select)
        .context("select columns")
}

fn log_selection_to_expr(s: &LogSelection) -> Expr {
    let mut expr: Expr = true.lit();

    if !s.address.is_empty() {
        let list = s.address.iter().map(|addr| addr.as_slice().lit()).collect();
        expr = expr.and(col("address").in_list(list, false));
    }

    for (i, topic) in s.topics.iter().enumerate() {
        let list = topic.iter().map(|t| t.as_slice().lit()).collect();
        let col = col(format!("topic{i}"));
        expr = expr.and(col.in_list(list, false));
    }

    expr
}

pub(crate) async fn query(state: &State) -> Result<()> {
    let ctx = create_ctx_from_state(state).context("create datafusion context")?;

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
