use std::collections::BTreeSet;

use crate::{
    schema::{self, concat_u64},
    types::{LogSelection, Query, QueryResultData, TransactionSelection},
};
use anyhow::{anyhow, Context, Result};
use arrow::{array::UInt64Array, datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    logical_expr::Literal,
    prelude::{col, DataFrame, Expr},
};
use itertools::Itertools;

use super::data_provider::DataProvider;

pub async fn execute_query(
    data_provider: &dyn DataProvider,
    query: &Query,
) -> Result<QueryResultData> {
    let mut blk_set = BTreeSet::<u64>::new();
    let mut tx_set = BTreeSet::<(u64, u64)>::new();

    let logs = if !query.logs.is_empty() {
        query_logs(data_provider, query)
            .await
            .context("query logs")?
            .collect()
            .await
            .context("collect logs")?
    } else {
        Vec::new()
    };

    let logs = logs
        .into_iter()
        .filter_map(|batch| {
            let blk_num = batch
                .column_by_name("block_number")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let tx_idx = batch
                .column_by_name("transaction_index")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            for (b, t) in blk_num.iter().zip(tx_idx.iter()) {
                let (b, t) = (b.unwrap(), t.unwrap());
                tx_set.insert((b, t));
                blk_set.insert(b);
            }

            project_batch(&batch, &query.field_selection.log).transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    let transactions = if !query.transactions.is_empty() || !tx_set.is_empty() {
        query_transactions(data_provider, query, tx_set)
            .await
            .context("query transactions")?
            .collect()
            .await
            .context("collect transactions")?
    } else {
        Vec::new()
    };

    let transactions = transactions
        .into_iter()
        .filter_map(|batch| {
            let blk_num = batch
                .column_by_name("block_number")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            for b in blk_num.iter() {
                blk_set.insert(b.unwrap());
            }

            project_batch(&batch, &query.field_selection.transaction).transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    let blocks = if query.include_all_blocks || !blk_set.is_empty() {
        query_blocks(data_provider, query, blk_set)
            .await
            .context("query blocks")?
            .collect()
            .await
            .context("collect blocks")?
    } else {
        Vec::new()
    };

    let blocks = blocks
        .into_iter()
        .filter_map(|batch| project_batch(&batch, &query.field_selection.block).transpose())
        .collect::<Result<Vec<_>>>()?;

    Ok(QueryResultData {
        logs,
        transactions,
        blocks,
    })
}

fn project_batch(
    batch: &RecordBatch,
    field_selection: &BTreeSet<String>,
) -> Result<Option<RecordBatch>> {
    if field_selection.is_empty() || batch.num_rows() == 0 {
        return Ok(None);
    }

    let indices = batch
        .schema()
        .fields
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            if field_selection.contains(field.name()) {
                Some(idx)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let batch = batch.project(&indices).context("project batch")?;

    Ok(Some(batch))
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

        select.push(col(field.name()));
    }

    Ok(select)
}

async fn query_blocks(
    data_provider: &dyn DataProvider,
    query: &Query,
    blk_set: BTreeSet<u64>,
) -> Result<DataFrame> {
    let mut field_selection = query.field_selection.block.clone();
    field_selection.insert("number".to_owned());

    let select =
        build_select(schema::block_header(), &field_selection).context("build select statement")?;

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

    data_provider
        .load_blocks()
        .await
        .context("get table")?
        .select(select)
        .context("select columns")?
        .filter(filter)
        .context("filter")
}

async fn query_transactions(
    data_provider: &dyn DataProvider,
    query: &Query,
    tx_set: BTreeSet<(u64, u64)>,
) -> Result<DataFrame> {
    let mut field_selection = query.field_selection.transaction.clone();

    for name in [
        "block_number",
        "transaction_index",
        "tx_id",
        "from",
        "to",
        "sighash",
        "status",
    ] {
        field_selection.insert(name.to_owned());
    }

    let select =
        build_select(schema::transaction(), &field_selection).context("build select statement")?;

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

    data_provider
        .load_transactions()
        .await
        .context("get table")?
        .select(select)
        .context("select columns")?
        .filter(filter)
        .context("filter")
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

async fn query_logs(data_provider: &dyn DataProvider, query: &Query) -> Result<DataFrame> {
    let mut field_selection = query.field_selection.log.clone();

    for name in [
        "block_number",
        "transaction_index",
        "address",
        "topic0",
        "topic1",
        "topic2",
        "topic3",
    ] {
        field_selection.insert(name.to_owned());
    }

    let select = build_select(schema::log(), &field_selection).context("build select statement")?;

    let log_selection = query.logs.iter().fold(false.lit(), |ex, selection| {
        ex.or(log_selection_to_expr(selection))
    });

    let mut range_filter = col("block_number").gt_eq(query.from_block.lit());
    if let Some(to_block) = query.to_block {
        range_filter = range_filter.and(col("block_number").lt(to_block.lit()));
    }

    data_provider
        .load_logs()
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
