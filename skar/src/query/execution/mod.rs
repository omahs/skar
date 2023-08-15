use std::{collections::BTreeSet, sync::Arc};

use crate::{
    hashmap::FastSet,
    state::ArrowChunk,
    types::{LogSelection, Query, QueryContext, QueryResultData, TransactionSelection},
};
use anyhow::{Context, Result};
use arrow2::{
    array::{Array, BinaryArray, BooleanArray, MutableBooleanArray, UInt64Array, UInt8Array},
    bitmap::Bitmap,
    chunk::Chunk,
    compute,
    datatypes::{DataType, Schema},
    scalar::PrimitiveScalar,
};
use rayon::prelude::*;

use super::data_provider::{ArrowBatch, DataProvider};

pub fn execute_query(provider: &dyn DataProvider, query: &Query) -> Result<QueryResultData> {
    let mut ctx = QueryContext {
        query: query.clone(),
        block_set: FastSet::<u64>::default(),
        transaction_set: FastSet::<(u64, u64)>::default(),
    };

    let logs = if !query.logs.is_empty() {
        let log_data = provider.load_logs(&ctx).context("load log data")?;
        query_logs(
            log_data,
            query,
            &mut ctx.transaction_set,
            &mut ctx.block_set,
        )
        .context("query logs")?
    } else {
        Vec::new()
    };

    let transactions = if !query.transactions.is_empty() || !ctx.transaction_set.is_empty() {
        let tx_data = provider
            .load_transactions(&ctx)
            .context("load transaction data")?;
        query_transactions(tx_data, query, &ctx.transaction_set, &mut ctx.block_set)
            .context("query transactions")?
    } else {
        Vec::new()
    };

    let blocks = if !query.field_selection.block.is_empty()
        && (query.include_all_blocks || !ctx.block_set.is_empty())
    {
        let block_data = provider.load_blocks(&ctx).context("load block data")?;
        query_blocks(block_data, query, &ctx.block_set).context("query blocks")?
    } else {
        Vec::new()
    };

    Ok(QueryResultData {
        logs,
        transactions,
        blocks,
    })
}

fn query_logs(
    data: Vec<ArrowBatch>,
    query: &Query,
    tx_set: &mut FastSet<(u64, u64)>,
    blk_set: &mut FastSet<u64>,
) -> Result<Vec<ArrowBatch>> {
    let mut res = Vec::new();

    for mut batch in data {
        let block_number = batch.column::<UInt64Array>("block_number")?;
        let range_filter = build_range_filter(block_number, query);
        let selections_filter =
            log_selections_to_filter(&batch, &query.logs).context("build selections filter")?;
        let filter = compute::boolean::and(&range_filter, &selections_filter);

        batch.chunk = filter_chunk(&batch.chunk, &filter)
            .map(Arc::new)
            .context("filter record batch")?;

        let tx_index = batch.column::<UInt64Array>("transaction_index")?;

        let block_number = batch.column::<UInt64Array>("block_number")?;

        for (b, t) in block_number.iter().zip(tx_index.iter()) {
            let (b, t) = (*b.unwrap(), *t.unwrap());

            blk_set.insert(b);
            tx_set.insert((b, t));
        }

        let batch = project_batch(&batch, &query.field_selection.log).context("project batch")?;

        if batch.chunk.len() > 0 {
            res.push(batch);
        }
    }

    Ok(res)
}

pub fn filter_chunk(
    columns: &Chunk<Box<dyn Array>>,
    filter_values: &BooleanArray,
) -> Result<Chunk<Box<dyn Array>>> {
    let arrays = columns.arrays();

    let filter = compute::filter::build_filter(filter_values).context("build filter fn")?;

    let filtered_arrays = arrays.par_iter().map(|a| filter(a.as_ref())).collect();

    Chunk::try_new(filtered_arrays).context("build chunk")
}

fn log_selections_to_filter(
    batch: &ArrowBatch,
    selections: &[LogSelection],
) -> Result<BooleanArray> {
    let address = batch.column::<BinaryArray<i32>>("address")?;

    let mut topics = Vec::new();
    for i in 0..4 {
        let name = format!("topic{i}");
        let topic = batch.column::<BinaryArray<i32>>(&name)?;
        topics.push(topic);
    }
    let topics: [_; 4] = topics.try_into().unwrap();

    let mut filter = unset_bool_array(address.len());

    for selection in selections.iter() {
        let selection = log_selection_to_filter(address, &topics, selection);
        filter = compute::boolean::or(&filter, &selection);
    }

    Ok(filter)
}

fn log_selection_to_filter(
    address: &BinaryArray<i32>,
    topics: &[&BinaryArray<i32>; 4],
    selection: &LogSelection,
) -> BooleanArray {
    let mut filter = set_bool_array(address.len());

    if !selection.address.is_empty() {
        let addrs = selection.address.iter().map(|b| b.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(address, &addrs));
    }

    for (topic_filter, topic) in selection.topics.iter().zip(topics.iter()) {
        if !topic_filter.is_empty() {
            let topic_filter = topic_filter.iter().map(|b| b.as_slice()).collect();
            filter = compute::boolean::and(&filter, &in_set_binary(topic, &topic_filter));
        }
    }

    filter
}

fn query_transactions(
    data: Vec<ArrowBatch>,
    query: &Query,
    tx_set: &FastSet<(u64, u64)>,
    blk_set: &mut FastSet<u64>,
) -> Result<Vec<ArrowBatch>> {
    let mut res = Vec::new();

    for mut batch in data {
        let block_number = batch.column::<UInt64Array>("block_number")?;
        let transaction_index = batch.column::<UInt64Array>("transaction_index")?;

        let range_filter = build_range_filter(block_number, query);
        let selections_filter = tx_selections_to_filter(&batch, &query.transactions)
            .context("build tx selections filter")?;
        let filter = compute::boolean::and(&range_filter, &selections_filter);

        let in_set = in_set_u64_double(block_number, transaction_index, tx_set);
        let filter = compute::boolean::or(&in_set, &filter);

        batch.chunk = compute::filter::filter_chunk(&batch.chunk, &filter)
            .map(Arc::new)
            .context("filter record batch")?;

        let block_number = batch.column::<UInt64Array>("block_number")?;

        for b in block_number.iter() {
            blk_set.insert(*b.unwrap());
        }

        let batch =
            project_batch(&batch, &query.field_selection.transaction).context("project batch")?;

        if batch.chunk.len() > 0 {
            res.push(batch);
        }
    }

    Ok(res)
}

fn tx_selections_to_filter(
    batch: &ArrowBatch,
    selections: &[TransactionSelection],
) -> Result<BooleanArray> {
    let from = batch.column::<BinaryArray<i32>>("from")?;

    let to = batch.column::<BinaryArray<i32>>("to")?;

    let sighash = batch.column::<BinaryArray<i32>>("sighash")?;

    let status = batch.column::<UInt8Array>("status")?;

    let mut filter = unset_bool_array(from.len());

    for selection in selections.iter() {
        let selection = tx_selection_to_filter(from, to, sighash, status, selection);
        filter = compute::boolean::or(&filter, &selection);
    }

    Ok(filter)
}

fn tx_selection_to_filter(
    from: &BinaryArray<i32>,
    to: &BinaryArray<i32>,
    sighash: &BinaryArray<i32>,
    status: &UInt8Array,
    selection: &TransactionSelection,
) -> BooleanArray {
    let mut filter = set_bool_array(from.len());

    if !selection.from.is_empty() {
        let set = selection.from.iter().map(|b| b.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(from, &set));
    }

    if !selection.to.is_empty() {
        let set = selection.to.iter().map(|b| b.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(to, &set));
    }

    if !selection.sighash.is_empty() {
        let set = selection.sighash.iter().map(|b| b.as_slice()).collect();
        filter = compute::boolean::and(&filter, &in_set_binary(sighash, &set));
    }

    if let Some(status_f) = selection.status {
        filter = compute::boolean::and(
            &filter,
            &compute::comparison::eq_scalar(status, &PrimitiveScalar::from(Some(status_f))),
        );
    }

    filter
}

fn query_blocks(
    data: Vec<ArrowBatch>,
    query: &Query,
    blk_set: &FastSet<u64>,
) -> Result<Vec<ArrowBatch>> {
    let mut res = Vec::new();

    for batch in data {
        let filter = build_block_filter(&batch, query, blk_set).context("build filter")?;

        let mut batch =
            project_batch(&batch, &query.field_selection.block).context("project batch")?;

        batch.chunk = compute::filter::filter_chunk(&batch.chunk, &filter)
            .context("filter chunk")
            .map(Arc::new)?;

        if batch.chunk.len() > 0 {
            res.push(batch);
        }
    }

    Ok(res)
}

fn build_block_filter(
    batch: &ArrowBatch,
    query: &Query,
    blk_set: &FastSet<u64>,
) -> Result<BooleanArray> {
    let block_number = batch.column::<UInt64Array>("number")?;

    let range_filter = build_range_filter(block_number, query);

    if !query.include_all_blocks {
        let set_filter = in_set_u64(block_number, blk_set);
        Ok(compute::boolean::and(&range_filter, &set_filter))
    } else {
        Ok(range_filter)
    }
}

fn project_batch(batch: &ArrowBatch, field_selection: &BTreeSet<String>) -> Result<ArrowBatch> {
    let mut select_indices = Vec::new();
    for col_name in field_selection.iter() {
        let (idx, _) = batch
            .schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| &f.name == col_name)
            .context(format!("couldn't find column {col_name} in schema"))?;
        select_indices.push(idx);
    }

    let schema: Schema = batch
        .schema
        .fields
        .iter()
        .filter(|f| field_selection.contains(&f.name))
        .cloned()
        .collect::<Vec<_>>()
        .into();
    let schema = Arc::new(schema);

    let columns = batch
        .chunk
        .columns()
        .iter()
        .enumerate()
        .filter(|(i, _)| select_indices.contains(i))
        .map(|(_, c)| c.clone())
        .collect::<Vec<_>>();
    let chunk = ArrowChunk::new(columns).into();

    Ok(ArrowBatch { chunk, schema })
}

fn build_range_filter(block_number: &UInt64Array, query: &Query) -> BooleanArray {
    let mut range_filter = compute::comparison::gt_eq_scalar(
        block_number,
        &PrimitiveScalar::from(Some(query.from_block)),
    );
    if let Some(to_block) = query.to_block {
        let block_num_lt =
            compute::comparison::lt_scalar(block_number, &PrimitiveScalar::from(Some(to_block)));
        range_filter = compute::boolean::and(&range_filter, &block_num_lt);
    }

    range_filter
}

fn in_set_u64(data: &UInt64Array, set: &FastSet<u64>) -> BooleanArray {
    let mut bools = MutableBooleanArray::with_capacity(data.len());

    for val in data.iter() {
        bools.push(val.map(|v| set.contains(v)));
    }

    bools.into()
}

fn in_set_binary(data: &BinaryArray<i32>, set: &FastSet<&[u8]>) -> BooleanArray {
    let mut bools = MutableBooleanArray::with_capacity(data.len());

    for val in data.iter() {
        bools.push(val.map(|v| set.contains(v)));
    }

    bools.into()
}

fn in_set_u64_double(
    left: &UInt64Array,
    right: &UInt64Array,
    set: &FastSet<(u64, u64)>,
) -> BooleanArray {
    let len = left.len();
    assert_eq!(len, right.len());

    let mut bools = MutableBooleanArray::with_capacity(len);

    for (l, r) in left.iter().zip(right.iter()) {
        let lr = l.and_then(|l| r.map(|r| (*l, *r)));
        bools.push(lr.map(|lr| set.contains(&lr)));
    }

    bools.into()
}

fn set_bool_array(len: usize) -> BooleanArray {
    let num_bytes = (len + 7) / 8 * 8;
    let ones = vec![0xffu8; num_bytes];

    BooleanArray::new(DataType::Boolean, Bitmap::from_u8_vec(ones, len), None)
}

fn unset_bool_array(len: usize) -> BooleanArray {
    BooleanArray::new(DataType::Boolean, Bitmap::new_zeroed(len), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_range_filter() {
        let filter = build_range_filter(
            &UInt64Array::from_slice([3, 0, 0, 1, 6, 9, 2, 2, 0]),
            &Query {
                from_block: 1,
                to_block: Some(3),
                transactions: Vec::new(),
                logs: Vec::new(),
                field_selection: Default::default(),
                include_all_blocks: false,
            },
        );

        assert_eq!(
            filter.into_iter().collect::<Vec<Option<bool>>>(),
            vec![
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                Some(false),
            ]
        );
    }

    #[test]
    fn test_in_set_u64() {
        let set = [3, 1, 6, 9, 2, 2].into_iter().collect::<FastSet<_>>();

        let filter = in_set_u64(&UInt64Array::from_slice([3, 0, 0, 1, 6, 9, 2, 2, 0]), &set);

        assert_eq!(
            filter.into_iter().collect::<Vec<Option<bool>>>(),
            vec![
                Some(true),
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(false),
            ]
        );
    }

    #[test]
    fn test_in_set_u64_double() {
        let set = [(3, 1), (6, 9), (2, 2)].into_iter().collect::<FastSet<_>>();

        let filter = in_set_u64_double(
            &UInt64Array::from_slice([3, 1, 6, 9, 2, 2]),
            &UInt64Array::from_slice([1, 3, 9, 6, 1, 2]),
            &set,
        );

        assert_eq!(
            filter.into_iter().collect::<Vec<Option<bool>>>(),
            vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                Some(true),
            ]
        );
    }

    #[test]
    fn test_set_bool_array() {
        let arr = set_bool_array(31);

        for val in arr.iter() {
            assert!(val.unwrap());
        }

        assert_eq!(arr.len(), 31);
    }

    #[test]
    fn test_unset_bool_array() {
        let arr = unset_bool_array(69);

        for val in arr.iter() {
            assert!(!val.unwrap());
        }

        assert_eq!(arr.len(), 69);
    }
}
