use std::{collections::BTreeSet, fs::File, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context, Result};
use arrow2::{datatypes::SchemaRef, io::parquet};
use wyhash::wyhash;

use crate::{
    db::{BlockRowGroupIndex, LogRowGroupIndex, RowGroupIndex, TransactionRowGroupIndex},
    schema,
    state::{ArrowChunk, InMemory},
    types::QueryContext,
};

type Data = Vec<ArrowBatch>;

pub struct ArrowBatch {
    pub chunk: Arc<ArrowChunk>,
    pub schema: SchemaRef,
}

impl ArrowBatch {
    pub fn column<T: 'static>(&self, name: &str) -> Result<&T> {
        match self
            .schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == name)
        {
            Some((idx, _)) => {
                let col = self.chunk.columns()[idx]
                    .as_any()
                    .downcast_ref::<T>()
                    .unwrap();
                Ok(col)
            }
            None => Err(anyhow!("field {} not found in schema", name)),
        }
    }
}

pub trait DataProvider: Send + Sync {
    fn load_logs(&self, ctx: &QueryContext) -> Result<Data>;
    fn load_transactions(&self, ctx: &QueryContext) -> Result<Data>;
    fn load_blocks(&self, ctx: &QueryContext) -> Result<Data>;
}

pub struct InMemDataProvider<'in_mem> {
    pub in_mem: &'in_mem InMemory,
}

impl<'in_mem> DataProvider for InMemDataProvider<'in_mem> {
    fn load_logs(&self, _ctx: &QueryContext) -> Result<Data> {
        let schema_ref = schema::log();

        Ok(self
            .in_mem
            .logs
            .data
            .iter()
            .map(|chunk| ArrowBatch {
                chunk: chunk.clone(),
                schema: schema_ref.clone(),
            })
            .collect())
    }

    fn load_transactions(&self, _ctx: &QueryContext) -> Result<Data> {
        let schema_ref = schema::transaction();

        Ok(self
            .in_mem
            .transactions
            .data
            .iter()
            .map(|chunk| ArrowBatch {
                chunk: chunk.clone(),
                schema: schema_ref.clone(),
            })
            .collect())
    }

    fn load_blocks(&self, _ctx: &QueryContext) -> Result<Data> {
        let schema_ref = schema::block_header();

        Ok(self
            .in_mem
            .blocks
            .data
            .iter()
            .map(|chunk| ArrowBatch {
                chunk: chunk.clone(),
                schema: schema_ref.clone(),
            })
            .collect())
    }
}

pub struct ParquetDataProvider {
    pub path: PathBuf,
    pub rg_index: RowGroupIndex,
}

impl ParquetDataProvider {
    fn load_table(
        &self,
        field_selection: &BTreeSet<String>,
        row_groups: &[usize],
        table_name: &str,
    ) -> Result<Data> {
        let mut path = self.path.clone();
        path.push(format!("{table_name}.parquet"));

        let mut reader = File::open(&path).context("open parquet file")?;
        let metadata =
            parquet::read::read_metadata(&mut reader).context("read parquet metadata")?;
        let schema = parquet::read::infer_schema(&metadata).context("infer parquet schema")?;

        let schema = schema.filter(|_index, field| field_selection.contains(&field.name));

        let row_groups = metadata
            .row_groups
            .into_iter()
            .enumerate()
            .filter(|(index, _)| row_groups.contains(index))
            .map(|(_, row_group)| row_group)
            .collect();

        let chunks =
            parquet::read::FileReader::new(reader, row_groups, schema.clone(), None, None, None);

        let chunks = chunks
            .map(|chunk| chunk.context("read row group").map(Arc::new))
            .collect::<Result<Vec<_>>>()?;

        let schema_ref = Arc::new(schema);

        Ok(chunks
            .into_iter()
            .map(|chunk| ArrowBatch {
                chunk,
                schema: schema_ref.clone(),
            })
            .collect())
    }
}

impl DataProvider for ParquetDataProvider {
    fn load_logs(&self, ctx: &QueryContext) -> Result<Data> {
        let row_groups = self
            .rg_index
            .log
            .iter()
            .enumerate()
            .filter_map(|(i, rg_index)| {
                if can_skip_log_row_group(ctx, rg_index) {
                    None
                } else {
                    Some(i)
                }
            })
            .collect::<Vec<_>>();

        let mut field_selection = ctx.query.field_selection.log.clone();
        field_selection.extend(LOG_QUERY_FIELDS.iter().map(|s| s.to_string()));

        self.load_table(&field_selection, &row_groups, "logs")
    }

    fn load_transactions(&self, ctx: &QueryContext) -> Result<Data> {
        let row_groups = self
            .rg_index
            .transaction
            .iter()
            .enumerate()
            .filter_map(|(i, rg_index)| {
                if can_skip_tx_row_group(ctx, rg_index) {
                    None
                } else {
                    Some(i)
                }
            })
            .collect::<Vec<_>>();

        let mut field_selection = ctx.query.field_selection.transaction.clone();
        field_selection.extend(TX_QUERY_FIELDS.iter().map(|s| s.to_string()));

        self.load_table(&field_selection, &row_groups, "transactions")
    }

    fn load_blocks(&self, ctx: &QueryContext) -> Result<Data> {
        let row_groups = self
            .rg_index
            .block
            .iter()
            .enumerate()
            .filter_map(|(i, rg_index)| {
                if can_skip_block_row_group(ctx, rg_index) {
                    None
                } else {
                    Some(i)
                }
            })
            .collect::<Vec<_>>();

        let mut field_selection = ctx.query.field_selection.block.clone();
        field_selection.extend(BLOCK_QUERY_FIELDS.iter().map(|s| s.to_string()));

        self.load_table(&field_selection, &row_groups, "blocks")
    }
}

fn can_skip_block_row_group(ctx: &QueryContext, rg_index: &BlockRowGroupIndex) -> bool {
    let from_block = ctx.query.from_block;
    let to_block = ctx.query.to_block;

    if from_block > rg_index.max_block_num {
        return true;
    }

    if let Some(to_block) = to_block {
        if to_block <= rg_index.min_block_num {
            return true;
        }
    }

    !ctx.query.include_all_blocks
        && !ctx.block_set.iter().any(|&block_num| {
            block_num >= rg_index.min_block_num && block_num <= rg_index.max_block_num
        })
}

fn can_skip_tx_row_group(ctx: &QueryContext, rg_index: &TransactionRowGroupIndex) -> bool {
    let from_block = ctx.query.from_block;
    let to_block = ctx.query.to_block;

    if from_block > rg_index.max_block_num {
        return true;
    }

    if let Some(to_block) = to_block {
        if to_block <= rg_index.min_block_num {
            return true;
        }
    }

    !ctx.query.transactions.iter().any(|tx| {
        let contains_from = tx.from.is_empty()
            || tx.from.iter().any(|addr| {
                let hash = wyhash(addr.as_slice(), 0);
                rg_index.from_address_filter.0.contains_hash(hash)
            });
        let contains_to = tx.to.is_empty()
            || tx.to.iter().any(|addr| {
                let hash = wyhash(addr.as_slice(), 0);
                rg_index.to_address_filter.0.contains_hash(hash)
            });
        contains_from && contains_to
    }) && !ctx.transaction_set.iter().any(|&(block_num, _)| {
        block_num >= rg_index.min_block_num && block_num <= rg_index.max_block_num
    })
}

fn can_skip_log_row_group(ctx: &QueryContext, rg_index: &LogRowGroupIndex) -> bool {
    let from_block = ctx.query.from_block;
    let to_block = ctx.query.to_block;

    if from_block > rg_index.max_block_num {
        return true;
    }

    if let Some(to_block) = to_block {
        if to_block <= rg_index.min_block_num {
            return true;
        }
    }

    !ctx.query.logs.iter().any(|log| {
        log.address.is_empty()
            || log.address.iter().any(|addr| {
                let hash = wyhash(addr.as_slice(), 0);
                rg_index.address_filter.0.contains_hash(hash)
            })
    })
}

const BLOCK_QUERY_FIELDS: &[&str] = &["number"];
const TX_QUERY_FIELDS: &[&str] = &[
    "block_number",
    "transaction_index",
    "from",
    "to",
    "sighash",
    "status",
];
const LOG_QUERY_FIELDS: &[&str] = &[
    "block_number",
    "transaction_index",
    "address",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
];
