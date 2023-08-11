use std::{cmp, fs, path::Path, sync::Arc};

use crate::{
    config::{ParquetConfig, TableConfig},
    schema,
    state::{ArrowChunk, InMemory},
};
use anyhow::{anyhow, Context, Error, Result};
use arrow2::{
    array::UInt32Array,
    compute::{
        self,
        sort::{lexsort_to_indices, SortColumn},
    },
    datatypes::{Schema, SchemaRef},
    io::parquet::write::{
        transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version,
        WriteOptions,
    },
};

const BLOCK_SORT_INDICES: &[usize] = &[
    0, // block.number
];

const TX_SORT_INDICES: &[usize] = &[
    1, // block_number
    9, // transaction_index
];

const LOG_SORT_INDICES: &[usize] = &[
    6, // block_number
    2, // transaction_index
    1, // log_index
];

async fn write_parquet_file(
    sort_indices: &[usize],
    data: &[Arc<ArrowChunk>],
    path: &Path,
    schema: SchemaRef,
    table_cfg: &TableConfig,
) -> Result<()> {
    tokio::task::block_in_place(|| {
        let row_groups = prepare_row_groups(sort_indices, data, table_cfg.max_row_group_size)
            .context("prepare row groups")?;

        let encodings = schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
            .collect();

        let row_groups = RowGroupIterator::try_new(
            row_groups.into_iter().map(Ok),
            &schema,
            parquet_write_options(),
            encodings,
        )
        .context("create row groups")?;

        let mut buf = Vec::new();
        let mut writer =
            FileWriter::try_new(&mut buf, Schema::clone(&schema), parquet_write_options())
                .context("create file writer")?;

        for group in row_groups {
            writer.write(group.unwrap()).context("write file data")?;
        }

        let _size = writer.end(None).context("write footer")?;

        fs::write(path, buf).context("write file to disk")?;

        Ok::<_, Error>(())
    })
}

fn prepare_row_groups(
    sort_indices: &[usize],
    data: &[Arc<ArrowChunk>],
    items_per_chunk: usize,
) -> Result<Vec<ArrowChunk>> {
    let chunk = concat_chunks(data).context("concatenate chunks")?;
    let chunk = lexsort_chunk(sort_indices, &chunk).context("sort chunk")?;

    let len = chunk.len();

    (0..len)
        .step_by(items_per_chunk)
        .map(|start| {
            let end = cmp::min(len, start + items_per_chunk);
            let length = end - start;
            Ok(ArrowChunk::new(
                chunk.iter().map(|arr| arr.sliced(start, length)).collect(),
            ))
        })
        .collect()
}

pub fn concat_chunks(chunks: &[Arc<ArrowChunk>]) -> Result<ArrowChunk> {
    if chunks.is_empty() {
        return Err(anyhow!("can't concat 0 chunks"));
    }

    let num_cols = chunks[0].columns().len();

    let cols = (0..num_cols)
        .map(|col| {
            let arrs = chunks
                .iter()
                .map(|chunk| {
                    chunk
                        .columns()
                        .get(col)
                        .map(|col| col.as_ref())
                        .context("get column")
                })
                .collect::<Result<Vec<_>>>()?;
            compute::concatenate::concatenate(&arrs).context("concat arrays")
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ArrowChunk::new(cols))
}

fn lexsort_chunk(sort_indices: &[usize], chunk: &ArrowChunk) -> Result<ArrowChunk> {
    let sort_cols = sort_indices
        .iter()
        .map(|i| {
            let values = chunk.columns().get(*i).context("get column")?.as_ref();

            Ok(SortColumn {
                values,
                options: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let indices: UInt32Array = lexsort_to_indices(&sort_cols, None).context("sort data")?;

    let cols = chunk
        .columns()
        .iter()
        .map(|col| compute::take::take(col.as_ref(), &indices).context("sort column"))
        .collect::<Result<Vec<_>>>()?;

    Ok(ArrowChunk::new(cols))
}

pub(crate) async fn write_folder(
    in_mem: &InMemory,
    path: &Path,
    cfg: &ParquetConfig,
) -> Result<()> {
    let blocks = {
        let mut path = path.to_owned();
        path.push("blocks.parquet");

        let in_mem = in_mem.clone();

        async move {
            write_parquet_file(
                BLOCK_SORT_INDICES,
                &in_mem.blocks.data,
                &path,
                schema::block_header(),
                &cfg.blocks,
            )
            .await
            .context("write blocks.parquet")?;

            Ok::<_, Error>(())
        }
    };

    let transactions = {
        let mut path = path.to_owned();
        path.push("transactions.parquet");

        let in_mem = in_mem.clone();

        async move {
            write_parquet_file(
                TX_SORT_INDICES,
                &in_mem.transactions.data,
                &path,
                schema::transaction(),
                &cfg.transactions,
            )
            .await
            .context("write transactions.parquet")?;

            Ok::<_, Error>(())
        }
    };

    let logs = {
        let mut path = path.to_owned();
        path.push("logs.parquet");
        async move {
            write_parquet_file(
                LOG_SORT_INDICES,
                &in_mem.logs.data,
                &path,
                schema::log(),
                &cfg.logs,
            )
            .await
            .context("write logs.parquet")?;

            Ok::<_, Error>(())
        }
    };

    let (b, t, l) = futures::future::join3(blocks, transactions, logs).await;

    b?;
    t?;
    l?;

    Ok(())
}

pub fn parquet_write_options() -> WriteOptions {
    WriteOptions {
        write_statistics: false,
        version: Version::V2,
        compression: CompressionOptions::Lz4Raw,
        data_pagesize_limit: Some(usize::MAX),
    }
}
