use std::path::Path;

use crate::{
    config::{ParquetConfig, TableConfig},
    schema,
    skar_runner::State,
};
use anyhow::{Context, Error, Result};
use arrow::{
    array::{Array, ArrayRef, UInt32Array},
    compute,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    row::{RowConverter, SortField},
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding},
    file::properties::{
        EnabledStatistics, WriterProperties, WriterPropertiesBuilder, WriterVersion,
    },
    format::SortingColumn,
};

fn common_write_properties(cfg: &TableConfig) -> WriterPropertiesBuilder {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::LZ4_RAW)
        .set_encoding(Encoding::PLAIN)
        .set_data_page_size_limit(cfg.data_page_size_limit)
        .set_max_row_group_size(cfg.max_row_group_size)
}

fn indices_to_sorting_columns(indices: &[usize]) -> Vec<SortingColumn> {
    indices
        .iter()
        .map(|column_idx| SortingColumn {
            column_idx: i32::try_from(*column_idx).unwrap(),
            descending: false,
            nulls_first: false,
        })
        .collect()
}

const BLOCK_SORT_INDICES: &[usize] = &[
    // block.number
    0,
];

const TX_SORT_INDICES: &[usize] = &[
    // block_number
    1, // transaction_index
    9,
];

const LOG_SORT_INDICES: &[usize] = &[
    // block_number
    6, // transaction_index
    2, // log_index
    1,
];

fn blocks_write_properties(cfg: &TableConfig) -> WriterProperties {
    common_write_properties(cfg)
        .set_sorting_columns(Some(indices_to_sorting_columns(BLOCK_SORT_INDICES)))
        .set_column_statistics_enabled("number".into(), EnabledStatistics::Chunk)
        .build()
}

fn transactions_write_properties(
    cfg: &TableConfig,
    from_ndv: u64,
    to_ndv: u64,
) -> WriterProperties {
    common_write_properties(cfg)
        .set_sorting_columns(Some(indices_to_sorting_columns(TX_SORT_INDICES)))
        .set_column_statistics_enabled("block_number".into(), EnabledStatistics::Chunk)
        .set_column_statistics_enabled("transaction_index".into(), EnabledStatistics::Chunk)
        .set_column_statistics_enabled("tx_id".into(), EnabledStatistics::Chunk)
        // Setup bloom filter
        .set_column_bloom_filter_enabled("from".into(), true)
        .set_column_bloom_filter_fpp("from".into(), 0.01)
        .set_column_bloom_filter_ndv("from".into(), from_ndv)
        .set_column_bloom_filter_enabled("to".into(), true)
        .set_column_bloom_filter_fpp("to".into(), 0.01)
        .set_column_bloom_filter_ndv("to".into(), to_ndv)
        .build()
}

fn logs_write_properties(cfg: &TableConfig, address_ndv: u64) -> WriterProperties {
    common_write_properties(cfg)
        .set_sorting_columns(Some(indices_to_sorting_columns(LOG_SORT_INDICES)))
        .set_column_statistics_enabled("block_number".into(), EnabledStatistics::Chunk)
        .set_column_statistics_enabled("log_index".into(), EnabledStatistics::Chunk)
        .set_column_statistics_enabled("transaction_index".into(), EnabledStatistics::Chunk)
        // Setup bloom filter
        .set_column_bloom_filter_enabled("address".into(), true)
        .set_column_bloom_filter_fpp("address".into(), 0.01)
        .set_column_bloom_filter_ndv("address".into(), address_ndv)
        .build()
}

async fn write_parquet_file(
    data: &RecordBatch,
    path: &Path,
    schema: SchemaRef,
    props: WriterProperties,
) -> Result<()> {
    tokio::task::block_in_place(|| {
        let mut file = std::fs::File::create(path).context("create parquet file for writing")?;
        let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props))
            .context("create parquet writer")?;
        writer
            .write(data)
            .context("write arrow data to parquet file")?;

        writer.close().context("close parquet writer")?;

        Ok::<_, Error>(())
    })
}

fn lexsort_to_indices(arrays: &[ArrayRef]) -> UInt32Array {
    let fields = arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect();
    let mut converter = RowConverter::new(fields).unwrap();
    let rows = converter.convert_columns(arrays).unwrap();
    let mut sort: Vec<_> = rows.iter().enumerate().collect();
    sort.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));
    UInt32Array::from_iter_values(sort.iter().map(|(i, _)| *i as u32))
}

fn lexsort(indices: &[usize], batch: &RecordBatch) -> RecordBatch {
    let cols = indices
        .iter()
        .map(|i| batch.column(*i).clone())
        .collect::<Vec<ArrayRef>>();
    let indices = lexsort_to_indices(cols.as_slice());

    let cols = batch
        .columns()
        .iter()
        .map(|c| compute::take(c, &indices, None).unwrap())
        .collect();

    RecordBatch::try_new(batch.schema(), cols).unwrap()
}

pub(crate) async fn write_folder(
    state: &State,
    path: &Path,
    cfg: &ParquetConfig,
    addr_ndv: u64,
) -> Result<()> {
    let blocks = {
        let mut path = path.to_owned();
        path.push("blocks.parquet");

        async move {
            write_parquet_file(
                &lexsort(BLOCK_SORT_INDICES, &state.in_mem.blocks),
                &path,
                schema::block_header(),
                blocks_write_properties(&cfg.blocks),
            )
            .await
            .context("write blocks.parquet")?;

            Ok::<_, Error>(())
        }
    };

    let transactions = {
        let mut path = path.to_owned();
        path.push("transactions.parquet");
        async move {
            write_parquet_file(
                &lexsort(TX_SORT_INDICES, &state.in_mem.transactions),
                &path,
                schema::transaction(),
                transactions_write_properties(&cfg.transactions, addr_ndv, addr_ndv),
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
                &lexsort(LOG_SORT_INDICES, &state.in_mem.logs),
                &path,
                schema::log(),
                logs_write_properties(&cfg.logs, addr_ndv),
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
