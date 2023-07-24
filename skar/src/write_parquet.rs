use std::path::{Path, PathBuf};

use crate::{
    config::{ParquetConfig, TableConfig},
    schema,
    skar_runner::State,
};
use anyhow::{Context, Error, Result};
use arrow::{compute::concat_batches, datatypes::SchemaRef, record_batch::RecordBatch};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding},
    file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
    format::SortingColumn,
};

fn blocks_write_properties(cfg: &TableConfig) -> WriterProperties {
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
        .set_bloom_filter_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_column_statistics_enabled("number".into(), EnabledStatistics::Chunk)
        // Encoding
        .set_column_encoding("number".into(), Encoding::DELTA_BINARY_PACKED)
        .set_column_encoding("difficulty".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_column_encoding("total_difficulty".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_column_encoding("extra_data".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_column_encoding("size".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_column_encoding("gas_limit".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_column_encoding("gas_used".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_column_encoding("timestamp".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_column_encoding("uncles".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .build()
}

fn transactions_write_properties(
    cfg: &TableConfig,
    from_ndv: u64,
    to_ndv: u64,
) -> WriterProperties {
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
        .set_bloom_filter_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
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
        // Encoding
        .set_column_encoding("block_number".into(), Encoding::RLE)
        .set_column_encoding("gas".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_column_encoding("gas_price".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_column_encoding("input".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_column_encoding("transaction_index".into(), Encoding::DELTA_BINARY_PACKED)
        .set_column_encoding("value".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_column_encoding("v".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_column_encoding("r".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_column_encoding("s".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .set_column_encoding(
            "cumulative_gas_used".into(),
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
        )
        .set_column_encoding(
            "effective_gas_price".into(),
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
        )
        .set_column_encoding("gas_used".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .build()
}

fn logs_write_properties(cfg: &TableConfig, address_ndv: u64) -> WriterProperties {
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
                column_idx: 2,
                descending: false,
                nulls_first: false,
            },
            SortingColumn {
                column_idx: 1,
                descending: false,
                nulls_first: false,
            },
        ]))
        .set_bloom_filter_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_column_statistics_enabled("block_number".into(), EnabledStatistics::Chunk)
        .set_column_statistics_enabled("log_index".into(), EnabledStatistics::Chunk)
        .set_column_statistics_enabled("transaction_index".into(), EnabledStatistics::Chunk)
        // Setup bloom filter
        .set_column_bloom_filter_enabled("address".into(), true)
        .set_column_bloom_filter_fpp("address".into(), 0.01)
        .set_column_bloom_filter_ndv("address".into(), address_ndv)
        // Encoding
        .set_column_encoding("log_index".into(), Encoding::DELTA_BINARY_PACKED)
        .set_column_encoding("transaction_index".into(), Encoding::DELTA_BINARY_PACKED)
        .set_column_encoding("block_number".into(), Encoding::RLE)
        .set_column_encoding("data".into(), Encoding::DELTA_LENGTH_BYTE_ARRAY)
        .build()
}

async fn write_parquet_file(
    batches: &[RecordBatch],
    path: &Path,
    schema: SchemaRef,
    props: WriterProperties,
) -> Result<()> {
    tokio::task::block_in_place(|| async move {
        let data = concat_batches(&schema, batches).context("concatenate arrow batches")?;

        let mut file = std::fs::File::create(path).context("create parquet file for writing")?;
        let mut writer = ArrowWriter::try_new(&mut file, schema, Some(props))
            .context("create parquet writer")?;
        writer
            .write(&data)
            .context("write arrow data to parquet file")?;

        writer.close().context("close parquet writer")?;

        Ok::<_, Error>(())
    })
    .await
}

pub(crate) async fn write_folder(
    state: &State,
    path: PathBuf,
    cfg: &ParquetConfig,
    addr_ndv: u64,
) -> Result<()> {
    let blocks = {
        let mut path = path.clone();
        path.push("blocks.parquet");
        async move {
            write_parquet_file(
                &state.in_mem.blocks.data,
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
        let mut path = path.clone();
        path.push("transactions.parquet");
        async move {
            write_parquet_file(
                &state.in_mem.transactions.data,
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
        let mut path = path.clone();
        path.push("logs.parquet");
        async move {
            write_parquet_file(
                &state.in_mem.logs.data,
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
