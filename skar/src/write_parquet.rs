use std::path::Path;

use crate::{
    config::{ParquetConfig, TableConfig},
    schema,
    skar_runner::State,
};
use anyhow::{Context, Error, Result};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
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

fn blocks_write_properties(cfg: &TableConfig) -> WriterProperties {
    common_write_properties(cfg)
        .set_sorting_columns(Some(vec![SortingColumn {
            // block.number
            column_idx: 0,
            descending: false,
            nulls_first: false,
        }]))
        .set_column_statistics_enabled("number".into(), EnabledStatistics::Chunk)
        .build()
}

fn transactions_write_properties(
    cfg: &TableConfig,
    from_ndv: u64,
    to_ndv: u64,
) -> WriterProperties {
    common_write_properties(cfg)
        .set_sorting_columns(Some(vec![
            SortingColumn {
                // block_number
                column_idx: 1,
                descending: false,
                nulls_first: false,
            },
            SortingColumn {
                // transaction_index
                column_idx: 9,
                descending: false,
                nulls_first: false,
            },
        ]))
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
        .set_sorting_columns(Some(vec![
            // block_number
            SortingColumn {
                column_idx: 6,
                descending: false,
                nulls_first: false,
            },
            // transaction_index
            SortingColumn {
                column_idx: 2,
                descending: false,
                nulls_first: false,
            },
            // log_index
            SortingColumn {
                column_idx: 1,
                descending: false,
                nulls_first: false,
            },
        ]))
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
                &state.in_mem.blocks,
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
                &state.in_mem.transactions,
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
                &state.in_mem.logs,
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
