use std::collections::BTreeSet;
use std::io::Cursor;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use arrow2::array::BinaryArray;
use arrow2::array::MutableUtf8Array;
use arrow2::array::Utf8Array;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::datatypes::Schema;
use arrow2::io::ipc;
use arrow2::io::json::write::RecordSerializer;
use axum::extract::Json as ReqJson;
use axum::extract::State as AxumState;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use skar_net_types::skar_net_types_capnp;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;

use crate::config::HttpServerConfig;
use crate::query::ArrowBatch;
use crate::query::Handler;
use crate::schema;
use crate::state::ArrowChunk;
use crate::types::{Query, QueryResultData};
use crate::write_parquet::concat_chunks;

struct ServerState {
    cfg: HttpServerConfig,
    handler: Arc<Handler>,
}

const MEGABYTES: usize = 1024 * 1024;

pub(crate) async fn run(cfg: HttpServerConfig, handler: Arc<Handler>) -> anyhow::Result<()> {
    let addr = cfg.addr;
    let state = ServerState { cfg, handler };
    let state = Arc::new(state);

    let app = axum::Router::new()
        .route(
            "/height",
            axum::routing::get(get_height).with_state(state.clone()),
        )
        .route(
            "/query",
            axum::routing::post(run_json_query).with_state(state.clone()),
        )
        .route(
            "/query/arrow-ipc",
            axum::routing::post(run_ipc_query).with_state(state),
        )
        .layer(ServiceBuilder::new().layer(CompressionLayer::new()));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .context("run http server")
}

async fn get_height(
    AxumState(state): AxumState<Arc<ServerState>>,
) -> Result<Json<serde_json::Value>, AppError> {
    let height = state
        .handler
        .archive_height()
        .await
        .context("get archive height")?;

    Ok(Json(serde_json::json!({
        "height": height,
    })))
}

async fn run_ipc_query(
    AxumState(state): AxumState<Arc<ServerState>>,
    ReqJson(query): ReqJson<Query>,
) -> Result<Response, AppError> {
    let query_start = Instant::now();

    let field_selection = query.field_selection.clone();

    let mut rx = state
        .handler
        .clone()
        .handle(query)
        .context("start running query")?;

    let write_opts = ipc::write::WriteOptions {
        compression: Some(ipc::write::Compression::ZSTD),
    };

    let mut blocks_writer = ipc::write::FileWriter::new(
        Cursor::new(Vec::with_capacity(512)),
        project_schema(&schema::block_header(), &field_selection.block)
            .context("project schema")?,
        None,
        write_opts,
    );
    blocks_writer.start().context("start writer")?;

    let mut transactions_writer = ipc::write::FileWriter::new(
        Cursor::new(Vec::with_capacity(512)),
        project_schema(&schema::transaction(), &field_selection.transaction)
            .context("project schema")?,
        None,
        write_opts,
    );
    transactions_writer.start().context("start writer")?;

    let mut logs_writer = ipc::write::FileWriter::new(
        Cursor::new(Vec::with_capacity(512)),
        project_schema(&schema::log(), &field_selection.log).context("project schema")?,
        None,
        write_opts,
    );
    logs_writer.start().context("start writer")?;

    let mut next_block = 0;

    while let Some(res) = rx.recv().await {
        let query_result = res.context("execute parquet query")?;
        let data = query_result.data;

        if !data.blocks.is_empty() {
            for chunk in data.blocks.iter() {
                blocks_writer
                    .write(&chunk.chunk, None)
                    .context("write blocks")?;
            }
        }

        if !data.transactions.is_empty() {
            for chunk in data.transactions.iter() {
                transactions_writer
                    .write(&chunk.chunk, None)
                    .context("write transactions")?;
            }
        }

        if !data.logs.is_empty() {
            for chunk in data.logs.iter() {
                logs_writer
                    .write(&chunk.chunk, None)
                    .context("write logs")?;
            }
        }

        next_block = query_result.next_block;

        if blocks_writer.inner().position()
            + transactions_writer.inner().position()
            + logs_writer.inner().position()
            >= (state.cfg.response_size_limit_mb * MEGABYTES)
                .try_into()
                .unwrap()
        {
            break;
        }
    }

    blocks_writer.finish().context("finish blocks")?;
    transactions_writer
        .finish()
        .context("finish transactions")?;
    logs_writer.finish().context("finish logs")?;

    let blocks = blocks_writer.into_inner().into_inner();
    let transactions = transactions_writer.into_inner().into_inner();
    let logs = logs_writer.into_inner().into_inner();

    let height = state
        .handler
        .archive_height()
        .await
        .context("get archive height")?;

    let mut message = capnp::message::Builder::new_default();
    {
        let mut query_response =
            message.init_root::<skar_net_types_capnp::query_response::Builder>();

        if let Some(height) = height {
            query_response.set_archive_height(height);
        }
        query_response
            .set_total_execution_time(query_start.elapsed().as_millis().try_into().unwrap());
        query_response.set_next_block(next_block);

        let mut data = query_response.init_data();
        data.set_blocks(&blocks);
        data.set_transactions(&transactions);
        data.set_logs(&logs);
    }

    let mut resp_body = Vec::with_capacity(512);
    capnp::serialize_packed::write_message(&mut resp_body, &message)
        .context("write capnp message")?;

    Ok(resp_body.into_response())
}

fn project_schema(
    schema: &Schema,
    field_selection: &BTreeSet<String>,
) -> Result<Schema, anyhow::Error> {
    let mut select_indices = Vec::new();
    for col_name in field_selection.iter() {
        let (idx, _) = schema
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| &f.name == col_name)
            .context(format!("couldn't find column {col_name} in schema"))?;
        select_indices.push(idx);
    }

    let schema: Schema = schema
        .fields
        .iter()
        .filter(|f| field_selection.contains(&f.name))
        .cloned()
        .collect::<Vec<_>>()
        .into();

    Ok(schema)
}

async fn run_json_query(
    AxumState(state): AxumState<Arc<ServerState>>,
    ReqJson(query): ReqJson<Query>,
) -> Result<Response, AppError> {
    let query_start = Instant::now();

    let mut rx = state
        .handler
        .clone()
        .handle(query)
        .context("start running query")?;

    let mut bytes = br#"{"data":["#.to_vec();

    let mut next_block = 0;

    let mut put_comma = false;
    while let Some(res) = rx.recv().await {
        let query_result = res.context("execute parquet query")?;

        put_comma |= extend_bytes_with_data(put_comma, &mut bytes, &query_result.data)?;

        next_block = query_result.next_block;

        if bytes.len() >= state.cfg.response_size_limit_mb * MEGABYTES {
            break;
        }
    }

    let height = state
        .handler
        .archive_height()
        .await
        .context("get archive height")?;

    write!(
        &mut bytes,
        r#"],"archive_height":{},"next_block":{},"total_execution_time":{}}}"#,
        height.map(|n| n.to_string()).unwrap_or("null".to_owned()),
        next_block,
        query_start.elapsed().as_millis(),
    )
    .unwrap();

    let mut response: Response = bytes.into_response();

    response
        .headers_mut()
        .insert("content-type", "application/json".try_into().unwrap());

    Ok(response)
}

// returns if it wrote any data
fn extend_bytes_with_data(
    put_comma_outer: bool,
    bytes: &mut Vec<u8>,
    data: &QueryResultData,
) -> Result<bool, AppError> {
    if data.logs.is_empty() && data.transactions.is_empty() && data.blocks.is_empty() {
        return Ok(false);
    }

    if put_comma_outer {
        bytes.push(b',');
    }

    let data = hex_encode_data(data).context("hex encode the data")?;

    bytes.push(b'{');

    let mut put_comma = false;
    if !data.logs.is_empty() {
        put_comma = true;

        bytes.extend_from_slice(br#""logs":"#);
        let json_rows =
            record_batches_to_json_rows(&data.logs).context("serialize arrow into json")?;
        bytes.extend_from_slice(&json_rows);
    }

    if !data.transactions.is_empty() {
        if put_comma {
            bytes.push(b',');
        }
        put_comma = true;

        bytes.extend_from_slice(br#""transactions":"#);
        let json_rows =
            record_batches_to_json_rows(&data.transactions).context("serialize arrow into json")?;
        bytes.extend_from_slice(&json_rows);
    }

    if !data.blocks.is_empty() {
        if put_comma {
            bytes.push(b',');
        }

        bytes.extend_from_slice(br#""blocks":"#);
        let json_rows =
            record_batches_to_json_rows(&data.blocks).context("serialize arrow into json")?;
        bytes.extend_from_slice(&json_rows);
    }

    bytes.push(b'}');

    Ok(true)
}

fn record_batches_to_json_rows(batches: &[ArrowBatch]) -> anyhow::Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(b"[]".to_vec());
    }

    let schema = Schema::clone(&batches[0].schema);

    let chunks = batches.iter().map(|b| b.chunk.clone()).collect::<Vec<_>>();
    let chunk = concat_chunks(&chunks).context("concat chunks")?;

    let serializer = RecordSerializer::new(schema, &chunk, Vec::new());

    let mut out = Vec::new();
    arrow2::io::json::write::write(&mut out, serializer).context("write to json")?;

    Ok(out)
}

// Make our own error that wraps `anyhow::Error`.
struct AppError(anyhow::Error);

// Tell axum how to convert `AppError` into a response.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {:?}", self.0),
        )
            .into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

fn hex_encode_data(res: &QueryResultData) -> anyhow::Result<QueryResultData> {
    let encode_batches = |batches: &[ArrowBatch]| {
        batches
            .iter()
            .map(hex_encode_batch)
            .collect::<anyhow::Result<Vec<_>>>()
    };

    let logs = encode_batches(&res.logs)?;
    let transactions = encode_batches(&res.transactions)?;
    let blocks = encode_batches(&res.blocks)?;

    Ok(QueryResultData {
        logs,
        transactions,
        blocks,
    })
}

fn hex_encode_batch(batch: &ArrowBatch) -> anyhow::Result<ArrowBatch> {
    let mut fields = Vec::new();
    let mut cols = Vec::new();

    for (idx, field) in batch.schema.fields.iter().enumerate() {
        let col = batch.chunk.columns().get(idx).context("get column")?;
        let col = match col.data_type() {
            DataType::Binary => Box::new(hex_encode(col.as_any().downcast_ref().unwrap())),
            _ => col.clone(),
        };

        let field = field.clone();
        fields.push(Field::new(
            field.name,
            col.data_type().clone(),
            field.is_nullable,
        ));
        cols.push(col);
    }

    Ok(ArrowBatch {
        chunk: ArrowChunk::new(cols).into(),
        schema: Schema::from(fields).into(),
    })
}

fn hex_encode(input: &BinaryArray<i32>) -> Utf8Array<i32> {
    let mut arr = MutableUtf8Array::new();

    for buf in input.iter() {
        arr.push(buf.map(prefix_hex::encode));
    }

    arr.into()
}
