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
use skar_net_types::FieldSelection;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;

use crate::config::HttpServerConfig;
use crate::query::ArrowBatch;
use crate::query::Handler;
use crate::schema;
use crate::state::ArrowChunk;
use crate::types::QueryResult;
use crate::types::{Query, QueryResultData};
use crate::write_parquet::concat_chunks;

struct ServerState {
    cfg: HttpServerConfig,
    handler: Arc<Handler>,
}

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

struct IpcResponseBuilder {
    blocks: Vec<ArrowBatch>,
    transactions: Vec<ArrowBatch>,
    logs: Vec<ArrowBatch>,
    next_block: u64,
    counts: ResponseEntityCounts,
    query_start: Instant,
}

impl Default for IpcResponseBuilder {
    fn default() -> Self {
        Self {
            blocks: Default::default(),
            transactions: Default::default(),
            logs: Default::default(),
            next_block: 0,
            counts: Default::default(),
            query_start: Instant::now(),
        }
    }
}

impl IpcResponseBuilder {
    fn add_result(&mut self, res: &QueryResult) {
        self.counts.add_counts(&res.data);
        self.next_block = res.next_block;

        self.blocks.extend_from_slice(&res.data.blocks);
        self.transactions.extend_from_slice(&res.data.transactions);
        self.logs.extend_from_slice(&res.data.logs);
    }

    fn build(
        self,
        field_selection: &FieldSelection,
        height: Option<u64>,
    ) -> anyhow::Result<Vec<u8>> {
        let blocks_ipc = Self::build_ipc_file(
            &schema::block_header(),
            &field_selection.block,
            &self.blocks,
        )
        .context("write blocks to arrow ipc")?;
        let transactions_ipc = Self::build_ipc_file(
            &schema::transaction(),
            &field_selection.transaction,
            &self.transactions,
        )
        .context("write transactions to arrow ipc")?;
        let logs_ipc = Self::build_ipc_file(&schema::log(), &field_selection.log, &self.logs)
            .context("write logs to arrow ipc")?;
        self.build_capnp_message(height, blocks_ipc, transactions_ipc, logs_ipc)
            .context("write capnp message")
    }

    fn build_ipc_file(
        schema: &Schema,
        field_selection: &BTreeSet<String>,
        data: &[ArrowBatch],
    ) -> anyhow::Result<Vec<u8>> {
        let write_opts = ipc::write::WriteOptions {
            compression: Some(ipc::write::Compression::ZSTD),
        };

        let mut writer = ipc::write::FileWriter::new(
            Cursor::new(Vec::with_capacity(512)),
            project_schema(schema, field_selection).context("project schema")?,
            None,
            write_opts,
        );
        writer.start().context("start writer")?;

        if !data.is_empty() {
            let chunks = data
                .iter()
                .map(|batch| batch.chunk.clone())
                .collect::<Vec<_>>();

            let chunk = concat_chunks(&chunks).context("concatenate arrow chunks")?;

            writer
                .write(&chunk, None)
                .context("write arrow batch to ipc")?;
        }

        writer.finish().context("finish logs")?;

        Ok(writer.into_inner().into_inner())
    }

    fn build_capnp_message(
        &self,
        height: Option<u64>,
        blocks: Vec<u8>,
        transactions: Vec<u8>,
        logs: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        let mut query_response =
            message.init_root::<skar_net_types_capnp::query_response::Builder>();

        let height: Option<i64> = height.map(|h| h.try_into().unwrap());
        query_response.set_archive_height(height.unwrap_or(-1));
        query_response
            .set_total_execution_time(self.query_start.elapsed().as_millis().try_into().unwrap());
        query_response.set_next_block(self.next_block);

        let mut data = query_response.init_data();
        data.set_blocks(&blocks);
        data.set_transactions(&transactions);
        data.set_logs(&logs);

        let mut resp_body = Vec::with_capacity(512);
        capnp::serialize_packed::write_message(&mut resp_body, &message)
            .context("write packed capnp message")?;

        Ok(resp_body)
    }
}

async fn run_ipc_query(
    AxumState(state): AxumState<Arc<ServerState>>,
    ReqJson(query): ReqJson<Query>,
) -> Result<Response, AppError> {
    let field_selection = query.field_selection.clone();

    let mut rx = state
        .handler
        .clone()
        .handle(query)
        .context("start running query")?;

    let mut response_builder = IpcResponseBuilder::default();

    while let Some(res) = rx.recv().await {
        let query_result = res.context("execute parquet query")?;
        response_builder.add_result(&query_result);

        if response_builder.counts.is_limit_reached(&state.cfg) {
            break;
        }
    }

    let height = state
        .handler
        .archive_height()
        .await
        .context("get archive height")?;
    let resp_body = response_builder
        .build(&field_selection, height)
        .context("build response")?;

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

#[derive(Debug, Clone, Copy, Default)]
struct ResponseEntityCounts {
    blocks: usize,
    transactions: usize,
    logs: usize,
}

impl ResponseEntityCounts {
    fn add_counts(&mut self, data: &QueryResultData) {
        self.blocks += data
            .blocks
            .iter()
            .fold(0, |acc, batch| acc + batch.chunk.len());
        self.transactions += data
            .transactions
            .iter()
            .fold(0, |acc, batch| acc + batch.chunk.len());
        self.logs += data
            .logs
            .iter()
            .fold(0, |acc, batch| acc + batch.chunk.len());
    }

    fn is_limit_reached(&self, cfg: &HttpServerConfig) -> bool {
        self.blocks >= cfg.response_num_blocks_limit
            || self.transactions >= cfg.response_num_transactions_limit
            || self.logs >= cfg.response_num_logs_limit
    }
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
    let mut counts = ResponseEntityCounts::default();

    let mut put_comma = false;
    while let Some(res) = rx.recv().await {
        let query_result = res.context("execute parquet query")?;

        counts.add_counts(&query_result.data);

        put_comma |= extend_bytes_with_data(put_comma, &mut bytes, &query_result.data)?;

        next_block = query_result.next_block;

        if counts.is_limit_reached(&state.cfg) {
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
