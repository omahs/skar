use std::cmp;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use arrow::array::BinaryArray;
use arrow::array::StringArray;
use arrow::array::StringBuilder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::json::writer::record_batches_to_json_rows;
use arrow::record_batch::RecordBatch;
use axum::extract::Json as ReqJson;
use axum::extract::State as AxumState;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use tokio::sync::mpsc;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;

use crate::config::HttpServerConfig;
use crate::query::QueryHandlerPool;
use crate::skar_runner::State;
use crate::types::{Query, QueryResultData};

struct ServerState {
    cfg: HttpServerConfig,
    handler_pool: Arc<QueryHandlerPool>,
}

const MEGABYTES: usize = 1024 * 1024;

pub(crate) async fn run(
    cfg: HttpServerConfig,
    handler_pool: Arc<QueryHandlerPool>,
) -> anyhow::Result<()> {
    let addr = cfg.addr;
    let state = ServerState { cfg, handler_pool };
    let state = Arc::new(state);

    let app = axum::Router::new()
        .route(
            "/height",
            axum::routing::get(get_height).with_state(state.clone()),
        )
        .route("/query", axum::routing::post(run_query).with_state(state))
        .layer(ServiceBuilder::new().layer(CompressionLayer::new()));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .context("run http server")
}

async fn get_archive_height(state: &State) -> Result<Option<u64>, AppError> {
    let db_max = state
        .db
        .get_next_block_num()
        .await
        .context("get next block num from db")
        .map_err(AppError::from)?;
    let mem_max = state.in_mem.to_block;

    Ok(if db_max == 0 && mem_max == 0 {
        None
    } else {
        Some(cmp::max(db_max, mem_max) - 1)
    })
}

async fn get_height(
    AxumState(state): AxumState<Arc<ServerState>>,
) -> Result<Json<serde_json::Value>, AppError> {
    let height = get_archive_height(&state.handler_pool.state.load()).await?;

    Ok(Json(serde_json::json!({
        "height": height,
    })))
}

async fn run_query(
    AxumState(state): AxumState<Arc<ServerState>>,
    ReqJson(query): ReqJson<Query>,
) -> Result<Response, AppError> {
    let (tx, mut rx) = mpsc::channel(1);

    let query_start = Instant::now();

    state
        .handler_pool
        .handle(query, tx)
        .await
        .context("start running query")?;

    let height = get_archive_height(&state.handler_pool.state.load())
        .await?
        .map(|h| h.to_string());

    let mut bytes = br#"{"data":["#.to_vec();

    let mut next_block = 0;

    let mut put_comma = false;
    while let Some(res) = rx.recv().await {
        let query_result = res.context("execute parquet query")?;

        for batch in query_result.data {
            if put_comma {
                bytes.push(b',');
            }

            put_comma = extend_bytes_with_data(&mut bytes, &batch)?;
        }

        next_block = query_result.next_block;

        if bytes.len() >= state.cfg.response_size_limit_mb * MEGABYTES {
            break;
        }
    }

    write!(
        &mut bytes,
        r#"],"archiveHeight":{},"nextBlock":{},"totalTime":{}}}"#,
        height.as_deref().unwrap_or("null"),
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
fn extend_bytes_with_data(bytes: &mut Vec<u8>, data: &QueryResultData) -> Result<bool, AppError> {
    if data.logs.is_empty() && data.transactions.is_empty() && data.blocks.is_empty() {
        return Ok(false);
    }

    let data = hex_encode_data(data).context("hex encode the data")?;

    bytes.push(b'{');

    let mut put_comma = false;
    if !data.logs.is_empty() {
        put_comma = true;

        bytes.extend_from_slice(br#""logs":"#);
        let json_rows =
            record_batches_to_json_rows(data.logs.iter().collect::<Vec<_>>().as_slice())
                .context("serialize arrow into json")?;
        bytes.extend_from_slice(&serde_json::to_vec(&json_rows).unwrap());
    }

    if !data.transactions.is_empty() {
        if put_comma {
            bytes.push(b',');
        }
        put_comma = true;

        bytes.extend_from_slice(br#""transactions":"#);
        let json_rows =
            record_batches_to_json_rows(data.transactions.iter().collect::<Vec<_>>().as_slice())
                .context("serialize arrow into json")?;
        bytes.extend_from_slice(&serde_json::to_vec(&json_rows).unwrap());
    }

    if !data.blocks.is_empty() {
        if put_comma {
            bytes.push(b',');
        }

        bytes.extend_from_slice(br#""blocks":"#);
        let json_rows =
            record_batches_to_json_rows(data.blocks.iter().collect::<Vec<_>>().as_slice())
                .context("serialize arrow into json")?;
        bytes.extend_from_slice(&serde_json::to_vec(&json_rows).unwrap());
    }

    bytes.push(b'}');

    Ok(true)
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
    let encode_batches = |batches: &[RecordBatch]| {
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

fn hex_encode_batch(batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
    let mut fields = Vec::new();
    let mut cols = Vec::new();

    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(idx);
        let col = match col.data_type() {
            DataType::Binary => Arc::new(hex_encode(col.as_any().downcast_ref().unwrap())),
            _ => col.clone(),
        };

        let field = field.clone();
        fields.push(Field::new(
            field.name(),
            col.data_type().clone(),
            field.is_nullable(),
        ));
        cols.push(col);
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).context("build record batch")
}

fn hex_encode(input: &BinaryArray) -> StringArray {
    let mut arr = StringBuilder::new();

    for buf in input.iter() {
        arr.append_option(buf.map(hex::encode));
    }

    arr.finish()
}
