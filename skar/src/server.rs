use std::cmp;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use arc_swap::ArcSwap;
use arrow::json::writer::record_batches_to_json_rows;
use axum::extract::Json as ReqJson;
use axum::extract::State as AxumState;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use tokio::sync::mpsc;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;

use crate::config::HttpServerConfig;
use crate::query::query_mem;
use crate::skar_runner::State;
use crate::types::{Query, QueryResultData};

struct ServerState {
    state: Arc<ArcSwap<State>>,
    cfg: HttpServerConfig,
}

const MEGABYTES: usize = 1024 * 1024;

pub(crate) async fn run(cfg: HttpServerConfig, state: Arc<ArcSwap<State>>) -> anyhow::Result<()> {
    let addr = cfg.addr;
    let state = ServerState { state, cfg };
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
        Some(cmp::max(db_max, mem_max))
    })
}

async fn get_height(
    AxumState(state): AxumState<Arc<ServerState>>,
) -> Result<Json<serde_json::Value>, AppError> {
    let height = get_archive_height(&state.state.load()).await?;

    Ok(Json(serde_json::json!({
        "height": height,
    })))
}

async fn run_query(
    AxumState(state): AxumState<Arc<ServerState>>,
    ReqJson(query): ReqJson<Query>,
) -> Result<Response, AppError> {
    let (tx, mut rx) = mpsc::channel(1);

    let data_state = state.state.load();

    let query_start = Instant::now();

    tokio::spawn({
        let db = data_state.db.clone();
        let query = query.clone();
        async move { db.query(&query, tx).await }
    });

    let height = get_archive_height(&data_state)
        .await?
        .map(|h| h.to_string());

    let mut bytes = br#"{"data":["#.to_vec();

    let mut next_block = 0;

    let mut put_comma = false;
    let mut hit_limit = false;
    while let Some(res) = rx.recv().await {
        if put_comma {
            bytes.push(b',');
        } else {
            put_comma = true;
        }

        let data = res.context("execute parquet query")?;

        extend_bytes_with_data(&mut bytes, &data.data)?;

        next_block = data.next_block;

        if bytes.len() >= state.cfg.response_size_limit_mb * MEGABYTES
            || query_start.elapsed().as_millis() >= state.cfg.response_time_limit_ms.into()
        {
            hit_limit = true;
            break;
        }
    }

    std::mem::drop(rx);

    if !hit_limit
        && next_block >= data_state.in_mem.from_block
        && next_block <= data_state.in_mem.to_block
    {
        let in_mem_res = query_mem(&data_state, &query)
            .await
            .context("query in memory data")?;

        if put_comma {
            bytes.push(b',');
        }

        extend_bytes_with_data(&mut bytes, &in_mem_res)?;

        next_block = data_state.in_mem.to_block;
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

fn extend_bytes_with_data(bytes: &mut Vec<u8>, data: &QueryResultData) -> Result<(), AppError> {
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

    Ok(())
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
