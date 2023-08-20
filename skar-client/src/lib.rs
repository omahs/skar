use std::{io::Cursor, num::NonZeroU64, sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use arrow2::{array::Array, chunk::Chunk, datatypes::SchemaRef, io::ipc};
use reqwest::Method;
use skar_net_types::{skar_net_types_capnp, Query};
use url::Url;

pub type ArrowChunk = Chunk<Box<dyn Array>>;

pub struct Config {
    pub url: Url,
    pub bearer_token: Option<String>,
    pub http_req_timeout_millis: NonZeroU64,
}

pub struct Client {
    http_client: reqwest::Client,
    cfg: Config,
}

impl Client {
    pub fn new(cfg: Config) -> Self {
        let http_client = reqwest::Client::builder()
            .gzip(true)
            .timeout(Duration::from_millis(cfg.http_req_timeout_millis.get()))
            .build()
            .unwrap();

        Self { http_client, cfg }
    }

    pub async fn send(&self, query: Query) -> Result<QueryResponse> {
        let mut req = self.http_client.request(Method::POST, self.cfg.url.clone());

        if let Some(bearer_token) = &self.cfg.bearer_token {
            req = req.bearer_auth(bearer_token);
        }

        let res = req.json(&query).send().await.context("execute http req")?;

        let status = res.status();
        if !status.is_success() {
            return Err(anyhow!("http response status code {}", status));
        }

        let bytes = res.bytes().await.context("read response body bytes")?;

        let res = parse_query_response(&bytes).context("parse query response")?;

        Ok(res)
    }
}

fn parse_query_response(bytes: &[u8]) -> Result<QueryResponse> {
    let message_reader = capnp::serialize_packed::read_message(bytes, Default::default())
        .context("create message reader")?;

    let query_response = message_reader
        .get_root::<skar_net_types_capnp::query_response::Reader>()
        .context("get root")?;

    let archive_height = match query_response.get_archive_height() {
        -1 => None,
        h => Some(
            h.try_into()
                .context("invalid archive height returned from server")?,
        ),
    };

    let data = query_response.get_data().context("read data")?;

    let blocks = read_chunks(data.get_blocks().context("get data")?).context("parse block data")?;
    let transactions =
        read_chunks(data.get_transactions().context("get data")?).context("parse tx data")?;
    let logs = read_chunks(data.get_logs().context("get data")?).context("parse log data")?;

    Ok(QueryResponse {
        archive_height,
        next_block: query_response.get_next_block(),
        total_execution_time: query_response.get_total_execution_time(),
        data: QueryResponseData {
            blocks,
            transactions,
            logs,
        },
    })
}

fn read_chunks(bytes: &[u8]) -> Result<Vec<ArrowBatch>> {
    let mut reader = Cursor::new(bytes);

    let metadata = ipc::read::read_file_metadata(&mut reader).context("read metadata")?;

    let schema = Arc::new(metadata.schema.clone());

    let reader = ipc::read::FileReader::new(reader, metadata, None, None);

    let chunks = reader
        .map(|chunk| {
            chunk.context("read chunk").map(|chunk| ArrowBatch {
                chunk: Arc::new(chunk),
                schema: schema.clone(),
            })
        })
        .collect::<Result<Vec<ArrowBatch>>>()?;

    Ok(chunks)
}

#[derive(Debug, Clone)]
pub struct QueryResponseData {
    pub blocks: Vec<ArrowBatch>,
    pub transactions: Vec<ArrowBatch>,
    pub logs: Vec<ArrowBatch>,
}

#[derive(Debug, Clone)]
pub struct QueryResponse {
    pub archive_height: Option<u64>,
    pub next_block: u64,
    pub total_execution_time: u64,
    pub data: QueryResponseData,
}

#[derive(Debug, Clone)]
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
