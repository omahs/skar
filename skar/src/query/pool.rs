use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use arc_swap::ArcSwap;
use datafusion::prelude::SessionContext;
use threadpool::ThreadPool;
use tokio::sync::mpsc;

use crate::skar_runner::{InMemory, State};
use crate::{
    config::QueryConfig,
    types::{Query, QueryResult},
};

use super::data_provider::InMemDataProvider;
use super::execution::execute_query;
use super::handler::Handler;

pub struct QueryHandlerPool {
    threadpool: Mutex<ThreadPool>,
    current_running: Arc<Mutex<usize>>,
    cfg: Arc<QueryConfig>,
    parquet_path: PathBuf,
    pub(crate) state: Arc<ArcSwap<State>>,
}

impl QueryHandlerPool {
    pub(crate) fn new(cfg: QueryConfig, parquet_path: &Path, state: Arc<ArcSwap<State>>) -> Self {
        Self {
            threadpool: Mutex::new(ThreadPool::new(cfg.max_concurrent_queries)),
            current_running: Arc::new(Mutex::new(0)),
            cfg: Arc::new(cfg),
            parquet_path: parquet_path.into(),
            state,
        }
    }

    pub async fn handle(&self, query: Query, tx: mpsc::Sender<Result<QueryResult>>) -> Result<()> {
        let state = self.state.load();
        if query.from_block >= state.in_mem.from_block {
            if query.from_block < state.in_mem.to_block {
                run_in_mem_query(&state.in_mem, query, tx).await;
                return Ok(());
            } else {
                return Ok(());
            }
        }

        {
            let mut current_running = self.current_running.lock().unwrap();
            if *current_running >= self.cfg.max_concurrent_queries {
                return Err(anyhow!("maximum number of concurrent queries reached"));
            } else {
                *current_running += 1;
            }
        }

        let handler = Handler::new(self.cfg.clone(), state.db.clone(), &self.parquet_path);

        std::mem::drop(state);

        let current_running = self.current_running.clone();
        let state = self.state.clone();
        self.threadpool.lock().unwrap().execute(move || {
            let mut next_block = query.from_block;
            for res in handler.handle(&query) {
                let is_err = res.is_err();
                if let Ok(r) = res.as_ref() {
                    next_block = r.next_block;
                }
                if tx.is_closed() || tx.blocking_send(res).is_err() || is_err {
                    break;
                }
            }

            let s = state.load();
            if next_block >= s.in_mem.from_block && next_block < s.in_mem.to_block {
                handler.handle.spawn(async move {
                    let s = state.load();
                    run_in_mem_query(&s.in_mem, query, tx).await;
                    let mut lock = current_running.lock().unwrap();
                    *lock = lock.saturating_sub(1);
                });
            } else {
                let mut lock = current_running.lock().unwrap();
                *lock = lock.saturating_sub(1);
            }
        });

        Ok(())
    }
}

async fn run_in_mem_query(in_mem: &InMemory, query: Query, tx: mpsc::Sender<Result<QueryResult>>) {
    let mut next_block = in_mem.to_block;
    if let Some(to_block) = query.to_block {
        next_block = std::cmp::min(to_block, next_block);
    }

    let res = execute_query(
        &InMemDataProvider {
            ctx: &SessionContext::new(),
            in_mem,
        },
        &query,
    )
    .await
    .map(|data| QueryResult {
        data: vec![data],
        next_block,
    });

    tx.send(res).await.ok();
}
