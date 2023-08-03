use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use anyhow::{Context, Result};
use datafusion::prelude::SessionContext;
use skar_format::Address;

use crate::{
    config::QueryConfig,
    db::Db,
    types::{LogSelection, Query, QueryResult, TransactionSelection},
};

use sbbf_rs_safe::Filter as SbbfFilter;

use wyhash::wyhash;

use super::{data_provider::ParquetDataProvider, execution::execute_query};

pub struct Handler {
    ctx: SessionContext,
    db: Arc<Db>,
    cfg: Arc<QueryConfig>,
    parquet_path: PathBuf,
    pub handle: tokio::runtime::Handle,
}

impl Handler {
    pub fn new(cfg: Arc<QueryConfig>, db: Arc<Db>, parquet_path: &Path) -> Self {
        Self {
            ctx: SessionContext::new(),
            db,
            cfg,
            parquet_path: parquet_path.to_owned(),
            handle: tokio::runtime::Handle::current(),
        }
    }

    pub fn handle<'handler, 'query>(
        &'handler self,
        query: &'query Query,
    ) -> QueryResultIterator<'handler, 'query> {
        QueryResultIterator {
            next_block: query.from_block,
            start_time: Instant::now(),
            handler: self,
            query,
        }
    }
}

pub struct QueryResultIterator<'handler, 'query> {
    next_block: u64,
    start_time: Instant,
    handler: &'handler Handler,
    query: &'query Query,
}

impl<'handler, 'query> Iterator for QueryResultIterator<'handler, 'query> {
    type Item = Result<QueryResult>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start_time.elapsed().as_millis() >= self.handler.cfg.time_limit_ms.into() {
            return None;
        }

        let batch = match self
            .handler
            .db
            .get_next_batch(self.next_block, self.handler.cfg.parquet_folder_batch_size)
        {
            Ok(batch) => batch,
            Err(e) => {
                return Some(Err(
                    e.context("failed to read next index batch from database")
                ))
            }
        };

        if batch.is_empty() {
            return None;
        }

        let mut query_result_data = Vec::new();

        for (block_range, filter) in batch {
            if let Some(to_block) = self.query.to_block {
                if block_range.0 >= to_block {
                    break;
                }
            }

            self.next_block = block_range.1;
            if let Some(to_block) = self.query.to_block {
                self.next_block = std::cmp::min(to_block, self.next_block);
            }

            let query = prune_query(self.query, filter);
            if query.logs.is_empty() && query.transactions.is_empty() && !query.include_all_blocks {
                continue;
            }

            let mut path = self.handler.parquet_path.clone();
            path.push(format!("{}-{}", block_range.0, block_range.1));

            let res = self
                .handler
                .handle
                .block_on(execute_query(
                    &ParquetDataProvider {
                        ctx: &self.handler.ctx,
                        path: &path,
                    },
                    &query,
                ))
                .context("execute query");

            match res {
                Ok(res) => query_result_data.push(res),
                Err(e) => return Some(Err(e)),
            }
        }

        Some(Ok(QueryResult {
            next_block: self.next_block,
            data: query_result_data,
        }))
    }
}

fn prune_query(query: &Query, filter: SbbfFilter) -> Query {
    let prune_addrs = |addrs: Vec<Address>| -> Option<Vec<Address>> {
        if !addrs.is_empty() {
            let out = addrs
                .into_iter()
                .filter(|addr| filter.contains_hash(wyhash(addr.as_slice(), 0)))
                .collect::<Vec<_>>();

            if out.is_empty() {
                None
            } else {
                Some(out)
            }
        } else {
            Some(Default::default())
        }
    };

    Query {
        logs: query
            .logs
            .iter()
            .cloned()
            .filter_map(|selection| {
                let address = prune_addrs(selection.address)?;
                Some(LogSelection {
                    address,
                    ..selection
                })
            })
            .collect(),
        transactions: query
            .transactions
            .iter()
            .cloned()
            .filter_map(|selection| {
                let from = prune_addrs(selection.from)?;
                let to = prune_addrs(selection.to)?;
                Some(TransactionSelection {
                    from,
                    to,
                    ..selection
                })
            })
            .collect(),
        from_block: query.from_block,
        to_block: query.to_block,
        field_selection: query.field_selection.clone(),
        include_all_blocks: query.include_all_blocks,
    }
}
