use std::{
    cmp,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use anyhow::{Context, Result};
use sbbf_rs_safe::Filter as SbbfFilter;
use skar_format::Address;
use tokio::sync::mpsc;
use wyhash::wyhash;

use crate::{
    config::QueryConfig,
    db::{BlockRange, FolderIndex, RowGroupIndex},
    state::State,
    types::{LogSelection, Query, QueryResult, QueryResultData, TransactionSelection},
};

use super::{
    data_provider::{InMemDataProvider, ParquetDataProvider},
    execution::execute_query,
};

pub struct Handler {
    state: Arc<State>,
    cfg: QueryConfig,
    parquet_path: PathBuf,
}

impl Handler {
    pub fn new(cfg: QueryConfig, state: Arc<State>, parquet_path: &Path) -> Self {
        Self {
            state,
            cfg,
            parquet_path: parquet_path.to_owned(),
        }
    }

    pub async fn archive_height(&self) -> Result<Option<u64>> {
        let to_block = self.state.in_mem.load().to_block;
        if to_block > 0 {
            return Ok(Some(to_block - 1));
        }

        let next_block_num = self
            .state
            .db
            .next_block_num()
            .await
            .context("get next block num from db")?;

        if next_block_num > 0 {
            Ok(Some(next_block_num - 1))
        } else {
            Ok(None)
        }
    }

    pub fn handle(self: Arc<Self>, query: Query) -> Result<mpsc::Receiver<Result<QueryResult>>> {
        let (tx, rx) = mpsc::channel(1);

        let folder_index_iterator = self
            .state
            .db
            .iterate_folder_indices(BlockRange(
                query.from_block,
                query.to_block.unwrap_or(u64::MAX),
            ))
            .context("start folder index iterator")?;

        let (task_tx, task_rx) = std::sync::mpsc::channel();

        let runner = QueryRunner {
            rx: task_rx,
            tx: tx.clone(),
            handler: self.clone(),
            query: query.clone(),
        };

        tokio::task::spawn_blocking(move || runner.run());

        if let Some(mut folder_index_iterator) = folder_index_iterator {
            tokio::task::spawn_blocking(move || {
                while let Some(folder_index) = folder_index_iterator.next() {
                    let folder_index = match folder_index {
                        Ok(fi) => fi,
                        Err(e) => {
                            task_tx.send(Err(e)).ok();
                            return;
                        }
                    };

                    let pruned_query = prune_query(&query, &folder_index.address_filter.0);

                    if pruned_query.logs.is_empty()
                        && pruned_query.transactions.is_empty()
                        && !pruned_query.include_all_blocks
                    {
                        task_tx
                            .send(Ok(Task::EmptyResult(QueryResult {
                                data: QueryResultData::default(),
                                next_block: calculate_next_block(
                                    folder_index.block_range.1,
                                    query.to_block,
                                ),
                            })))
                            .ok();
                    } else {
                        let rg_index = match folder_index_iterator
                            .read_row_group_index(folder_index.row_group_index_offset)
                        {
                            Ok(rg_index) => rg_index,
                            Err(e) => {
                                task_tx.send(Err(e)).ok();
                                return;
                            }
                        };

                        if task_tx
                            .send(Ok(Task::QueryFolder(folder_index, rg_index, pruned_query)))
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            });
        }

        Ok(rx)
    }
}

enum Task {
    QueryFolder(FolderIndex, RowGroupIndex, Query),
    EmptyResult(QueryResult),
}

struct QueryRunner {
    rx: std::sync::mpsc::Receiver<Result<Task>>,
    tx: mpsc::Sender<Result<QueryResult>>,
    handler: Arc<Handler>,
    query: Query,
}

impl QueryRunner {
    fn run(self) -> Result<()> {
        let start_time = Instant::now();

        while let Ok(task) = self.rx.recv() {
            if start_time.elapsed().as_millis() >= self.handler.cfg.time_limit_ms as u128 {
                return Ok(());
            }

            let task = match task {
                Ok(task) => task,
                Err(e) => {
                    self.tx.blocking_send(Err(e)).ok();
                    break;
                }
            };

            let res = match task {
                Task::QueryFolder(folder_index, rg_index, pruned_query) => {
                    let mut path = self.handler.parquet_path.clone();
                    path.push(format!(
                        "{}-{}",
                        folder_index.block_range.0, folder_index.block_range.1
                    ));

                    let data_provider = ParquetDataProvider { path, rg_index };

                    let next_block =
                        calculate_next_block(folder_index.block_range.1, pruned_query.to_block);

                    execute_query(&data_provider, &pruned_query)
                        .map(|data| QueryResult { data, next_block })
                }
                Task::EmptyResult(res) => Ok(res),
            };

            let is_err = res.is_err();

            if self.tx.blocking_send(res).is_err() || is_err {
                return Ok(());
            }
        }

        let in_mem = self.handler.state.in_mem.load();

        if let Some(to_block) = self.query.to_block {
            if to_block <= in_mem.from_block {
                return Ok(());
            }
        }

        if self.query.from_block >= in_mem.to_block {
            return Ok(());
        }

        let data_provider = InMemDataProvider { in_mem: &in_mem };

        let query_res = execute_query(&data_provider, &self.query).map(|data| QueryResult {
            data,
            next_block: calculate_next_block(in_mem.to_block, self.query.to_block),
        });

        self.tx.blocking_send(query_res).ok();

        Ok(())
    }
}

fn prune_query(query: &Query, filter: &SbbfFilter) -> Query {
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

fn calculate_next_block(mut to_block: u64, query_limit: Option<u64>) -> u64 {
    if let Some(limit) = query_limit {
        to_block = cmp::min(limit, to_block);
    }

    to_block
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prune_query_empty_filter() {
        let query = Query {
            from_block: 0,
            to_block: None,
            field_selection: Default::default(),
            include_all_blocks: false,
            transactions: vec![TransactionSelection {
                from: vec![
                    hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                        .try_into()
                        .unwrap(),
                ],
                to: vec![],
                sighash: vec![],
                status: None,
            }],
            logs: vec![LogSelection {
                address: vec![
                    hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                        .try_into()
                        .unwrap(),
                ],
                topics: Default::default(),
            }],
        };

        let filter = SbbfFilter::new(15, 31);

        let pruned_query = prune_query(&query, &filter);

        assert_eq!(pruned_query.from_block, 0);
        assert_eq!(pruned_query.to_block, None);
        assert!(!pruned_query.include_all_blocks);
        assert_eq!(pruned_query.transactions.len(), 0);
        assert_eq!(pruned_query.logs.len(), 0);

        let query = Query {
            from_block: 0,
            to_block: None,
            field_selection: Default::default(),
            include_all_blocks: false,
            transactions: vec![TransactionSelection {
                from: vec![],
                to: vec![],
                sighash: vec![],
                status: None,
            }],
            logs: vec![LogSelection {
                address: vec![],
                topics: Default::default(),
            }],
        };

        let pruned_query = prune_query(&query, &filter);

        assert_eq!(pruned_query.transactions.len(), 1);
        assert_eq!(pruned_query.logs.len(), 1);

        let query = Query {
            from_block: 0,
            to_block: None,
            field_selection: Default::default(),
            include_all_blocks: false,
            transactions: vec![TransactionSelection {
                from: vec![],
                to: vec![],
                sighash: vec![],
                status: None,
            }],
            logs: vec![LogSelection {
                address: vec![
                    hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                        .try_into()
                        .unwrap(),
                ],
                topics: Default::default(),
            }],
        };

        let pruned_query = prune_query(&query, &filter);

        assert_eq!(pruned_query.transactions.len(), 1);
        assert_eq!(pruned_query.logs.len(), 0);
    }

    #[test]
    fn test_prune_query() {
        let mut filter = SbbfFilter::new(100, 1000);
        filter.insert_hash(wyhash(
            &hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502"),
            0,
        ));
        let filter = filter;

        let query = Query {
            from_block: 0,
            to_block: None,
            field_selection: Default::default(),
            include_all_blocks: false,
            transactions: vec![
                TransactionSelection {
                    from: vec![
                        hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                            .try_into()
                            .unwrap(),
                    ],
                    to: vec![],
                    sighash: vec![],
                    status: None,
                },
                TransactionSelection {
                    from: vec![
                        hex_literal::hex!("12bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                            .try_into()
                            .unwrap(),
                    ],
                    to: vec![],
                    sighash: vec![],
                    status: None,
                },
            ],
            logs: vec![
                LogSelection {
                    address: vec![
                        hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                            .try_into()
                            .unwrap(),
                    ],
                    topics: Default::default(),
                },
                LogSelection {
                    address: vec![
                        hex_literal::hex!("12bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                            .try_into()
                            .unwrap(),
                    ],
                    topics: Default::default(),
                },
                LogSelection {
                    address: vec![],
                    topics: Default::default(),
                },
            ],
        };

        let pruned_query = prune_query(&query, &filter);

        assert_eq!(pruned_query.from_block, 0);
        assert_eq!(pruned_query.to_block, None);
        assert!(!pruned_query.include_all_blocks);
        assert_eq!(pruned_query.transactions.len(), 1);
        assert_eq!(pruned_query.logs.len(), 2);
    }
}
