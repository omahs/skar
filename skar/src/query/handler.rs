use std::{
    cmp,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use anyhow::{Context, Result};
use arrayvec::ArrayVec;
use sbbf_rs_safe::Filter as SbbfFilter;
use skar_format::{Address, LogArgument};
use tokio::sync::mpsc;
use wyhash::wyhash;

use crate::{
    config::QueryConfig,
    db::{BlockRange, FolderIndexIterator},
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
        let handler = self.clone();
        let (tx, rx) = mpsc::channel(1);

        let folder_index_iterator = self
            .state
            .db
            .iterate_folder_indices(BlockRange(
                query.from_block,
                query.to_block.unwrap_or(u64::MAX),
            ))
            .context("start folder index iterator")?;

        tokio::task::spawn_blocking(move || {
            let iter = QueryResultIterator {
                finished: false,
                start_time: Instant::now(),
                handler,
                query,
                folder_index_iterator,
            };

            for res in iter {
                let is_err = res.is_err();
                if tx.blocking_send(res).is_err() {
                    break;
                }
                if is_err {
                    break;
                }
            }
        });

        Ok(rx)
    }
}

pub struct QueryResultIterator {
    finished: bool,
    start_time: Instant,
    handler: Arc<Handler>,
    query: Query,
    folder_index_iterator: Option<FolderIndexIterator>,
}

impl Iterator for QueryResultIterator {
    type Item = Result<QueryResult>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if self.start_time.elapsed().as_millis() >= self.handler.cfg.time_limit_ms as u128 {
            self.finished = true;
            return None;
        }

        let folder_index = match self.folder_index_iterator.as_mut().and_then(|i| i.next()) {
            Some(folder_index) => folder_index,
            None => {
                self.finished = true;

                let in_mem = self.handler.state.in_mem.load();

                if let Some(to_block) = self.query.to_block {
                    if to_block <= in_mem.from_block {
                        return None;
                    }
                }

                if self.query.from_block >= in_mem.to_block {
                    return None;
                }

                let data_provider = InMemDataProvider { in_mem: &in_mem };

                let query_res = execute_query(&data_provider, &self.query)
                    .map(|data| QueryResult {
                        data,
                        next_block: next_block(in_mem.to_block, self.query.to_block),
                    })
                    .context("execute in memory query");

                return Some(query_res);
            }
        };

        let folder_index = match folder_index {
            Ok(folder_index) => folder_index,
            Err(e) => return Some(Err(e.context("failed to read folder index"))),
        };

        let pruned_query = prune_query(
            &self.query,
            &folder_index.address_filter.0,
            &folder_index.topic_filter.0,
        );

        if pruned_query.logs.is_empty()
            && pruned_query.transactions.is_empty()
            && !pruned_query.include_all_blocks
        {
            return Some(Ok(QueryResult {
                data: QueryResultData::default(),
                next_block: next_block(folder_index.block_range.1, self.query.to_block),
            }));
        }

        let rg_index = match self
            .folder_index_iterator
            .as_mut()
            .unwrap()
            .read_row_group_index(folder_index.row_group_index_offset)
        {
            Ok(rg_index) => rg_index,
            Err(e) => return Some(Err(e.context("read row group index"))),
        };

        let mut path = self.handler.parquet_path.clone();
        path.push(format!(
            "{}-{}",
            folder_index.block_range.0, folder_index.block_range.1
        ));

        let data_provider = ParquetDataProvider { path, rg_index };

        let query_result = execute_query(&data_provider, &pruned_query).map(|data| QueryResult {
            data,
            next_block: next_block(folder_index.block_range.1, self.query.to_block),
        });

        Some(query_result)
    }
}

fn prune_query(query: &Query, address_filter: &SbbfFilter, topic_filter: &SbbfFilter) -> Query {
    let prune_addrs = |addrs: Vec<Address>| -> Option<Vec<Address>> {
        if !addrs.is_empty() {
            let out = addrs
                .into_iter()
                .filter(|addr| address_filter.contains_hash(wyhash(addr.as_slice(), 0)))
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

    let prune_topics =
        |topics: ArrayVec<Vec<LogArgument>, 4>| -> Option<ArrayVec<Vec<LogArgument>, 4>> {
            if topics.iter().all(|topic| {
                topic.is_empty()
                    || topic.iter().any(|t| {
                        let hash = wyhash(t.as_slice(), 0);
                        topic_filter.contains_hash(hash)
                    })
            }) {
                Some(topics)
            } else {
                None
            }
        };

    Query {
        logs: query
            .logs
            .iter()
            .cloned()
            .filter_map(|selection| {
                let address = prune_addrs(selection.address)?;
                let topics = prune_topics(selection.topics)?;
                Some(LogSelection { address, topics })
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

fn next_block(mut to_block: u64, query_limit: Option<u64>) -> u64 {
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

        let pruned_query = prune_query(&query, &filter, &filter);

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

        let pruned_query = prune_query(&query, &filter, &filter);

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

        let pruned_query = prune_query(&query, &filter, &filter);

        assert_eq!(pruned_query.transactions.len(), 1);
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
                topics: ArrayVec::try_from(
                    [vec![hex_literal::hex!(
                        "00000000000000000000000048bBf1c68037BF35b0eB090f1B5E0fa52F690502"
                    )
                    .try_into()
                    .unwrap()]]
                    .as_slice(),
                )
                .unwrap(),
            }],
        };

        let pruned_query = prune_query(&query, &filter, &filter);

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
        filter.insert_hash(wyhash(
            &hex_literal::hex!("00000000000000000000000048bBf1c68037BF35b0eB090f1B5E0fa52F690502"),
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

        let pruned_query = prune_query(&query, &filter, &filter);

        assert_eq!(pruned_query.from_block, 0);
        assert_eq!(pruned_query.to_block, None);
        assert!(!pruned_query.include_all_blocks);
        assert_eq!(pruned_query.transactions.len(), 1);
        assert_eq!(pruned_query.logs.len(), 2);

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
                topics: ArrayVec::try_from(
                    [vec![hex_literal::hex!(
                        "00000000000000000000000048bBf1c68037BF35b0eB090f1B5E0fa52F690502"
                    )
                    .try_into()
                    .unwrap()]]
                    .as_slice(),
                )
                .unwrap(),
            }],
        };

        let pruned_query = prune_query(&query, &filter, &filter);

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
                topics: ArrayVec::try_from(
                    [vec![hex_literal::hex!(
                        "00000000000000000000000058bBf1c68037BF35b0eB090f1B5E0fa52F690502"
                    )
                    .try_into()
                    .unwrap()]]
                    .as_slice(),
                )
                .unwrap(),
            }],
        };

        let pruned_query = prune_query(&query, &filter, &filter);

        assert_eq!(pruned_query.transactions.len(), 1);
        assert_eq!(pruned_query.logs.len(), 0);
    }
}
