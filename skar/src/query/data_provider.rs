use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context, Result};
use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::SchemaRef,
    io::parquet::{self, read::ArrayIter},
};
use rayon::prelude::*;
use wyhash::wyhash;

use crate::{
    db::{BlockRowGroupIndex, LogRowGroupIndex, RowGroupIndex, TransactionRowGroupIndex},
    open_file_reader::open_file_reader,
    schema,
    state::{ArrowChunk, InMemory},
    types::QueryContext,
};

type Data = Vec<ArrowBatch>;

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

pub trait DataProvider: Send + Sync {
    fn load_logs(&self, ctx: &QueryContext) -> Result<Data>;
    fn load_transactions(&self, ctx: &QueryContext) -> Result<Data>;
    fn load_blocks(&self, ctx: &QueryContext) -> Result<Data>;
}

pub struct InMemDataProvider<'in_mem> {
    pub in_mem: &'in_mem InMemory,
}

impl<'in_mem> DataProvider for InMemDataProvider<'in_mem> {
    fn load_logs(&self, _ctx: &QueryContext) -> Result<Data> {
        let schema_ref = schema::log();

        Ok(self
            .in_mem
            .logs
            .data
            .iter()
            .map(|chunk| ArrowBatch {
                chunk: chunk.clone(),
                schema: schema_ref.clone(),
            })
            .collect())
    }

    fn load_transactions(&self, _ctx: &QueryContext) -> Result<Data> {
        let schema_ref = schema::transaction();

        Ok(self
            .in_mem
            .transactions
            .data
            .iter()
            .map(|chunk| ArrowBatch {
                chunk: chunk.clone(),
                schema: schema_ref.clone(),
            })
            .collect())
    }

    fn load_blocks(&self, _ctx: &QueryContext) -> Result<Data> {
        let schema_ref = schema::block_header();

        Ok(self
            .in_mem
            .blocks
            .data
            .iter()
            .map(|chunk| ArrowBatch {
                chunk: chunk.clone(),
                schema: schema_ref.clone(),
            })
            .collect())
    }
}

pub struct ParquetDataProvider {
    pub path: PathBuf,
    pub rg_index: RowGroupIndex,
}

fn deserialize_parallel(iters: &mut [ArrayIter<'static>]) -> Result<Chunk<Box<dyn Array>>> {
    let arrays = iters
        .par_iter_mut()
        .map(|iter| iter.next().transpose().context("decode col"))
        .collect::<Result<Vec<_>>>()?;

    Chunk::try_new(arrays.into_iter().map(|x| x.unwrap()).collect()).context("build arrow chunk")
}

impl ParquetDataProvider {
    fn load_table(
        &self,
        field_selection: &BTreeSet<String>,
        row_groups: &[usize],
        table_name: &str,
    ) -> Result<Data> {
        let mut path = self.path.clone();
        path.push(format!("{table_name}.parquet"));

        let mut reader = open_file_reader(&path).context("open parquet file")?;

        let metadata =
            parquet::read::read_metadata(&mut reader).context("read parquet metadata")?;
        let schema = parquet::read::infer_schema(&metadata).context("infer parquet schema")?;

        let schema = schema.filter(|_index, field| field_selection.contains(&field.name));

        let row_groups = metadata
            .row_groups
            .into_iter()
            .enumerate()
            .filter(|(index, _)| row_groups.contains(index))
            .map(|(_, row_group)| row_group)
            .collect::<Vec<_>>();

        let mut chunks = Vec::new();

        for rg in row_groups {
            let chunk_size = usize::MAX;

            let mut columns = parquet::read::read_columns_many(
                &mut reader,
                &rg,
                schema.fields.clone(),
                Some(chunk_size),
                None,
                None,
            )
            .context("read columns")?;
            let mut num_rows = rg.num_rows();
            while num_rows > 0 {
                num_rows = num_rows.saturating_sub(chunk_size);
                let chunk = deserialize_parallel(&mut columns).context("deserialize chunk")?;
                chunks.push(Arc::new(chunk));
            }
        }

        let schema_ref = Arc::new(schema);

        Ok(chunks
            .into_iter()
            .map(|chunk| ArrowBatch {
                chunk,
                schema: schema_ref.clone(),
            })
            .collect())
    }
}

impl DataProvider for ParquetDataProvider {
    fn load_logs(&self, ctx: &QueryContext) -> Result<Data> {
        let row_groups = self
            .rg_index
            .log
            .iter()
            .enumerate()
            .filter_map(|(i, rg_index)| {
                if can_skip_log_row_group(ctx, rg_index) {
                    None
                } else {
                    Some(i)
                }
            })
            .collect::<Vec<_>>();

        let mut field_selection = ctx.query.field_selection.log.clone();
        field_selection.extend(LOG_QUERY_FIELDS.iter().map(|s| s.to_string()));

        self.load_table(&field_selection, &row_groups, "logs")
    }

    fn load_transactions(&self, ctx: &QueryContext) -> Result<Data> {
        let row_groups = self
            .rg_index
            .transaction
            .iter()
            .enumerate()
            .filter_map(|(i, rg_index)| {
                if can_skip_tx_row_group(ctx, rg_index) {
                    None
                } else {
                    Some(i)
                }
            })
            .collect::<Vec<_>>();

        let mut field_selection = ctx.query.field_selection.transaction.clone();
        field_selection.extend(TX_QUERY_FIELDS.iter().map(|s| s.to_string()));

        self.load_table(&field_selection, &row_groups, "transactions")
    }

    fn load_blocks(&self, ctx: &QueryContext) -> Result<Data> {
        let row_groups = self
            .rg_index
            .block
            .iter()
            .enumerate()
            .filter_map(|(i, rg_index)| {
                if can_skip_block_row_group(ctx, rg_index) {
                    None
                } else {
                    Some(i)
                }
            })
            .collect::<Vec<_>>();

        let mut field_selection = ctx.query.field_selection.block.clone();
        field_selection.extend(BLOCK_QUERY_FIELDS.iter().map(|s| s.to_string()));

        self.load_table(&field_selection, &row_groups, "blocks")
    }
}

fn can_skip_block_row_group(ctx: &QueryContext, rg_index: &BlockRowGroupIndex) -> bool {
    let from_block = ctx.query.from_block;
    let to_block = ctx.query.to_block;

    if from_block > rg_index.max_block_num {
        return true;
    }

    if let Some(to_block) = to_block {
        if to_block <= rg_index.min_block_num {
            return true;
        }
    }

    !ctx.query.include_all_blocks
        && !ctx.block_set.iter().any(|&block_num| {
            block_num >= rg_index.min_block_num && block_num <= rg_index.max_block_num
        })
}

fn can_skip_tx_row_group(ctx: &QueryContext, rg_index: &TransactionRowGroupIndex) -> bool {
    let from_block = ctx.query.from_block;
    let to_block = ctx.query.to_block;

    if from_block > rg_index.max_block_num {
        return true;
    }

    if let Some(to_block) = to_block {
        if to_block <= rg_index.min_block_num {
            return true;
        }
    }

    !ctx.query.transactions.iter().any(|tx| {
        let contains_from = tx.from.is_empty()
            || tx.from.iter().any(|addr| {
                let hash = wyhash(addr.as_slice(), 0);
                rg_index.from_address_filter.0.contains_hash(hash)
            });
        let contains_to = tx.to.is_empty()
            || tx.to.iter().any(|addr| {
                let hash = wyhash(addr.as_slice(), 0);
                rg_index.to_address_filter.0.contains_hash(hash)
            });
        contains_from && contains_to
    }) && !ctx.transaction_set.iter().any(|&(block_num, _)| {
        block_num >= rg_index.min_block_num && block_num <= rg_index.max_block_num
    })
}

fn can_skip_log_row_group(ctx: &QueryContext, rg_index: &LogRowGroupIndex) -> bool {
    let from_block = ctx.query.from_block;
    let to_block = ctx.query.to_block;

    if from_block > rg_index.max_block_num {
        return true;
    }

    if let Some(to_block) = to_block {
        if to_block <= rg_index.min_block_num {
            return true;
        }
    }

    !ctx.query.logs.iter().any(|log| {
        let addr = log.address.is_empty()
            || log.address.iter().any(|addr| {
                let hash = wyhash(addr.as_slice(), 0);
                rg_index.address_filter.0.contains_hash(hash)
            });

        let topics = log.topics.is_empty()
            || log
                .topics
                .iter()
                .zip(rg_index.topic_filters.iter())
                .all(|(topic, filter)| {
                    topic.is_empty()
                        || topic.iter().any(|t| {
                            let hash = wyhash(t.as_slice(), 0);
                            filter.0.contains_hash(hash)
                        })
                });

        addr && topics
    })
}

const BLOCK_QUERY_FIELDS: &[&str] = &["number"];
const TX_QUERY_FIELDS: &[&str] = &[
    "block_number",
    "transaction_index",
    "from",
    "to",
    "sighash",
    "status",
];
const LOG_QUERY_FIELDS: &[&str] = &[
    "block_number",
    "transaction_index",
    "address",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
];

#[cfg(test)]
mod tests {
    use arrayvec::ArrayVec;
    use sbbf_rs_safe::Filter;

    use crate::{
        db::BloomFilter,
        types::{FieldSelection, LogSelection, Query, TransactionSelection},
    };

    use super::*;

    #[test]
    fn test_skip_block_row_group() {
        let can_skip = |from_block: u64,
                        to_block: Option<u64>,
                        min_block_num: u64,
                        max_block_num: u64|
         -> bool {
            can_skip_block_row_group(
                &QueryContext {
                    query: Query {
                        logs: Vec::new(),
                        transactions: Vec::new(),
                        include_all_blocks: true,
                        field_selection: FieldSelection::default(),
                        from_block,
                        to_block,
                    },
                    transaction_set: BTreeSet::new(),
                    block_set: BTreeSet::new(),
                },
                &BlockRowGroupIndex {
                    min_block_num,
                    max_block_num,
                },
            )
        };

        assert!(!can_skip(0, None, 1, 1));
        assert!(!can_skip(1, None, 1, 1));
        assert!(can_skip(1, Some(2), 2, 2));
        assert!(can_skip(5, None, 2, 2));
        assert!(!can_skip(2, None, 2, 2));
        assert!(can_skip(3, None, 2, 2));
    }

    #[test]
    fn test_skip_block_row_group_with_set() {
        let can_skip = |from_block: u64,
                        to_block: Option<u64>,
                        min_block_num: u64,
                        max_block_num: u64,
                        block_set: Vec<u64>|
         -> bool {
            can_skip_block_row_group(
                &QueryContext {
                    query: Query {
                        logs: Vec::new(),
                        transactions: Vec::new(),
                        include_all_blocks: false,
                        field_selection: FieldSelection::default(),
                        from_block,
                        to_block,
                    },
                    transaction_set: BTreeSet::new(),
                    block_set: block_set.into_iter().collect(),
                },
                &BlockRowGroupIndex {
                    min_block_num,
                    max_block_num,
                },
            )
        };

        assert!(!can_skip(0, None, 1, 1, vec![1, 15]));
        assert!(can_skip(0, None, 1, 1, vec![2, 15]));
    }

    #[allow(clippy::too_many_arguments)]
    fn can_skip_tx_rg_test(
        from_block: u64,
        to_block: Option<u64>,
        transactions: Vec<TransactionSelection>,
        min_block_num: u64,
        max_block_num: u64,
        from_addrs: Vec<&[u8]>,
        to_addrs: Vec<&[u8]>,
        transaction_set: Vec<(u64, u64)>,
    ) -> bool {
        can_skip_tx_row_group(
            &QueryContext {
                query: Query {
                    logs: Vec::new(),
                    transactions,
                    include_all_blocks: false,
                    field_selection: FieldSelection::default(),
                    from_block,
                    to_block,
                },
                transaction_set: transaction_set.into_iter().collect(),
                block_set: BTreeSet::new(),
            },
            &TransactionRowGroupIndex {
                min_block_num,
                max_block_num,
                from_address_filter: BloomFilter(from_addrs.iter().fold(
                    Filter::new(100, from_addrs.len()),
                    |mut filter, addr| {
                        filter.insert_hash(wyhash(addr, 0));
                        filter
                    },
                )),
                to_address_filter: BloomFilter(to_addrs.iter().fold(
                    Filter::new(100, to_addrs.len()),
                    |mut filter, addr| {
                        filter.insert_hash(wyhash(addr, 0));
                        filter
                    },
                )),
            },
        )
    }

    #[test]
    fn test_skip_tx_row_group() {
        assert!(can_skip_tx_rg_test(
            0,
            Some(10),
            Vec::new(),
            11,
            23,
            vec![],
            vec![],
            Vec::new(),
        ));
        assert!(!can_skip_tx_rg_test(
            0,
            Some(10),
            vec![TransactionSelection {
                from: vec![],
                to: vec![],
                sighash: vec![],
                status: None,
            }],
            9,
            23,
            vec![b"123123"],
            vec![],
            Vec::new(),
        ));
        assert!(can_skip_tx_rg_test(
            0,
            Some(5),
            vec![TransactionSelection {
                from: vec![],
                to: vec![],
                sighash: vec![],
                status: None,
            }],
            9,
            23,
            vec![b"123123"],
            vec![],
            Vec::new(),
        ));
        assert!(!can_skip_tx_rg_test(
            0,
            None,
            vec![TransactionSelection {
                from: vec![],
                to: vec![],
                sighash: vec![],
                status: None,
            }],
            9,
            23,
            vec![b"123123"],
            vec![],
            Vec::new(),
        ));
        assert!(can_skip_tx_rg_test(
            0,
            None,
            vec![TransactionSelection {
                from: vec![],
                to: vec![
                    hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                        .try_into()
                        .unwrap()
                ],
                sighash: vec![],
                status: None,
            }],
            9,
            23,
            vec![&hex_literal::hex!(
                "48bBf1c68037BF35b0eB090f1B5E0fa52F690502"
            )],
            vec![],
            Vec::new(),
        ));
        assert!(!can_skip_tx_rg_test(
            0,
            None,
            vec![TransactionSelection {
                from: vec![
                    hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                        .try_into()
                        .unwrap()
                ],
                to: vec![],
                sighash: vec![],
                status: None,
            }],
            9,
            23,
            vec![&hex_literal::hex!(
                "48bBf1c68037BF35b0eB090f1B5E0fa52F690502"
            )],
            vec![],
            Vec::new(),
        ));
        assert!(!can_skip_tx_rg_test(
            0,
            None,
            vec![TransactionSelection {
                from: vec![],
                to: vec![
                    hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                        .try_into()
                        .unwrap()
                ],
                sighash: vec![],
                status: None,
            }],
            9,
            23,
            vec![],
            vec![&hex_literal::hex!(
                "48bBf1c68037BF35b0eB090f1B5E0fa52F690502"
            )],
            Vec::new(),
        ));
        assert!(!can_skip_tx_rg_test(
            0,
            None,
            vec![TransactionSelection {
                from: vec![],
                to: vec![
                    hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                        .try_into()
                        .unwrap()
                ],
                sighash: vec![],
                status: None,
            }],
            9,
            23,
            vec![],
            vec![],
            vec![(12, 5)],
        ));
        assert!(can_skip_tx_rg_test(
            0,
            None,
            vec![TransactionSelection {
                from: vec![],
                to: vec![
                    hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                        .try_into()
                        .unwrap()
                ],
                sighash: vec![],
                status: None,
            }],
            9,
            23,
            vec![],
            vec![],
            vec![],
        ));
    }

    #[allow(clippy::too_many_arguments)]
    fn can_skip_log_rg_test(
        from_block: u64,
        to_block: Option<u64>,
        logs: Vec<LogSelection>,
        min_block_num: u64,
        max_block_num: u64,
        addrs: Vec<&[u8]>,
        topics: Vec<Vec<&[u8]>>,
    ) -> bool {
        can_skip_log_row_group(
            &QueryContext {
                query: Query {
                    logs,
                    transactions: Vec::new(),
                    include_all_blocks: false,
                    field_selection: FieldSelection::default(),
                    from_block,
                    to_block,
                },
                transaction_set: BTreeSet::new(),
                block_set: BTreeSet::new(),
            },
            &LogRowGroupIndex {
                min_block_num,
                max_block_num,
                address_filter: BloomFilter(addrs.iter().fold(
                    Filter::new(100, addrs.len()),
                    |mut filter, addr| {
                        filter.insert_hash(wyhash(addr, 0));
                        filter
                    },
                )),
                topic_filters: topics
                    .iter()
                    .map(|t| {
                        BloomFilter(t.iter().fold(Filter::new(100, t.len()), |mut filter, t| {
                            filter.insert_hash(wyhash(t, 0));
                            filter
                        }))
                    })
                    .collect::<Vec<_>>()
                    .try_into()
                    .unwrap(),
            },
        )
    }

    #[test]
    fn test_skip_log_row_group() {
        assert!(can_skip_log_rg_test(
            0,
            Some(10),
            Vec::new(),
            11,
            23,
            vec![],
            vec![vec![], vec![], vec![], vec![]],
        ));
        assert!(!can_skip_log_rg_test(
            0,
            Some(15),
            vec![LogSelection {
                address: vec![
                    hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                        .try_into()
                        .unwrap()
                ],
                topics: Default::default(),
            }],
            11,
            23,
            vec![
                &hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502"),
                &hex_literal::hex!("11bBf1c68037BF35b0eB090f1B5E0fa52F690402")
            ],
            vec![vec![], vec![], vec![], vec![]],
        ));
        assert!(can_skip_log_rg_test(
            0,
            Some(3),
            vec![LogSelection {
                address: vec![
                    hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                        .try_into()
                        .unwrap()
                ],
                topics: Default::default(),
            }],
            11,
            23,
            vec![
                &hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502"),
                &hex_literal::hex!("11bBf1c68037BF35b0eB090f1B5E0fa52F690402")
            ],
            vec![vec![], vec![], vec![], vec![]],
        ));
        assert!(!can_skip_log_rg_test(
            0,
            Some(12),
            vec![LogSelection {
                address: vec![],
                topics: Default::default(),
            }],
            11,
            23,
            vec![],
            vec![vec![], vec![], vec![], vec![]],
        ));
        assert!(!can_skip_log_rg_test(
            0,
            Some(12),
            vec![LogSelection {
                address: vec![
                    hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                        .try_into()
                        .unwrap()
                ],
                topics: ArrayVec::try_from(
                    [
                        vec![],
                        vec![],
                        vec![hex_literal::hex!(
                            "00000000000000000000000048bBf1c68037BF35b0eB090f1B5E0fa52F690502"
                        )
                        .try_into()
                        .unwrap()],
                        vec![]
                    ]
                    .as_slice()
                )
                .unwrap(),
            }],
            11,
            23,
            vec![&hex_literal::hex!(
                "48bBf1c68037BF35b0eB090f1B5E0fa52F690502"
            )],
            vec![
                vec![],
                vec![],
                vec![&hex_literal::hex!(
                    "00000000000000000000000048bBf1c68037BF35b0eB090f1B5E0fa52F690502"
                )],
                vec![]
            ],
        ));
        assert!(can_skip_log_rg_test(
            0,
            Some(12),
            vec![LogSelection {
                address: vec![
                    hex_literal::hex!("48bBf1c68037BF35b0eB090f1B5E0fa52F690502")
                        .try_into()
                        .unwrap()
                ],
                topics: ArrayVec::try_from(
                    [
                        vec![],
                        vec![],
                        vec![hex_literal::hex!(
                            "69000000000000000000000018bBf1c68037BF35b0eB090f1B5E0fa52F690502"
                        )
                        .try_into()
                        .unwrap()],
                        vec![]
                    ]
                    .as_slice()
                )
                .unwrap(),
            }],
            11,
            23,
            vec![&hex_literal::hex!(
                "48bBf1c68037BF35b0eB090f1B5E0fa52F690502"
            )],
            vec![
                vec![],
                vec![],
                vec![&hex_literal::hex!(
                    "00000000000000000000000048bBf1c68037BF35b0eB090f1B5E0fa52F690502"
                )],
                vec![]
            ],
        ));
    }
}
