use std::{
    borrow::Cow,
    mem,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use reth_libmdbx::{Environment, EnvironmentFlags, Geometry, Mode, PageSize, SyncMode, WriteMap};
use skar_format::Address;
use tokio::sync::mpsc;
use wyhash::wyhash;

use crate::{
    query::query_folder,
    types::{LogSelection, Query, QueryResult, TransactionSelection},
};

pub struct Db {
    env: Environment<WriteMap>,
    parquet_path: PathBuf,
}

impl Db {
    pub fn new(path: &Path, parquet_path: &Path) -> Result<Self> {
        let mut env = Environment::new();

        env.set_geometry(Geometry {
            size: Some(0..(32 * GIGABYTE)),
            growth_step: Some(GIGABYTE as isize),
            // The database never shrinks
            shrink_threshold: None,
            page_size: Some(PageSize::Set(default_page_size())),
        });
        env.set_flags(EnvironmentFlags {
            mode: Mode::ReadWrite {
                sync_mode: SyncMode::Durable,
            },
            no_rdahead: false,
            coalesce: true,
            ..Default::default()
        });
        env.set_max_readers(DEFAULT_MAX_READERS);

        let env = env.open(path).context("open mdbx database")?;

        Ok(Self {
            env,
            parquet_path: parquet_path.to_owned(),
        })
    }

    pub async fn insert_folder_record(
        &self,
        from_block: u64,
        to_block: u64,
        filter: &[u8],
    ) -> Result<()> {
        tokio::task::block_in_place(|| {
            let txn = self
                .env
                .begin_rw_txn()
                .context("begin rw txn to insert folder")?;
            let db = txn.open_db(None).context("open default db from txn")?;

            let mut cursor = txn.cursor(&db).context("open cursor")?;

            let last = cursor
                .last::<[u8; 8], Cow<'_, _>>()
                .context("get last element from db")?;

            if let Some((key, _)) = last {
                let last = u64::from_be_bytes(key);
                if from_block != last {
                    return Err(anyhow!(
                        "from_block({from_block}) and last block in db ({last}) don't match"
                    ));
                }
            }

            mem::drop(cursor);

            txn.put(db.dbi(), to_block.to_be_bytes(), filter, Default::default())
                .context("insert folder record into db")?;

            txn.commit().context("commit db txn")?;

            Ok(())
        })
    }

    pub fn get_next_block_num(&self) -> Result<u64> {
        let txn = self
            .env
            .begin_ro_txn()
            .context("begin read only txn to get next block num")?;
        let db = txn.open_db(None).context("open default db from txn")?;

        let mut cursor = txn.cursor(&db).context("open cursor")?;

        let last = cursor
            .last::<[u8; 8], Cow<'_, _>>()
            .context("get last element from db")?;

        let last = match last {
            Some((key, _)) => u64::from_be_bytes(key),
            None => 0,
        };

        Ok(last)
    }

    #[allow(dead_code)]
    pub async fn query(&self, query: &Query, tx: mpsc::Sender<Result<QueryResult>>) -> Result<()> {
        tokio::task::block_in_place(|| {
            let txn = self
                .env
                .begin_ro_txn()
                .context("begin read only txn to get a batch of indices")?;
            let db = txn.open_db(None).context("open default db from txn")?;

            let mut cursor = txn.cursor(&db).context("open cursor")?;

            let key = query.from_block.to_be_bytes();
            let kv = cursor
                .set_range::<[u8; 8], Cow<'_, _>>(key.as_slice())
                .context("set range of cursor")?;
            match kv {
                Some((k, _)) => {
                    if k != key {
                        cursor
                            .prev::<[u8; 8], Cow<'_, _>>()
                            .context("cursor.prev")?;
                    }
                }
                None => return Ok(()),
            }

            let prev = match cursor.prev::<[u8; 8], Cow<'_, _>>().context("get prev")? {
                Some((k, _)) => u64::from_be_bytes(k),
                None => 0,
            };

            let mut prev = prev;
            for kv in cursor.iter::<[u8; 8], Cow<'_, _>>() {
                let (next, filter) = kv.context("iter db")?;
                let next = u64::from_be_bytes(next);
                let query = prune_query(query.clone(), &filter);

                let mut path = self.parquet_path.clone();
                path.push(format!("{}-{}", prev, next));

                let tx = tx.clone();
                let stop = tokio::runtime::Handle::current().block_on(async move {
                    let res = query_folder(&path, &query).await;
                    tx.send(res).await.is_err()
                });

                if stop {
                    break;
                }

                prev = next;
            }

            Ok(())
        })
    }
}

fn prune_query(query: Query, filter: &[u8]) -> Query {
    let filter = sbbf_rs_safe::Filter::from_bytes(filter).unwrap();
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
            Some(Vec::new())
        }
    };

    Query {
        logs: query
            .logs
            .into_iter()
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
            .into_iter()
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
        ..query
    }
}

pub(crate) fn default_page_size() -> usize {
    let os_page_size = page_size::get();

    // source: https://gitflic.ru/project/erthink/libmdbx/blob?file=mdbx.h#line-num-821
    let libmdbx_max_page_size = 0x10000;

    // May lead to errors if it's reduced further because of the potential size of the
    // data.
    let min_page_size = 4096;

    os_page_size.clamp(min_page_size, libmdbx_max_page_size)
}

/// MDBX allows up to 32767 readers (`MDBX_READERS_LIMIT`), but we limit it to slightly below that
const DEFAULT_MAX_READERS: u64 = 32_000;

const GIGABYTE: usize = 1024 * 1024 * 1024;
