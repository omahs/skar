use std::{borrow::Cow, mem, path::Path};

use anyhow::{anyhow, Context, Result};
use reth_libmdbx::{Environment, EnvironmentFlags, Geometry, Mode, PageSize, SyncMode, WriteMap};

pub struct Db {
    env: Environment<WriteMap>,
}

impl Db {
    pub fn new(path: &Path) -> Result<Self> {
        let mut env = Environment::new();

        env.set_geometry(Geometry {
            // Maximum database size of 32 gigabytes
            size: Some(0..(32 * GIGABYTE)),
            // We grow the database in increments of 4 gigabytes
            growth_step: Some(4 * GIGABYTE as isize),
            // The database never shrinks
            shrink_threshold: None,
            page_size: Some(PageSize::Set(default_page_size())),
        });
        env.set_flags(EnvironmentFlags {
            mode: Mode::ReadWrite {
                sync_mode: SyncMode::Durable,
            },
            // We disable readahead because it improves performance for linear scans, but
            // worsens it for random access (which is our access pattern outside of sync)
            no_rdahead: false,
            coalesce: true,
            ..Default::default()
        });
        env.set_max_readers(DEFAULT_MAX_READERS);

        let env = env.open(path).context("open mdbx database")?;

        Ok(Self { env })
    }

    pub fn insert_folder_record(
        &self,
        from_block: u64,
        to_block: u64,
        filter: &[u8],
    ) -> Result<()> {
        let txn = self
            .env
            .begin_rw_txn()
            .context("begin rw txn to insert folder")?;
        let db = txn.open_db(None).context("open default db from txn")?;

        let mut cursor = txn.cursor(&db).context("open cursor")?;

        let last = cursor
            .last::<[u8; 8], Cow<'_, _>>()
            .context("get last element from db")?;

        let last = match last {
            Some((key, _)) => u64::from_be_bytes(key),
            None => 0,
        };

        if from_block != last {
            return Err(anyhow!("from_block and last block in db don't match"));
        }

        mem::drop(cursor);

        txn.put(db.dbi(), to_block.to_be_bytes(), filter, Default::default())
            .context("insert folder record into db")?;

        txn.commit().context("commit db txn")?;

        Ok(())
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
