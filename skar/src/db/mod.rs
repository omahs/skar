use std::{
    cmp,
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use reth_libmdbx::{Environment, EnvironmentFlags, Geometry, Mode, NoWriteMap, PageSize, SyncMode};

mod bloom_filter;
mod types;

pub use bloom_filter::BloomFilter;
pub use types::{
    BlockRange, BlockRowGroupIndex, FolderIndex, LogRowGroupIndex, RowGroupIndex,
    TransactionRowGroupIndex,
};

pub struct Db {
    env: Environment<NoWriteMap>,
    folder_index_path: PathBuf,
    row_group_index_path: PathBuf,
}

impl Db {
    pub fn new(path: &Path) -> Result<Self> {
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

        let mut path = path.to_owned();

        path.push("mdbx");
        let env = env.open(&path).context("open mdbx database")?;
        path.pop();

        let mut folder_index_path = path.clone();
        folder_index_path.push("folder_index.bin");

        let mut row_group_index_path = path.clone();
        row_group_index_path.push("row_group_index.bin");

        Ok(Self {
            env,
            folder_index_path,
            row_group_index_path,
        })
    }

    pub async fn next_block_num(&self) -> Result<u64> {
        tokio::task::block_in_place(|| {
            let txn = self.env.begin_ro_txn().context("begin read only txn")?;
            let db = txn.open_db(None).context("open default db from txn")?;

            let mut cursor = txn.cursor(&db).context("open cursor")?;

            let last = cursor
                .last::<[u8; 16], [u8; 4]>()
                .context("get last element from db")?;

            let last = match last {
                Some((key, _)) => block_range_from_key(key).1,
                None => 0,
            };

            Ok(last)
        })
    }

    pub async fn insert_folder_index(
        &self,
        folder_index: FolderIndex,
        rg_index: RowGroupIndex,
    ) -> Result<()> {
        tokio::task::block_in_place(|| self.insert_folder_index_impl(folder_index, rg_index))
    }

    fn insert_folder_index_impl(
        &self,
        folder_index: FolderIndex,
        rg_index: RowGroupIndex,
    ) -> Result<()> {
        let txn = self.env.begin_rw_txn().context("begin read write txn")?;
        let db = txn.open_db(None).context("open default db from txn")?;

        let mut cursor = txn.cursor(&db).context("open cursor")?;

        let last = cursor
            .last::<[u8; 16], [u8; 4]>()
            .context("get last element from db")?;

        let offset = match last {
            Some((last_range, offset)) => {
                let last_range = block_range_from_key(last_range);

                if last_range.1 != folder_index.block_range.0 {
                    return Err(anyhow!(
                        "last_index.to ({}) and folder_index.from ({}) don't match",
                        last_range.1,
                        folder_index.block_range.0
                    ));
                }

                Some(u32::from_be_bytes(offset))
            }
            None => {
                if folder_index.block_range.0 != 0 {
                    return Err(anyhow!(
                        "there are no blocks in db but folder_infex.from={}",
                        folder_index.block_range.0
                    ));
                }

                None
            }
        };

        let mut folder_index_f = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.folder_index_path)
            .context("open folder index file")?;

        let row_group_index_offset = match offset {
            Some(offset) => {
                folder_index_f
                    .seek(SeekFrom::Start(offset.into()))
                    .context("seek to tip offset")?;
                let fidx =
                    read_folder_index(&mut folder_index_f).context("read tip folder index")?;
                Some(fidx.row_group_index_offset)
            }
            None => None,
        };

        let mut rg_index_f = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.row_group_index_path)
            .context("open row group index file")?;

        let mut folder_index = folder_index;

        if let Some(row_group_index_offset) = row_group_index_offset {
            rg_index_f
                .seek(SeekFrom::Start(row_group_index_offset.into()))
                .context("seek to rg tip offset")?;
            let size = read_size(&mut rg_index_f).context("read rg idx size")?;

            folder_index.row_group_index_offset = row_group_index_offset + size;
        }

        rg_index_f
            .seek(SeekFrom::Start(folder_index.row_group_index_offset.into()))
            .context("seek to tip of rg index file")?;

        let rg_index = bincode::serialize(&rg_index).context("serialize rg index")?;
        let size: u32 = rg_index.len().try_into().unwrap();
        rg_index_f
            .write_all(&size.to_be_bytes())
            .context("write size of rg index")?;
        rg_index_f.write_all(&rg_index).context("write rg index")?;
        rg_index_f.sync_all().context("sync file to disk")?;

        let block_range = folder_index.block_range;
        let folder_index = bincode::serialize(&folder_index).context("serialize folder index")?;
        let size: u32 = folder_index.len().try_into().unwrap();

        let offset = folder_index_f
            .stream_position()
            .context("get folder idx offset")?;
        let offset: u32 = offset.try_into().unwrap();

        folder_index_f
            .write_all(&size.to_be_bytes())
            .context("write size of index")?;
        folder_index_f
            .write_all(&folder_index)
            .context("write folder index")?;
        folder_index_f.sync_all().context("sync file to disk")?;

        txn.put(
            db.dbi(),
            block_range_to_key(block_range),
            offset.to_be_bytes(),
            Default::default(),
        )
        .context("write folder idx to mdb")?;

        txn.commit().context("commit txn")?;

        Ok(())
    }

    pub fn iterate_folder_indices(&self, block_range: BlockRange) -> Result<FolderIndexIterator> {
        let mut folder_index =
            BufReader::new(File::open(&self.folder_index_path).context("open folder index file")?);
        let row_group_index = BufReader::new(
            File::open(&self.row_group_index_path).context("open row group index file")?,
        );

        let txn = self.env.begin_ro_txn().context("begin read only txn")?;
        let db = txn.open_db(None).context("open default db from txn")?;

        let mut cursor = txn.cursor(&db).context("open cursor")?;

        let last = cursor
            .last::<[u8; 16], [u8; 4]>()
            .context("get last element from db")?;

        let last = match last {
            Some((key, _)) => block_range_from_key(key).1,
            None => {
                return Ok(FolderIndexIterator {
                    finished: true,
                    to_block: 0,
                    folder_index,
                    row_group_index,
                })
            }
        };

        let key = block_range_to_key(BlockRange(block_range.0, 0));

        match cursor
            .set_range::<[u8; 16], [u8; 4]>(&key)
            .context("get start pos")?
        {
            Some((_, offset)) => {
                let offset = u32::from_be_bytes(offset);
                folder_index
                    .seek(SeekFrom::Start(offset as u64))
                    .context("seek to the start of the folder index")?;
            }
            None => {
                return Ok(FolderIndexIterator {
                    finished: true,
                    to_block: 0,
                    folder_index,
                    row_group_index,
                })
            }
        }

        let to_block = cmp::min(block_range.1, last);

        Ok(FolderIndexIterator {
            finished: false,
            to_block,
            folder_index,
            row_group_index,
        })
    }
}

fn read_size<R: Read>(reader: &mut R) -> Result<u32> {
    let mut size = [0u8; 4];
    reader.read_exact(&mut size).context("read size")?;
    Ok(u32::from_be_bytes(size))
}

fn read_folder_index<R: Read>(reader: &mut R) -> Result<FolderIndex> {
    let size = read_size(reader).context("read size of folder index")?;
    let mut buf = vec![0u8; size.try_into().unwrap()];
    reader.read_exact(&mut buf).context("read folder index")?;
    bincode::deserialize(&buf).context("deserialize folder index")
}

pub struct FolderIndexIterator {
    finished: bool,
    to_block: u64,
    folder_index: BufReader<File>,
    row_group_index: BufReader<File>,
}

impl FolderIndexIterator {
    pub fn read_row_group_index(&mut self, offset: u32) -> Result<RowGroupIndex> {
        self.row_group_index
            .seek(SeekFrom::Start(offset.into()))
            .context("seek to offset")?;
        let size = read_size(&mut self.row_group_index).context("read size of row group index")?;

        let mut buf = vec![0u8; size.try_into().unwrap()];
        self.row_group_index
            .read_exact(&mut buf)
            .context("read folder index")?;

        bincode::deserialize(&buf).context("deserialize row group index")
    }
}

impl Iterator for FolderIndexIterator {
    type Item = Result<FolderIndex>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let folder_index = match read_folder_index(&mut self.folder_index) {
            Ok(folder_index) => folder_index,
            Err(e) => return Some(Err(e)),
        };
        if folder_index.block_range.1 >= self.to_block {
            self.finished = true;
        }

        Some(Ok(folder_index))
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

fn block_range_to_key(block_range: BlockRange) -> [u8; 16] {
    let mut buf = [0; 16];

    buf[..8].copy_from_slice(block_range.0.to_be_bytes().as_slice());
    buf[8..].copy_from_slice(block_range.1.to_be_bytes().as_slice());

    buf
}

fn block_range_from_key(key: [u8; 16]) -> BlockRange {
    BlockRange(
        u64::from_be_bytes(key[..8].try_into().unwrap()),
        u64::from_be_bytes(key[8..].try_into().unwrap()),
    )
}
