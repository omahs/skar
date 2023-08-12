use std::{
    cmp,
    fs::File,
    io::{BufReader, ErrorKind, Read, Seek, SeekFrom, Write},
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

use crate::open_file_reader::{open_file, open_file_reader};

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
                        "there are no blocks in db but folder_index.from={}",
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

            folder_index.row_group_index_offset = row_group_index_offset + size + 4;
        } else {
            folder_index.row_group_index_offset = 0;
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
        rg_index_f.flush().context("sync file to disk")?;

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
        folder_index_f.flush().context("sync file to disk")?;

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

    pub fn iterate_folder_indices(
        &self,
        block_range: BlockRange,
    ) -> Result<Option<FolderIndexIterator>> {
        let txn = self.env.begin_ro_txn().context("begin read only txn")?;
        let db = txn.open_db(None).context("open default db from txn")?;

        let mut folder_index = match open_file(&self.folder_index_path) {
            Ok(folder_index) => BufReader::new(folder_index),
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return Ok(None);
                } else {
                    return Err(anyhow!("failed to open folder index file: {e}"));
                }
            }
        };

        let row_group_index =
            open_file_reader(&self.row_group_index_path).context("open row group index file")?;

        let mut cursor = txn.cursor(&db).context("open cursor")?;

        let last = cursor
            .last::<[u8; 16], [u8; 4]>()
            .context("get last element from db")?;

        let last = match last {
            Some((key, _)) => block_range_from_key(key).1,
            None => {
                return Ok(None);
            }
        };

        let key = block_range_to_key(BlockRange(block_range.0, 0));

        let offset = if let Some((range, offset)) = cursor
            .set_range::<[u8; 16], [u8; 4]>(&key)
            .context("get start pos")?
        {
            let range = block_range_from_key(range);

            if range.0 <= block_range.0 {
                if block_range.1 <= range.0 {
                    return Ok(None);
                }

                Some(u32::from_be_bytes(offset))
            } else {
                None
            }
        } else {
            None
        };

        let offset = match offset {
            None => match cursor
                .prev::<[u8; 16], [u8; 4]>()
                .context("get start pos")?
            {
                Some((range, offset)) => {
                    let range = block_range_from_key(range);

                    if block_range.0 >= range.1 {
                        return Ok(None);
                    }

                    u32::from_be_bytes(offset)
                }
                None => {
                    return Ok(None);
                }
            },
            Some(offset) => offset,
        };

        folder_index
            .seek(SeekFrom::Start(offset as u64))
            .context("seek to the start of the folder index")?;

        let to_block = cmp::min(block_range.1, last);

        Ok(Some(FolderIndexIterator {
            finished: false,
            to_block,
            folder_index,
            row_group_index,
        }))
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

#[cfg(test)]
mod tests {
    use super::*;
    use sbbf_rs_safe::Filter;
    use std::env::temp_dir;

    #[test]
    fn test_iter() {
        let mut tmp = temp_dir();
        tmp.push(format!("{}", uuid::Uuid::new_v4()));

        let folder_index_path = tmp.clone();

        tmp.pop();
        tmp.push(format!("{}", uuid::Uuid::new_v4()));

        let row_group_index_path = tmp.clone();

        tmp.pop();
        tmp.push(format!("{}", uuid::Uuid::new_v4()));

        let db_path = tmp;

        let db = Db {
            folder_index_path,
            row_group_index_path,
            env: Environment::new().open(&db_path).unwrap(),
        };

        let err_res = db.insert_folder_index_impl(
            FolderIndex {
                block_range: BlockRange(1, 123456),
                address_filter: BloomFilter(Filter::new(8, 10000)),
                row_group_index_offset: 0,
            },
            RowGroupIndex {
                block: Vec::new(),
                transaction: Vec::new(),
                log: Vec::new(),
            },
        );

        assert!(err_res.is_err());

        db.insert_folder_index_impl(
            FolderIndex {
                block_range: BlockRange(0, 123456),
                address_filter: BloomFilter(Filter::new(8, 10000)),
                row_group_index_offset: 0,
            },
            RowGroupIndex {
                block: Vec::new(),
                transaction: Vec::new(),
                log: Vec::new(),
            },
        )
        .unwrap();

        let indices = db
            .iterate_folder_indices(BlockRange(0, 123))
            .unwrap()
            .unwrap()
            .map(|a| a.unwrap().block_range)
            .collect::<Vec<_>>();
        assert_eq!(indices, vec![BlockRange(0, 123456)]);

        let indices = db
            .iterate_folder_indices(BlockRange(123456, 99999999999))
            .unwrap();
        assert!(indices.is_none());

        let indices = db
            .iterate_folder_indices(BlockRange(123455, 99999999999))
            .unwrap()
            .unwrap()
            .map(|a| a.unwrap().block_range)
            .collect::<Vec<_>>();
        assert_eq!(indices, vec![BlockRange(0, 123456)]);

        let indices = db
            .iterate_folder_indices(BlockRange(1, 123))
            .unwrap()
            .unwrap()
            .map(|a| a.unwrap().block_range)
            .collect::<Vec<_>>();
        assert_eq!(indices, vec![BlockRange(0, 123456)]);

        let indices = db.iterate_folder_indices(BlockRange(0, 0)).unwrap();
        assert!(indices.is_none());

        db.insert_folder_index_impl(
            FolderIndex {
                block_range: BlockRange(123456, 1234567),
                address_filter: BloomFilter(Filter::new(8, 10000)),
                row_group_index_offset: 0,
            },
            RowGroupIndex {
                block: Vec::new(),
                transaction: Vec::new(),
                log: Vec::new(),
            },
        )
        .unwrap();

        let indices = db
            .iterate_folder_indices(BlockRange(0, 123))
            .unwrap()
            .unwrap()
            .map(|a| a.unwrap().block_range)
            .collect::<Vec<_>>();
        assert_eq!(indices, vec![BlockRange(0, 123456)]);

        let indices = db
            .iterate_folder_indices(BlockRange(123456, 99999999999))
            .unwrap()
            .unwrap()
            .map(|a| a.unwrap().block_range)
            .collect::<Vec<_>>();
        assert_eq!(indices, vec![BlockRange(123456, 1234567)]);

        let indices = db
            .iterate_folder_indices(BlockRange(123455, 99999999999))
            .unwrap()
            .unwrap()
            .map(|a| a.unwrap().block_range)
            .collect::<Vec<_>>();
        assert_eq!(
            indices,
            vec![BlockRange(0, 123456), BlockRange(123456, 1234567)]
        );

        let indices = db
            .iterate_folder_indices(BlockRange(1, 123))
            .unwrap()
            .unwrap()
            .map(|a| a.unwrap().block_range)
            .collect::<Vec<_>>();
        assert_eq!(indices, vec![BlockRange(0, 123456)]);

        let indices = db.iterate_folder_indices(BlockRange(0, 0)).unwrap();
        assert!(indices.is_none());

        let folder_indices = db
            .iterate_folder_indices(BlockRange(0, u64::MAX))
            .unwrap()
            .unwrap()
            .map(|i| i.unwrap())
            .collect::<Vec<_>>();

        let rg_index = db
            .iterate_folder_indices(BlockRange(0, u64::MAX))
            .unwrap()
            .unwrap()
            .read_row_group_index(0)
            .unwrap();
        assert_eq!(rg_index.block.len(), 0);
        assert_eq!(rg_index.transaction.len(), 0);
        assert_eq!(rg_index.log.len(), 0);

        let len_rg_index_first = bincode::serialize(&rg_index).unwrap();

        assert_eq!(folder_indices.len(), 2);
        assert_eq!(folder_indices[0].row_group_index_offset, 0);
        assert_eq!(
            folder_indices[1].row_group_index_offset as usize,
            len_rg_index_first.len() + 4
        );
    }
}
