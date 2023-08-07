use std::{cmp, collections::BTreeSet, fs::File, path::Path};

use anyhow::{Context, Result};
use arrow2::{
    array::{BinaryArray, UInt64Array},
    io::parquet,
};
use sbbf_rs_safe::Filter as BFilter;
use wyhash::wyhash;

use crate::{
    db::{
        BlockRange, BlockRowGroupIndex, BloomFilter, FolderIndex, LogRowGroupIndex, RowGroupIndex,
        TransactionRowGroupIndex,
    },
    state::ArrowChunk,
};

pub fn build_parquet_indices(path: &Path) -> Result<(FolderIndex, RowGroupIndex)> {
    let blocks = {
        let mut path = path.to_owned();
        path.push("blocks.parquet");

        load_file(&path).context("load blocks")?
    };
    let transactions = {
        let mut path = path.to_owned();
        path.push("transactions.parquet");

        load_file(&path).context("load transactions")?
    };
    let logs = {
        let mut path = path.to_owned();
        path.push("logs.parquet");

        load_file(&path).context("load logs")?
    };

    let mut folder_addr_set = BTreeSet::new();

    let mut rg_index = RowGroupIndex {
        block: Vec::new(),
        transaction: Vec::new(),
        log: Vec::new(),
    };

    let mut folder_min_block_num = u64::MAX;
    let mut folder_max_block_num = u64::MIN;
    for chunk in blocks {
        let block_num = chunk.columns()[0]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let mut min_block_num = u64::MAX;
        let mut max_block_num = u64::MIN;

        for b in block_num.iter().flatten() {
            min_block_num = cmp::min(min_block_num, *b);
            max_block_num = cmp::max(max_block_num, *b);
        }

        rg_index.block.push(BlockRowGroupIndex {
            min_block_num,
            max_block_num,
        });

        folder_min_block_num = cmp::min(folder_min_block_num, min_block_num);
        folder_max_block_num = cmp::max(folder_max_block_num, max_block_num);
    }

    for chunk in transactions {
        let block_num = chunk.columns()[1]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let mut min_block_num = u64::MAX;
        let mut max_block_num = u64::MIN;

        for b in block_num.iter().flatten() {
            min_block_num = cmp::min(min_block_num, *b);
            max_block_num = cmp::max(max_block_num, *b);
        }

        let from = chunk.columns()[2]
            .as_any()
            .downcast_ref::<BinaryArray<i32>>()
            .unwrap();
        let mut from_addr_set = BTreeSet::new();

        for f in from.iter().flatten() {
            from_addr_set.insert(f);
            folder_addr_set.insert(f.to_vec());
        }

        let mut from_address_filter = BFilter::new(8, from_addr_set.len());
        for addr in from_addr_set.into_iter() {
            from_address_filter.insert_hash(wyhash(addr, 0));
        }

        let to = chunk.columns()[8]
            .as_any()
            .downcast_ref::<BinaryArray<i32>>()
            .unwrap();
        let mut to_addr_set = BTreeSet::new();

        for t in to.iter().flatten() {
            to_addr_set.insert(t);
            folder_addr_set.insert(t.to_vec());
        }

        let mut to_address_filter = BFilter::new(8, to_addr_set.len());
        for addr in to_addr_set.into_iter() {
            to_address_filter.insert_hash(wyhash(addr, 0));
        }

        rg_index.transaction.push(TransactionRowGroupIndex {
            min_block_num,
            max_block_num,
            to_address_filter: BloomFilter(to_address_filter),
            from_address_filter: BloomFilter(from_address_filter),
        });
    }

    for chunk in logs {
        let block_num = chunk.columns()[5]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let mut min_block_num = u64::MAX;
        let mut max_block_num = u64::MIN;

        for b in block_num.iter().flatten() {
            min_block_num = cmp::min(min_block_num, *b);
            max_block_num = cmp::max(max_block_num, *b);
        }

        let address = chunk.columns()[6]
            .as_any()
            .downcast_ref::<BinaryArray<i32>>()
            .unwrap();
        let mut addr_set = BTreeSet::new();

        for addr in address.iter().flatten() {
            addr_set.insert(addr);
            folder_addr_set.insert(addr.to_vec());
        }

        let mut address_filter = BFilter::new(8, addr_set.len());
        for addr in addr_set.into_iter() {
            address_filter.insert_hash(wyhash(addr, 0));
        }

        rg_index.log.push(LogRowGroupIndex {
            min_block_num,
            max_block_num,
            address_filter: BloomFilter(address_filter),
        });
    }

    let mut address_filter = BFilter::new(8, cmp::min(folder_addr_set.len(), 32 * 1024));
    for addr in folder_addr_set.into_iter() {
        address_filter.insert_hash(wyhash(&addr, 0));
    }

    let folder_index = FolderIndex {
        block_range: BlockRange(folder_min_block_num, folder_max_block_num + 1),
        address_filter: BloomFilter(address_filter),
        row_group_index_offset: 0,
    };

    Ok((folder_index, rg_index))
}

fn load_file(path: &Path) -> Result<Vec<ArrowChunk>> {
    let mut reader = File::open(path).context("open parquet file")?;
    let metadata = parquet::read::read_metadata(&mut reader).context("read metadata")?;
    let schema = parquet::read::infer_schema(&metadata).context("infer schema")?;
    let chunks = parquet::read::FileReader::new(
        reader,
        metadata.row_groups,
        schema.clone(),
        None,
        None,
        None,
    );

    chunks
        .into_iter()
        .map(|c| c.context("read arrow chunk"))
        .collect()
}
