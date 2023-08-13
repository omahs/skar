use super::bloom_filter::BloomFilter;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BlockRange(pub u64, pub u64);

#[derive(Debug, Serialize, Deserialize)]
pub struct FolderIndex {
    pub block_range: BlockRange,
    pub address_filter: BloomFilter,
    pub row_group_index_offset: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RowGroupIndex {
    pub block: Vec<BlockRowGroupIndex>,
    pub transaction: Vec<TransactionRowGroupIndex>,
    pub log: Vec<LogRowGroupIndex>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockRowGroupIndex {
    pub min_block_num: u64,
    pub max_block_num: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionRowGroupIndex {
    pub min_block_num: u64,
    pub max_block_num: u64,
    pub from_address_filter: BloomFilter,
    pub to_address_filter: BloomFilter,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogRowGroupIndex {
    pub min_block_num: u64,
    pub max_block_num: u64,
    pub address_filter: BloomFilter,
    pub topic_filters: [BloomFilter; 4],
}
