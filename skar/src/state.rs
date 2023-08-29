use std::sync::Arc;

use arc_swap::ArcSwap;
use arrow2::array::{Array, BinaryArray, UInt64Array};
use arrow2::chunk::Chunk;

use crate::db::Db;

pub type ArrowChunk = Chunk<Box<dyn Array>>;

pub struct State {
    pub in_mem: ArcSwap<InMemory>,
    pub db: Arc<Db>,
}

#[derive(Clone)]
pub struct InMemory {
    pub blocks: InMemoryTable,
    pub transactions: InMemoryTable,
    pub logs: InMemoryTable,
    pub from_block: u64,
    pub to_block: u64,
}

impl InMemory {
    /// Cuts this data from the end using given offset.
    ///
    /// Returns the data in range [..data.len()-offset).
    /// Leaves this object containing the data in range (data.len()-offset..)
    fn split_off(&mut self, offset: usize) -> Self {
        let blocks = self.blocks.split_off(offset);
        let transactions = self.transactions.split_off(offset);
        let logs = self.logs.split_off(offset);

        self.to_block -= u64::try_from(blocks.num_rows).unwrap();

        Self {
            to_block: self.to_block + u64::try_from(blocks.num_rows).unwrap(),
            blocks,
            transactions,
            logs,
            from_block: self.to_block,
        }
    }

    /// Split the number off given blocks from the tip.
    ///
    /// Might split off more blocks than the given number.
    ///
    /// Returns the data for blocks in range approximately [start..len-num_blocks).
    /// Keeps data in range approximately (len-num_blocks..)
    pub fn split_off_num_blocks(&mut self, num_blocks: u64) -> Self {
        let mut total_num_blocks = 0;
        let mut num_chunks = 0;
        for data in self.blocks.data.iter().rev() {
            total_num_blocks += data.len();
            num_chunks += 1;
            if u64::try_from(total_num_blocks).unwrap() >= num_blocks {
                break;
            }
        }
        self.split_off(self.blocks.data.len().checked_sub(num_chunks).unwrap())
    }

    /// Iterates block hashes starting from the tip, intended to be used for rollback
    pub fn iter_block_hashes(&self) -> impl Iterator<Item = (&u64, &[u8])> {
        self.blocks.data.iter().flat_map(|chunk| {
            let number = chunk.columns()[0]
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let hash = chunk.columns()[1]
                .as_any()
                .downcast_ref::<BinaryArray<i32>>()
                .unwrap();

            number.values_iter().rev().zip(hash.values_iter().rev())
        })
    }
}

#[derive(Default, Clone)]
pub struct InMemoryTable {
    pub data: Vec<Arc<ArrowChunk>>,
    pub num_rows: usize,
}

impl InMemoryTable {
    pub fn extend(&mut self, chunk: Arc<ArrowChunk>) {
        self.num_rows += chunk.len();
        self.data.push(chunk);
    }

    /// Cuts the chunks in this table using the offset from the end of chunk vector
    pub fn split_off(&mut self, index: usize) -> Self {
        log::debug!("splitting InMemTable with index: {}", index);

        let data = self.data.split_off(index);
        let num_rows = data.iter().fold(0, |acc, chunk| acc + chunk.len());

        self.num_rows -= num_rows;

        log::debug!("num rows: {}", num_rows);

        Self { num_rows, data }
    }
}
