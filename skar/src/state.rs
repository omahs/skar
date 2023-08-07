use std::sync::Arc;

use arc_swap::ArcSwap;
use arrow2::array::Array;
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

impl Default for InMemory {
    fn default() -> Self {
        Self {
            from_block: u64::MAX,
            to_block: 0,
            blocks: Default::default(),
            transactions: Default::default(),
            logs: Default::default(),
        }
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
}
