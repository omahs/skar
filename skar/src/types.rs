pub use skar_net_types::{FieldSelection, LogSelection, Query, TransactionSelection};

use crate::hashmap::FastSet;
use crate::query::ArrowBatch;

#[derive(Default)]
pub struct QueryResult {
    pub data: QueryResultData,
    pub next_block: u64,
}

#[derive(Default)]
pub struct QueryResultData {
    pub logs: Vec<ArrowBatch>,
    pub transactions: Vec<ArrowBatch>,
    pub blocks: Vec<ArrowBatch>,
}

pub struct QueryContext {
    pub query: Query,
    // these "set"s are used for joining transactions and blocks
    pub transaction_set: FastSet<(u64, u64)>,
    pub block_set: FastSet<u64>,
}
