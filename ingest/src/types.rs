use skar_format::{Block, Transaction, TransactionReceipt};

pub struct BatchData {
    pub blocks: Vec<Block<Transaction>>,
    pub receipts: Vec<Vec<TransactionReceipt>>,
    pub from_block: u64,
    pub to_block: u64,
}
