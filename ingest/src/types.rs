use skar_format::{Block, Transaction, TransactionReceipt};

pub struct BatchData {
    pub blocks: Vec<Block<Transaction>>,
    pub receipts: Vec<TransactionReceipt>,
}
