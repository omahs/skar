use std::collections::BTreeSet;

use arrayvec::ArrayVec;
use serde::{Deserialize, Serialize};
use skar_format::{Address, FixedSizeData, LogArgument};

pub type Sighash = FixedSizeData<4>;

pub mod skar_net_types_capnp {
    include!(concat!(env!("OUT_DIR"), "/skar_net_types_capnp.rs"));
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LogSelection {
    #[serde(default)]
    pub address: Vec<Address>,
    #[serde(default)]
    pub topics: ArrayVec<Vec<LogArgument>, 4>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransactionSelection {
    #[serde(default)]
    pub from: Vec<Address>,
    #[serde(default)]
    pub to: Vec<Address>,
    #[serde(default)]
    pub sighash: Vec<Sighash>,
    pub status: Option<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Query {
    pub from_block: u64,
    pub to_block: Option<u64>,
    #[serde(default)]
    pub logs: Vec<LogSelection>,
    #[serde(default)]
    pub transactions: Vec<TransactionSelection>,
    #[serde(default)]
    pub include_all_blocks: bool,
    #[serde(default)]
    pub field_selection: FieldSelection,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct FieldSelection {
    #[serde(default)]
    pub block: BTreeSet<String>,
    #[serde(default)]
    pub transaction: BTreeSet<String>,
    #[serde(default)]
    pub log: BTreeSet<String>,
}
