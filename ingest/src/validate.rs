use std::collections::{BTreeMap, BTreeSet};

use crate::BatchData;
use anyhow::{anyhow, Result};

use ethbloom::{Bloom, Input};

pub fn validate_batch_data(data: &BatchData) -> Result<()> {
    let mut receipts_map = BTreeMap::new();

    for receipt in data.receipts.iter().flat_map(|r| r.iter()) {
        receipts_map.insert((*receipt.block_number, *receipt.transaction_index), receipt);
    }

    for block in data.blocks.iter() {
        let mut logs_bloom = Bloom::default();

        for tx in block.transactions.iter() {
            let receipt = receipts_map
                .remove(&(*tx.block_number, *tx.transaction_index))
                .ok_or_else(|| {
                    anyhow!(
                        "tx {} doesn't have a receipt in block {}",
                        *tx.transaction_index,
                        *tx.block_number
                    )
                })?;

            for log in receipt.logs.iter() {
                logs_bloom.accrue(Input::Raw(log.address.as_slice()));
                for topic in log.topics.iter() {
                    logs_bloom.accrue(Input::Raw(topic.as_slice()));
                }
            }
        }

        if logs_bloom.as_fixed_bytes() != &**block.header.logs_bloom {
            return Err(anyhow!(
                "logs_bloom for block {} doesn't match",
                *block.header.number
            ));
        }
    }

    if !receipts_map.is_empty() {
        return Err(anyhow!(
            "some receipts don't have corresponding transactions"
        ));
    }

    let blk_nums = data
        .blocks
        .iter()
        .map(|b| *b.header.number)
        .collect::<BTreeSet<_>>();

    let mut current = data.from_block;
    for i in blk_nums {
        if current != i {
            return Err(anyhow!("block {} is missing", current));
        }

        current += 1;
    }

    if current != data.to_block {
        return Err(anyhow!("block {} is missing", data.to_block - 1));
    }

    Ok(())
}
