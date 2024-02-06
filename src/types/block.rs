use std::fmt::Display;

use ethers::types::{Address, Block as EtherBlock, H256, H64};
use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub number: u64,
    pub timestamp: u64,
    pub hash: H256,
    pub parent_hash: H256,
    pub transaction_count: u32,
    pub nonce: H64,
    pub miner: Address,
    pub difficulty: u64,
    //pub total_difficulty: f64,
    pub total_difficulty: u64,
    pub size: u32,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub base_fee_per_gas: u64,
}

impl Display for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            to_string_pretty(self).expect("Failed to serialize trace")
        )
    }
}

impl AsRef<Block> for Block {
    fn as_ref(&self) -> &Block {
        self
    }
}

impl Block {
    pub fn from_ethers<T>(block: EtherBlock<T>) -> Option<Block> {
        match block {
            EtherBlock {
                hash: Some(hash),
                parent_hash,
                author,
                number: Some(number),
                gas_used,
                gas_limit,
                timestamp,
                difficulty,
                total_difficulty,
                transactions,
                size,
                nonce,
                base_fee_per_gas,
                ..
            } => Some(Block {
                number: number.as_u64(),
                timestamp: timestamp.as_u64(),
                hash,
                parent_hash,
                transaction_count: transactions.len() as u32,
                nonce: nonce.unwrap_or_default(),
                miner: author.unwrap_or_default(),
                difficulty: difficulty.as_u64(),
                total_difficulty: total_difficulty.unwrap_or_default().as_u64(),
                size: size.unwrap_or_default().as_u32(),
                gas_limit: gas_limit.as_u64(),
                gas_used: gas_used.as_u64(),
                base_fee_per_gas: base_fee_per_gas.unwrap_or_default().as_u64(),
            }),
            _ => None,
        }
    }
}
