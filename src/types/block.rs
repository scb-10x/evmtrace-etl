use std::fmt::Display;

use alloy_primitives::{Address, B256, B64};
use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub number: u64,
    pub timestamp: u64,
    pub hash: B256,
    pub parent_hash: B256,
    pub transaction_count: u32,
    pub nonce: B64,
    pub miner: Address,
    pub difficulty: u64,
    pub total_difficulty: f64,
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
