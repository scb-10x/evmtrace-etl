use std::fmt::{Display, Formatter};

use alloy_primitives::{Address, Bytes, B256};
use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    pub transaction_index: Option<u32>,
    pub from_address: Option<Address>,
    pub to_address: Option<Address>,
    pub value: Option<u128>,
    pub input: Option<Bytes>,
    pub output: Option<Bytes>,
    pub trace_type: Option<String>,
    pub call_type: Option<String>,
    pub reward_type: Option<String>,
    pub gas: Option<u64>,
    pub gas_used: Option<u64>,
    pub subtraces: u64,
    pub trace_address: Vec<u32>,
    pub error: Option<String>,
    pub status: u64,
    pub transaction_hash: Option<B256>,
    pub block_number: u64,
    pub block_timestamp: Option<u64>,
    pub block_hash: Option<B256>,
    //#[serde(rename = "type")]
    //pub ty: String,
    //pub trace_id: String,
    //pub trace_index: u64,
    //pub item_id: String,
    //pub item_timestamp: Option<String>,
}

impl Display for Trace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            to_string_pretty(self).expect("Failed to serialize trace")
        )
    }
}
