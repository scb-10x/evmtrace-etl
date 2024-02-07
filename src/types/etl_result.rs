use ethers::{
    types::{Address, Bytes, H256, H32, U256},
    utils::to_checksum,
};
use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;
use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
};
use structstruck::strike;

use crate::dumper::Insertable;

use super::Block;

strike! {
    #[strikethrough[derive(Debug, Clone, Serialize, Deserialize)]]
    pub enum EtlResult {
        BlockWithChainId(struct {
            pub chain_id: u64,
            pub block: Block,
        }),
        /// Contract result
        Contract(struct {
            pub chain_id: u64,
            pub address: Address,
            pub function_signatures: HashSet<H32>,
            pub degree: u8,
            pub ec_recover_count: u16,
            pub ec_add_count: u16,
            pub ec_mul_count: u16,
            pub ec_pairing_count: u16,
            /// The size of the input to the pairing operation in bytes
            pub ec_pairing_input_sizes: Vec<u32>,
            /// Lower degree call addresses
            pub call: HashSet<Address>,
        }),
        /// Transaction result
        Transaction(struct {
            pub chain_id: u64,
            pub from_address: Address,
            pub to_address: Address,
            pub closest_address: HashSet<Address>,
            pub function_signature: H32,
            pub transaction_hash: H256,
            pub transaction_index: u32,
            pub block_number: u64,
            pub block_timestamp: Option<u64>,
            pub block_hash: Option<H256>,
            pub value: U256,
            pub input: Bytes,
            pub gas_used: struct {
                pub total: u64,
                pub first_degree: u64,
                pub second_degree: u64,
            },
            pub ec_recover_count: u16,
            pub ec_add_count: u16,
            pub ec_mul_count: u16,
            pub ec_pairing_count: u16,
            /// The size of the input to the pairing operation in bytes
            pub ec_pairing_input_sizes: Vec<u32>,
            pub ec_recover_addresses: HashSet<Address>,
            pub error: Option<String>,
        }),
    }
}

impl From<Transaction> for EtlResult {
    fn from(value: Transaction) -> Self {
        Self::Transaction(value)
    }
}

impl From<Contract> for EtlResult {
    fn from(value: Contract) -> Self {
        Self::Contract(value)
    }
}

impl From<BlockWithChainId> for EtlResult {
    fn from(value: BlockWithChainId) -> Self {
        Self::BlockWithChainId(value)
    }
}

impl Display for Contract {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            to_string_pretty(self).expect("Failed to serialize contract")
        )
    }
}

impl Display for Transaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            to_string_pretty(self).expect("Failed to serialize transaction")
        )
    }
}

impl Display for BlockWithChainId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            to_string_pretty(self).expect("Failed to serialize block")
        )
    }
}

impl Display for EtlResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(contract) => write!(f, "Contract: {}", contract),
            Self::Transaction(transaction) => write!(f, "Tranasction: {}", transaction),
            Self::BlockWithChainId(block) => {
                write!(f, "Block: {}", block)
            }
        }
    }
}

impl EtlResult {
    pub fn chain_id(&self) -> u64 {
        match self {
            Self::Contract(contract) => contract.chain_id,
            Self::Transaction(transaction) => transaction.chain_id,
            Self::BlockWithChainId(block) => block.chain_id,
        }
    }
}

impl AsRef<Transaction> for Transaction {
    fn as_ref(&self) -> &Transaction {
        self
    }
}

impl AsRef<Contract> for Contract {
    fn as_ref(&self) -> &Contract {
        self
    }
}

impl Insertable for Transaction {
    const INSERT_QUERY: &'static str = "INSERT INTO transactions (
        chain_id, from_address, to_address, closest_address,
        function_signature, transaction_hash, transaction_index,
        block_number, block_timestamp, block_hash, value, input,
        gas_used_total, gas_used_first_degree, gas_used_second_degree,
        ec_recover_count, ec_add_count, ec_mul_count, ec_pairing_count, ec_pairing_input_sizes, ec_recover_addresses, error
    ) VALUES {values} ON CONFLICT (chain_id, transaction_hash) DO NOTHING";

    fn value(&self) -> String {
        format!(
            "({},'{}','{}','{{{}}}','{:?}','{:?}',{},{},{},{},{},'{}',{},{},{},{},{},{},{},'{{{}}}','{{{}}}',{})",
            self.chain_id,
            to_checksum(&self.from_address, None),
            to_checksum(&self.to_address, None),
            self.closest_address
                .iter()
                .map(|e| format!("\"{}\"", to_checksum(e, None)))
                .collect::<Vec<_>>()
                .join(","),
            self.function_signature,
            self.transaction_hash,
            self.transaction_index,
            self.block_number,
            self.block_timestamp
                .map(|e| format!("'{}'", e))
                .unwrap_or("NULL".to_string()), // Handle Option<u64> appropriately
            self.block_hash
                .map(|e| format!("'{:?}'", e))
                .unwrap_or("NULL".to_string()), // Handle Option<H256> appropriately
            self.value,
            self.input,
            self.gas_used.total,
            self.gas_used.first_degree,
            self.gas_used.second_degree,
            self.ec_recover_count,
            self.ec_add_count,
            self.ec_mul_count,
            self.ec_pairing_count,
            self.ec_pairing_input_sizes
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
            self.ec_recover_addresses
                .iter()
                .map(|e| format!("\"{}\"", to_checksum(e, None)))
                .collect::<Vec<_>>()
                .join(","),
            self.error.as_ref().map(|e| format!("'{}'", e)).unwrap_or("NULL".to_string())
        )
    }
}

impl Insertable for Contract {
    const INSERT_QUERY: &'static str = "INSERT INTO contracts (
        chain_id, address, function_signatures, degree,
        ec_recover_count, ec_add_count, ec_mul_count, ec_pairing_count, ec_pairing_input_sizes, call
    ) VALUES {values} ON CONFLICT (chain_id, address, function_signatures) DO NOTHING";

    fn value(&self) -> String {
        format!(
            "({},'{}','{{{}}}',{},{},{},{},{}, '{{{}}}', '{{{}}}')",
            self.chain_id,
            to_checksum(&self.address, None),
            self.function_signatures
                .iter()
                .map(|e| format!("\"{:?}\"", e))
                .collect::<Vec<_>>()
                .join(","),
            self.degree,
            self.ec_recover_count,
            self.ec_add_count,
            self.ec_mul_count,
            self.ec_pairing_count,
            self.ec_pairing_input_sizes
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
            self.call
                .iter()
                .map(|e| format!("\"{}\"", to_checksum(e, None)))
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

impl Contract {
    pub fn cache_key(&self) -> String {
        format!(
            "c:{}:{}:{}",
            self.chain_id,
            to_checksum(&self.address, None),
            self.function_signatures
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("-")
        )
    }
}

impl Insertable for BlockWithChainId {
    const INSERT_QUERY: &'static str = "INSERT INTO blocks (chain_id, number, timestamp, hash, parent_hash, transaction_count, nonce, miner, difficulty, total_difficulty, size, gas_limit, gas_used, base_fee_per_gas)
    VALUES {values} 
    ON CONFLICT (chain_id, number) DO UPDATE SET
    timestamp = EXCLUDED.timestamp,
    hash = EXCLUDED.hash,
    parent_hash = EXCLUDED.parent_hash,
    transaction_count = EXCLUDED.transaction_count,
    nonce = EXCLUDED.nonce,
    miner = EXCLUDED.miner,
    difficulty = EXCLUDED.difficulty,
    total_difficulty = EXCLUDED.total_difficulty,
    size = EXCLUDED.size,
    gas_limit = EXCLUDED.gas_limit,
    gas_used = EXCLUDED.gas_used,
    base_fee_per_gas = EXCLUDED.base_fee_per_gas";

    fn value(&self) -> String {
        format!(
            "({},{},{},'{:?}','{:?}',{},'{:?}','{}',{},{},{},{},{},{})",
            self.chain_id,
            self.block.number,
            self.block.timestamp,
            self.block.hash,
            self.block.parent_hash,
            self.block.transaction_count,
            self.block.nonce,
            to_checksum(&self.block.miner, None),
            self.block.difficulty,
            self.block.total_difficulty,
            self.block.size,
            self.block.gas_limit,
            self.block.gas_used,
            self.block.base_fee_per_gas,
        )
    }

    fn remove_duplicates(v: &mut Vec<String>) {
        v.reverse();
        v.dedup_by(|v1, v2| v1.split(',').take(2).eq(v2.split(',').take(2)));
        v.reverse();
    }
}
