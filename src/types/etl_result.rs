use alloy_primitives::{aliases::B32, Address, Bytes, B256};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;
use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
};
use structstruck::strike;

strike! {
    #[strikethrough[derive(Debug, Clone, Serialize, Deserialize)]]
    pub enum EtlResult {
        /// Contract result
        Contract(struct {
            pub chain_id: u64,
            pub address: Address,
            pub function_signatures: HashSet<B32>,
            pub degree: u8,
            pub ec_mul_count: u16,
            pub ec_pairing_count: u16,
            /// The size of the input to the pairing operation in bytes
            pub ec_pairing_input_sizes: Vec<u32>,
            /// Lower degree call addresses
            pub call: HashSet<Address>,
        }),
        Transaction(struct {
            pub chain_id: u64,
            pub from_address: Address,
            pub to_address: Address,
            pub closest_address: HashSet<Address>,
            pub function_signature: B32,
            pub transaction_hash: B256,
            pub transaction_index: u32,
            pub block_number: u64,
            pub block_timestamp: Option<u64>,
            pub block_hash: Option<B256>,
            pub value: u128,
            pub input: Bytes,
            pub gas_used: struct {
                pub requested: u64,
                pub total: u64,
                pub first_degree: u64,
                pub second_degree: u64,
            },
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

impl Display for EtlResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(contract) => write!(f, "Contract: {}", contract),
            Self::Transaction(transaction) => write!(f, "Tranasction: {}", transaction),
        }
    }
}

impl EtlResult {
    pub fn chain_id(&self) -> u64 {
        match self {
            Self::Contract(contract) => contract.chain_id,
            Self::Transaction(transaction) => transaction.chain_id,
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

impl Transaction {
    const INSERT_QUERY: &'static str = "INSERT INTO transactions (
        chain_id, from_address, to_address, closest_address,
        function_signature, transaction_hash, transaction_index,
        block_number, block_timestamp, block_hash, value, input,
        gas_used_requested, gas_used_total, gas_used_first_degree, gas_used_second_degree
    ) VALUES";
    const CONFLICT_CLAUSE: &'static str = "ON CONFLICT (chain_id, transaction_hash) DO NOTHING";

    pub async fn inserts<'a, T: AsRef<Self>>(
        values: &[T],
        transaction: &'a tokio_postgres::Transaction<'a>,
    ) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let params_string = values
            .iter()
            .map(|v| {
                let v = v.as_ref();
                format!(
                    "({},'{}','{}','{{{}}}','{}','{}',{},{},{},{},{},'{}',{},{},{},{})",
                    v.chain_id,
                    v.from_address,
                    v.to_address,
                    v.closest_address
                        .iter()
                        .map(|e| format!("\"{}\"", e))
                        .collect::<Vec<_>>()
                        .join(","),
                    v.function_signature,
                    v.transaction_hash,
                    v.transaction_index,
                    v.block_number,
                    v.block_timestamp
                        .map(|e| format!("'{}'", e))
                        .unwrap_or("NULL".to_string()), // Handle Option<u64> appropriately
                    v.block_hash
                        .map(|e| format!("'{}'", e))
                        .unwrap_or("NULL".to_string()), // Handle Option<B256> appropriately
                    v.value,
                    v.input,
                    v.gas_used.requested,
                    v.gas_used.total,
                    v.gas_used.first_degree,
                    v.gas_used.second_degree
                )
            })
            .collect::<Vec<String>>()
            .join(",");

        let query = format!(
            "{} {} {}",
            Self::INSERT_QUERY,
            params_string,
            Self::CONFLICT_CLAUSE
        );
        transaction.execute(&query, &[]).await?;
        Ok(())
    }
}

impl Contract {
    const INSERTS_QUERY: &'static str = "INSERT INTO contracts (
        chain_id, address, function_signatures, degree,
        ec_mul_count, ec_pairing_count, ec_pairing_input_sizes, call
    ) VALUES";
    const CONFLICT_CLAUSE: &'static str =
        "ON CONFLICT (chain_id, address, function_signatures) DO NOTHING";

    pub fn cache_key(&self) -> String {
        format!(
            "c:{}:{}:{}",
            self.chain_id,
            self.address,
            self.function_signatures
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("-")
        )
    }

    pub async fn inserts<'a, T: AsRef<Self>>(
        values: &[T],
        transaction: &'a tokio_postgres::Transaction<'a>,
    ) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let params_string = values
            .iter()
            .map(|v| {
                let v = v.as_ref();
                format!(
                    "({},'{}','{{{}}}',{}, {}, {}, '{{{}}}', '{{{}}}')",
                    v.chain_id,
                    v.address,
                    v.function_signatures
                        .iter()
                        .map(|e| format!("\"{}\"", e))
                        .collect::<Vec<_>>()
                        .join(","),
                    v.degree,
                    v.ec_mul_count,
                    v.ec_pairing_count,
                    v.ec_pairing_input_sizes
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    v.call
                        .iter()
                        .map(|e| format!("\"{}\"", e))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            })
            .collect::<Vec<String>>()
            .join(",");

        let query = format!(
            "{} {} {}",
            Self::INSERTS_QUERY,
            params_string,
            Self::CONFLICT_CLAUSE
        );
        transaction.execute(&query, &[]).await?;

        Ok(())
    }

    //pub async fn insert<'a>(&self, transaction: &'a tokio_postgres::Transaction<'a>) -> Result<()> {
    //let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[
    //&(self.chain_id as i64),
    //&self.address.to_string(),
    //&self
    //.function_signatures
    //.iter()
    //.map(ToString::to_string)
    //.collect::<Vec<_>>(),
    //&(self.degree as i16),
    //&(self.ec_mul_count as i16),
    //&(self.ec_pairing_count as i16),
    //&self
    //.ec_pairing_input_sizes
    //.iter()
    //.map(|v| *v as i32)
    //.collect::<Vec<_>>(),
    //&self
    //.call
    //.iter()
    //.map(ToString::to_string)
    //.collect::<Vec<_>>(),
    //];
    //transaction.execute(Self::INSERT_QUERY, params).await?;

    //Ok(())
    //}
}
