use serde::{Deserialize, Serialize};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Chain {
    Provider(ProviderChainConfig),
    Kafka(KafkaChainConfig),
}

#[derive(Debug, Clone, Serialize_tuple, Deserialize_tuple)]
pub struct ProviderChainConfig {
    pub id: u64,
    pub rpc_url: String,
    pub ws_url: String,
    pub index_block: bool,
    pub index_tx: bool,
}

#[derive(Debug, Clone, Serialize_tuple, Deserialize_tuple)]
pub struct KafkaChainConfig {
    pub id: u64,
    pub blocks_topic: Option<String>,
    pub traces_topic: Option<String>,
}

impl Chain {
    pub fn chain_id(&self) -> u64 {
        match self {
            Chain::Provider(config) => config.id,
            Chain::Kafka(config) => config.id,
        }
    }
}
