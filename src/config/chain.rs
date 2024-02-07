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
    pub traces_topic: Option<String>,
    pub blocks_topic: Option<String>,
}

impl Chain {
    pub fn chain_id(&self) -> u64 {
        match self {
            Chain::Provider(config) => config.id,
            Chain::Kafka(config) => config.id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correct_chain_serialization() {
        let config = vec![
            Chain::Provider(ProviderChainConfig {
                id: 1,
                rpc_url: "http://localhost:8545".to_string(),
                ws_url: "ws://localhost:8546".to_string(),
                index_block: true,
                index_tx: true,
            }),
            Chain::Kafka(KafkaChainConfig {
                id: 2,
                blocks_topic: Some("blocks".to_string()),
                traces_topic: Some("traces".to_string()),
            }),
        ];

        assert_eq!(
            serde_json::to_string(&config).expect("serialization failed"),
            r#"[{"Provider":[1,"http://localhost:8545","ws://localhost:8546",true,true]},{"Kafka":[2,"traces","blocks"]}]"#
        );
    }
}
