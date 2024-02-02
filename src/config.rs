use std::env::var;

use deadpool_postgres::{Config as PostgresConfig, ManagerConfig, RecyclingMethod};
use once_cell::sync::Lazy;
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use structstruck::strike;

pub static CONFIG: Lazy<Config> = Lazy::new(|| Config::new());

strike! {
    #[strikethrough[derive(Debug, Clone, Serialize, Deserialize)]]
    pub struct Config {
        pub kafka: pub struct {
            pub url: String,
            pub group_id: String,
            pub username: String,
            pub password: String,
        },
        pub postgres: pub struct {
            pub host: String,
            pub username: String,
            pub password: String,
            pub db: String,
        },
        pub redis: String,
        pub chains: Vec<pub struct {
            pub id: u64,
            pub kafka_trace_topic: String,
            pub kafka_block_topic: Option<String>,
        }>,
    }
}

impl Chains {
    pub fn trace<T: ToString>(id: u64, topic: T) -> Self {
        Chains {
            id,
            kafka_trace_topic: topic.to_string(),
            kafka_block_topic: None,
        }
    }
}

impl Config {
    pub fn new() -> Self {
        Config {
            kafka: Kafka {
                url: var("KAFKA_URL").expect("KAFKA_URL must be set"),
                group_id: var("KAFKA_GROUP_ID").expect("KAFKA_GROUP_ID must be set"),
                username: var("KAFKA_USERNAME").expect("KAFKA_USERNAME must be set"),
                password: var("KAFKA_PASSWORD").expect("KAFKA_PASSWORD must be set"),
            },
            postgres: Postgres {
                host: var("POSTGRES_HOST").expect("POSTGRES_HOST must be set"),
                username: var("POSTGRES_USERNAME").expect("POSTGRES_USERNAME must be set"),
                password: var("POSTGRES_PASSWORD").expect("POSTGRES_PASSWORD must be set"),
                db: var("POSTGRES_DB").expect("POSTGRES_DB must be set"),
            },
            redis: var("REDIS_URL").expect("REDIS_URL must be set"),
            chains: vec![
                Chains::trace(1, "ethereum_traces"),
                Chains::trace(42161, "arbitrum_traces"),
                Chains::trace(10, "optimism_traces"),
                Chains::trace(137, "polygon_traces"),
                Chains::trace(43114, "avalanche_traces"),
                Chains::trace(8453, "base_traces"),
                Chains::trace(56, "bsc_traces"),
            ],
        }
    }

    pub fn postgres_config(&self) -> PostgresConfig {
        self.into()
    }
    pub fn kafka_config(&self) -> ClientConfig {
        self.into()
    }
}

impl Into<PostgresConfig> for &Config {
    fn into(self) -> PostgresConfig {
        PostgresConfig {
            host: Some(self.postgres.host.to_string()),
            user: Some(self.postgres.username.to_string()),
            password: Some(self.postgres.password.to_string()),
            dbname: Some(self.postgres.db.to_string()),
            manager: Some(ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            }),
            ..Default::default()
        }
    }
}

impl Into<ClientConfig> for &Config {
    fn into(self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", self.kafka.url.as_str());
        config.set("security.protocol", "SASL_PLAINTEXT");
        config.set("sasl.mechanisms", "SCRAM-SHA-256");
        config.set("group.id", self.kafka.group_id.as_str());
        config.set("sasl.username", self.kafka.username.as_str());
        config.set("sasl.password", self.kafka.password.as_str());
        config.set("auto.offset.reset", "earliest");
        config.set("socket.timeout.ms", "20000");
        config.set("session.timeout.ms", "60000");
        config
    }
}