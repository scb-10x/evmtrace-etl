use std::env::var;

use deadpool_postgres::{Config as PostgresConfig, ManagerConfig, RecyclingMethod};
use once_cell::sync::Lazy;
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use structstruck::strike;

mod chain;
pub use chain::*;

pub static CONFIG: Lazy<Config> = Lazy::new(Config::new);

strike! {
    #[strikethrough[derive(Debug, Clone)]]
    #[derive(Serialize, Deserialize)]
    pub struct Config {
        pub kafka: Option<
            #[derive(Serialize_tuple, Deserialize_tuple)]
            pub struct {
                pub url: String,
                pub group_id: String,
                pub username: String,
                pub password: String,
            }
        >,
        pub postgres:
            #[derive(Serialize, Deserialize)]
            pub struct {
                pub host: String,
                pub username: String,
                pub password: String,
                pub db: String,
            }
        ,
        pub redis: String,
        pub chains: Vec<Chain>,
        pub port: u16,
    }
}

impl Config {
    pub fn new() -> Self {
        Config {
            kafka: var("KAFKA")
                .ok()
                .map(|kafka| from_str(&kafka).expect("KAFKA must be a valid JSON array")),
            postgres: Postgres {
                host: var("POSTGRES_HOST").expect("POSTGRES_HOST must be set"),
                username: var("POSTGRES_USERNAME").expect("POSTGRES_USERNAME must be set"),
                password: var("POSTGRES_PASSWORD").expect("POSTGRES_PASSWORD must be set"),
                db: var("POSTGRES_DB").expect("POSTGRES_DB must be set"),
            },
            redis: var("REDIS_URL").expect("REDIS_URL must be set"),
            chains: var("CHAINS")
                .ok()
                .map(|chains| from_str(&chains).expect("CHAINS must be a valid JSON array"))
                .unwrap_or_default(),
            port: var("PORT")
                .unwrap_or("8080".to_string())
                .parse()
                .expect("PORT must be a number"),
        }
    }

    pub fn postgres_config(&self) -> PostgresConfig {
        self.into()
    }

    pub fn kafka_config(&self) -> Option<ClientConfig> {
        self.into()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

impl From<&Config> for PostgresConfig {
    fn from(val: &Config) -> Self {
        PostgresConfig {
            host: Some(val.postgres.host.to_string()),
            user: Some(val.postgres.username.to_string()),
            password: Some(val.postgres.password.to_string()),
            dbname: Some(val.postgres.db.to_string()),
            manager: Some(ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            }),
            ..Default::default()
        }
    }
}

impl From<&Config> for Option<ClientConfig> {
    fn from(val: &Config) -> Self {
        val.kafka.as_ref().map(|kafka| {
            let mut config = ClientConfig::new();
            config.set("bootstrap.servers", kafka.url.as_str());
            config.set("security.protocol", "SASL_PLAINTEXT");
            config.set("sasl.mechanisms", "SCRAM-SHA-256");
            config.set("group.id", kafka.group_id.as_str());
            config.set("sasl.username", kafka.username.as_str());
            config.set("sasl.password", kafka.password.as_str());
            config.set("auto.offset.reset", "earliest");
            config.set("socket.timeout.ms", "20000");
            config.set("session.timeout.ms", "60000");
            config
        })
    }
}
