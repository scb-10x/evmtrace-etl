use std::env::var;

use deadpool_postgres::{Config as PostgresConfig, ManagerConfig, RecyclingMethod};
use once_cell::sync::Lazy;
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

        println!("{}", serde_json::to_string(&config).unwrap());
    }
}
