use std::env::var;

use deadpool_postgres::{Config as PostgresConfig, ManagerConfig, RecyclingMethod};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use structstruck::strike;

pub static CONFIG: Lazy<Config> = Lazy::new(Config::new);

strike! {
    #[strikethrough[derive(Debug, Clone)]]
    #[derive(Serialize, Deserialize)]
    pub struct Config {
        pub postgres:
            #[derive(Serialize, Deserialize)]
            pub struct {
                pub host: String,
                pub username: String,
                pub password: String,
                pub db: String,
        },
        pub redis: String,
        pub chains: Vec<
            #[derive(Serialize_tuple, Deserialize_tuple)]
            pub struct {
                pub id: u64,
                pub rpc_url: String,
                pub ws_url: String,
        }>,
        pub port: u16,
    }
}

impl Chains {
    pub fn new<T: ToString>(id: u64, rpc_url: T, ws_url: T) -> Self {
        Chains {
            id,
            rpc_url: rpc_url.to_string(),
            ws_url: ws_url.to_string(),
        }
    }
}

impl Config {
    pub fn new() -> Self {
        Config {
            postgres: Postgres {
                host: var("POSTGRES_HOST").expect("POSTGRES_HOST must be set"),
                username: var("POSTGRES_USERNAME").expect("POSTGRES_USERNAME must be set"),
                password: var("POSTGRES_PASSWORD").expect("POSTGRES_PASSWORD must be set"),
                db: var("POSTGRES_DB").expect("POSTGRES_DB must be set"),
            },
            redis: var("REDIS_URL").expect("REDIS_URL must be set"),
            chains: from_str(&var("CHAINS").expect("CHAINS must be set"))
                .expect("CHAINS must be a valid JSON array"),
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
