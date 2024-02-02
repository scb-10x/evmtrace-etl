use std::collections::HashSet;

use crate::{
    config::CONFIG,
    traces::{Contract, TraceResult, Transaction},
};
use anyhow::Result;
use deadpool_postgres::{Pool as PostgresPool, Runtime};
use once_cell::sync::Lazy;
use redis::{AsyncCommands, Client as RedisClient};
use redis_pool::{RedisPool, SingleRedisPool};
use tokio_postgres::NoTls;

pub static POSTGRESQL_DUMPER: Lazy<PostgreSQLDumper> =
    Lazy::new(|| PostgreSQLDumper::new().expect("Failed to create PostgreSQLDumper"));

#[derive(Clone)]
pub struct PostgreSQLDumper {
    postgres_pool: PostgresPool,
    redis_pool: SingleRedisPool,
}

impl PostgreSQLDumper {
    pub fn new() -> Result<Self> {
        Ok(Self {
            postgres_pool: CONFIG
                .postgres_config()
                .create_pool(Some(Runtime::Tokio1), NoTls)?,
            redis_pool: RedisPool::from(RedisClient::open(CONFIG.redis.as_str())?),
        })
    }

    pub async fn insert_results(&self, results: &[TraceResult]) -> Result<()> {
        let mut key_to_set = HashSet::new();

        let mut contracts: Vec<&Contract> = vec![];
        let mut transactions: Vec<&Transaction> = vec![];

        let mut redis = self.redis_pool.aquire().await?;
        let mut postgres = self.postgres_pool.get().await?;
        for result in results {
            match result {
                TraceResult::Contract(c) => {
                    let key = c.cache_key();
                    let found_cache: Option<String> = redis.get::<&str, _>(&key).await?;
                    if found_cache.is_none() {
                        key_to_set.insert(key);
                        contracts.push(c);
                    };
                }
                TraceResult::Transaction(t) => {
                    transactions.push(t);
                }
            }
        }

        let transaction = postgres.transaction().await?;
        Contract::inserts(&contracts, &transaction).await?;
        Transaction::inserts(&transactions, &transaction).await?;
        transaction.commit().await?;

        redis
            .mset::<&str, &str, ()>(
                &key_to_set
                    .iter()
                    .map(|k| (k.as_str(), ""))
                    .collect::<Vec<_>>(),
            )
            .await
            .ok();

        Ok(())
    }
}
