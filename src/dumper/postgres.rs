use std::collections::HashSet;

use crate::{config::CONFIG, types::EtlResult};
use anyhow::{Error, Result};
use deadpool_postgres::{Pool as PostgresPool, Runtime};
use futures_util::future::OptionFuture;
use once_cell::sync::Lazy;
use redis::{AsyncCommands, Client as RedisClient};
use redis_pool::{RedisPool, SingleRedisPool};
use tokio_postgres::NoTls;

mod insert_tree;
mod insertable;
pub use insertable::*;

use self::insert_tree::InsertTree;

pub static POSTGRESQL_DUMPER: Lazy<PostgreSQLDumper> =
    Lazy::new(|| PostgreSQLDumper::new().expect("Failed to create PostgreSQLDumper"));

#[derive(Clone)]
pub struct PostgreSQLDumper {
    postgres_pool: PostgresPool,
    redis_pool: Option<SingleRedisPool>,
}

impl PostgreSQLDumper {
    pub fn new() -> Result<Self> {
        Ok(Self {
            postgres_pool: CONFIG
                .postgres_config()
                .create_pool(Some(Runtime::Tokio1), NoTls)?,
            redis_pool: CONFIG
                .redis
                .as_ref()
                .map(|r| Ok::<_, Error>(RedisPool::from(RedisClient::open(r.as_ref())?)))
                .transpose()?,
        })
    }

    pub async fn insert_results(&self, results: &[EtlResult]) -> Result<()> {
        let mut key_to_set = HashSet::new();

        let mut redis = OptionFuture::from(self.redis_pool.as_ref().map(|f| f.aquire()))
            .await
            .transpose()?;
        //let mut redis = match &self.redis_pool {
        //Some(pool) => Some(pool.aquire().await?),
        //None => None,
        //};
        let mut postgres = self.postgres_pool.get().await?;
        let mut insert_tree = InsertTree::new();
        for result in results {
            match result {
                EtlResult::Contract(c) => {
                    let key = c.cache_key();
                    if let Some(redis) = redis.as_mut() {
                        let found_cache: Option<String> = redis.get::<&str, _>(&key).await?;
                        if found_cache.is_none() {
                            key_to_set.insert(key);
                            insert_tree.insert(c)
                        };
                    } else {
                        insert_tree.insert(c);
                    }
                }
                EtlResult::Transaction(t) => {
                    insert_tree.insert(t);
                }
                EtlResult::BlockWithChainId(b) => insert_tree.insert(b),
            }
        }

        let transaction = postgres.transaction().await?;
        insert_tree.execute(&transaction).await?;
        transaction.commit().await?;

        // Uses `ok` to ignore the result of the mset, sometimes redis raise args error
        if let Some(redis) = redis.as_mut() {
            redis
                .mset::<&str, &str, ()>(
                    &key_to_set
                        .iter()
                        .map(|k| (k.as_str(), ""))
                        .collect::<Vec<_>>(),
                )
                .await
                .ok();
        }

        Ok(())
    }
}
