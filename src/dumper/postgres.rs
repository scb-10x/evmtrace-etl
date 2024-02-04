use std::collections::{HashMap, HashSet};

use crate::{config::CONFIG, types::EtlResult};
use anyhow::Result;
use deadpool_postgres::{Pool as PostgresPool, Runtime};
use once_cell::sync::Lazy;
use redis::{AsyncCommands, Client as RedisClient};
use redis_pool::{RedisPool, SingleRedisPool};
use tokio_postgres::NoTls;

pub static POSTGRESQL_DUMPER: Lazy<PostgreSQLDumper> =
    Lazy::new(|| PostgreSQLDumper::new().expect("Failed to create PostgreSQLDumper"));

pub trait Insertable {
    const INSERT_QUERY: &'static str;
    fn value(v: &Self) -> String;
}

#[derive(Debug, Clone, Default)]
pub struct InsertTree(HashMap<&'static str, Vec<String>>);

impl InsertTree {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert<T: Insertable>(&mut self, v: &T) {
        self.0.entry(T::INSERT_QUERY).or_default().push(T::value(v));
    }

    pub async fn execute<'a>(
        &self,
        transaction: &'a tokio_postgres::Transaction<'a>,
    ) -> Result<()> {
        for (query, values) in self.0.iter() {
            let final_query = query.replace("{values}", &values.join(","));
            transaction.execute(final_query.as_str(), &[]).await?;
        }
        Ok(())
    }
}

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

    pub async fn insert_results(&self, results: &[EtlResult]) -> Result<()> {
        let mut key_to_set = HashSet::new();

        let mut redis = self.redis_pool.aquire().await?;
        let mut postgres = self.postgres_pool.get().await?;
        let mut insert_tree = InsertTree::new();
        for result in results {
            match result {
                EtlResult::Contract(c) => {
                    let key = c.cache_key();
                    let found_cache: Option<String> = redis.get::<&str, _>(&key).await?;
                    if found_cache.is_none() {
                        key_to_set.insert(key);
                        insert_tree.insert(c)
                    };
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
