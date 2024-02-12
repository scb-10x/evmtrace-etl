use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use tokio::{spawn, sync::RwLock, task::JoinHandle};

use crate::{channels::CHANNEL, consumer::Commiter, types::EtlResult};

pub static STATS: Lazy<Stats> = Lazy::new(Stats::new);

type StatsMap = Arc<RwLock<HashMap<(&'static str, Option<String>), u64>>>;
#[derive(Debug, Default)]
pub struct Stats(StatsMap);

impl Stats {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }

    pub async fn read(&self) -> Vec<(String, Value)> {
        let stats = self.0.read().await;
        stats
            .iter()
            .map(|(k, v)| {
                (
                    match k.1 {
                        Some(ref chain_id) => format!("{}_{}", k.0, chain_id),
                        None => k.0.to_string(),
                    },
                    json!(*v),
                )
            })
            .collect()
    }

    pub fn watch(&self) -> JoinHandle<Result<()>> {
        let stats = self.0.clone();
        spawn(async move {
            let mut rx = CHANNEL.result_tx.subscribe();
            while let Ok((results, tc)) = rx.recv().await {
                for result in results {
                    match result {
                        EtlResult::BlockWithChainId(b) => {
                            stats
                                .write()
                                .await
                                .entry(("latest_block", Some(b.chain_id.to_string())))
                                .and_modify(|v| *v = b.block.number)
                                .or_insert(b.block.number);
                        }
                        EtlResult::Transaction(tx) => {
                            stats
                                .write()
                                .await
                                .entry(("latest_transaction_block", Some(tx.chain_id.to_string())))
                                .and_modify(|v| *v = tx.block_number)
                                .or_insert(tx.block_number);
                        }
                        _ => {}
                    };
                }

                if let Commiter::Kafka(tc) = tc {
                    stats
                        .write()
                        .await
                        .insert((tc.topic_id, None), tc.offset as u64);
                }
            }

            Ok(())
        })
    }

    pub fn queue() -> usize {
        CHANNEL.result_tx.len()
    }
}
