use std::{collections::HashMap, fmt::Debug, marker::PhantomData, pin::Pin, sync::Arc};

use anyhow::{anyhow, Result};
use futures_util::{stream::BoxStream, Future, StreamExt};
use rdkafka::{
    consumer::{CommitMode, Consumer, DefaultConsumerContext, StreamConsumer},
    util::DefaultRuntime,
    Message, Offset, TopicPartitionList,
};
use serde::de::DeserializeOwned;
use serde_json::from_str;
use tokio::task::{JoinHandle, JoinSet};

mod block;
mod trace;
pub use block::*;
pub use trace::*;

use crate::utils::join_set_else_pending;

use super::Commiter;

type ArcConsumer = Arc<StreamConsumer<DefaultConsumerContext, DefaultRuntime>>;
pub struct KafkaStreamConsumer<T> {
    pub consumers: HashMap<&'static str, (u64, ArcConsumer)>,
    pub _data: PhantomData<T>,
}

impl<T: DeserializeOwned + Sync + Unpin> KafkaStreamConsumer<T>
where
    Self: KafkaConsumer<Data = T>,
{
    pub fn poll() -> JoinHandle<Result<()>> {
        let s = Self::new();
        let mut set = JoinSet::<Result<()>>::new();
        for (topic_id, (chain_id, consumer)) in &s.consumers {
            let topic_id = *topic_id;
            let chain_id = *chain_id;
            let consumer = consumer.clone();
            set.spawn(async move {
                let stream = consumer.stream().map(|msg| -> Result<(T, TopicCommiter)> {
                    let m = msg?;
                    let payload = m
                        .payload_view::<str>()
                        .and_then(|p| p.ok())
                        .ok_or_else(|| anyhow!("Invalid payload"))?;
                    let data = from_str::<T>(payload).map_err(|e| {
                        anyhow!("Serialization Error: {e}, original {payload}, chain id {chain_id}")
                    })?;

                    Ok((
                        data,
                        TopicCommiter {
                            chain_id,
                            topic_id,
                            commit_fn: {
                                let mut topic_partition = TopicPartitionList::new();
                                topic_partition.add_partition_offset(
                                    m.topic(),
                                    m.partition(),
                                    Offset::from_raw(m.offset() - 1),
                                )?;
                                let consumer = consumer.clone();
                                Arc::new(move || -> Result<()> {
                                    consumer.commit(&topic_partition, CommitMode::Async)?;
                                    Ok(())
                                })
                            },
                            offset: m.offset(),
                        },
                    ))
                });
                Self::handle_data_stream(topic_id, chain_id, Box::pin(stream)).await?;
                Ok(())
            });
        }

        join_set_else_pending(set)
    }
}

pub trait KafkaConsumer {
    type Data: DeserializeOwned;

    fn new() -> Self;
    fn handle_data_stream<'a>(
        topic_id: &'static str,
        chain_id: u64,
        stream: BoxStream<'a, Result<(Self::Data, TopicCommiter)>>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>
    where
        Self: Sync + 'a;
}

#[derive(Clone)]
pub struct TopicCommiter {
    pub chain_id: u64,
    pub topic_id: &'static str,
    pub offset: i64,
    pub commit_fn: Arc<dyn Fn() -> Result<()> + Send + Sync>,
}

impl Debug for TopicCommiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicCommit")
            .field("chain_id", &self.chain_id)
            .field("topic_id", &self.topic_id)
            .finish()
    }
}

impl TopicCommiter {
    pub fn commit(&self) -> Result<()> {
        (self.commit_fn)()
    }
}

impl Into<Commiter> for TopicCommiter {
    fn into(self) -> Commiter {
        Commiter::Kafka(self)
    }
}
