use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, bail, Result};
use futures_util::StreamExt;
use log::info;
use once_cell::sync::Lazy;
use rdkafka::{
    config::FromClientConfig,
    consumer::{CommitMode, Consumer, DefaultConsumerContext, StreamConsumer},
    util::DefaultRuntime,
    Message, Offset, TopicPartitionList,
};
use serde_json::from_str;
use tokio::{
    spawn,
    task::{JoinHandle, JoinSet},
};

use crate::{
    channels::CHANNEL, config::CONFIG, consumer::trace::trace_tree::TraceTree, traces::Trace,
};

mod trace_tree;

pub static TRACE_CONSUMER: Lazy<TraceConsumer> = Lazy::new(|| TraceConsumer::new());

pub type TraceStreamConsumer = Arc<StreamConsumer<DefaultConsumerContext, DefaultRuntime>>;
pub struct TraceConsumer {
    consumers: HashMap<&'static str, (u64, TraceStreamConsumer)>,
}

impl TraceConsumer {
    pub fn new() -> Self {
        let config = CONFIG.kafka_config();
        let consumers = CONFIG
            .chains
            .iter()
            .map(|c| {
                let consumer =
                    StreamConsumer::<DefaultConsumerContext, DefaultRuntime>::from_config(&config)
                        .expect("Failed to create consumer");
                consumer
                    .subscribe(&[&c.kafka_trace_topic])
                    .expect("Failed to subscribe to topic");
                (c.kafka_trace_topic.as_str(), (c.id, Arc::new(consumer)))
            })
            .collect::<HashMap<_, _>>();
        Self { consumers }
    }

    pub fn commit(&self, topic_id: &str, topic_partition_list: TopicPartitionList) -> Result<()> {
        if let Some((_, consumer)) = self.consumers.get(topic_id) {
            consumer.commit(&topic_partition_list, CommitMode::Async)?;
        }
        Ok(())
    }

    pub fn poll(&self) -> JoinHandle<Result<()>> {
        let mut set = JoinSet::<Result<()>>::new();
        for (topic_id, (chain_id, consumer)) in &self.consumers {
            let topic_id = *topic_id;
            let chain_id = *chain_id;
            let consumer = consumer.clone();
            set.spawn(async move {
                let mut trace_tree = TraceTree::new(topic_id, chain_id);

                let mut stream = consumer.stream();
                info!("Starting trace consumer for {}", topic_id);
                while let Some(msg) = stream.next().await {
                    let m = msg?;
                    let payload = m
                        .payload_view::<str>()
                        .and_then(|p| p.ok())
                        .ok_or_else(|| anyhow!("Invalid payload"))?;
                    let trace = from_str::<Trace>(payload)
                        .map_err(|e| anyhow!("Serialization Error: {e}, original {payload}"))?;

                    if trace.trace_address.is_empty() {
                        if let Some(results) = trace_tree.commit() {
                            let mut topic_partition = TopicPartitionList::new();
                            topic_partition.add_partition_offset(
                                m.topic(),
                                m.partition(),
                                Offset::from_raw(m.offset() - 1),
                            )?;
                            CHANNEL.send_result(results, (topic_id, topic_partition));
                        }

                        trace_tree.reset(&trace);
                    }

                    trace_tree.add_trace(trace);
                }
                Ok(())
            });
        }

        spawn(async move {
            while let Some(r) = set.join_next().await {
                match r {
                    Err(e) => bail!(e),
                    Ok(Err(e)) => bail!(e),
                    _ => {}
                };
            }
            Ok(())
        })
    }
}
