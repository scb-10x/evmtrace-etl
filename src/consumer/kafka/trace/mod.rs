use std::{collections::HashMap, marker::PhantomData, pin::Pin, sync::Arc};

use anyhow::Result;
use futures_util::{stream::BoxStream, Future, StreamExt};
use log::info;
use rdkafka::{
    config::FromClientConfig,
    consumer::{Consumer, StreamConsumer},
};

use crate::{
    channels::CHANNEL,
    config::{Chain, KafkaChainConfig, CONFIG},
    types::{Trace, TraceTree},
};

use super::{KafkaConsumer, KafkaStreamConsumer, TopicCommiter};

pub type TraceConsumer = KafkaStreamConsumer<Trace>;

impl KafkaConsumer for TraceConsumer {
    type Data = Trace;

    fn new() -> Self {
        let consumers = match CONFIG.kafka_config() {
            Some(config) => CONFIG
                .chains
                .iter()
                .filter_map(|c| match c {
                    Chain::Kafka(KafkaChainConfig {
                        id,
                        traces_topic: Some(traces_topic),
                        ..
                    }) => {
                        let consumer = StreamConsumer::from_config(&config)
                            .expect("Failed to create consumer");
                        consumer
                            .subscribe(&[&traces_topic])
                            .expect("Failed to subscribe to topic");
                        Some((traces_topic.as_str(), (*id, Arc::new(consumer))))
                    }
                    _ => None,
                })
                .collect::<HashMap<_, _>>(),
            None => HashMap::new(),
        };
        Self {
            consumers,
            _data: PhantomData,
        }
    }

    fn handle_data_stream<'a>(
        topic_id: &'static str,
        chain_id: u64,
        mut stream: BoxStream<'a, Result<(Self::Data, TopicCommiter)>>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>
    where
        Self: Sync + 'a,
    {
        Box::pin(async move {
            let mut trace_tree = TraceTree::new(chain_id);

            info!("Starting trace consumer for {}", topic_id);
            while let Some(t) = stream.next().await {
                let (trace, tpl) = t?;

                if trace.trace_address.is_empty() {
                    if let Some(results) = trace_tree.commit() {
                        CHANNEL.send_result(results, tpl);
                    }

                    trace_tree.reset(&trace);
                }

                trace_tree.add_trace(trace);
            }
            Ok(())
        })
    }
}
