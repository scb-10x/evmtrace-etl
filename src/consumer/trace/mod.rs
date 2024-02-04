use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use anyhow::Result;
use futures_util::{stream::BoxStream, StreamExt};
use log::info;
use once_cell::sync::Lazy;
use rdkafka::{
    config::FromClientConfig,
    consumer::{Consumer, DefaultConsumerContext, StreamConsumer},
    util::DefaultRuntime,
    TopicPartitionList,
};

use crate::{
    channels::CHANNEL, config::CONFIG, consumer::trace::trace_tree::TraceTree, types::Trace,
};

use super::{KafkaConsumer, KafkaStreamConsumer};

mod trace_tree;

pub static TRACE_CONSUMER: Lazy<TraceConsumer> = Lazy::new(|| TraceConsumer::new());

pub type TraceConsumer = KafkaStreamConsumer<Trace>;

impl KafkaConsumer for TraceConsumer {
    type Data = Trace;

    fn new() -> Self {
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
        Self {
            consumers,
            _data: PhantomData,
        }
    }

    fn handle_data_stream<'a>(
        topic_id: &'static str,
        chain_id: u64,
        mut stream: BoxStream<'a, Result<(Trace, TopicPartitionList)>>,
    ) -> std::pin::Pin<Box<dyn futures::prelude::Future<Output = Result<()>> + Send + 'a>>
    where
        Self: Sync + 'a,
    {
        Box::pin(async move {
            let mut trace_tree = TraceTree::new(topic_id, chain_id);

            info!("Starting trace consumer for {}", topic_id);
            while let Some(t) = stream.next().await {
                let (trace, tpl) = t?;

                if trace.trace_address.is_empty() {
                    if let Some(results) = trace_tree.commit() {
                        CHANNEL.send_result(results, (topic_id, tpl));
                    }

                    trace_tree.reset(&trace);
                }

                trace_tree.add_trace(trace);
            }
            Ok(())
        })
    }
}
