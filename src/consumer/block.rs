use std::{collections::HashMap, marker::PhantomData, pin::Pin, sync::Arc};

use anyhow::Result;
use futures_util::{stream::BoxStream, Future, StreamExt};
use log::info;
use once_cell::sync::Lazy;
use rdkafka::{
    config::FromClientConfig,
    consumer::{Consumer, DefaultConsumerContext, StreamConsumer},
    util::DefaultRuntime,
};

use crate::{config::CONFIG, types::Block};

use super::{KafkaConsumer, KafkaStreamConsumer, TopicCommiter};

pub static BLOCK_CONSUMER: Lazy<BlockConsumer> = Lazy::new(BlockConsumer::new);
pub type BlockConsumer = KafkaStreamConsumer<Block>;

impl KafkaConsumer for BlockConsumer {
    type Data = Block;

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
                    .subscribe(&[&c.kafka_block_topic])
                    .expect("Failed to subscribe to topic");
                (c.kafka_block_topic.as_str(), (c.id, Arc::new(consumer)))
            })
            .collect::<HashMap<_, _>>();
        Self {
            consumers,
            _data: PhantomData,
        }
    }

    fn handle_data_stream<'a>(
        topic_id: &'static str,
        _chain_id: u64,
        mut stream: BoxStream<'a, Result<(Self::Data, TopicCommiter)>>,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>
    where
        Self: Sync + 'a,
    {
        Box::pin(async move {
            info!("Starting block consumer for {}", topic_id);
            while let Some(t) = stream.next().await {
                let (block, _) = t?;

                info!("Received block from {}: {}", topic_id, block);
            }
            Ok(())
        })
    }
}
