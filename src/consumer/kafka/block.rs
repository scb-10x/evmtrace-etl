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
    types::{Block, BlockWithChainId},
};

use super::{KafkaConsumer, KafkaStreamConsumer, TopicCommiter};

pub type BlockConsumer = KafkaStreamConsumer<Block>;

impl KafkaConsumer for BlockConsumer {
    type Data = Block;

    fn new() -> Self {
        let consumers = match CONFIG.kafka_config() {
            Some(config) => CONFIG
                .chains
                .iter()
                .filter_map(|c| match c {
                    Chain::Kafka(KafkaChainConfig {
                        id,
                        blocks_topic: Some(blocks_topic),
                        ..
                    }) => {
                        let consumer = StreamConsumer::from_config(&config)
                            .expect("Failed to create consumer");
                        consumer
                            .subscribe(&[&blocks_topic])
                            .expect("Failed to subscribe to topic");
                        Some((blocks_topic.as_str(), (*id, Arc::new(consumer))))
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
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>
    where
        Self: Sync + 'a,
    {
        Box::pin(async move {
            info!("Starting block consumer for {}", topic_id);
            while let Some(t) = stream.next().await {
                let (block, tc) = t?;
                CHANNEL.send_result(vec![BlockWithChainId { chain_id, block }.into()], tc);
            }
            Ok(())
        })
    }
}
