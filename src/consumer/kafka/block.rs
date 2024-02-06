use std::pin::Pin;

use anyhow::Result;
use futures_util::{stream::BoxStream, Future, StreamExt};
use log::info;
use once_cell::sync::Lazy;

use crate::{
    channels::CHANNEL,
    types::{Block, BlockWithChainId},
};

use super::{KafkaConsumer, KafkaStreamConsumer, TopicCommiter};

pub static BLOCK_CONSUMER: Lazy<BlockConsumer> = Lazy::new(BlockConsumer::new);
pub type BlockConsumer = KafkaStreamConsumer<Block>;

impl KafkaConsumer for BlockConsumer {
    type Data = Block;

    fn new() -> Self {
        todo!()
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
