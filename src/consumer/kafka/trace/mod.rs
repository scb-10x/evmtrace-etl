use std::pin::Pin;

use anyhow::Result;
use futures_util::{stream::BoxStream, Future, StreamExt};
use log::info;
use once_cell::sync::Lazy;

use crate::{
    channels::CHANNEL,
    types::{Trace, TraceTree},
};

use super::{KafkaConsumer, KafkaStreamConsumer, TopicCommiter};

pub static TRACE_CONSUMER: Lazy<TraceConsumer> = Lazy::new(TraceConsumer::new);

pub type TraceConsumer = KafkaStreamConsumer<Trace>;

impl KafkaConsumer for TraceConsumer {
    type Data = Trace;

    fn new() -> Self {
        todo!()
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
