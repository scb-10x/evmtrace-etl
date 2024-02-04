use once_cell::sync::Lazy;
use rdkafka::TopicPartitionList;
use tokio::{
    spawn,
    sync::broadcast::{channel, Sender},
};

use crate::types::EtlResult;

pub static CHANNEL: Lazy<Channel> = Lazy::new(|| Channel::new());

pub struct Channel {
    pub result_tx: Sender<(Vec<EtlResult>, (&'static str, TopicPartitionList))>,
}

impl Channel {
    pub fn new() -> Self {
        let (result_tx, _) = channel(100_000);
        Self { result_tx }
    }

    pub fn send_result(
        &self,
        result: Vec<EtlResult>,
        topic_partition_list: (&'static str, TopicPartitionList),
    ) {
        let result_tx = self.result_tx.clone();
        spawn(async move {
            result_tx
                .send((result, topic_partition_list))
                .expect("Failed to send result");
        });
    }
}
