use once_cell::sync::Lazy;
use tokio::{
    spawn,
    sync::broadcast::{channel, Sender},
};

use crate::{consumer::TopicCommiter, types::EtlResult};

pub static CHANNEL: Lazy<Channel> = Lazy::new(Channel::new);

pub struct Channel {
    pub result_tx: Sender<(Vec<EtlResult>, TopicCommiter)>,
}

impl Channel {
    pub fn new() -> Self {
        let (result_tx, _) = channel(100_000);
        Self { result_tx }
    }

    pub fn send_result(&self, result: Vec<EtlResult>, topic_commiter: TopicCommiter) {
        let result_tx = self.result_tx.clone();
        spawn(async move {
            result_tx
                .send((result, topic_commiter))
                .expect("Failed to send result");
        });
    }
}

impl Default for Channel {
    fn default() -> Self {
        Self::new()
    }
}
