mod kafka;
mod ws;
pub use kafka::*;
pub use ws::*;

#[derive(Debug, Clone)]
pub enum Commiter {
    None,
    Kafka(TopicCommiter),
}

impl Default for Commiter {
    fn default() -> Self {
        Self::None
    }
}

impl From<()> for Commiter {
    fn from(_: ()) -> Self {
        Self::None
    }
}
