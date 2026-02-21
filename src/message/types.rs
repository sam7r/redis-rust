use std::sync::mpsc;

pub type SubscriberId = usize;

#[derive(Clone, Debug)]
pub struct Message {
    pub topic: String,
    pub payload: String,
}

pub struct Subscriber {
    pub topics: Vec<TopicFilter>,
    pub sender: SubscriberTx,
}

pub struct SubscriberTx {
    pub channel: mpsc::Sender<Message>,
}

pub type SubscriberRx = mpsc::Receiver<Message>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TopicFilter {
    Exact(String),
    Pattern(String),
}
