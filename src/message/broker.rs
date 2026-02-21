use std::{
    collections::HashMap,
    sync::{
        RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use glob_match::glob_match;

use crate::message::types::{
    Message, Subscriber, SubscriberId, SubscriberRx, SubscriberTx, TopicFilter,
};

pub struct Broker {
    subscribers: RwLock<HashMap<SubscriberId, Subscriber>>,
    subscriptions: RwLock<HashMap<TopicFilter, Vec<SubscriberId>>>,
}

impl Broker {
    pub fn new() -> Self {
        Broker {
            subscribers: RwLock::new(HashMap::new()),
            subscriptions: RwLock::new(HashMap::new()),
        }
    }
}

impl MessageBroker for Broker {
    fn register_subscriber(
        &self,
    ) -> Result<(SubscriberId, SubscriberRx), Box<dyn std::error::Error>> {
        let id = create_subscriber_id();
        let (tx, rx) = std::sync::mpsc::channel();
        let subscriber = Subscriber {
            topics: Vec::new(),
            sender: SubscriberTx { channel: tx },
        };
        {
            let mut subs = match self.subscribers.write() {
                Ok(guard) => guard,
                Err(_) => return Err("Lock poisoned".into()),
            };
            subs.insert(id, subscriber);
        }
        println!("Registered subscriber with ID: {}", id);
        Ok((id, rx))
    }

    fn drop_subscriber(&self, id: SubscriberId) -> Result<(), Box<dyn std::error::Error>> {
        {
            let mut subs = match self.subscribers.write() {
                Ok(guard) => guard,
                Err(_) => return Err("Lock poisoned".into()),
            };
            subs.remove(&id);
        }
        println!("Dropped subscriber with ID: {}", id);
        Ok(())
    }

    fn subscribe(
        &self,
        id: SubscriberId,
        topic: TopicFilter,
    ) -> Result<Option<usize>, Box<dyn std::error::Error>> {
        {
            let mut subz = match self.subscriptions.write() {
                Ok(guard) => guard,
                Err(_) => return Err("Lock poisoned".into()),
            };
            let entry = subz.entry(topic.clone()).or_insert_with(Vec::new);
            if !entry.contains(&id) {
                entry.push(id);
                println!("Subscriber {} subscribed to topic {:?}", id, topic);
            }
        }
        {
            let mut subs = match self.subscribers.write() {
                Ok(guard) => guard,
                Err(_) => return Err("Lock poisoned".into()),
            };
            if let Some(sub) = subs.get_mut(&id) {
                if !sub.topics.contains(&topic) {
                    sub.topics.push(topic.clone());
                    println!("Subscriber {} added topic {:?} to their list", id, topic);
                }
                Ok(Some(sub.topics.len()))
            } else {
                Err("Subscriber not found".into())
            }
        }
    }

    fn unsubscribe(
        &self,
        id: SubscriberId,
        topic: TopicFilter,
    ) -> Result<Option<usize>, Box<dyn std::error::Error>> {
        {
            let mut subz = match self.subscriptions.write() {
                Ok(guard) => guard,
                Err(_) => return Err("Lock poisoned".into()),
            };
            if let Some(entry) = subz.get_mut(&topic) {
                entry.retain(|&sub_id| sub_id != id);
            }
            println!("Subscriber {} unsubscribed from topic {:?}", id, topic);
        }
        {
            let mut subs = match self.subscribers.write() {
                Ok(guard) => guard,
                Err(_) => return Err("Lock poisoned".into()),
            };
            if let Some(sub) = subs.get_mut(&id) {
                sub.topics.retain(|t| t != &topic);
                println!(
                    "Subscriber {} removed topic {:?} from their list",
                    id, topic
                );
                Ok(Some(sub.topics.len()))
            } else {
                Err("Subscriber not found".into())
            }
        }
    }

    fn unsubscribe_all(&self, id: SubscriberId) -> Result<(), Box<dyn std::error::Error>> {
        {
            let mut subz = match self.subscriptions.write() {
                Ok(guard) => guard,
                Err(_) => return Err("Lock poisoned".into()),
            };
            for entry in subz.values_mut() {
                entry.retain(|&sub_id| sub_id != id);
            }
            println!("Subscriber {} unsubscribed from all topics", id);
        }
        {
            let mut subs = match self.subscribers.write() {
                Ok(guard) => guard,
                Err(_) => return Err("Lock poisoned".into()),
            };
            if let Some(sub) = subs.get_mut(&id) {
                sub.topics.clear();
                println!("Subscriber {} cleared their topic list", id);
                Ok(())
            } else {
                Err("Subscriber not found".into())
            }
        }
    }

    fn publish(&self, message: Message) -> Result<Option<usize>, Box<dyn std::error::Error>> {
        let subz = match self.subscriptions.read() {
            Ok(guard) => guard,
            Err(_) => return Err("Lock poisoned".into()),
        };

        let mut published = 0;
        subz.iter()
            .filter(|(t, _)| match t {
                TopicFilter::Exact(ex) => ex == &message.topic,
                TopicFilter::Pattern(p) => glob_match(p, &message.topic),
            })
            .for_each(|(_, sub_ids)| {
                if let Ok(subscribers) = self.subscribers.read() {
                    let target: Vec<(&usize, &Subscriber)> = subscribers
                        .iter()
                        .filter(|(id, _)| sub_ids.contains(id))
                        .collect();

                    target.iter().for_each(|(_, subscriber)| {
                        println!(
                            "Publishing message on topic '{}' to subscriber with topics {:?}",
                            message.topic, subscriber.topics
                        );
                        let _ = subscriber.sender.channel.send(message.clone());
                        published += 1;
                    });
                }
            });
        Ok(Some(published))
    }
}

pub trait MessageBroker {
    fn publish(&self, message: Message) -> Result<Option<usize>, Box<dyn std::error::Error>>;
    fn unsubscribe_all(&self, id: SubscriberId) -> Result<(), Box<dyn std::error::Error>>;
    fn drop_subscriber(&self, id: SubscriberId) -> Result<(), Box<dyn std::error::Error>>;
    fn register_subscriber(
        &self,
    ) -> Result<(SubscriberId, SubscriberRx), Box<dyn std::error::Error>>;
    fn subscribe(
        &self,
        id: SubscriberId,
        topic: TopicFilter,
    ) -> Result<Option<usize>, Box<dyn std::error::Error>>;
    fn unsubscribe(
        &self,
        id: SubscriberId,
        topic: TopicFilter,
    ) -> Result<Option<usize>, Box<dyn std::error::Error>>;
}

fn create_subscriber_id() -> usize {
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
