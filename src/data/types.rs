use std::{
    cmp::{self, Eq, Ordering},
    collections::BTreeMap,
    sync::mpsc,
};

pub type StringKey = String;
pub type StreamKey = String;
pub type StreamEntryId = (u128, usize);
pub type StreamEntry = BTreeMap<StreamEntryId, Vec<(String, String)>>;
pub type SortedSet = BTreeMap<Score, String>;

#[derive(PartialEq, Clone, Debug)]
pub struct Score(f64, String);

impl Score {
    pub fn new(score: f64, member: String) -> Self {
        Score(score, member)
    }

    pub fn add_score(&mut self, delta: f64) {
        self.0 += delta;
    }

    pub fn get_score(&self) -> f64 {
        self.0
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.1.cmp(&other.1))
    }
}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Score {}

#[derive(Clone)]
pub enum Value {
    String(String),
    List(Vec<String>),
    Stream(StreamEntry),
    SortedSet(SortedSet),
}

impl cmp::PartialEq<String> for Value {
    fn eq(&self, other: &String) -> bool {
        match self {
            Value::String(s) => s == other,
            _ => false,
        }
    }
}

#[non_exhaustive]
#[derive(Clone)]
pub enum Event {
    PushedToList(StringKey),
    PushedToStream(StreamKey, StreamEntryId),
    ExpireUpdated(u128),
    CloseListener(usize),
}

pub type Subscribers = Vec<(usize, mpsc::Sender<Event>)>;

#[derive(Clone, Debug)]
pub enum StreamOption {
    Block(u64),
    Count(usize),
    Streams(Vec<(StreamKey, StringKey)>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::upper_case_acronyms)]
pub enum AddOption {
    NX,
    XX,
    LT,
    GT,
    CH,
    INCR,
}

impl AddOption {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "NX" => Some(AddOption::NX),
            "XX" => Some(AddOption::XX),
            "LT" => Some(AddOption::LT),
            "GT" => Some(AddOption::GT),
            "CH" => Some(AddOption::CH),
            "INCR" => Some(AddOption::INCR),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::upper_case_acronyms)]
pub enum SetOption {
    // Only set if the key does not exist
    NX,
    // Only set if the key already exists
    XX,
    // Set the key’s value and expiration only if its current value is equal to ifeq-value.
    // If the key doesn’t exist, it won’t be created.
    IFEQ(String),
    // Set the key’s value and expiration only if its current value is not equal to ifne-value.
    // If the key doesn’t exist, it will be created.
    IFNE(String),
    // Set the key’s value and expiration only if the hash digest of its current value is equal to ifeq-digest.
    // If the key doesn’t exist, it won’t be created.
    IFDEQ(String),
    // Set the key’s value and expiration only if the hash digest of its current value is equal to ifeq-digest.
    // If the key doesn’t exist, it won’t be created
    IFDNE(String),
    // Return the old string stored at key, or nil if key did not exist.
    // An error is returned and SET aborted if the value stored at key is not a string.
    GET,
    // Set the specified expire time, in seconds.
    EX(u64),
    // Set the specified expire time, in milliseconds.
    PX(u128),
    // Set the specified expire time, in seconds since the Unix epoch.
    EXAT(u64),
    // Set the specified expire time, in milliseconds since the Unix epoch
    PXAT(u128),
    // Retain the time to live associated with the key.
    KEEPTTL,
}
