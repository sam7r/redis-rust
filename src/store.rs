use std::{
    cmp,
    collections::HashMap,
    fmt,
    sync::{
        self,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
};

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

pub enum DataStoreError {
    LockError,
}

impl fmt::Display for DataStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataStoreError::LockError => write!(f, "Lock error"),
        }
    }
}

impl From<sync::PoisonError<sync::RwLockReadGuard<'_, HashMap<StringKey, Value>>>>
    for DataStoreError
{
    fn from(_: sync::PoisonError<sync::RwLockReadGuard<HashMap<StringKey, Value>>>) -> Self {
        DataStoreError::LockError
    }
}

impl From<sync::PoisonError<sync::RwLockWriteGuard<'_, HashMap<StringKey, Value>>>>
    for DataStoreError
{
    fn from(_: sync::PoisonError<sync::RwLockWriteGuard<HashMap<StringKey, Value>>>) -> Self {
        DataStoreError::LockError
    }
}

pub type StringKey = String;

#[derive(Clone)]
pub enum Value {
    String(String),
    List(Vec<String>),
}

impl cmp::PartialEq<String> for Value {
    fn eq(&self, other: &String) -> bool {
        match self {
            Value::String(s) => s == other,
            _ => false,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::String(s) => write!(f, "{}", s),
            Value::List(l) => write!(f, "{:?}", l),
        }
    }
}

#[allow(dead_code)]
#[derive(Clone)]
pub enum Event {
    Pushed(StringKey),
    Close(usize),
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Event::Pushed(key) => write!(f, "pushed to key: {}", key),
            Event::Close(_) => write!(f, "close event"),
        }
    }
}

type Subscribers = Vec<(usize, mpsc::Sender<Event>)>;

pub struct DataStore {
    data: sync::RwLock<HashMap<StringKey, Value>>,
    expires: sync::RwLock<HashMap<StringKey, u128>>,
    channels: sync::RwLock<HashMap<StringKey, Subscribers>>,
}

impl DataStore {
    pub fn new() -> Self {
        DataStore {
            data: sync::RwLock::new(HashMap::new()),
            expires: sync::RwLock::new(HashMap::new()),
            channels: sync::RwLock::new(HashMap::new()),
        }
    }

    pub fn set(
        &self,
        key: &str,
        value: String,
        _options: Vec<SetOption>,
    ) -> Result<Option<Value>, DataStoreError> {
        let mut ttl_options = Vec::new();
        let mut mod_options = Vec::new();

        _options.into_iter().for_each(|opt| match opt {
            SetOption::EX(_)
            | SetOption::PX(_)
            | SetOption::EXAT(_)
            | SetOption::PXAT(_)
            | SetOption::KEEPTTL => {
                ttl_options.push(opt);
            }
            SetOption::NX
            | SetOption::XX
            | SetOption::IFEQ(_)
            | SetOption::IFNE(_)
            | SetOption::IFDEQ(_)
            | SetOption::IFDNE(_)
            | SetOption::GET => {
                mod_options.push(opt);
            }
        });

        for option in ttl_options {
            match option {
                SetOption::EX(seconds) => {
                    let expire_time = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis()
                        + (seconds as u128 * 1000);
                    self.expires
                        .write()
                        .unwrap()
                        .insert(String::from(key), expire_time);
                }
                SetOption::EXAT(seconds) => {
                    self.expires
                        .write()
                        .unwrap()
                        .insert(String::from(key), seconds as u128 * 1000);
                }
                SetOption::PX(milliseconds) => {
                    let expire_time = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis()
                        + milliseconds;
                    self.expires
                        .write()
                        .unwrap()
                        .insert(String::from(key), expire_time);
                }
                SetOption::PXAT(milliseconds) => {
                    let expire_time = milliseconds;
                    self.expires
                        .write()
                        .unwrap()
                        .insert(String::from(key), expire_time);
                }
                SetOption::KEEPTTL => {
                    // Do nothing, retain existing TTL
                }
                _ => {}
            }
        }

        let mut data = self.data.write()?;

        for option in mod_options {
            match option {
                SetOption::NX => {
                    if data.contains_key(key) {
                        return Ok(None);
                    }
                }
                SetOption::XX => {
                    if !data.contains_key(key) {
                        return Ok(None);
                    }
                }
                SetOption::IFEQ(ifeq_value) => {
                    if let Some(current_value) = data.get(key) {
                        if current_value != &ifeq_value {
                            return Ok(None);
                        }
                    } else {
                        return Ok(None);
                    }
                }
                SetOption::IFNE(ifne_value) => {
                    if let Some(current_value) = data.get(key)
                        && current_value == &ifne_value
                    {
                        return Ok(None);
                    }
                }
                SetOption::IFDEQ(_ifeq_digest) => {}
                SetOption::IFDNE(_ifne_digest) => {}
                SetOption::GET => {}
                _ => {}
            }
        }

        data.insert(String::from(key), Value::String(value));

        Ok(Some(Value::String("OK".to_string())))
    }

    pub fn lrange(
        &self,
        key: &str,
        range: (i64, i64),
    ) -> Result<Option<Vec<String>>, DataStoreError> {
        let data = self.data.read()?;
        if let Some(value) = data.get(key) {
            match value {
                Value::List(list) => {
                    let (mut start, mut stop) = range;
                    let len = list.len() as i64;

                    // normalize negative indices
                    start = if start >= 0 { start } else { len + start };
                    stop = if stop < 0 { len + stop } else { stop };

                    if start < 0 {
                        start = 0;
                    }
                    if stop >= len {
                        stop = len - 1;
                    }

                    if start >= len || start > stop {
                        return Ok(Some(Vec::new()));
                    }

                    Ok(Some(list[start as usize..=stop as usize].to_vec()))
                }
                _ => Ok(None),
            }
        } else {
            Ok(Some(vec![]))
        }
    }

    pub fn rpush(&self, key: &str, value: Vec<String>) -> Result<Option<usize>, DataStoreError> {
        let mut value_list = value;
        let mut data = self.data.write()?;

        if let Some(existing_value) = data.get(key).cloned() {
            match existing_value {
                Value::List(mut list) => {
                    list.extend(value_list);
                    value_list = list;
                }
                _ => return Ok(None), // Key exists but is not a list
            };
        }

        let len = value_list.len();
        data.insert(String::from(key), Value::List(value_list));

        self.publish_to_subscribers(key, Event::Pushed(String::from(key)))?;

        Ok(Some(len))
    }

    pub fn lpush(&self, key: &str, value: Vec<String>) -> Result<Option<usize>, DataStoreError> {
        let mut value_list = value;
        value_list.reverse();
        let mut data = self.data.write()?;

        if let Some(existing_value) = data.get(key).cloned() {
            match existing_value {
                Value::List(list) => {
                    value_list.extend(list);
                }
                _ => return Ok(None), // Key exists but is not a list
            };
        }

        let len = value_list.len();
        data.insert(String::from(key), Value::List(value_list));

        self.publish_to_subscribers(key, Event::Pushed(String::from(key)))?;

        Ok(Some(len))
    }

    pub fn llen(&self, key: StringKey) -> Result<Option<usize>, DataStoreError> {
        let data = self.data.read()?;
        if let Some(value) = data.get(&key) {
            match value {
                Value::List(list) => Ok(Some(list.len())),
                _ => Ok(None), // Key exists but is not a list
            }
        } else {
            Ok(Some(0)) // Key does not exist, treat as empty list
        }
    }

    pub fn lpop(&self, key: &str, count: u8) -> Result<Option<Vec<String>>, DataStoreError> {
        let mut data = self.data.write()?;
        if let Some(value) = data.get(key).cloned() {
            match value {
                Value::List(mut list) => {
                    let mut split_from = count;
                    if count as usize > list.len() {
                        split_from = list.len() as u8;
                    }

                    let new_list = list.split_off(split_from as usize);
                    data.insert(String::from(key), Value::List(new_list));

                    self.publish_to_subscribers(key, Event::Pushed(String::from(key)))?;

                    Ok(Some(list))
                }
                _ => Ok(None), // Key exists but is not a list
            }
        } else {
            Ok(None) // Key does not exist
        }
    }

    pub fn blpop(
        &self,
        key: &str,
        wait_time_seconds: f32,
    ) -> Result<Option<Vec<String>>, DataStoreError> {
        if let Ok(v) = self.try_lpop_one(key)
            && let Some(value) = v
        {
            return Ok(Some(vec![value]));
        }

        let (sender, receiver) = mpsc::channel();

        match self.add_channel_subscriber(key, sender.clone()) {
            Ok(sub_id) => {
                if wait_time_seconds > 0f32 {
                    let closer = sender.clone();
                    thread::spawn(move || {
                        thread::sleep(std::time::Duration::from_secs_f32(wait_time_seconds));
                        let _ = closer.send(Event::Close(sub_id));
                    });
                }

                while let Ok(event) = receiver.recv() {
                    match event {
                        Event::Pushed(key) => {
                            if let Ok(v) = self.try_lpop_one(&key)
                                && let Some(value) = v
                            {
                                return Ok(Some(vec![value]));
                            }
                        }
                        Event::Close(id) => {
                            if id == sub_id {
                                let _ = self.remove_channel_subscriber(key, id);
                                return Ok(None);
                            }
                        }
                    }
                }
            }
            Err(_) => return Err(DataStoreError::LockError),
        }

        Ok(None)
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, DataStoreError> {
        let data = self.data.read()?;
        let value = data.get(key).cloned();

        if let Some(v) = value {
            match v {
                Value::String(result) => return Ok(Some(result)),
                _ => return Ok(None), // Key exists but is not a string
            }
        }

        Ok(None)
    }

    pub fn get_type(&self, key: &str) -> Result<Option<String>, DataStoreError> {
        let data = self.data.read()?;
        match data.get(key) {
            Some(v) => Ok(Some(value_type_as_str(v).to_string())),
            None => Ok(None),
        }
    }

    fn try_lpop_one(&self, key: &str) -> Result<Option<String>, DataStoreError> {
        match self.lpop(key, 1) {
            Ok(v) => match v {
                Some(list) => {
                    if !list.is_empty() {
                        Ok(Some(list[0].to_string()))
                    } else {
                        Ok(None)
                    }
                }
                None => Ok(None),
            },
            Err(_) => Ok(None),
        }
    }

    fn add_channel_subscriber(
        &self,
        key: &str,
        subscriber: mpsc::Sender<Event>,
    ) -> Result<usize, DataStoreError> {
        match self.channels.write() {
            Ok(mut channels) => {
                let id = get_id();
                channels
                    .entry(String::from(key))
                    .or_insert_with(Vec::new)
                    .push((id, subscriber));
                Ok(id)
            }
            Err(_) => Err(DataStoreError::LockError),
        }
    }

    fn remove_channel_subscriber(&self, key: &str, id: usize) -> Result<(), DataStoreError> {
        match self.channels.write() {
            Ok(mut channels) => {
                if let Some(subscribers) = channels.get_mut(key) {
                    subscribers.retain(|(sub_id, _)| *sub_id != id);
                }
                Ok(())
            }
            Err(_) => Err(DataStoreError::LockError),
        }
    }

    fn publish_to_subscribers(&self, key: &str, event: Event) -> Result<(), DataStoreError> {
        match self.channels.read() {
            Ok(channels) => {
                if let Some(subscribers) = channels.get(key) {
                    for (_, subscriber) in subscribers {
                        let _ = subscriber.send(event.clone());
                    }
                }
                Ok(())
            }
            Err(_) => Err(DataStoreError::LockError),
        }
    }

    pub fn cleanup(&self) {
        let expired_keys: Vec<String>;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        {
            // scope block to release read lock on expirable
            let expirable = self.expires.read().unwrap();
            expired_keys = expirable
                .iter()
                .filter(|(_, timestamp)| *timestamp <= &now)
                .map(|(key, _)| key.clone())
                .collect();
        }

        expired_keys.iter().for_each(|key| {
            let _ = self.del(key);
        });
    }

    fn del(&self, key: &str) -> Result<(), DataStoreError> {
        {
            let mut data = self.data.write()?;
            data.remove(key);
        }
        {
            let mut expirable = self.expires.write().unwrap();
            expirable.remove(key);
        }

        Ok(())
    }
}

fn get_id() -> usize {
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

fn value_type_as_str<'a>(v: &Value) -> &'a str {
    match v {
        Value::List(_) => "list",
        Value::String(_) => "string",
    }
}
