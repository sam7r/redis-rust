use std::{
    cmp, collections, fmt,
    sync::{self},
};

#[derive(Debug)]
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

impl From<sync::PoisonError<sync::RwLockReadGuard<'_, collections::HashMap<StringKey, Value>>>>
    for DataStoreError
{
    fn from(
        _: sync::PoisonError<sync::RwLockReadGuard<collections::HashMap<String, Value>>>,
    ) -> Self {
        DataStoreError::LockError
    }
}

impl From<sync::PoisonError<sync::RwLockWriteGuard<'_, collections::HashMap<String, Value>>>>
    for DataStoreError
{
    fn from(
        _: sync::PoisonError<sync::RwLockWriteGuard<collections::HashMap<String, Value>>>,
    ) -> Self {
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
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Event::Pushed(key) => write!(f, "pushed to key: {}", key),
        }
    }
}

pub struct DataStore {
    data: sync::RwLock<collections::HashMap<StringKey, Value>>,
    expires: sync::RwLock<collections::HashMap<StringKey, u128>>,
    channels: sync::RwLock<collections::HashMap<StringKey, Vec<sync::mpsc::Sender<Event>>>>,
}

impl DataStore {
    pub fn new() -> Self {
        DataStore {
            data: sync::RwLock::new(collections::HashMap::new()),
            expires: sync::RwLock::new(collections::HashMap::new()),
            channels: sync::RwLock::new(collections::HashMap::new()),
        }
    }

    pub fn set(
        &self,
        key: String,
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
                        .insert(key.clone(), expire_time);
                }
                SetOption::EXAT(seconds) => {
                    self.expires
                        .write()
                        .unwrap()
                        .insert(key.clone(), seconds as u128 * 1000);
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
                        .insert(key.clone(), expire_time);
                }
                SetOption::PXAT(milliseconds) => {
                    let expire_time = milliseconds;
                    self.expires
                        .write()
                        .unwrap()
                        .insert(key.clone(), expire_time);
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
                    if data.contains_key(&key) {
                        return Ok(None);
                    }
                }
                SetOption::XX => {
                    if !data.contains_key(&key) {
                        return Ok(None);
                    }
                }
                SetOption::IFEQ(ifeq_value) => {
                    if let Some(current_value) = data.get(&key) {
                        if current_value != &ifeq_value {
                            return Ok(None);
                        }
                    } else {
                        return Ok(None);
                    }
                }
                SetOption::IFNE(ifne_value) => {
                    if let Some(current_value) = data.get(&key)
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

        data.insert(key, Value::String(value));

        Ok(Some(Value::String("OK".to_string())))
    }

    pub fn lrange(
        &self,
        key: StringKey,
        range: (i64, i64),
    ) -> Result<Option<Vec<String>>, DataStoreError> {
        let data = self.data.read()?;
        if let Some(value) = data.get(&key) {
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

    pub fn rpush(
        &self,
        key: StringKey,
        value: Vec<String>,
    ) -> Result<Option<usize>, DataStoreError> {
        let mut value_list = value;
        let mut data = self.data.write()?;

        if let Some(existing_value) = data.get(&key).cloned() {
            match existing_value {
                Value::List(mut list) => {
                    list.extend(value_list);
                    value_list = list;
                }
                _ => return Ok(None), // Key exists but is not a list
            };
        }

        let len = value_list.len();
        data.insert(key.clone(), Value::List(value_list));

        self.publish_to_subscribers(key.clone(), Event::Pushed(key))?;

        Ok(Some(len))
    }

    pub fn lpush(
        &self,
        key: StringKey,
        value: Vec<String>,
    ) -> Result<Option<usize>, DataStoreError> {
        let mut value_list = value;
        value_list.reverse();
        let mut data = self.data.write()?;

        if let Some(existing_value) = data.get(&key).cloned() {
            match existing_value {
                Value::List(list) => {
                    value_list.extend(list);
                }
                _ => return Ok(None), // Key exists but is not a list
            };
        }

        let len = value_list.len();
        data.insert(key.clone(), Value::List(value_list));

        self.publish_to_subscribers(key.clone(), Event::Pushed(key))?;

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

    pub fn lpop(&self, key: StringKey, count: u8) -> Result<Option<Vec<String>>, DataStoreError> {
        let mut data = self.data.write()?;
        if let Some(value) = data.get(&key).cloned() {
            match value {
                Value::List(mut list) => {
                    let mut split_from = count;
                    if count as usize > list.len() {
                        split_from = list.len() as u8;
                    }

                    let new_list = list.split_off(split_from as usize);
                    data.insert(key.clone(), Value::List(new_list));

                    self.publish_to_subscribers(key.clone(), Event::Pushed(key))?;

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
        key: StringKey,
        wait_time_seconds: u64,
    ) -> Result<Option<Vec<String>>, DataStoreError> {
        if wait_time_seconds != 0 {
            // TODO: handle timeout
        }

        // early return if value is already available
        if let Ok(v) = self.lpop(key.clone(), 1)
            && let Some(value) = v
            && !value.is_empty()
        {
            return Ok(Some(value));
        }

        let (sender, receiver) = sync::mpsc::channel();
        match self.add_channel_subscriber(key, sender) {
            Ok(_) => {
                if let Ok(event) = receiver.recv() {
                    match event {
                        Event::Pushed(key) => {
                            return self.lpop(key, 1);
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

    pub fn add_channel_subscriber(
        &self,
        key: StringKey,
        subscriber: sync::mpsc::Sender<Event>,
    ) -> Result<(), DataStoreError> {
        match self.channels.write() {
            Ok(mut channels) => {
                channels
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .push(subscriber);
                Ok(())
            }
            Err(_) => Err(DataStoreError::LockError),
        }
    }

    pub fn publish_to_subscribers(
        &self,
        key: StringKey,
        event: Event,
    ) -> Result<(), DataStoreError> {
        match self.channels.read() {
            Ok(channels) => {
                if let Some(subscribers) = channels.get(&key) {
                    for subscriber in subscribers {
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
