use std::{cmp, collections, fmt, sync, time};

#[derive(Debug)]
#[allow(dead_code)]
#[allow(clippy::upper_case_acronyms)]
pub enum SetOption {
    NX, // Only set if the key does not exist
    XX, // Only set if the key already exists
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
    EX(u64),    // Set the specified expire time, in seconds.
    PX(u128),   // Set the specified expire time, in milliseconds.
    EXAT(u64),  // Set the specified expire time, in seconds since the Unix epoch.
    PXAT(u128), // Set the specified expire time, in milliseconds since the Unix epoch
    KEEPTTL,    // Retain the time to live associated with the key.
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

impl From<sync::PoisonError<sync::RwLockReadGuard<'_, collections::HashMap<String, Value>>>>
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
#[allow(dead_code)]
pub enum Value {
    String(String),
    List(Vec<String>),
    Set(collections::HashSet<String>),
    Hash(collections::HashMap<String, String>),
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
            Value::Set(s) => write!(f, "{:?}", s),
            Value::Hash(h) => write!(f, "{:?}", h),
        }
    }
}

pub struct DataStore {
    data: sync::RwLock<collections::HashMap<String, Value>>,
    expires: sync::RwLock<collections::HashMap<String, u128>>,
}

impl DataStore {
    pub fn new() -> Self {
        DataStore {
            data: sync::RwLock::new(collections::HashMap::new()),
            expires: sync::RwLock::new(collections::HashMap::new()),
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

        // TODO: handle digest comparisons for IFDEQ and IFDNE
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
        data.insert(key, Value::List(value_list));
        Ok(Some(len))
    }

    pub fn get(&self, key: &str) -> Result<Option<Value>, DataStoreError> {
        let data = self.data.read()?;
        let value = data.get(key).cloned();

        Ok(value)
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

pub enum CleanupType {
    Scheduled(time::Duration), // Cleanup every n milliseconds
}

pub struct Governor {
    datastore: sync::Arc<DataStore>,
    cleanup_type: CleanupType,
}

impl Governor {
    pub fn new(datastore: sync::Arc<DataStore>, cleanup_type: CleanupType) -> Self {
        Governor {
            datastore,
            cleanup_type,
        }
    }

    pub fn start_cleanup(self) {
        match self.cleanup_type {
            CleanupType::Scheduled(interval) => {
                std::thread::spawn(move || {
                    loop {
                        std::thread::sleep(interval);
                        self.datastore.cleanup();
                    }
                });
            }
        }
    }
}
