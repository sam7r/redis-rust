use std::{
    cmp,
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{
        self,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::SystemTime,
};

pub enum StreamOption {
    Block(u64),
    Count(usize),
    Streams(Vec<(StreamKey, StringKey)>),
}

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

pub struct Error {
    error: DataStoreError,
}

impl Error {
    fn from(t: DataStoreError) -> Self {
        Error { error: t }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

pub enum DataStoreError {
    LockError,
    StringNotNumber,
    InvalidStreamEntryId,
    StreamEntryIdMustBeGreaterThan(String),
    StreamEntryIdLessThanLastEntry,
    UnexpectedEmptyStream,
}

impl fmt::Display for DataStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataStoreError::LockError => write!(f, "ERR Failed to access data"),
            DataStoreError::InvalidStreamEntryId => write!(f, "ERR Invalid stream entry ID"),
            DataStoreError::StringNotNumber => {
                write!(f, "ERR value is not an integer or out of range")
            }
            DataStoreError::UnexpectedEmptyStream => {
                write!(f, "ERR Encountered unexpected empty stream")
            }
            DataStoreError::StreamEntryIdLessThanLastEntry => {
                write!(
                    f,
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                )
            }
            DataStoreError::StreamEntryIdMustBeGreaterThan(v) => {
                write!(f, "ERR The ID specified in XADD must be greater than {v}")
            }
        }
    }
}

impl From<sync::PoisonError<sync::RwLockReadGuard<'_, HashMap<StringKey, Value>>>> for Error {
    fn from(_: sync::PoisonError<sync::RwLockReadGuard<HashMap<StringKey, Value>>>) -> Self {
        Error::from(DataStoreError::LockError)
    }
}

impl From<sync::PoisonError<sync::RwLockWriteGuard<'_, HashMap<StringKey, Value>>>> for Error {
    fn from(_: sync::PoisonError<sync::RwLockWriteGuard<HashMap<StringKey, Value>>>) -> Self {
        Error::from(DataStoreError::LockError)
    }
}

pub type StringKey = String;
pub type StreamKey = String;
pub type StreamEntryId = (u128, usize);
pub type StreamEntry = BTreeMap<StreamEntryId, Vec<(String, String)>>;

#[derive(Clone)]
pub enum Value {
    String(String),
    List(Vec<String>),
    Stream(StreamEntry),
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
    CloseListener(usize),
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

    pub fn xread(
        &self,
        options: Vec<StreamOption>,
    ) -> Result<Vec<(StreamKey, Option<StreamEntry>)>, Error> {
        let mut block: Option<u64> = None;
        let mut count: Option<usize> = None;
        let mut streams: Vec<(StreamKey, StringKey)> = Vec::new();

        options.into_iter().for_each(|opt| match opt {
            StreamOption::Block(n) => {
                block = Some(n);
            }
            StreamOption::Count(c) => {
                count = Some(c);
            }
            StreamOption::Streams(s) => {
                streams = s;
            }
        });

        let mut results = Vec::new();
        for (stream_key, entry_from) in streams {
            // special case, block for any new entry
            if entry_from == "$" {
                if let Some(response) = self.xstream_wait_for(&stream_key, None, count, block)? {
                    results.push((stream_key, Some(response)));
                };
                continue;
            }

            let (millis, seq_opt) = parse_entry_id_query(&entry_from, true)?;
            // if block is set, increment the sequence number
            let entry_id = if block.is_some() {
                (millis, seq_opt.unwrap_or(0) + 1)
            } else {
                (millis, seq_opt.unwrap_or(0))
            };

            let entry_id_str = format!("{}-{}", entry_id.0, entry_id.1);

            if let Ok(result) = self.xrange(&stream_key, &entry_id_str, "+", count.unwrap_or(0)) {
                let no_result = result.is_none() || result.to_owned().is_some_and(|r| r.is_empty());
                // if no result and block is set, wait for the result
                if block.is_some() && no_result {
                    if let Some(response) =
                        self.xstream_wait_for(&stream_key, Some(entry_id), count, block)?
                    {
                        results.push((stream_key, Some(response)));
                    }
                } else {
                    results.push((stream_key, result));
                }
            }
        }

        Ok(results)
    }

    pub fn xrange(
        &self,
        key: &str,
        entry_id_start_str: &str,
        entry_id_stop_str: &str,
        limit: usize,
    ) -> Result<Option<StreamEntry>, Error> {
        let (entry_id_start_millis, entry_id_start_seq) =
            parse_entry_id_query(entry_id_start_str, true)?;

        let (entry_id_stop_millis, entry_id_stop_seq) =
            parse_entry_id_query(entry_id_stop_str, false)?;

        let data = self.data.read()?;
        match data.get(key) {
            Some(Value::Stream(stream)) => {
                let start_entry_id = (entry_id_start_millis, entry_id_start_seq.unwrap_or(0));
                let stop_entry_id = (
                    entry_id_stop_millis,
                    entry_id_stop_seq.unwrap_or(stream.len()),
                );
                let take = if limit == 0 { usize::MAX } else { limit };

                let range: StreamEntry = stream
                    .range(start_entry_id..=stop_entry_id)
                    .take(take)
                    .map(|(&k, v)| (k, v.clone()))
                    .collect::<StreamEntry>();

                Ok(Some(range))
            }
            Some(_) | None => Ok(None),
        }
    }

    pub fn xadd(
        &self,
        key: &str,
        entry_id_str: &str,
        fields: Vec<(String, String)>,
    ) -> Result<Option<String>, Error> {
        let (entry_millis, entry_seq_opt) = parse_entry_id_add(entry_id_str)?;
        let mut data = self.data.write()?;

        let Some(value) = data.get(key).cloned() else {
            let entry_seq = entry_seq_opt.unwrap_or(if entry_millis == 0 { 1 } else { 0 });
            let mut stream = BTreeMap::new();

            let entry_id = (entry_millis, entry_seq);
            stream.insert(entry_id, fields);
            data.insert(String::from(key), Value::Stream(stream));

            self.publish_to_subscribers(key, Event::PushedToStream(String::from(key), entry_id))?;
            return Ok(Some(format!("{}-{}", entry_millis, entry_seq)));
        };

        match value {
            Value::Stream(mut stream) => {
                let Some((last_entry_id, _)) = stream.iter().next_back() else {
                    return Err(Error::from(DataStoreError::UnexpectedEmptyStream));
                };
                if entry_millis < last_entry_id.0 {
                    return Err(Error::from(DataStoreError::StreamEntryIdLessThanLastEntry));
                }
                let entry_seq = entry_seq_opt.unwrap_or(
                    if entry_millis == last_entry_id.0 || entry_millis == 0 {
                        last_entry_id.1 + 1
                    } else {
                        0
                    },
                );
                if entry_millis == last_entry_id.0 && entry_seq <= last_entry_id.1 {
                    return Err(Error::from(DataStoreError::StreamEntryIdLessThanLastEntry));
                }

                let entry_id = (entry_millis, entry_seq);
                stream.insert(entry_id, fields);
                data.insert(String::from(key), Value::Stream(stream));

                self.publish_to_subscribers(
                    key,
                    Event::PushedToStream(String::from(key), entry_id),
                )?;
                Ok(Some(format!("{}-{}", entry_millis, entry_seq)))
            }
            _ => Ok(None),
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, Error> {
        let data = self.data.read()?;
        let value = data.get(key).cloned();

        if let Some(v) = value {
            match v {
                Value::String(result) => return Ok(Some(result)),
                _ => return Ok(None),
            }
        }

        Ok(None)
    }

    pub fn incr(&self, key: &str) -> Result<Option<i64>, Error> {
        let mut data = self.data.write()?;
        let value = data.get(key);
        let mut incr = 1;

        if let Some(v) = value {
            match v {
                Value::String(result) => match result.parse::<i64>() {
                    Ok(v) => {
                        incr += v;
                    }
                    Err(_) => {
                        return Err(Error::from(DataStoreError::StringNotNumber));
                    }
                },
                _ => return Ok(None),
            }
        }

        data.insert(String::from(key), Value::String(incr.to_string()));
        Ok(Some(incr))
    }

    pub fn set(
        &self,
        key: &str,
        value: String,
        _options: Vec<SetOption>,
    ) -> Result<Option<String>, Error> {
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

        Ok(Some("OK".to_string()))
    }

    pub fn lrange(&self, key: &str, range: (i64, i64)) -> Result<Option<Vec<String>>, Error> {
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

    pub fn push(&self, key: &str, value: Vec<String>, lpush: bool) -> Result<Option<usize>, Error> {
        let mut value_list = value;
        let mut data = self.data.write()?;

        if let Some(existing_value) = data.get(key).cloned() {
            match existing_value {
                Value::List(mut list) => {
                    if lpush {
                        value_list.reverse();
                        value_list.extend(list);
                    } else {
                        list.extend(value_list);
                        value_list = list;
                    }
                }
                _ => return Ok(None), // Key exists but is not a list
            };
        }

        let len = value_list.len();
        data.insert(String::from(key), Value::List(value_list));

        self.publish_to_subscribers(key, Event::PushedToList(String::from(key)))?;

        Ok(Some(len))
    }

    pub fn llen(&self, key: StringKey) -> Result<Option<usize>, Error> {
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

    pub fn lpop(&self, key: &str, count: u8) -> Result<Option<Vec<String>>, Error> {
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

                    self.publish_to_subscribers(key, Event::PushedToList(String::from(key)))?;

                    Ok(Some(list))
                }
                _ => Ok(None), // Key exists but is not a list
            }
        } else {
            Ok(None) // Key does not exist
        }
    }

    pub fn blpop(&self, key: &str, wait_time_seconds: f32) -> Result<Option<Vec<String>>, Error> {
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
                        let _ = closer.send(Event::CloseListener(sub_id));
                    });
                }

                while let Ok(event) = receiver.recv() {
                    match event {
                        Event::PushedToList(key) => {
                            if let Ok(v) = self.try_lpop_one(&key)
                                && let Some(value) = v
                            {
                                self.remove_channel_subscriber(&key, sub_id)?;
                                return Ok(Some(vec![value]));
                            }
                        }
                        Event::CloseListener(id) => {
                            if id == sub_id {
                                self.remove_channel_subscriber(key, id)?;
                                return Ok(None);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(_) => return Err(Error::from(DataStoreError::LockError)),
        }

        Ok(None)
    }

    pub fn get_type(&self, key: &str) -> Result<Option<String>, Error> {
        let data = self.data.read()?;
        match data.get(key) {
            Some(v) => Ok(Some(value_type_as_str(v).to_string())),
            None => Ok(None),
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

    fn xstream_wait_for(
        &self,
        stream_key: &str,
        entry_id: Option<(u128, usize)>,
        count: Option<usize>,
        block: Option<u64>,
    ) -> Result<Option<StreamEntry>, Error> {
        let (sender, receiver) = mpsc::channel();

        match self.add_channel_subscriber(stream_key, sender.clone()) {
            Ok(sub_id) => {
                // 0 will block indefinitely, otherwise setup timed closer
                if block.unwrap_or(0) > 0 {
                    let closer = sender.clone();
                    thread::spawn(move || {
                        thread::sleep(std::time::Duration::from_millis(block.unwrap_or(0)));
                        let _ = closer.send(Event::CloseListener(sub_id));
                    });
                }

                while let Ok(event) = receiver.recv() {
                    match event {
                        Event::PushedToStream(key, (millis, seq)) => {
                            if key != stream_key {
                                continue;
                            }

                            let take_entry = if let Some((wait_millis, wait_seq)) = entry_id {
                                millis > wait_millis || (wait_millis == millis && seq >= wait_seq)
                            } else {
                                true
                            };

                            // if no entry id is provided, take the pushed
                            let stream_entry_id = if let Some((wait_millis, wait_seq)) = entry_id {
                                format!("{}-{}", wait_millis, wait_seq)
                            } else {
                                format!("{}-{}", millis, seq)
                            };

                            if take_entry
                                && let Ok(v) = self.xrange(
                                    stream_key,
                                    &stream_entry_id,
                                    "+", // get max
                                    count.unwrap_or(0),
                                )
                            {
                                self.remove_channel_subscriber(stream_key, sub_id)?;
                                return Ok(v);
                            }
                        }
                        Event::CloseListener(id) => {
                            if id == sub_id {
                                self.remove_channel_subscriber(stream_key, id)?;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(_) => return Err(Error::from(DataStoreError::LockError)),
        }
        Ok(None)
    }

    fn del(&self, key: &str) -> Result<(), Error> {
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

    fn try_lpop_one(&self, key: &str) -> Result<Option<String>, Error> {
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
    ) -> Result<usize, Error> {
        match self.channels.write() {
            Ok(mut channels) => {
                let id = create_subscriber_id();
                channels
                    .entry(String::from(key))
                    .or_insert_with(Vec::new)
                    .push((id, subscriber));
                Ok(id)
            }
            Err(_) => Err(Error::from(DataStoreError::LockError)),
        }
    }

    fn remove_channel_subscriber(&self, key: &str, id: usize) -> Result<(), Error> {
        match self.channels.write() {
            Ok(mut channels) => {
                if let Some(subscribers) = channels.get_mut(key) {
                    subscribers.retain(|(sub_id, _)| *sub_id != id);
                }
                Ok(())
            }
            Err(_) => Err(Error::from(DataStoreError::LockError)),
        }
    }

    fn publish_to_subscribers(&self, key: &str, event: Event) -> Result<(), Error> {
        match self.channels.read() {
            Ok(channels) => {
                if let Some(subscribers) = channels.get(key) {
                    for (_, subscriber) in subscribers {
                        let _ = subscriber.send(event.clone());
                    }
                }
                Ok(())
            }
            Err(_) => Err(Error::from(DataStoreError::LockError)),
        }
    }
}

fn create_subscriber_id() -> usize {
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

fn value_type_as_str<'a>(v: &Value) -> &'a str {
    match v {
        Value::List(_) => "list",
        Value::String(_) => "string",
        Value::Stream(_) => "stream",
    }
}

fn parse_entry_id_query(
    entry_id_str: &str,
    is_start: bool,
) -> Result<(u128, Option<usize>), Error> {
    if entry_id_str == "$" {
        return Ok((0, None));
    }
    let entry_id_str = if is_start && entry_id_str == "-" {
        "0-0"
    } else if !is_start && entry_id_str == "+" {
        &format!("{}-{}", u128::MAX, usize::MAX)
    } else {
        entry_id_str
    };
    let entry_id_str_split: Vec<&str> = entry_id_str.split("-").collect();

    if entry_id_str_split.is_empty() {
        return Err(Error::from(DataStoreError::InvalidStreamEntryId));
    }
    let Ok(entry_id_millis) = entry_id_str_split[0].parse::<u128>() else {
        return Err(Error::from(DataStoreError::InvalidStreamEntryId));
    };
    if entry_id_str_split.len() > 1 {
        let Ok(entry_id_seq) = entry_id_str_split[1].parse::<usize>() else {
            return Err(Error::from(DataStoreError::InvalidStreamEntryId));
        };
        Ok((entry_id_millis, Some(entry_id_seq)))
    } else {
        Ok((entry_id_millis, None))
    }
}

fn parse_entry_id_add(entry_id_str: &str) -> Result<(u128, Option<usize>), Error> {
    if entry_id_str == "*" {
        let entry_seq: usize = 0;
        let entry_millis: u128 = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        Ok((entry_millis, Some(entry_seq)))
    } else {
        let entry_id_str_split: Vec<&str> = entry_id_str.split('-').collect();
        if entry_id_str_split.len() != 2 {
            return Err(Error::from(DataStoreError::InvalidStreamEntryId));
        }

        let Some(entry_millis) = entry_id_str_split[0].parse::<u128>().ok() else {
            return Err(Error::from(DataStoreError::InvalidStreamEntryId));
        };

        if entry_id_str_split[1] == "*" {
            return Ok((entry_millis, None));
        }

        let Some(entry_seq) = entry_id_str_split[1].parse::<usize>().ok() else {
            return Err(Error::from(DataStoreError::InvalidStreamEntryId));
        };

        if entry_millis == 0 && entry_seq == 0 {
            return Err(Error::from(DataStoreError::StreamEntryIdMustBeGreaterThan(
                "0-0".to_string(),
            )));
        }

        Ok((entry_millis, Some(entry_seq)))
    }
}
