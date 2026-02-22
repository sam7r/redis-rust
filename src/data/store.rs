use glob_match::glob_match;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{self, mpsc},
    thread,
};

use crate::data::{
    errors::{DataStoreError, Error},
    types::{
        AddOption, Event, Score, SetOption, StreamEntry, StreamKey, StreamOption, StringKey,
        Subscribers, Value,
    },
    utils::{create_subscriber_id, parse_entry_id_add, parse_entry_id_query, value_type_as_str},
};

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
}

/**
 * Sorted Set commands:
 * ZADD key [NX|XX|LT|GT] score member [score member ...]
 * ZRANGE key start stop [WITHSCORES]
 * ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
 * ZREM key member [member ...]
 * ZCARD key
 * ZSCORE key member
 */
impl DataStore {
    pub fn zadd(
        &self,
        key: &str,
        options: Vec<AddOption>,
        score_members: Vec<(f64, String)>,
    ) -> Result<Option<usize>, Error> {
        let mut data = self
            .data
            .write()
            .map_err(|_| Error::from(DataStoreError::LockError))?;

        let has = |opt: &AddOption| options.contains(opt);

        match data.get_mut(key) {
            Some(Value::SortedSet(set)) => {
                let mut update_count = 0;

                for (score, member) in &score_members {
                    let new_score = Score::new(*score);

                    let existing_entry = set.iter().find(|(_, m)| *m == member);
                    let existing_score = existing_entry.map(|(s, _)| s.clone());

                    if existing_score.is_some() && has(&AddOption::NX) {
                        continue;
                    }
                    if existing_score.is_none() && has(&AddOption::XX) && !has(&AddOption::INCR) {
                        continue;
                    }

                    if let Some(ref existing) = existing_score {
                        if has(&AddOption::LT) && new_score >= *existing {
                            continue;
                        }
                        if has(&AddOption::GT) && new_score <= *existing {
                            continue;
                        }
                    }

                    if has(&AddOption::INCR) {
                        if let Some(existing) = existing_score {
                            let mut incremented = existing.clone();
                            incremented.add(*score);
                            set.remove(&existing);
                            set.insert(incremented, member.clone());
                        } else {
                            set.insert(new_score, member.clone());
                            update_count += 1;
                        }
                    } else {
                        if let Some(existing) = existing_score {
                            set.remove(&existing);
                        } else {
                            update_count += 1;
                        }
                        set.insert(new_score, member.clone());
                    }
                }

                Ok(Some(update_count))
            }
            None => {
                if has(&AddOption::XX) && !has(&AddOption::INCR) {
                    return Ok(None);
                }

                let count = score_members.len();
                let set: BTreeMap<Score, String> = score_members
                    .into_iter()
                    .map(|(score, member)| (Score::new(score), member))
                    .collect();

                data.insert(String::from(key), Value::SortedSet(set));
                Ok(Some(count))
            }
            Some(_) => Ok(None),
        }
    }
}

/**
 * Stream commands:
 * XADD key [entry-id] field value [field value ...]
 * XRANGE key start end [COUNT count]
 * XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
 */
impl DataStore {
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
            if entry_from == "$" {
                if let Some(response) = self.xstream_wait_for(&stream_key, None, count, block)? {
                    results.push((stream_key, Some(response)));
                };
                continue;
            }

            let (millis, seq_opt) = parse_entry_id_query(&entry_from, true)?;
            let entry_id = if block.is_some() {
                (millis, seq_opt.unwrap_or(0) + 1)
            } else {
                (millis, seq_opt.unwrap_or(0))
            };

            let entry_id_str = format!("{}-{}", entry_id.0, entry_id.1);

            if let Ok(result) = self.xrange(&stream_key, &entry_id_str, "+", count.unwrap_or(0)) {
                let no_result = result.is_none() || result.to_owned().is_some_and(|r| r.is_empty());
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

        let data = self
            .data
            .read()
            .map_err(|_| Error::from(DataStoreError::LockError))?;

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
        let mut data = self
            .data
            .write()
            .map_err(|_| Error::from(DataStoreError::LockError))?;

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
}

/**
 * String commands:
 * GET key
 * SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT timestamp|KEEPTTL] [NX|XX|IFEQ value|IFNE value|IFDEQ digest|IFDNE digest] [GET]
 * INCR key
 */
impl DataStore {
    pub fn get(&self, key: &str) -> Result<Option<String>, Error> {
        let data = self
            .data
            .read()
            .map_err(|_| Error::from(DataStoreError::LockError))?;

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
        let mut data = self
            .data
            .write()
            .map_err(|_| Error::from(DataStoreError::LockError))?;

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

    pub fn set_expire(&self, key: &str, expire_time_millis: u128) -> Result<(), Error> {
        self.expires
            .write()
            .map_err(|_| Error::from(DataStoreError::LockError))?
            .insert(String::from(key), expire_time_millis);
        self.publish_to_subscribers(key, Event::ExpireUpdated(expire_time_millis))?;
        Ok(())
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
                    self.set_expire(key, expire_time)?;
                }
                SetOption::EXAT(seconds) => {
                    self.set_expire(key, seconds as u128 * 1000)?;
                }
                SetOption::PX(milliseconds) => {
                    let expire_time = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis()
                        + milliseconds;
                    self.set_expire(key, expire_time)?;
                }
                SetOption::PXAT(expire_time) => {
                    self.set_expire(key, expire_time)?;
                }
                SetOption::KEEPTTL => {
                    // Do nothing, retain existing TTL
                }
                _ => {}
            }
        }

        let mut data = self
            .data
            .write()
            .map_err(|_| Error::from(DataStoreError::LockError))?;

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
}

/**
 * List commands:
 * LPUSH key value [value ...]
 * RPUSH key value [value ...]
 * LLEN key
 * LRANGE key start stop
 * LPOP key [count]
 * BLPOP key [count] [BLOCK wait_time_seconds]
 */
impl DataStore {
    pub fn lrange(&self, key: &str, range: (i64, i64)) -> Result<Option<Vec<String>>, Error> {
        let data = self
            .data
            .read()
            .map_err(|_| Error::from(DataStoreError::LockError))?;

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
        let mut data = self
            .data
            .write()
            .map_err(|_| Error::from(DataStoreError::LockError))?;

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
                _ => return Ok(None),
            };
        }

        let len = value_list.len();
        data.insert(String::from(key), Value::List(value_list));

        self.publish_to_subscribers(key, Event::PushedToList(String::from(key)))?;

        Ok(Some(len))
    }

    pub fn llen(&self, key: StringKey) -> Result<Option<usize>, Error> {
        let data = self
            .data
            .read()
            .map_err(|_| Error::from(DataStoreError::LockError))?;

        if let Some(value) = data.get(&key) {
            match value {
                Value::List(list) => Ok(Some(list.len())),
                _ => Ok(None),
            }
        } else {
            Ok(Some(0))
        }
    }

    pub fn lpop(&self, key: &str, count: u8) -> Result<Option<Vec<String>>, Error> {
        let mut data = self
            .data
            .write()
            .map_err(|_| Error::from(DataStoreError::LockError))?;

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
                _ => Ok(None),
            }
        } else {
            Ok(None)
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
}

/**
 * Other commands:
 * KEYS pattern
 * TYPE key
 *
 * includes:
 * - utility functions for RDB and cleaning up expired keys
 * - pub/sub functionality for notifying subscribers of changes to keys
 */
impl DataStore {
    pub fn keys(&self, query: &str) -> Result<Vec<String>, Error> {
        let data = self
            .data
            .read()
            .map_err(|_| Error::from(DataStoreError::LockError))?;

        let keys = data
            .keys()
            .filter(|k| glob_match(query, k))
            .cloned()
            .collect::<Vec<String>>();
        Ok(keys)
    }

    pub fn get_type(&self, key: &str) -> Result<Option<String>, Error> {
        let data = self
            .data
            .read()
            .map_err(|_| Error::from(DataStoreError::LockError))?;

        match data.get(key) {
            Some(v) => Ok(Some(value_type_as_str(v).to_string())),
            None => Ok(None),
        }
    }

    pub fn iter(&self) -> Result<DataStoreIter, Error> {
        let data = self
            .data
            .read()
            .map_err(|_| Error::from(DataStoreError::LockError))?;

        let expires = self
            .expires
            .read()
            .map_err(|_| Error::from(DataStoreError::LockError))?;

        Ok(DataStoreIter {
            data: data.clone(),
            expires: expires.clone(),
        })
    }

    pub fn cleanup(&self) -> Result<(), Error> {
        let expired_keys: Vec<String>;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        {
            // scope block to release read lock on expirable
            let expirable = self
                .expires
                .read()
                .map_err(|_| Error::from(DataStoreError::LockError))?;

            expired_keys = expirable
                .iter()
                .filter(|(_, timestamp)| *timestamp <= &now)
                .map(|(key, _)| key.clone())
                .collect();
        }

        expired_keys.iter().for_each(|key| {
            let _ = self.del(key);
        });

        Ok(())
    }

    fn del(&self, key: &str) -> Result<(), Error> {
        {
            let mut data = self
                .data
                .write()
                .map_err(|_| Error::from(DataStoreError::LockError))?;
            data.remove(key);
        }
        {
            let mut expirable = self
                .expires
                .write()
                .map_err(|_| Error::from(DataStoreError::LockError))?;

            expirable.remove(key);
        }

        Ok(())
    }

    pub fn add_channel_subscriber(
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

    pub fn remove_channel_subscriber(&self, key: &str, id: usize) -> Result<(), Error> {
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

    pub fn publish_to_subscribers(&self, key: &str, event: Event) -> Result<(), Error> {
        match self.channels.read() {
            Ok(channels) => {
                if let Some(subscribers) = channels.get(key) {
                    for (_, subscriber) in subscribers {
                        let _ = subscriber.send(event.clone());
                    }
                }
                if let Some(subscribers) = channels.get("*") {
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

#[derive(Clone)]
pub struct DataStoreIter {
    pub data: HashMap<StringKey, Value>,
    pub expires: HashMap<StringKey, u128>,
}

impl DataStoreIter {
    pub fn iter(&self) -> impl Iterator<Item = (&StringKey, &Value, Option<u128>)> {
        self.data.iter().map(|(k, v)| {
            let expiry = self.expires.get(k).copied();
            (k, v, expiry)
        })
    }
}
