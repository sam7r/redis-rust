use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::SystemTime,
};

use crate::data::{
    errors::{DataStoreError, Error},
    types::Value,
};

pub fn create_subscriber_id() -> usize {
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub fn value_type_as_str<'a>(v: &Value) -> &'a str {
    match v {
        Value::List(_) => "list",
        Value::String(_) => "string",
        Value::Stream(_) => "stream",
        Value::SortedSet(_) => "sortedset",
    }
}

pub fn parse_entry_id_query(
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

pub fn parse_entry_id_add(entry_id_str: &str) -> Result<(u128, Option<usize>), Error> {
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
