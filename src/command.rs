use super::governor::types::Info;
use super::resp::{DataType, RespBuilder, RespParser};
use super::store::{SetOption, StreamKey, StreamOption, StringKey};

#[derive(Clone, Debug)]
pub enum Command {
    // server
    Ping,
    Echo(String),
    Info(Vec<Info>),
    ReplConf(String, String),
    Psync(String, i64),
    Wait(u8, u64),
    ConfigGet(Vec<String>),
    // store
    BgSave,
    Keys(String),
    Type(StringKey),
    // string
    Set(StringKey, String, Vec<SetOption>),
    Get(StringKey),
    Incr(StringKey),
    // list
    Rpush(StringKey, Vec<String>),
    Lpush(StringKey, Vec<String>),
    Lrange(StringKey, i64, i64),
    Llen(StringKey),
    Lpop(StringKey, u8),
    Blpop(StringKey, f32),
    // stream
    Xadd(StreamKey, StringKey, Vec<(String, String)>),
    Xrange(StreamKey, StringKey, StringKey),
    Xread(Vec<StreamOption>),
    // transaction
    Multi,
    Exec,
    Discard,
    // pub/sub
    Publish(String, String),
    Subscribe(Vec<String>),
    Psubscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    Punsubscribe(Vec<String>),
    Reset,
    Quit,
}

pub fn prepare_commands(data: &str) -> Vec<(Option<Command>, usize)> {
    let mut commands = Vec::new();
    let mut parser = RespParser::new(data);
    let mut prev_cursor = 0;

    while parser.cursor < parser.data.len() {
        let cmd = prepare_command_with_parser(&mut parser);
        let size = parser.cursor - prev_cursor;
        commands.push((cmd, size));
        prev_cursor = parser.cursor;
    }
    commands
}

pub fn prepare_command(data: &str) -> Option<Command> {
    prepare_command_with_parser(&mut RespParser::new(data))
}

pub fn prepare_command_with_parser(parser: &mut RespParser) -> Option<Command> {
    match DataType::from_char(parser.read_char().unwrap_or('\0')) {
        DataType::Array => {
            let elements: u8 = parser.read_line()?.parse().ok()?;
            let mut command_parts = Vec::new();

            for _ in 0..elements {
                let datatype_char = parser.read_char()?;
                let datatype = DataType::from_char(datatype_char);

                if let DataType::BulkString = datatype {
                    let len: usize = parser.read_line()?.parse().ok()?;
                    command_parts.push(parser.read_str(len)?);
                } else {
                    return None; // Unexpected data type
                }
            }

            if command_parts.is_empty() {
                return None;
            }

            let command_name = command_parts[0].to_uppercase();
            match command_name.as_str() {
                "INFO" => {
                    let mut sections: Vec<&str> = command_parts.iter().skip(1).cloned().collect();
                    if sections.is_empty() {
                        sections.push("default");
                    }
                    let info_options = prepare_info_options(sections);
                    Some(Command::Info(info_options))
                }
                "PING" => Some(Command::Ping),
                "ECHO" => {
                    if command_parts.len() >= 2 {
                        Some(Command::Echo(command_parts[1].to_string()))
                    } else {
                        None
                    }
                }
                "TYPE" => {
                    if command_parts.len() >= 2 {
                        Some(Command::Type(StringKey::from(command_parts[1])))
                    } else {
                        None
                    }
                }
                "SET" => {
                    if command_parts.len() >= 3 {
                        let mut options = Vec::new();
                        if command_parts.len() > 3 {
                            options = prepare_set_options(command_parts[3..].to_vec());
                        }
                        Some(Command::Set(
                            StringKey::from(command_parts[1]),
                            String::from(command_parts[2]),
                            options,
                        ))
                    } else {
                        None
                    }
                }
                "GET" => {
                    if command_parts.len() >= 2 {
                        Some(Command::Get(StringKey::from(command_parts[1])))
                    } else {
                        None
                    }
                }
                "INCR" => {
                    if command_parts.len() >= 2 {
                        Some(Command::Incr(StringKey::from(command_parts[1])))
                    } else {
                        None
                    }
                }
                "RPUSH" => {
                    if command_parts.len() >= 3 {
                        let list = command_parts[2..].iter().map(|&s| s.into()).collect();
                        Some(Command::Rpush(StringKey::from(command_parts[1]), list))
                    } else {
                        None
                    }
                }
                "LPUSH" => {
                    if command_parts.len() >= 3 {
                        let list = command_parts[2..].iter().map(|&s| s.into()).collect();
                        Some(Command::Lpush(StringKey::from(command_parts[1]), list))
                    } else {
                        None
                    }
                }
                "LLEN" => {
                    if command_parts.len() >= 2 {
                        Some(Command::Llen(StringKey::from(command_parts[1])))
                    } else {
                        None
                    }
                }
                "LRANGE" => {
                    if command_parts.len() >= 4 {
                        let start: i64 = command_parts[2].parse().ok()?;
                        let stop: i64 = command_parts[3].parse().ok()?;
                        Some(Command::Lrange(
                            StringKey::from(command_parts[1]),
                            start,
                            stop,
                        ))
                    } else {
                        None
                    }
                }
                "LPOP" => {
                    if command_parts.len() >= 2 {
                        let mut count = 1;
                        if let Some(pop_count) = command_parts.get(2) {
                            count = pop_count.parse().ok()?;
                        }
                        Some(Command::Lpop(StringKey::from(command_parts[1]), count))
                    } else {
                        None
                    }
                }
                "BLPOP" => {
                    if command_parts.len() >= 3 {
                        Some(Command::Blpop(
                            StringKey::from(command_parts[1]),
                            command_parts[2].parse().ok()?,
                        ))
                    } else {
                        None
                    }
                }
                "XADD" => {
                    if command_parts.len() >= 4 {
                        let stream_key = StreamKey::from(command_parts[1]);
                        let entry_id = String::from(command_parts[2]);
                        let mut fields = Vec::new();
                        for i in (3..command_parts.len()).step_by(2) {
                            if let Some(field) = command_parts.get(i)
                                && let Some(value) = command_parts.get(i + 1)
                            {
                                fields.push((field.to_string(), value.to_string()));
                            } else {
                                return None; // Invalid field-value pair
                            }
                        }
                        Some(Command::Xadd(stream_key, entry_id, fields))
                    } else {
                        None
                    }
                }
                "XRANGE" => {
                    if command_parts.len() >= 4 {
                        let stream_key = StreamKey::from(command_parts[1]);
                        let entry_id_start = String::from(command_parts[2]);
                        let entry_id_stop = String::from(command_parts[3]);
                        Some(Command::Xrange(stream_key, entry_id_start, entry_id_stop))
                    } else {
                        None
                    }
                }
                "XREAD" => {
                    if command_parts.len() >= 4 {
                        let options = prepare_stream_options(command_parts[1..].to_vec());
                        Some(Command::Xread(options))
                    } else {
                        None
                    }
                }
                "MULTI" => Some(Command::Multi),
                "EXEC" => Some(Command::Exec),
                "DISCARD" => Some(Command::Discard),
                "REPLCONF" => {
                    if command_parts.len() >= 3 {
                        Some(Command::ReplConf(
                            command_parts[1].to_string(),
                            command_parts[2].to_string(),
                        ))
                    } else {
                        None
                    }
                }
                "PSYNC" => {
                    if command_parts.len() >= 3 {
                        let offset = command_parts[2].parse::<i64>().ok()?;
                        Some(Command::Psync(command_parts[1].to_string(), offset))
                    } else {
                        None
                    }
                }
                "WAIT" => {
                    if command_parts.len() >= 3 {
                        let replica_count = command_parts[1].parse::<u8>().ok()?;
                        let wait_time = command_parts[2].parse::<u64>().ok()?;
                        Some(Command::Wait(replica_count, wait_time))
                    } else {
                        None
                    }
                }
                "CONFIG" => {
                    if command_parts.len() >= 3 {
                        match command_parts[1].to_uppercase().as_str() {
                            "GET" => {
                                let list = command_parts[2..].iter().map(|&s| s.into()).collect();
                                Some(Command::ConfigGet(list))
                            }
                            _ => None,
                        }
                    } else {
                        None
                    }
                }
                "BGSAVE" => Some(Command::BgSave),
                "KEYS" => {
                    if command_parts.len() >= 2 {
                        Some(Command::Keys(command_parts[1].to_string()))
                    } else {
                        None
                    }
                }
                "PUBLISH" => {
                    if command_parts.len() >= 3 {
                        Some(Command::Publish(
                            command_parts[1].to_string(),
                            command_parts[2].to_string(),
                        ))
                    } else {
                        None
                    }
                }
                "SUBSCRIBE" => {
                    if command_parts.len() >= 2 {
                        let channels = command_parts[1..].iter().map(|&c| c.into()).collect();
                        Some(Command::Subscribe(channels))
                    } else {
                        None
                    }
                }
                "PSUBSCRIBE" => {
                    if command_parts.len() >= 2 {
                        let channels = command_parts[1..].iter().map(|&c| c.into()).collect();
                        Some(Command::Psubscribe(channels))
                    } else {
                        None
                    }
                }
                "UNSUBSCRIBE" => {
                    if command_parts.len() >= 2 {
                        let channels = command_parts[1..].iter().map(|&c| c.into()).collect();
                        Some(Command::Unsubscribe(channels))
                    } else {
                        None
                    }
                }
                "PUNSUBSCRIBE" => {
                    if command_parts.len() >= 2 {
                        let channel_patterns =
                            command_parts[1..].iter().map(|&c| c.into()).collect();
                        Some(Command::Punsubscribe(channel_patterns))
                    } else {
                        None
                    }
                }
                "RESET" => Some(Command::Reset),
                "QUIT" => Some(Command::Quit),

                _ => None,
            }
        }
        _ => {
            println!(
                "received unsupported input data type: {}",
                parser.data[parser.cursor..].trim()
            );
            None
        }
    }
}

fn prepare_stream_options(args: Vec<&str>) -> Vec<StreamOption> {
    let mut options = Vec::new();
    if args.is_empty() {
        return options;
    }

    let mut iter = args.iter().peekable();

    while let Some(opt) = iter.next() {
        let upper_opt = opt.to_uppercase();

        match upper_opt.as_str() {
            "COUNT" => {
                if let Some(next_arg) = iter.peek()
                    && let Ok(n) = next_arg.parse::<usize>()
                {
                    options.push(StreamOption::Count(n));
                    iter.next();
                }
            }
            "BLOCK" => {
                if let Some(next_arg) = iter.peek()
                    && let Ok(millis) = next_arg.parse::<u64>()
                {
                    options.push(StreamOption::Block(millis));
                    iter.next();
                }
            }
            "STREAMS" => {
                // STREAMS stream_key_1 stream_key_2 stream_entry_id1 stream_entry_id2
                let mut streams: Vec<(StreamKey, StringKey)> = Vec::new();
                let mut stream_args: Vec<&str> = Vec::new();
                iter.to_owned().for_each(|v| stream_args.push(v));

                for i in 1..=(stream_args.len() / 2) {
                    streams.push((
                        stream_args[i - 1].to_string(),
                        stream_args[((stream_args.len() / 2) - 1) + i].to_string(),
                    ));
                }

                options.push(StreamOption::Streams(streams));
            }
            _ => {}
        }
    }

    options
}

fn prepare_set_options(args: Vec<&str>) -> Vec<SetOption> {
    let mut options = Vec::new();
    if args.is_empty() {
        return options;
    }

    let mut iter = args.iter().peekable();

    while let Some(opt) = iter.next() {
        let upper_opt = opt.to_uppercase();

        match upper_opt.as_str() {
            "NX" => options.push(SetOption::NX),
            "XX" => options.push(SetOption::XX),
            "GET" => options.push(SetOption::GET),
            "KEEPTTL" => options.push(SetOption::KEEPTTL),
            "PX" => {
                if let Some(next_arg) = iter.peek()
                    && let Ok(milliseconds) = next_arg.parse::<u128>()
                {
                    options.push(SetOption::PX(milliseconds));
                    iter.next();
                }
            }
            "PXAT" => {
                if let Some(next_arg) = iter.peek()
                    && let Ok(milliseconds) = next_arg.parse::<u128>()
                {
                    options.push(SetOption::PXAT(milliseconds));
                    iter.next();
                }
            }
            "EX" => {
                if let Some(next_arg) = iter.peek()
                    && let Ok(seconds) = next_arg.parse::<u64>()
                {
                    options.push(SetOption::EX(seconds));
                    iter.next();
                }
            }
            "EXAT" => {
                if let Some(next_arg) = iter.peek()
                    && let Ok(seconds) = next_arg.parse::<u64>()
                {
                    options.push(SetOption::EXAT(seconds));
                    iter.next();
                }
            }
            "IFEQ" => {
                if let Some(next_arg) = iter.peek() {
                    options.push(SetOption::IFEQ(next_arg.to_string()));
                    iter.next();
                }
            }
            "IFNE" => {
                if let Some(next_arg) = iter.peek() {
                    options.push(SetOption::IFNE(next_arg.to_string()));
                    iter.next();
                }
            }
            "IFDEQ" => {
                if let Some(next_arg) = iter.peek() {
                    options.push(SetOption::IFDEQ(next_arg.to_string()));
                    iter.next();
                }
            }
            "IFDNE" => {
                if let Some(next_arg) = iter.peek() {
                    options.push(SetOption::IFDNE(next_arg.to_string()));
                    iter.next();
                }
            }
            _ => {}
        }
    }

    options
}

fn prepare_info_options(args: Vec<&str>) -> Vec<Info> {
    let mut options = Vec::new();
    if args.is_empty() {
        options.push(Info::Default);
        return options;
    }

    for arg in args {
        let upper_arg = arg.to_uppercase();
        match upper_arg.as_str() {
            "DEFAULT" => options.push(Info::Default),
            "ALL" => options.push(Info::All),
            "EVERYTHING" => options.push(Info::Everything),
            "SERVER" => options.push(Info::Server),
            "CLIENTS" => options.push(Info::Clients),
            "MEMORY" => options.push(Info::Memory),
            "PERSISTENCE" => options.push(Info::Persistence),
            "STATS" => options.push(Info::Stats),
            "REPLICATION" => options.push(Info::Replication),
            "CPU" => options.push(Info::Cpu),
            "COMMANDSTATS" => options.push(Info::Commandstats),
            "CLUSTER" => options.push(Info::Cluster),
            "KEYSPACE" => options.push(Info::Keyspace),
            _ => {}
        }
    }

    options
}

pub fn serialize_command(command: Command) -> String {
    match command {
        Command::Publish(chan, msg) => RespBuilder::new()
            .add_array(&3)
            .add_bulk_string("PUBLISH")
            .add_bulk_string(&chan)
            .add_bulk_string(&msg)
            .to_string(),
        Command::Ping => RespBuilder::new()
            .add_array(&1)
            .add_bulk_string("PING")
            .to_string(),
        Command::Echo(s) => RespBuilder::new()
            .add_array(&2)
            .add_bulk_string("ECHO")
            .add_bulk_string(&s)
            .to_string(),
        Command::Info(_) => RespBuilder::new()
            .add_array(&1)
            .add_bulk_string("INFO")
            .to_string(),
        Command::ReplConf(arg, value) => RespBuilder::new()
            .add_array(&3)
            .add_bulk_string("REPLCONF")
            .add_bulk_string(&arg)
            .add_bulk_string(&value)
            .to_string(),
        Command::Psync(repl_id, offset) => RespBuilder::new()
            .add_array(&3)
            .add_bulk_string("PSYNC")
            .add_bulk_string(&repl_id)
            .add_bulk_string(&offset.to_string())
            .to_string(),
        Command::Type(key) => RespBuilder::new()
            .add_array(&2)
            .add_bulk_string("TYPE")
            .add_bulk_string(&key.to_string())
            .to_string(),
        Command::Set(key, value, options) => {
            let mut builder = RespBuilder::new();
            let mut parts = vec!["SET".to_string(), key.to_string(), value];
            for opt in options {
                match opt {
                    SetOption::NX => parts.push("NX".to_string()),
                    SetOption::XX => parts.push("XX".to_string()),
                    SetOption::IFEQ(v) => {
                        parts.push("IFEQ".to_string());
                        parts.push(v.clone());
                    }
                    SetOption::IFNE(v) => {
                        parts.push("IFNE".to_string());
                        parts.push(v.clone());
                    }
                    SetOption::IFDEQ(v) => {
                        parts.push("IFDEQ".to_string());
                        parts.push(v.clone());
                    }
                    SetOption::IFDNE(v) => {
                        parts.push("IFDNE".to_string());
                        parts.push(v.clone());
                    }
                    SetOption::GET => parts.push("GET".to_string()),
                    SetOption::EX(v) => {
                        parts.push("EX".to_string());
                        parts.push(v.to_string());
                    }
                    SetOption::PX(v) => {
                        parts.push("PX".to_string());
                        parts.push(v.to_string());
                    }
                    SetOption::EXAT(v) => {
                        parts.push("EXAT".to_string());
                        parts.push(v.to_string());
                    }
                    SetOption::PXAT(v) => {
                        parts.push("PXAT".to_string());
                        parts.push(v.to_string());
                    }
                    SetOption::KEEPTTL => parts.push("KEEPTTL".to_string()),
                }
            }
            builder.add_array(&parts.len());
            for part in parts {
                builder.add_bulk_string(&part);
            }
            builder.to_string()
        }
        Command::Get(key) => RespBuilder::new()
            .add_array(&2)
            .add_bulk_string("GET")
            .add_bulk_string(&key.to_string())
            .to_string(),
        Command::Incr(key) => RespBuilder::new()
            .add_array(&2)
            .add_bulk_string("INCR")
            .add_bulk_string(&key.to_string())
            .to_string(),
        Command::Rpush(key, values) => {
            let mut parts = vec!["RPUSH".to_string(), key.to_string()];
            parts.extend(values);
            let mut builder = RespBuilder::new();
            builder.add_array(&parts.len());
            for part in parts {
                builder.add_bulk_string(&part);
            }
            builder.to_string()
        }
        Command::Lpush(key, values) => {
            let mut parts = vec!["LPUSH".to_string(), key.to_string()];
            parts.extend(values);
            let mut builder = RespBuilder::new();
            builder.add_array(&parts.len());
            for part in parts {
                builder.add_bulk_string(&part);
            }
            builder.to_string()
        }
        Command::Lrange(key, start, stop) => RespBuilder::new()
            .add_array(&4)
            .add_bulk_string("LRANGE")
            .add_bulk_string(&key.to_string())
            .add_bulk_string(&start.to_string())
            .add_bulk_string(&stop.to_string())
            .to_string(),
        Command::Llen(key) => RespBuilder::new()
            .add_array(&2)
            .add_bulk_string("LLEN")
            .add_bulk_string(&key.to_string())
            .to_string(),
        Command::Lpop(key, count) => RespBuilder::new()
            .add_array(&3)
            .add_bulk_string("LPOP")
            .add_bulk_string(&key.to_string())
            .add_bulk_string(&count.to_string())
            .to_string(),
        Command::Blpop(key, timeout) => RespBuilder::new()
            .add_array(&3)
            .add_bulk_string("BLPOP")
            .add_bulk_string(&key.to_string())
            .add_bulk_string(&timeout.to_string())
            .to_string(),
        Command::Xadd(stream_key, key, fields) => {
            let mut parts = vec!["XADD".to_string(), stream_key.to_string(), key.to_string()];
            for (field, value) in fields {
                parts.push(field);
                parts.push(value);
            }
            let mut builder = RespBuilder::new();
            builder.add_array(&parts.len());
            for part in parts {
                builder.add_bulk_string(&part);
            }
            builder.to_string()
        }
        Command::Xrange(start, end, count) => RespBuilder::new()
            .add_array(&4)
            .add_bulk_string("XRANGE")
            .add_bulk_string(&start.to_string())
            .add_bulk_string(&end.to_string())
            .add_bulk_string(&count.to_string())
            .to_string(),
        Command::Xread(_) => RespBuilder::new()
            .add_array(&1)
            .add_bulk_string("XREAD")
            .to_string(),
        Command::Multi => RespBuilder::new()
            .add_array(&1)
            .add_bulk_string("MULTI")
            .to_string(),
        Command::Exec => RespBuilder::new()
            .add_array(&1)
            .add_bulk_string("EXEC")
            .to_string(),
        Command::Discard => RespBuilder::new()
            .add_array(&1)
            .add_bulk_string("DISCARD")
            .to_string(),
        Command::Wait(count, wait) => RespBuilder::new()
            .add_array(&3)
            .add_bulk_string("WAIT")
            .add_bulk_string(&count.to_string())
            .add_bulk_string(&wait.to_string())
            .to_string(),
        Command::ConfigGet(args) => {
            let mut resp = RespBuilder::new();
            resp.add_array(&(args.len() + 2));
            resp.add_bulk_string("CONFIG");
            resp.add_bulk_string("GET");
            for arg in args {
                resp.add_bulk_string(&arg);
            }
            resp.to_string()
        }
        Command::BgSave => RespBuilder::new()
            .add_array(&1)
            .add_bulk_string("BGSAVE")
            .to_string(),
        Command::Keys(query) => RespBuilder::new()
            .add_array(&2)
            .add_bulk_string("KEYS")
            .add_bulk_string(&query)
            .to_string(),
        Command::Subscribe(channels) => {
            let mut resp = RespBuilder::new();
            resp.add_array(&(channels.len() + 2));
            resp.add_bulk_string("SUBSCRIBE");
            for ch in channels {
                resp.add_bulk_string(&ch);
            }
            resp.to_string()
        }
        Command::Psubscribe(channels) => {
            let mut resp = RespBuilder::new();
            resp.add_array(&(channels.len() + 2));
            resp.add_bulk_string("PSUBSCRIBE");
            for ch in channels {
                resp.add_bulk_string(&ch);
            }
            resp.to_string()
        }
        Command::Unsubscribe(channels) => {
            let mut resp = RespBuilder::new();
            resp.add_array(&(channels.len() + 2));
            resp.add_bulk_string("UNSUBSCRIBE");
            for ch in channels {
                resp.add_bulk_string(&ch);
            }
            resp.to_string()
        }
        Command::Punsubscribe(channels) => {
            let mut resp = RespBuilder::new();
            resp.add_array(&(channels.len() + 2));
            resp.add_bulk_string("PUNSUBSCRIBE");
            for ch in channels {
                resp.add_bulk_string(&ch);
            }
            resp.to_string()
        }
        Command::Quit => RespBuilder::new()
            .add_array(&1)
            .add_bulk_string("QUIT")
            .to_string(),
        Command::Reset => RespBuilder::new()
            .add_array(&1)
            .add_bulk_string("RESET")
            .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepares_multiple_commands() {
        let data = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n";
        let result = prepare_commands(data);
        assert_eq!(result.len(), 3);
    }
}
