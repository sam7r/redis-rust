use super::resp::{DataType, RespParser};
use super::store::{SetOption, StreamKey, StreamOption, StringKey};

pub enum Command {
    // connection
    Ping,
    Echo(String),
    Type(StringKey),
    // string
    Set(StringKey, String, Vec<SetOption>),
    Get(StringKey),
    // list
    Rpush(StringKey, Vec<String>),
    Lpush(StringKey, Vec<String>),
    Lrange(StringKey, i64, i64),
    Llen(StringKey),
    Lpop(StringKey, u8),
    Blpop(StringKey, f32),
    // srteam
    Xadd(StreamKey, StringKey, Vec<(String, String)>),
    Xrange(StreamKey, StringKey, StringKey),
    Xread(Vec<StreamOption>),
}

pub fn prepare_command(data: &str) -> Option<Command> {
    let mut parser = RespParser::new(data);

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
                    && let Ok(millis) = next_arg.parse::<u128>()
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
