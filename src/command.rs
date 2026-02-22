use crate::data::types::SortedRangeOption;

use super::data::types::{AddOption, SetOption, StreamKey, StreamOption, StringKey};
use super::governor::types::Info;
use super::resp::{DataType, RespParser};

#[derive(Clone)]
#[allow(dead_code)]
pub struct PreparedCommand {
    pub cmd: Command,
    pub raw: String,
    pub acl: CommandAcl,
}

impl PreparedCommand {}

#[derive(Clone)]
#[allow(dead_code)]
pub struct CommandAcl {
    pub client_context: ClientContext,
    pub command_type: CommandType,
    pub modes: Vec<CommandMode>,
}

#[derive(Clone)]
pub enum ClientContext {
    Master,
    Replica,
    Any,
}

#[derive(Clone)]
pub enum CommandType {
    Read,
    Write,
    Admin,
}

#[derive(Clone)]
pub enum CommandMode {
    Normal,
    Multi,
    PubSub,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
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
    // sorted set
    Zadd(StringKey, Vec<AddOption>, Vec<(f64, StringKey)>),
    Zrank(StringKey, StringKey, bool),
    Zrange(StringKey, i64, i64, Vec<SortedRangeOption>),
    Zcard(StringKey),
    Zscore(StringKey, StringKey),
    Zrem(StringKey, Vec<StringKey>),
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

impl Command {
    pub fn name(&self) -> &'static str {
        match self {
            Command::Ping => "PING",
            Command::Echo(_) => "ECHO",
            Command::Info(_) => "INFO",
            Command::ReplConf(_, _) => "REPLCONF",
            Command::Psync(_, _) => "PSYNC",
            Command::Wait(_, _) => "WAIT",
            Command::ConfigGet(_) => "CONFIG",
            Command::BgSave => "BGSAVE",
            Command::Keys(_) => "KEYS",
            Command::Type(_) => "TYPE",
            Command::Set(_, _, _) => "SET",
            Command::Get(_) => "GET",
            Command::Incr(_) => "INCR",
            Command::Rpush(_, _) => "RPUSH",
            Command::Lpush(_, _) => "LPUSH",
            Command::Lrange(_, _, _) => "LRANGE",
            Command::Llen(_) => "LLEN",
            Command::Lpop(_, _) => "LPOP",
            Command::Blpop(_, _) => "BLPOP",
            Command::Xadd(_, _, _) => "XADD",
            Command::Xrange(_, _, _) => "XRANGE",
            Command::Xread(_) => "XREAD",
            Command::Multi => "MULTI",
            Command::Exec => "EXEC",
            Command::Discard => "DISCARD",
            Command::Publish(_, _) => "PUBLISH",
            Command::Subscribe(_) => "SUBSCRIBE",
            Command::Psubscribe(_) => "PSUBSCRIBE",
            Command::Unsubscribe(_) => "UNSUBSCRIBE",
            Command::Punsubscribe(_) => "PUNSUBSCRIBE",
            Command::Reset => "RESET",
            Command::Quit => "QUIT",
            Command::Zadd(_, _, _) => "ZADD",
            Command::Zrank(_, _, _) => "ZRANK",
            Command::Zrange(_, _, _, _) => "ZRANGE",
            Command::Zcard(_) => "ZCARD",
            Command::Zscore(_, _) => "ZSCORE",
            Command::Zrem(_, _) => "ZREM",
        }
    }

    pub fn should_replicate(&self) -> bool {
        matches!(self.acl().command_type, CommandType::Write)
    }

    pub fn acl(&self) -> CommandAcl {
        match self {
            // Read commands - valid in all contexts
            Command::Ping => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::Multi, CommandMode::PubSub],
            },
            Command::Quit => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::Multi, CommandMode::PubSub],
            },
            Command::Reset => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::PubSub],
            },

            // Server commands
            Command::Echo(_) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Info(_) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal],
            },
            Command::ReplConf(_, _) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal],
            },
            Command::Psync(_, _) => CommandAcl {
                client_context: ClientContext::Replica,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal],
            },
            Command::Wait(_, _) => CommandAcl {
                client_context: ClientContext::Master,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal],
            },
            Command::ConfigGet(_) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal],
            },
            Command::BgSave => CommandAcl {
                client_context: ClientContext::Master,
                command_type: CommandType::Admin,
                modes: vec![CommandMode::Normal],
            },

            // Store commands
            Command::Keys(_) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Type(_) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },

            // String commands
            Command::Set(_, _, _) => CommandAcl {
                client_context: ClientContext::Master,
                command_type: CommandType::Write,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Get(_) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Incr(_) => CommandAcl {
                client_context: ClientContext::Master,
                command_type: CommandType::Write,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },

            // List commands
            Command::Rpush(_, _) => CommandAcl {
                client_context: ClientContext::Master,
                command_type: CommandType::Write,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Lpush(_, _) => CommandAcl {
                client_context: ClientContext::Master,
                command_type: CommandType::Write,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Lrange(_, _, _) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Llen(_) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Lpop(_, _) => CommandAcl {
                client_context: ClientContext::Master,
                command_type: CommandType::Write,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Blpop(_, _) => CommandAcl {
                client_context: ClientContext::Master,
                command_type: CommandType::Write,
                modes: vec![CommandMode::Normal],
            },

            // Stream commands
            Command::Xadd(_, _, _) => CommandAcl {
                client_context: ClientContext::Master,
                command_type: CommandType::Write,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Xrange(_, _, _) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Xread(_) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal],
            },

            // Transaction commands
            Command::Multi => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal],
            },
            Command::Exec => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Multi],
            },
            Command::Discard => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Multi],
            },

            // Pub/Sub commands
            Command::Publish(_, _) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Write,
                modes: vec![CommandMode::Normal],
            },
            Command::Subscribe(_) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal],
            },
            Command::Psubscribe(_) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal],
            },
            Command::Unsubscribe(_) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::PubSub],
            },
            Command::Punsubscribe(_) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::PubSub],
            },

            // sorted set commands
            Command::Zadd(_, _, _) => CommandAcl {
                client_context: ClientContext::Master,
                command_type: CommandType::Write,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Zrank(_, _, _) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Zrange(_, _, _, _) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Zcard(_) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Zscore(_, _) => CommandAcl {
                client_context: ClientContext::Any,
                command_type: CommandType::Read,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
            Command::Zrem(_, _) => CommandAcl {
                client_context: ClientContext::Master,
                command_type: CommandType::Write,
                modes: vec![CommandMode::Normal, CommandMode::Multi],
            },
        }
    }
}

pub fn prepare_commands(data: &str) -> Vec<Option<PreparedCommand>> {
    let mut commands = Vec::new();
    let mut parser = RespParser::new(data);

    while parser.cursor < parser.data.len() {
        let start = parser.cursor;
        let cmd = prepare_command_with_parser(&mut parser);
        let raw = data[start..parser.cursor].to_string();

        let prepared = cmd.map(|c| {
            let acl = c.acl();
            PreparedCommand { cmd: c, raw, acl }
        });

        commands.push(prepared);
    }
    commands
}

pub fn prepare_command(data: &str) -> Option<PreparedCommand> {
    let mut parser = RespParser::new(data);
    let cmd = prepare_command_with_parser(&mut parser)?;
    let raw = data[..parser.cursor].to_string();
    let acl = cmd.acl();

    Some(PreparedCommand { cmd, raw, acl })
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
                "ZADD" => {
                    if command_parts.len() >= 3 {
                        let key = StringKey::from(command_parts[1]);
                        let mut options = Vec::new();
                        let mut score_member_pairs = Vec::new();
                        if command_parts.len() > 3 {
                            options = prepare_add_options(command_parts[2..].to_vec());
                        }
                        for i in ((2 + options.len())..command_parts.len()).step_by(2) {
                            if let Some(score_str) = command_parts.get(i)
                                && let Some(member_str) = command_parts.get(i + 1)
                                && let Ok(score) = score_str.parse::<f64>()
                            {
                                score_member_pairs.push((score, member_str.to_string()));
                            } else {
                                return None; // Invalid score-member pair
                            }
                        }
                        Some(Command::Zadd(key, options, score_member_pairs))
                    } else {
                        None
                    }
                }
                "ZRANK" => {
                    if command_parts.len() >= 3 {
                        let key = StringKey::from(command_parts[1]);
                        let member = StringKey::from(command_parts[2]);
                        let with_score = command_parts
                            .get(3)
                            .is_some_and(|&opt| opt.to_uppercase() == "WITHSCORE");

                        Some(Command::Zrank(key, member, with_score))
                    } else {
                        None
                    }
                }
                "ZRANGE" => {
                    if command_parts.len() >= 4 {
                        let key = StringKey::from(command_parts[1]);
                        let start: i64 = command_parts[2].parse().ok()?;
                        let stop: i64 = command_parts[3].parse().ok()?;
                        let mut options = Vec::new();
                        if command_parts.len() > 4 {
                            let opts = command_parts[4..].to_vec();
                            let mut opts_iter = opts.iter();
                            while let Some(opt) = opts_iter.next() {
                                match opt.to_uppercase().as_str() {
                                    "WITHSCORES" => options.push(SortedRangeOption::WITHSCORES),
                                    "BYSCORE" => options.push(SortedRangeOption::BYSCORE),
                                    "BYLEX" => options.push(SortedRangeOption::BYLEX),
                                    "REV" => options.push(SortedRangeOption::REV),
                                    "LIMIT" => {
                                        if let (Some(offset_str), Some(count_str)) =
                                            (opts_iter.next(), opts_iter.next())
                                            && let (Ok(offset), Ok(count)) = (
                                                offset_str.parse::<usize>(),
                                                count_str.parse::<usize>(),
                                            )
                                        {
                                            options.push(SortedRangeOption::LIMIT(offset, count));
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        Some(Command::Zrange(key, start, stop, options))
                    } else {
                        None
                    }
                }
                "ZCARD" => {
                    if command_parts.len() >= 2 {
                        Some(Command::Zcard(StringKey::from(command_parts[1])))
                    } else {
                        None
                    }
                }
                "ZSCORE" => {
                    if command_parts.len() >= 3 {
                        Some(Command::Zscore(
                            StringKey::from(command_parts[1]),
                            StringKey::from(command_parts[2]),
                        ))
                    } else {
                        None
                    }
                }
                "ZREM" => {
                    if command_parts.len() >= 3 {
                        let key = StringKey::from(command_parts[1]);
                        let members = command_parts[2..]
                            .iter()
                            .map(|&m| StringKey::from(m))
                            .collect();
                        Some(Command::Zrem(key, members))
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

fn prepare_add_options(args: Vec<&str>) -> Vec<AddOption> {
    let mut options = Vec::new();
    if args.is_empty() {
        return options;
    }
    let mut iter = args.iter();
    while let Some(opt) = iter.next() {
        let upper_opt = opt.to_uppercase();
        match upper_opt.as_str() {
            "NX" | "XX" | "GT" | "LT" | "CH" | "INCR" => {
                if let Some(option) = AddOption::from_str(upper_opt.as_str()) {
                    options.push(option);
                }
                iter.next();
            }
            _ => {}
        }
    }
    options
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepares_multiple_commands() {
        let data = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n";
        let result = prepare_commands(data);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn prepared_zadd() {
        let data = "*7\r\n$4\r\nZADD\r\n$3\r\nmyz\r\n$2\r\nNX\r\n$3\r\n1.0\r\n$3\r\none\r\n$3\r\n5.2\r\n$3\r\ntwo\r\n";
        let prepared = prepare_command(data).unwrap();
        match prepared.cmd {
            Command::Zadd(key, options, score_member_pairs) => {
                assert_eq!(key, "myz");
                assert!(options.contains(&AddOption::NX));
                assert_eq!(score_member_pairs.len(), 2);
                assert_eq!(score_member_pairs[0].0, 1.0);
                assert_eq!(score_member_pairs[0].1, "one");
                assert_eq!(score_member_pairs[1].0, 5.2);
                assert_eq!(score_member_pairs[1].1, "two");
            }
            _ => panic!("Expected ZADD command"),
        }
    }
}
