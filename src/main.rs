use governor::{CleanupType, Governor};
use resp::{DataType, RespBuilder, RespParser};
use std::{io::Read, io::Write, net::TcpListener, sync::Arc, thread, time::Duration};
use store::{DataStore, SetOption, StringKey};

mod governor;
mod resp;
mod store;

enum Command {
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
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let store = Arc::new(DataStore::new());

    let governor = Governor::new(
        Arc::clone(&store),
        governor::Options {
            cleanup_type: CleanupType::Scheduled(Duration::from_millis(10)),
        },
    );

    governor.start();

    for stream in listener.incoming() {
        let kv_store = Arc::clone(&store);
        thread::spawn(move || {
            handle_client(stream.unwrap(), kv_store);
        });
    }
}

fn handle_client(mut stream: std::net::TcpStream, store: Arc<DataStore>) {
    loop {
        let mut buffer = [0; 512];
        let bytes_read = stream.read(&mut buffer);

        match bytes_read {
            Ok(0) => break,
            Ok(n) => {
                let input = String::from_utf8_lossy(&buffer[..n]);
                let kv_store = Arc::clone(&store);

                if let Some(cmd) = prepare_command(&input) {
                    handle_command(&mut stream, kv_store, cmd);
                } else {
                    write_error_to_stream(&mut stream, "unable to handle request");
                }
            }
            Err(err) => {
                eprintln!("error reading from stream: {}", err);
                break;
            }
        }
    }
}

fn prepare_command(data: &str) -> Option<Command> {
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

fn handle_command(stream: &mut std::net::TcpStream, store: Arc<DataStore>, command: Command) {
    match command {
        Command::Ping => {
            write_to_stream(
                stream,
                RespBuilder::new().add_simple_string("PONG").as_bytes(),
            );
        }
        Command::Echo(echo) => {
            write_to_stream(stream, RespBuilder::new().add_bulk_string(&echo).as_bytes());
        }
        Command::Type(key) => match store.get_type(&key) {
            Ok(t) => {
                let mut resp = RespBuilder::new();
                if let Some(value_type) = t {
                    resp.add_simple_string(&value_type);
                } else {
                    resp.add_simple_string("none");
                }
                write_to_stream(stream, resp.as_bytes());
            }
            Err(_) => write_error_to_stream(stream, "failed to get type"),
        },
        Command::Set(key, value, options) => match store.set(&key, value, options) {
            Ok(v) => {
                let mut resp = RespBuilder::new();
                if let Some(result) = v {
                    resp.add_simple_string(&result.to_string());
                } else {
                    resp.add_simple_string("OK");
                }
                write_to_stream(stream, resp.as_bytes());
            }
            Err(err) => {
                eprintln!("error setting value: {}", err);
                write_error_to_stream(stream, "error setting value");
            }
        },
        Command::Get(value) => match store.get(&value) {
            Ok(result) => {
                let mut resp = RespBuilder::new();
                if let Some(v) = result {
                    resp.add_bulk_string(&v.to_string());
                } else {
                    resp.negative_bulk_string();
                }
                write_to_stream(stream, resp.as_bytes());
            }
            Err(err) => {
                eprintln!("error getting value: {}", err);
                write_error_to_stream(stream, "error getting value");
            }
        },
        Command::Rpush(key, list) => match store.rpush(&key, list) {
            Ok(result) => {
                if let Some(n) = result {
                    write_to_stream(
                        stream,
                        RespBuilder::new().add_integer(&n.to_string()).as_bytes(),
                    );
                } else {
                    write_error_to_stream(stream, "not a list");
                }
            }
            Err(err) => {
                eprintln!("error pushing to list: {}", err);
                write_error_to_stream(stream, "error pushing to list");
            }
        },
        Command::Lpush(key, list) => match store.lpush(&key, list) {
            Ok(result) => {
                if let Some(n) = result {
                    write_to_stream(
                        stream,
                        RespBuilder::new().add_integer(&n.to_string()).as_bytes(),
                    );
                } else {
                    write_error_to_stream(stream, "not a list");
                }
            }
            Err(err) => {
                eprintln!("error pushing to list: {}", err);
                write_error_to_stream(stream, "error pushing to list");
            }
        },
        Command::Lrange(key, start, stop) => match store.lrange(&key, (start, stop)) {
            Ok(result) => {
                if let Some(list) = result {
                    let mut resp = RespBuilder::new();
                    resp.add_array(&list.len());
                    list.iter().for_each(|item| {
                        resp.add_bulk_string(item);
                    });
                    write_to_stream(stream, resp.as_bytes());
                } else {
                    write_error_to_stream(stream, "not a list");
                }
            }
            Err(err) => {
                eprintln!("error getting list range: {}", err);
                write_error_to_stream(stream, "error getting list range");
            }
        },
        Command::Llen(key) => match store.llen(key) {
            Ok(result) => {
                if let Some(n) = result {
                    write_to_stream(
                        stream,
                        RespBuilder::new().add_integer(&n.to_string()).as_bytes(),
                    );
                } else {
                    write_error_to_stream(stream, "not a list");
                }
            }
            Err(err) => {
                eprintln!("error getting list length: {}", err);
                write_error_to_stream(stream, "error getting list length");
            }
        },
        Command::Lpop(key, count) => match store.lpop(&key, count) {
            Ok(result) => {
                if let Some(list) = result {
                    let mut resp = RespBuilder::new();
                    if list.len() > 1 {
                        resp.add_array(&list.len());
                    }
                    list.iter().for_each(|item| {
                        resp.add_bulk_string(item);
                    });
                    write_to_stream(stream, resp.as_bytes());
                } else {
                    write_error_to_stream(stream, "not a list or empty list");
                }
            }
            Err(err) => {
                eprintln!("error popping from list: {}", err);
                write_error_to_stream(stream, "error popping from list");
            }
        },
        Command::Blpop(key, timeout) => match store.blpop(&key, timeout) {
            Ok(result) => match result {
                Some(list) => {
                    let mut resp = RespBuilder::new();
                    resp.add_array(&(list.len() + 1));
                    resp.add_bulk_string(&key);
                    list.iter().for_each(|item| {
                        resp.add_bulk_string(item);
                    });
                    write_to_stream(stream, resp.as_bytes());
                }
                None => {
                    let mut resp = RespBuilder::new();
                    resp.negative_array();
                    write_to_stream(stream, resp.as_bytes());
                }
            },
            Err(err) => {
                eprintln!("error popping from list: {}", err);
                write_error_to_stream(stream, "error popping from list");
            }
        },
    }
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

fn write_to_stream(stream: &mut std::net::TcpStream, data: &[u8]) {
    if let Ok(n) = stream.write(data)
        && n != data.len()
    {
        eprintln!("error writing to stream: only wrote {} bytes", n);
    }
}

fn write_error_to_stream(stream: &mut std::net::TcpStream, message: &str) {
    write_to_stream(
        stream,
        RespBuilder::new().add_simple_error(message).as_bytes(),
    );
}
