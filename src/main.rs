use std::{io::Read, io::Write, net::TcpListener, sync::Arc, thread, time::Duration};

use data_store::{CleanupType, DataStore, Governor, SetOption, StringKey};
use resp_parser::RespParser;

mod data_store;
mod resp_parser;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let store = Arc::new(DataStore::new());

    let governor = Governor::new(
        Arc::clone(&store),
        CleanupType::Scheduled(Duration::from_millis(10)),
    );
    governor.start_cleanup();

    for stream in listener.incoming() {
        let kv_store = Arc::clone(&store);
        thread::spawn(move || {
            handle_client(stream.unwrap(), kv_store);
        });
    }
}

// RESP2 data types
enum DataType {
    SimpleString,
    SimpleError,
    Integer,
    BulkString,
    Array,
    None,
}

impl DataType {
    fn from_char(c: char) -> Self {
        match c {
            '*' => DataType::Array,
            '+' => DataType::SimpleString,
            '-' => DataType::SimpleError,
            '$' => DataType::BulkString,
            ':' => DataType::Integer,
            _ => DataType::None,
        }
    }
    fn to_char(&self) -> char {
        match self {
            DataType::SimpleString => '+',
            DataType::BulkString => '$',
            DataType::Integer => ':',
            DataType::Array => '*',
            DataType::SimpleError => '-',
            DataType::None => '\0',
        }
    }
}

fn get_set_options(args: Vec<&str>) -> Vec<SetOption> {
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

#[allow(dead_code)]
enum Command {
    // connection
    Ping,
    Echo(String),
    // string
    Set(StringKey, String, Vec<SetOption>),
    Get(StringKey),
    // list
    Rpush(StringKey, Vec<String>),
    Lpush(StringKey, Vec<String>),
    Lrange(StringKey, i64, i64),
    Llen(StringKey),
    Lpop(StringKey),
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
                if let Some(cmd) = handle_input(&input) {
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

fn handle_input(data: &str) -> Option<Command> {
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
                "SET" => {
                    dbg!(&command_parts);
                    if command_parts.len() >= 3 {
                        let mut options = Vec::new();
                        if command_parts.len() > 3 {
                            options = get_set_options(command_parts[3..].to_vec());
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
                        Some(Command::Lpop(StringKey::from(command_parts[1])))
                    } else {
                        None
                    }
                }
                _ => {
                    println!(
                        "received unsupported command: {}",
                        parser.data[parser.cursor..].trim()
                    );
                    None
                }
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
            let mut resp_str = String::new();
            resp_str.push(DataType::SimpleString.to_char());
            resp_str.push_str("PONG");
            resp_str.push_str("\r\n");

            write_to_stream(stream, resp_str.as_bytes());
        }
        Command::Echo(echo) => {
            let mut resp_str = String::new();
            resp_str.push(DataType::BulkString.to_char());
            resp_str.push_str(echo.len().to_string().as_str());
            resp_str.push_str("\r\n");
            resp_str.push_str(&echo);
            resp_str.push_str("\r\n");

            write_to_stream(stream, resp_str.as_bytes());
        }
        Command::Set(key, value, options) => match store.set(key, value, options) {
            Ok(v) => {
                let mut resp_str = String::new();
                resp_str.push(DataType::SimpleString.to_char());

                if let Some(result) = v {
                    resp_str.push_str(&result.to_string());
                } else {
                    resp_str.push_str("OK");
                }

                resp_str.push_str("\r\n");
                write_to_stream(stream, resp_str.as_bytes());
            }
            Err(err) => {
                eprintln!("error setting value: {}", err);
                write_error_to_stream(stream, "error setting value");
            }
        },
        Command::Get(value) => match store.get(&value) {
            Ok(result) => {
                let mut resp_str = String::new();
                resp_str.push(DataType::BulkString.to_char());

                if let Some(v) = result {
                    resp_str.push_str(v.to_string().len().to_string().as_str());
                    resp_str.push_str("\r\n");
                    resp_str.push_str(&v.to_string());
                } else {
                    resp_str.push_str("-1");
                }

                resp_str.push_str("\r\n");
                write_to_stream(stream, resp_str.as_bytes());
            }
            Err(err) => {
                eprintln!("error getting value: {}", err);
                write_error_to_stream(stream, "error getting value");
            }
        },
        Command::Rpush(key, list) => match store.rpush(key, list) {
            Ok(result) => {
                if let Some(n) = result {
                    let mut resp_str = String::new();
                    resp_str.push(DataType::Integer.to_char());
                    resp_str.push_str(&n.to_string());
                    resp_str.push_str("\r\n");
                    write_to_stream(stream, resp_str.as_bytes());
                } else {
                    write_error_to_stream(stream, "not a list");
                }
            }
            Err(err) => {
                eprintln!("error pushing to list: {}", err);
                write_error_to_stream(stream, "error pushing to list");
            }
        },
        Command::Lpush(key, list) => match store.lpush(key, list) {
            Ok(result) => {
                if let Some(n) = result {
                    let mut resp_str = String::new();
                    resp_str.push(DataType::Integer.to_char());
                    resp_str.push_str(&n.to_string());
                    resp_str.push_str("\r\n");
                    write_to_stream(stream, resp_str.as_bytes());
                } else {
                    write_error_to_stream(stream, "not a list");
                }
            }
            Err(err) => {
                eprintln!("error pushing to list: {}", err);
                write_error_to_stream(stream, "error pushing to list");
            }
        },
        Command::Lrange(key, start, stop) => match store.lrange(key, (start, stop)) {
            Ok(result) => {
                if let Some(list) = result {
                    let mut resp_str = String::new();
                    resp_str.push(DataType::Array.to_char());
                    resp_str.push_str(list.len().to_string().as_str());
                    resp_str.push_str("\r\n");

                    list.iter().for_each(|item| {
                        resp_str.push(DataType::BulkString.to_char());
                        resp_str.push_str(item.len().to_string().as_str());
                        resp_str.push_str("\r\n");
                        resp_str.push_str(item);
                        resp_str.push_str("\r\n");
                    });

                    write_to_stream(stream, resp_str.as_bytes());
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
                    let mut resp_str = String::new();
                    resp_str.push(DataType::Integer.to_char());
                    resp_str.push_str(&n.to_string());
                    resp_str.push_str("\r\n");
                    write_to_stream(stream, resp_str.as_bytes());
                } else {
                    write_error_to_stream(stream, "not a list");
                }
            }
            Err(err) => {
                eprintln!("error getting list length: {}", err);
                write_error_to_stream(stream, "error getting list length");
            }
        },
        Command::Lpop(key) => match store.lpop(key) {
            Ok(result) => {
                if let Some(item) = result {
                    let mut resp_str = String::new();
                    resp_str.push(DataType::BulkString.to_char());
                    resp_str.push_str(item.to_string().len().to_string().as_str());
                    resp_str.push_str("\r\n");
                    resp_str.push_str(&item.to_string());
                    resp_str.push_str("\r\n");
                    write_to_stream(stream, resp_str.as_bytes());
                } else {
                    write_error_to_stream(stream, "not a list or empty list");
                }
            }
            Err(err) => {
                eprintln!("error popping from list: {}", err);
                write_error_to_stream(stream, "error popping from list");
            }
        },
    }
}

fn write_to_stream(stream: &mut std::net::TcpStream, data: &[u8]) {
    if let Ok(n) = stream.write(data)
        && n != data.len()
    {
        eprintln!("error writing to stream: only wrote {} bytes", n);
    }
}

fn write_error_to_stream(stream: &mut std::net::TcpStream, message: &str) {
    let mut resp_str = String::new();
    resp_str.push(DataType::SimpleError.to_char());
    resp_str.push_str(message);
    resp_str.push_str("\r\n");
    write_to_stream(stream, resp_str.as_bytes());
}
