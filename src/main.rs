use std::{io::Read, io::Write, net::TcpListener, sync::Arc, thread, time::Duration};

use data_store::{CleanupType, DataStore, Governor, SetOption};
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

struct BulkString {
    content: String,
}

impl BulkString {
    fn new(content: &str) -> Self {
        BulkString {
            content: String::from(content),
        }
    }

    fn len(&self) -> usize {
        self.content.len()
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
    Ping,
    Echo(BulkString),
    Set(BulkString, BulkString, Vec<SetOption>),
    Get(BulkString),
    Unknown,
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
                        Some(Command::Echo(BulkString::new(command_parts[1])))
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
                            BulkString::new(command_parts[1]),
                            BulkString::new(command_parts[2]),
                            options,
                        ))
                    } else {
                        None
                    }
                }
                "GET" => {
                    if command_parts.len() >= 2 {
                        Some(Command::Get(BulkString::new(command_parts[1])))
                    } else {
                        None
                    }
                }
                _ => Some(Command::Unknown),
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
            resp_str.push_str(&echo.content);
            resp_str.push_str("\r\n");

            write_to_stream(stream, resp_str.as_bytes());
        }
        Command::Set(key, value, options) => match store.set(key.content, value.content, options) {
            Ok(v) => {
                let mut resp_str = String::new();
                resp_str.push(DataType::SimpleString.to_char());

                if let Some(result) = v {
                    resp_str.push_str(&result);
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
        Command::Get(value) => match store.get(&value.content) {
            Ok(result) => {
                let mut resp_str = String::new();
                resp_str.push(DataType::BulkString.to_char());

                if let Some(v) = result {
                    println!("got value: {}", v);
                    resp_str.push_str(v.len().to_string().as_str());
                    resp_str.push_str("\r\n");
                    resp_str.push_str(&v);
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
        _ => {
            write_error_to_stream(stream, "unknown command");
        }
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
