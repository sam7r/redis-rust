#![allow(unused_imports)]
use std::{
    collections::HashMap,
    fmt::Display,
    io::BufReader,
    io::Read,
    io::Write,
    net::TcpListener,
    rc::Rc,
    str::Split,
    sync::{Arc, Mutex},
    thread,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let store = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    for stream in listener.incoming() {
        let kv_store = Arc::clone(&store);
        thread::spawn(move || {
            handle_client(stream.unwrap(), kv_store);
        });
    }
}

const CRLF: &str = "\r\n";

// for full list see https://redis.io/docs/latest/develop/reference/protocol-spec/#resp-protocol-description
enum DataType {
    SimpleString,
    BulkString,
    Integer,
    Array,
    None,
}

impl DataType {
    fn from_char(c: char) -> Self {
        match c {
            '*' => DataType::Array,
            '+' => DataType::SimpleString,
            '$' => DataType::BulkString,
            ':' => DataType::Integer,
            _ => DataType::None,
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::SimpleString => write!(f, "SimpleString"),
            DataType::BulkString => write!(f, "BulkString"),
            DataType::Integer => write!(f, "Integer"),
            DataType::Array => write!(f, "Array"),
            DataType::None => write!(f, "None"),
        }
    }
}

#[derive(Debug)]
struct BulkString {
    content: String,
}

impl BulkString {
    fn new(content: &str) -> Self {
        BulkString {
            content: String::from(content),
        }
    }

    fn empty() -> Self {
        BulkString {
            content: String::new(),
        }
    }

    fn len(&self) -> usize {
        self.content.len()
    }
}

#[derive(Debug)]
enum Command {
    Ping,
    Echo(BulkString),
    Set(BulkString, BulkString),
    Get(BulkString),
    Unknown,
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Ping => write!(f, "PING"),
            Command::Echo(echo) => write!(f, "ECHO {}", echo.content),
            Command::Set(k, v) => write!(f, "SET {} {}", k.content, v.content),
            Command::Get(k) => write!(f, "GET {}", k.content),
            Command::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

impl Command {
    fn from_str(s: &str) -> Self {
        let parts: Vec<&str> = s.split_whitespace().collect();
        match parts[0].to_uppercase().as_str() {
            "PING" => Command::Ping,
            "ECHO" => Command::Echo(BulkString::empty()),
            "SET" => Command::Set(BulkString::empty(), BulkString::empty()),
            "GET" => Command::Get(BulkString::empty()),
            _ => Command::Unknown,
        }
    }
}

fn handle_command(
    stream: &mut std::net::TcpStream,
    command: Command,
    store: Arc<Mutex<HashMap<String, String>>>,
) {
    match command {
        Command::Ping => {
            let msg = "PONG";
            let response = format!("+{}{CRLF}", msg);
            let _ = stream.write(response.as_bytes()).unwrap();
        }
        Command::Echo(echo) => {
            let response = format!("${}{CRLF}{}{CRLF}", echo.len(), echo.content);
            let _ = stream.write(response.as_bytes()).unwrap();
        }
        Command::Set(key, value) => {
            store.lock().unwrap().insert(key.content, value.content);
            let response = format!("+{}{CRLF}", "OK");
            let _ = stream.write(response.as_bytes()).unwrap();
        }
        Command::Get(value) => {
            let kv = store.lock().unwrap();
            let result = kv.get(&value.content);

            if let Some(v) = result {
                let response = format!("${}{CRLF}{}{CRLF}", v.len(), v);
                let _ = stream.write(response.as_bytes()).unwrap();
            } else {
                let _ = stream.write(b"$-1\r\n").unwrap();
            }
        }
        _ => {
            let _ = stream.write(b"-ERR unknown command\r\n").unwrap();
        }
    }
}

fn handle_client(mut stream: std::net::TcpStream, store: Arc<Mutex<HashMap<String, String>>>) {
    println!("accepted new connection");
    loop {
        let mut buffer = [0; 512];
        let bytes_read = stream.read(&mut buffer);

        match bytes_read {
            Ok(0) => break,
            Ok(n) => {
                let input = String::from_utf8_lossy(&buffer[..n]);
                dbg!(&input);

                let kv_store = Arc::clone(&store);
                handle_input(&mut stream, &input, kv_store);
            }
            Err(err) => {
                eprint!("error reading from stream: {}", err);
                break;
            }
        }
    }
}

fn handle_input(
    stream: &mut std::net::TcpStream,
    data: &str,
    store: Arc<Mutex<HashMap<String, String>>>,
) {
    let mut input_char_itter = data.chars();
    match DataType::from_char(input_char_itter.next().unwrap_or('\0')) {
        // *<number-of-elements>\r\n<element-1>...<element-n>
        DataType::Array => {
            let input = input_char_itter.as_str();
            let mut args = input.split(CRLF);
            let elements: u8 = str::parse(args.next().unwrap_or("0")).unwrap();

            for _ in 0..elements {
                let next = args.next().unwrap_or("");
                let datatype = DataType::from_char(next.chars().next().unwrap_or('\0'));

                match datatype {
                    // $<length>\r\n<content>\r\n$<length>\r\n<content>\r\n
                    DataType::BulkString => {
                        let bulk_str = args.next().unwrap().to_string();
                        let mut cmd = Command::from_str(&bulk_str);

                        match cmd {
                            Command::Echo(ref mut v) => {
                                args.next();
                                *v = BulkString::new(args.next().unwrap());
                            }
                            Command::Set(ref mut k, ref mut v) => {
                                args.next();
                                *k = BulkString::new(args.next().unwrap());
                                args.next();
                                *v = BulkString::new(args.next().unwrap());
                            }
                            Command::Get(ref mut k) => {
                                args.next();
                                *k = BulkString::new(args.next().unwrap());
                            }
                            _ => (),
                        }

                        dbg!(&cmd);
                        handle_command(stream, cmd, store.clone());
                    }
                    _ => {
                        break;
                    }
                }
            }
        }
        _ => {
            let input = input_char_itter.as_str();
            println!("received unknown data type: {}", input.trim());
        }
    }
}
