#![allow(unused_imports)]
use std::{fmt::Display, io::BufReader, io::Read, io::Write, net::TcpListener, thread};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        thread::spawn(move || {
            handle_client(stream.unwrap());
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
enum Command {
    Ping,
    Echo(String),
    Unknown,
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Ping => write!(f, "PING"),
            Command::Echo(echo) => write!(f, "ECHO {}", echo),
            Command::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

impl Command {
    fn from_str(s: &str) -> Self {
        let parts: Vec<&str> = s.split_whitespace().collect();
        match parts[0].to_uppercase().as_str() {
            "PING" => Command::Ping,
            "ECHO" => Command::Echo(String::new()),
            _ => Command::Unknown,
        }
    }
}

fn handle_command(stream: &mut std::net::TcpStream, command: Command) {
    match command {
        Command::Ping => {
            let msg = "PONG";
            let response = format!("+{}{CRLF}", msg);
            let _ = stream.write(response.as_bytes()).unwrap();
        }
        Command::Echo(echo) => {
            let response = format!("${}{CRLF}{}{CRLF}", echo.len(), echo);
            let _ = stream.write(response.as_bytes()).unwrap();
        }
        _ => {
            let _ = stream.write(b"-ERR unknown command\r\n").unwrap();
        }
    }
}

fn handle_client(mut stream: std::net::TcpStream) {
    println!("accepted new connection");
    loop {
        let mut buffer = [0; 512];
        let bytes_read = stream.read(&mut buffer);

        match bytes_read {
            Ok(0) => break,
            Ok(n) => {
                let input = String::from_utf8_lossy(&buffer[..n]);
                dbg!(&input);
                handle_data(&mut stream, &input);
            }
            Err(err) => {
                eprint!("error reading from stream: {}", err);
                break;
            }
        }
    }
}

fn handle_data(stream: &mut std::net::TcpStream, data: &str) {
    let mut input_char_itter = data.chars();
    match DataType::from_char(input_char_itter.next().unwrap_or('\0')) {
        // *<number-of-elements>\r\n<element-1>...<element-n>
        DataType::Array => {
            let input = input_char_itter.as_str();
            let mut args = input.split(CRLF);
            let elements: u8 = str::parse(args.next().unwrap_or("0")).unwrap();

            for _ in 0..elements {
                let next = args.next().unwrap_or("");
                let maybe_datatype = DataType::from_char(next.chars().next().unwrap_or('\0'));

                match maybe_datatype {
                    DataType::SimpleString => {
                        let cmd = Command::from_str(args.next().unwrap());
                        dbg!("received simple string", &cmd);
                        handle_command(stream, cmd);
                    }
                    DataType::BulkString => {
                        let bulk_str = args.next().unwrap().to_string();
                        let mut cmd = Command::from_str(&bulk_str);
                        dbg!("received bulk string", &cmd);

                        if let Command::Echo(_) = cmd {
                            _ = args.next(); // skip the bulk string length
                            cmd = Command::Echo(args.next().unwrap().to_string());
                        }

                        handle_command(stream, cmd);
                    }
                    DataType::Integer => {
                        // println!("received integer: {}", next.trim());
                    }
                    _ => {
                        break;
                    }
                }
            }
        }
        DataType::SimpleString => {
            let cmd = Command::from_str(input_char_itter.as_str());
            handle_command(stream, cmd);
        }
        // $<length>\r\n<data>\r\n
        DataType::BulkString => {
            // println!("received bulk string: {}", input.trim());
        }
        DataType::Integer => {
            // println!("received integer: {}", input.trim());
        }
        DataType::None => {
            // println!("received unknown data type: {}", input.trim());
        }
    }
}
