use command::{Command, prepare_command};
use governor::{CleanupType, Governor};
use resp::RespBuilder;
use std::{io::Read, io::Write, net::TcpListener, sync::Arc, thread, time::Duration};
use store::DataStore;

mod command;
mod governor;
mod resp;
mod store;

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

enum Mode {
    Normal,
    Transaction,
}

fn handle_client(mut stream: std::net::TcpStream, store: Arc<DataStore>) {
    let mut mode = Mode::Normal;
    loop {
        let mut buffer = [0; 512];
        let bytes_read = stream.read(&mut buffer);

        match bytes_read {
            Ok(0) => break,
            Ok(n) => {
                let input = String::from_utf8_lossy(&buffer[..n]);
                let kv_store = Arc::clone(&store);

                if let Some(cmd) = prepare_command(&input) {
                    match mode {
                        Mode::Transaction => {
                            handle_transaction(&mut stream, kv_store, cmd, &mut mode)
                        }
                        Mode::Normal => handle_command(&mut stream, kv_store, cmd, &mut mode),
                    }
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

fn handle_transaction(
    stream: &mut std::net::TcpStream,
    store: Arc<DataStore>,
    command: Command,
    mode: &mut Mode,
) {
    match command {
        _ => {}
    }
}

fn handle_command(
    stream: &mut std::net::TcpStream,
    store: Arc<DataStore>,
    command: Command,
    mode: &mut Mode,
) {
    match command {
        Command::Multi => {
            *mode = Mode::Transaction;
            write_to_stream(
                stream,
                RespBuilder::new().add_simple_string("OK").as_bytes(),
            );
        }
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
                write_error_to_stream(stream, err.to_string().as_str());
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
                write_error_to_stream(stream, err.to_string().as_str());
            }
        },
        Command::Incr(key) => match store.incr(&key) {
            Ok(result) => {
                if let Some(n) = result {
                    write_to_stream(
                        stream,
                        RespBuilder::new().add_integer(&n.to_string()).as_bytes(),
                    );
                } else {
                    write_error_to_stream(stream, "not a string");
                }
            }
            Err(err) => {
                write_error_to_stream(stream, err.to_string().as_str());
            }
        },
        Command::Rpush(key, list) => match store.push(&key, list, false) {
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
                write_error_to_stream(stream, err.to_string().as_str());
            }
        },
        Command::Lpush(key, list) => match store.push(&key, list, true) {
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
                write_error_to_stream(stream, err.to_string().as_str());
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
                write_error_to_stream(stream, err.to_string().as_str());
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
                write_error_to_stream(stream, err.to_string().as_str());
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
                write_error_to_stream(stream, err.to_string().as_str());
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
                write_error_to_stream(stream, err.to_string().as_str());
            }
        },
        Command::Xadd(key, entry_id, fields) => match store.xadd(&key, &entry_id, fields) {
            Ok(result) => {
                if let Some(id) = result {
                    write_to_stream(stream, RespBuilder::new().add_bulk_string(&id).as_bytes());
                } else {
                    write_error_to_stream(stream, "not a stream");
                }
            }
            Err(err) => {
                write_error_to_stream(stream, err.to_string().as_str());
            }
        },
        Command::Xrange(key, entry_id_start, entry_id_stop) => {
            match store.xrange(&key, &entry_id_start, &entry_id_stop, 0) {
                Ok(result) => {
                    if let Some(stream_range) = result {
                        let mut resp = RespBuilder::new();
                        resp.add_array(&stream_range.len());

                        for ((entry_millis, entry_seq), items) in stream_range.iter() {
                            resp.add_array(&2);
                            resp.add_bulk_string(&format!("{}-{}", entry_millis, entry_seq));
                            resp.add_array(&(items.len() * 2));

                            for (field, value) in items {
                                resp.add_bulk_string(field);
                                resp.add_bulk_string(value);
                            }
                        }
                        write_to_stream(stream, resp.as_bytes());
                    } else {
                        write_error_to_stream(stream, "not a stream");
                    }
                }
                Err(err) => {
                    write_error_to_stream(stream, err.to_string().as_str());
                }
            }
        }
        Command::Xread(options) => match store.xread(options) {
            Ok(result) => {
                let mut resp = RespBuilder::new();
                if result.is_empty() {
                    resp.negative_array();
                } else {
                    resp.add_array(&result.len());

                    for stream_entry in result {
                        match stream_entry {
                            (stream_key, Some(stream_range)) => {
                                resp.add_array(&2);
                                resp.add_bulk_string(&stream_key);
                                resp.add_array(&stream_range.len());

                                for ((entry_millis, entry_seq), items) in stream_range.iter() {
                                    resp.add_array(&2);
                                    resp.add_bulk_string(&format!(
                                        "{}-{}",
                                        entry_millis, entry_seq
                                    ));
                                    resp.add_array(&(items.len() * 2));

                                    for (field, value) in items {
                                        resp.add_bulk_string(field);
                                        resp.add_bulk_string(value);
                                    }
                                }
                            }
                            (stream_key, None) => {
                                resp.add_array(&2);
                                resp.add_bulk_string(&stream_key);
                                resp.negative_array();
                            }
                        }
                    }
                }

                write_to_stream(stream, resp.as_bytes());
            }
            Err(err) => {
                write_error_to_stream(stream, err.to_string().as_str());
            }
        },
        _ => {}
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
    write_to_stream(
        stream,
        RespBuilder::new().add_simple_error(message).as_bytes(),
    );
}
