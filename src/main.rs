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
            cleanup_type: CleanupType::Scheduled(Duration::from_millis(5)),
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

#[derive(Debug)]
enum Mode {
    Normal,
    Transaction,
}

fn handle_client(mut stream: std::net::TcpStream, store: Arc<DataStore>) {
    let mut mode = Mode::Normal;
    let mut queue: Vec<Command> = Vec::new();

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
                            let resp = handle_tx(kv_store, cmd, &mut mode, &mut queue);
                            write_to_stream(&mut stream, resp.as_bytes());
                        }
                        Mode::Normal => {
                            let resp = handle_cmd(kv_store, cmd, &mut mode);
                            write_to_stream(&mut stream, resp.as_bytes());
                        }
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

fn handle_tx(
    store: Arc<DataStore>,
    command: Command,
    mode: &mut Mode,
    queue: &mut Vec<Command>,
) -> RespBuilder {
    match command {
        Command::Exec => {
            let mut resp = RespBuilder::new();
            resp.add_array(&queue.len());
            for cmd in queue.iter() {
                let cmd_resp = handle_cmd(store.clone(), cmd.clone(), mode);
                resp.join(&cmd_resp.to_string());
            }
            queue.clear();
            *mode = Mode::Normal;
            resp
        }
        _ => {
            queue.push(command);
            let mut resp = RespBuilder::new();
            resp.add_simple_string("QUEUED");
            resp
        }
    }
}

fn handle_cmd(store: Arc<DataStore>, command: Command, mode: &mut Mode) -> RespBuilder {
    match command {
        Command::Exec => {
            let mut resp = RespBuilder::new();
            resp.add_simple_error("ERR EXEC without MULTI");
            resp
        }
        Command::Multi => {
            *mode = Mode::Transaction;
            let mut resp = RespBuilder::new();
            resp.add_simple_string("OK");
            resp
        }
        Command::Ping => {
            let mut resp = RespBuilder::new();
            resp.add_simple_string("PONG");
            resp
        }
        Command::Echo(echo) => {
            let mut resp = RespBuilder::new();
            resp.add_bulk_string(&echo);
            resp
        }
        Command::Type(key) => match store.get_type(&key) {
            Ok(t) => {
                let mut resp = RespBuilder::new();
                if let Some(value_type) = t {
                    resp.add_simple_string(&value_type);
                } else {
                    resp.add_simple_string("none");
                }
                resp
            }
            Err(_) => {
                let mut resp = RespBuilder::new();
                resp.add_simple_error("failed to get type");
                resp
            }
        },
        Command::Set(key, value, options) => match store.set(&key, value, options) {
            Ok(v) => {
                let mut resp = RespBuilder::new();
                if let Some(result) = v {
                    resp.add_simple_string(&result.to_string());
                } else {
                    resp.add_simple_string("OK");
                }
                resp
            }
            Err(err) => {
                let mut resp = RespBuilder::new();
                resp.add_simple_error(err.to_string().as_str());
                resp
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
                resp
            }
            Err(err) => {
                let mut resp = RespBuilder::new();
                resp.add_simple_error(err.to_string().as_str());
                resp
            }
        },
        Command::Incr(key) => match store.incr(&key) {
            Ok(result) => {
                if let Some(n) = result {
                    let mut resp = RespBuilder::new();
                    resp.add_integer(&n.to_string());
                    resp
                } else {
                    let mut resp = RespBuilder::new();
                    resp.add_simple_error("not a string");
                    resp
                }
            }
            Err(err) => {
                let mut resp = RespBuilder::new();
                resp.add_simple_error(err.to_string().as_str());
                resp
            }
        },
        Command::Rpush(key, list) => match store.push(&key, list, false) {
            Ok(result) => {
                if let Some(n) = result {
                    let mut resp = RespBuilder::new();
                    resp.add_integer(&n.to_string());
                    resp
                } else {
                    let mut resp = RespBuilder::new();
                    resp.add_simple_error("not a list");
                    resp
                }
            }
            Err(err) => {
                let mut resp = RespBuilder::new();
                resp.add_simple_error(err.to_string().as_str());
                resp
            }
        },
        Command::Lpush(key, list) => match store.push(&key, list, true) {
            Ok(result) => {
                if let Some(n) = result {
                    let mut resp = RespBuilder::new();
                    resp.add_integer(&n.to_string());
                    resp
                } else {
                    let mut resp = RespBuilder::new();
                    resp.add_simple_error("not a list");
                    resp
                }
            }
            Err(err) => {
                let mut resp = RespBuilder::new();
                resp.add_simple_error(err.to_string().as_str());
                resp
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
                    resp
                } else {
                    let mut resp = RespBuilder::new();
                    resp.add_simple_error("not a list");
                    resp
                }
            }
            Err(err) => {
                let mut resp = RespBuilder::new();
                resp.add_simple_error(err.to_string().as_str());
                resp
            }
        },
        Command::Llen(key) => match store.llen(key) {
            Ok(result) => {
                if let Some(n) = result {
                    let mut resp = RespBuilder::new();
                    resp.add_integer(&n.to_string());
                    resp
                } else {
                    let mut resp = RespBuilder::new();
                    resp.add_simple_error("not a list");
                    resp
                }
            }
            Err(err) => {
                let mut resp = RespBuilder::new();
                resp.add_simple_error(err.to_string().as_str());
                resp
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
                    resp
                } else {
                    let mut resp = RespBuilder::new();
                    resp.add_simple_error("not a list or empty list");
                    resp
                }
            }
            Err(err) => {
                let mut resp = RespBuilder::new();
                resp.add_simple_error(err.to_string().as_str());
                resp
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
                    resp
                }
                None => {
                    let mut resp = RespBuilder::new();
                    resp.negative_array();
                    resp
                }
            },
            Err(err) => {
                let mut resp = RespBuilder::new();
                resp.add_simple_error(err.to_string().as_str());
                resp
            }
        },
        Command::Xadd(key, entry_id, fields) => match store.xadd(&key, &entry_id, fields) {
            Ok(result) => {
                if let Some(id) = result {
                    let mut resp = RespBuilder::new();
                    resp.add_bulk_string(&id);
                    resp
                } else {
                    let mut resp = RespBuilder::new();
                    resp.add_simple_error("not a stream");
                    resp
                }
            }
            Err(err) => {
                let mut resp = RespBuilder::new();
                resp.add_simple_error(err.to_string().as_str());
                resp
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
                        resp
                    } else {
                        let mut resp = RespBuilder::new();
                        resp.add_simple_error("not a stream");
                        resp
                    }
                }
                Err(err) => {
                    let mut resp = RespBuilder::new();
                    resp.add_simple_error(err.to_string().as_str());
                    resp
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

                resp
            }
            Err(err) => {
                let mut resp = RespBuilder::new();
                resp.add_simple_error(err.to_string().as_str());
                resp
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
    write_to_stream(
        stream,
        RespBuilder::new().add_simple_error(message).as_bytes(),
    );
}
