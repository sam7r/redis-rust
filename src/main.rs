use args::Value;
use command::{Command, prepare_command};
use config::Config;
use governor::{
    instance::GovernorInstance,
    master::MasterGovernor,
    slave::SlaveGovernor,
    traits::{Governor, Master, Slave},
    types::{ExpireStrategy, Role},
};
use resp::RespBuilder;
use std::{io::Read, io::Write, net::TcpListener, sync::Arc, thread};
use store::DataStore;

mod args;
mod command;
mod config;
mod governor;
mod resp;
mod store;

fn main() {
    let config = Config::new();
    let listener = TcpListener::bind(format!("{}:{}", config.host, config.port)).unwrap();
    println!("Server listening on {}:{}", config.host, config.port);
    println!(
        "Running in mode {}",
        match config.replica_of {
            Some(Value::Single(ref addr)) => format!("slave (replica of {})", addr),
            _ => "master".to_string(),
        }
    );

    let role = match config.replica_of {
        Some(Value::Single(_)) => Role::Slave,
        _ => Role::Master,
    };

    let store = Arc::new(DataStore::new());

    let mut governor_instance = match role {
        Role::Slave => {
            GovernorInstance::Slave(SlaveGovernor::new(Arc::clone(&store), ExpireStrategy::Lazy))
        }
        Role::Master => GovernorInstance::Master(MasterGovernor::new(
            Arc::clone(&store),
            ExpireStrategy::Lazy,
        )),
    };

    match governor_instance {
        GovernorInstance::Master(ref mut master_gov) => {
            master_gov.start_expire_manager();
        }
        GovernorInstance::Slave(ref mut slave_gov) => {
            if let Some(Value::Single(master_addr)) = config.replica_of {
                println!("Starting replication from master at {}", master_addr);
                match slave_gov.start_replication(&master_addr, &config.port) {
                    Ok(_) => println!("Replication started successfully"),
                    Err(err) => eprintln!("Failed to start replication: {}", err),
                }
                slave_gov.start_expire_manager();
            }
        }
    }

    let gov = Arc::new(governor_instance);
    for stream in listener.incoming() {
        println!("New client connected");
        let kv_store = Arc::clone(&store);
        let store_gov = Arc::clone(&gov);

        thread::spawn(move || {
            handle_client(stream.unwrap(), kv_store, store_gov);
        });
    }
}

enum Mode {
    Normal,
    Transaction,
}

fn handle_client(
    mut stream: std::net::TcpStream,
    store: Arc<DataStore>,
    governor: Arc<GovernorInstance>,
) {
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
                let store_gov = Arc::clone(&governor);

                if let Some(cmd) = prepare_command(&input) {
                    if let Command::Psync(replication_id, offset) = cmd.clone()
                        && let GovernorInstance::Master(master_gov) = governor.as_ref()
                    {
                        let _ = master_gov.handle_psync(stream, &replication_id, offset);
                        break;
                    } else {
                        match mode {
                            Mode::Transaction => {
                                let resp =
                                    handle_tx(kv_store, store_gov, cmd, &mut mode, &mut queue);
                                write_to_stream(&mut stream, resp.as_bytes());
                            }
                            Mode::Normal => {
                                let resp = handle_cmd(kv_store, store_gov, cmd, &mut mode);
                                write_to_stream(&mut stream, resp.as_bytes());
                            }
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
    governor: Arc<GovernorInstance>,
    command: Command,
    mode: &mut Mode,
    queue: &mut Vec<Command>,
) -> RespBuilder {
    match command {
        Command::Discard => {
            queue.clear();
            *mode = Mode::Normal;
            let mut resp = RespBuilder::new();
            resp.add_simple_string("OK");
            resp
        }
        Command::Exec => {
            let mut resp = RespBuilder::new();
            resp.add_array(&queue.len());
            for cmd in queue.iter() {
                let cmd_resp = handle_cmd(store.clone(), governor.clone(), cmd.clone(), mode);
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

fn handle_cmd(
    store: Arc<DataStore>,
    governor: Arc<GovernorInstance>,
    command: Command,
    mode: &mut Mode,
) -> RespBuilder {
    if let GovernorInstance::Master(master_gov) = governor.as_ref() {
        master_gov.propagate_command(command.clone());
    }

    if let Command::ReplConf(arg, value) = command {
        let mut resp = RespBuilder::new();
        match (arg.to_uppercase().as_str(), value.to_uppercase().as_str()) {
            ("LISTENING-PORT", _) => {
                resp.add_simple_string("OK");
            }
            ("CAPA", "PSYNC2") => {
                resp.add_simple_string("OK");
            }
            _ => {
                resp.add_simple_error("ERR Unsupported REPLCONF option");
            }
        }
        return resp;
    }

    if let Command::Wait(repl_count, wait_time) = command {
        let mut resp = RespBuilder::new();
        match governor.confirm_replica_ack(repl_count, wait_time) {
            Ok(result) => match result {
                Some(n) => {
                    resp.add_integer(&n.to_string());
                }
                None => {
                    resp.add_integer(&0.to_string());
                }
            },
            Err(err) => {
                resp.add_simple_error(&err.to_string());
            }
        }
        return resp;
    }

    if let Command::Info(options) = command {
        let mut resp = RespBuilder::new();
        let info = governor.get_info(options);
        match info {
            Ok(v) => {
                if v.is_empty() {
                    resp.add_bulk_string("OK");
                } else {
                    let mut info_str = String::new();
                    for (key, value) in v.iter() {
                        info_str.push_str(&format!("{}:{}\r\n", key, value));
                    }
                    resp.add_bulk_string(&info_str);
                }
            }
            Err(err) => {
                resp.add_simple_error(err.to_string().as_str());
                return resp;
            }
        }
        return resp;
    }

    perform_command(store, command, mode)
}

fn perform_command(store: Arc<DataStore>, command: Command, mode: &mut Mode) -> RespBuilder {
    match command {
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
        Command::Psync(_, _) => {
            let mut resp = RespBuilder::new();
            resp.add_simple_error("ERR unhandled PSYNC request");
            resp
        }
        Command::Wait(_, _) => {
            let mut resp = RespBuilder::new();
            resp.add_simple_error("ERR unhandled PSYNC request");
            resp
        }
        Command::ReplConf(_, _) | Command::Info(_) => {
            let mut resp = RespBuilder::new();
            resp.add_simple_string("OK");
            resp
        }
        Command::Discard => {
            let mut resp = RespBuilder::new();
            resp.add_simple_error("ERR DISCARD without MULTI");
            resp
        }
        Command::Exec => {
            let mut resp = RespBuilder::new();
            resp.add_simple_error("ERR EXEC without MULTI");
            resp
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
    write_to_stream(
        stream,
        RespBuilder::new().add_simple_error(message).as_bytes(),
    );
}
