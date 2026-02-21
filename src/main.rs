use args::Value;
use command::{Command, prepare_command};
use governor::{
    instance::GovernorInstance,
    master::MasterGovernor,
    slave::SlaveGovernor,
    traits::{Governor, Master, Slave},
    types::{Config, ExpireStrategy, Role},
};
use resp::RespBuilder;
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::Arc,
    thread,
};
use store::DataStore;

use crate::message::{
    broker::{Broker, MessageBroker},
    types::{Message, SubscriberId, TopicFilter},
};

mod args;
mod command;
mod config;
mod governor;
mod message;
mod persistence;
mod resp;
mod store;

fn main() {
    let config = config::Config::new();
    let listener = TcpListener::bind(format!("{}:{}", config.host, config.port)).unwrap();
    println!("Server listening on {}:{}", config.host, config.port);
    println!(
        "Running in replica mode {}",
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
            Config {
                db_filename: config.db_filename,
                db_directory: config.db_dir,
            },
        )),
    };

    match governor_instance {
        GovernorInstance::Master(ref mut master_gov) => {
            master_gov.start_expire_manager();
            master_gov.load_rdb_from_file().unwrap_or_else(|err| {
                eprintln!("ERR Failed to load RDB from file: {}", err);
                std::process::exit(1);
            });
        }
        GovernorInstance::Slave(ref mut slave_gov) => {
            if let Some(Value::Single(master_addr)) = config.replica_of {
                println!("Starting replication from master at {}", master_addr);
                match slave_gov.start_replication(&master_addr, &config.port) {
                    Ok(_) => println!("Replication started successfully"),
                    Err(err) => eprintln!("ERR Failed to start replication: {}", err),
                }
                slave_gov.start_expire_manager();
            }
        }
    }

    let gov = Arc::new(governor_instance);
    let messenger = Arc::new(Broker::new());

    for stream in listener.incoming() {
        println!("New client connected");
        let kv_store = Arc::clone(&store);
        let store_gov = Arc::clone(&gov);
        let message_broker = Arc::clone(&messenger);

        thread::spawn(move || {
            handle_client(stream.unwrap(), kv_store, store_gov, message_broker);
        });
    }
}

enum Mode {
    Normal,
    Transaction,
    Subscribe,
}

fn handle_client(
    mut stream: std::net::TcpStream,
    store: Arc<DataStore>,
    governor: Arc<GovernorInstance>,
    messenger: Arc<Broker>,
) {
    let mut mode = Mode::Normal;
    let mut queue: Vec<Command> = Vec::new();
    let mut subscriber_id: Option<SubscriberId> = None;

    loop {
        let mut buffer = [0; 512];
        let bytes_read = stream.read(&mut buffer);

        match bytes_read {
            Ok(0) => {
                println!("Client disconnected");
                if let Mode::Subscribe = mode
                    && let Some(id) = subscriber_id
                {
                    messenger.drop_subscriber(id).unwrap_or_else(|err| {
                        eprintln!("ERR Failed to drop subscriber {}: {}", id, err);
                    });
                }
                break;
            }
            Ok(n) => {
                let input = String::from_utf8_lossy(&buffer[..n]);
                let kv_store = Arc::clone(&store);
                let store_gov = Arc::clone(&governor);
                let message_broker = Arc::clone(&messenger);

                if let Some(cmd) = prepare_command(&input) {
                    match mode {
                        Mode::Subscribe => {
                            let resp = process_subscribe_cmd(
                                message_broker,
                                cmd,
                                subscriber_id.unwrap_or(0),
                                &mut mode,
                            );
                            write_to_stream(&mut stream, resp.as_bytes());
                        }
                        Mode::Transaction => {
                            let resp = process_transaction_cmd(
                                kv_store,
                                store_gov,
                                message_broker,
                                cmd,
                                &mut mode,
                                &mut queue,
                            );
                            write_to_stream(&mut stream, resp.as_bytes());
                        }
                        Mode::Normal => {
                            if let Command::Psync(replication_id, offset) = cmd.clone()
                                && let GovernorInstance::Master(master_gov) = governor.as_ref()
                            {
                                let _ = master_gov.handle_psync(stream, &replication_id, offset);
                                break;
                            }
                            if let Command::Subscribe(topics) = cmd.clone() {
                                handle_new_subscriber(
                                    message_broker,
                                    &mut subscriber_id,
                                    &mut mode,
                                    topics,
                                    stream.try_clone().unwrap(),
                                );
                                continue;
                            }
                            let resp = process_normal_cmd(
                                kv_store,
                                store_gov,
                                message_broker,
                                cmd,
                                &mut mode,
                            );
                            write_to_stream(&mut stream, resp.as_bytes());
                        }
                    }
                } else {
                    write_error_to_stream(&mut stream, "ERR Unable to handle request");
                }
            }
            Err(err) => {
                eprintln!("ERR unable to read from stream: {}", err);
                break;
            }
        }
    }
}

fn handle_new_subscriber(
    messenger: Arc<Broker>,
    subscriber_id: &mut Option<SubscriberId>,
    mode: &mut Mode,
    topics: Vec<String>,
    mut stream: std::net::TcpStream,
) {
    match messenger.register_subscriber() {
        Ok((id, rx)) => {
            *subscriber_id = Some(id);
            *mode = Mode::Subscribe;

            for topic in topics.iter() {
                messenger
                    .subscribe(id, TopicFilter::Exact(topic.clone()))
                    .unwrap_or_else(|err| {
                        eprintln!("Failed to subscribe subscriber {}: {}", id, err);
                        Some(0)
                    });
            }

            let mut resp = RespBuilder::new();
            resp.add_array(&(topics.len() + 2));
            resp.add_bulk_string("subscribe");
            for topic in topics.iter() {
                resp.add_bulk_string(topic);
            }
            resp.add_integer(&(topics.len().to_string()));
            write_to_stream(&mut stream, resp.as_bytes());

            thread::spawn(move || {
                for message in rx.iter() {
                    let mut msg_resp = RespBuilder::new();
                    msg_resp.add_array(&3);
                    msg_resp.add_bulk_string("message");
                    msg_resp.add_bulk_string(&message.topic);
                    msg_resp.add_bulk_string(&message.payload);
                    write_to_stream(&mut stream, msg_resp.as_bytes());
                }
                println!("Subscriber thread exiting");
            });
        }
        Err(err) => {
            eprintln!("Failed to register subscriber: {}", err);
            write_error_to_stream(&mut stream, "ERR Failed to register subscriber");
        }
    }
}

fn process_subscribe_cmd(
    messenger: Arc<Broker>,
    command: Command,
    subscriber_id: SubscriberId,
    mode: &mut Mode,
) -> RespBuilder {
    match command {
        Command::Quit => {
            *mode = Mode::Normal;
            messenger
                .drop_subscriber(subscriber_id)
                .unwrap_or_else(|err| {
                    eprintln!("Failed to drop subscriber {}: {}", subscriber_id, err);
                });
            let mut resp = RespBuilder::new();
            resp.add_simple_string("OK");
            resp
        }
        Command::Reset => {
            messenger
                .unsubscribe_all(subscriber_id)
                .unwrap_or_else(|err| {
                    eprintln!(
                        "Failed to unsubscribe subscriber {}: {}",
                        subscriber_id, err
                    );
                });
            let mut resp = RespBuilder::new();
            resp.add_simple_string("OK");
            resp
        }
        Command::Subscribe(topics) => {
            let mut subs = 0;
            for topic in topics.iter() {
                if let Some(count) = messenger
                    .subscribe(subscriber_id, TopicFilter::Exact(topic.clone()))
                    .unwrap_or_else(|err| {
                        eprintln!("Failed to subscribe subscriber {}: {}", subscriber_id, err);
                        Some(0)
                    })
                {
                    subs = count;
                }
            }

            let mut resp = RespBuilder::new();
            resp.add_array(&(topics.len() + 2));
            resp.add_bulk_string("subscribe");
            for topic in topics.iter() {
                resp.add_bulk_string(topic);
            }
            resp.add_integer(&(subs.to_string()));
            resp
        }
        Command::Unsubscribe(topics) => {
            let mut subs = 0;
            for topic in topics.iter() {
                if let Some(count) = messenger
                    .unsubscribe(subscriber_id, TopicFilter::Exact(topic.clone()))
                    .unwrap_or_else(|err| {
                        eprintln!("Failed to subscribe subscriber {}: {}", subscriber_id, err);
                        Some(0)
                    })
                {
                    subs = count;
                }
            }

            let mut resp = RespBuilder::new();
            resp.add_array(&(topics.len() + 2));
            resp.add_bulk_string("unsubscribe");
            for topic in topics.iter() {
                resp.add_bulk_string(topic);
            }
            resp.add_integer(&(subs.to_string()));
            resp
        }
        Command::Psubscribe(topics) => {
            let mut subs = 0;
            for topic in topics.iter() {
                if let Some(count) = messenger
                    .subscribe(subscriber_id, TopicFilter::Pattern(topic.clone()))
                    .unwrap_or_else(|err| {
                        eprintln!("Failed to subscribe subscriber {}: {}", subscriber_id, err);
                        Some(0)
                    })
                {
                    subs = count;
                }
            }

            let mut resp = RespBuilder::new();
            resp.add_array(&(topics.len() + 2));
            resp.add_bulk_string("subscribe");
            for topic in topics.iter() {
                resp.add_bulk_string(topic);
            }
            resp.add_integer(&(subs.to_string()));
            resp
        }
        Command::Punsubscribe(topics) => {
            let mut subs = 0;
            for topic in topics.iter() {
                if let Some(count) = messenger
                    .unsubscribe(subscriber_id, TopicFilter::Pattern(topic.clone()))
                    .unwrap_or_else(|err| {
                        eprintln!("Failed to subscribe subscriber {}: {}", subscriber_id, err);
                        Some(0)
                    })
                {
                    subs = count;
                }
            }

            let mut resp = RespBuilder::new();
            resp.add_array(&(topics.len() + 2));
            resp.add_bulk_string("unsubscribe");
            for topic in topics.iter() {
                resp.add_bulk_string(topic);
            }
            resp.add_integer(&(subs.to_string()));
            resp
        }
        Command::Ping => {
            let mut resp = RespBuilder::new();
            resp.add_array(&2);
            resp.add_bulk_string("pong");
            resp.empty_bulk_string();
            resp
        }
        command => {
            let raw = command::serialize_command(command);
            let cmd = raw.split_whitespace().nth(2).unwrap();
            let mut resp = RespBuilder::new();
            resp.add_simple_error(&format!("ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", &cmd.to_lowercase()));
            resp
        }
    }
}

fn process_transaction_cmd(
    store: Arc<DataStore>,
    governor: Arc<GovernorInstance>,
    messenger: Arc<Broker>,
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
                let cmd_resp = process_normal_cmd(
                    store.clone(),
                    governor.clone(),
                    messenger.clone(),
                    cmd.clone(),
                    mode,
                );
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

fn process_normal_cmd(
    store: Arc<DataStore>,
    governor: Arc<GovernorInstance>,
    messenger: Arc<Broker>,
    command: Command,
    mode: &mut Mode,
) -> RespBuilder {
    if let GovernorInstance::Master(master_gov) = governor.as_ref() {
        master_gov.propagate_command(command.clone());
    }

    if let Command::Publish(topic, payload) = command {
        let message = Message {
            topic: topic.clone(),
            payload,
        };
        let mut resp = RespBuilder::new();
        if let Ok(Some(count)) = messenger.publish(message) {
            resp.add_integer(&count.to_string());
            return resp;
        } else {
            resp.add_simple_error("ERR Failed to publish message");
        }
        return resp;
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

    if let Command::ConfigGet(args) = command {
        let mut config_args: Vec<&str> = Vec::new();
        let config = governor.get_config();
        for arg in args {
            match arg.to_string().as_str() {
                "dir" => {
                    config_args.push("dir");
                    config_args.push(&config.db_directory);
                }
                "dbfilename" => {
                    config_args.push("dbfilename");
                    config_args.push(&config.db_filename);
                }
                _ => {}
            }
        }
        let mut resp = RespBuilder::new();
        resp.add_array(&config_args.len());
        for arg in config_args {
            resp.add_bulk_string(arg);
        }
        return resp;
    }

    if let Command::BgSave = command {
        let mut resp = RespBuilder::new();
        match governor.bgsave() {
            Ok(msg) => {
                resp.add_simple_string(&msg);
            }
            Err(err) => {
                resp.add_simple_error(err.to_string().as_str());
            }
        }
        return resp;
    }

    perform_command(store, command, mode)
}

fn perform_command(store: Arc<DataStore>, command: Command, mode: &mut Mode) -> RespBuilder {
    match command {
        Command::Keys(query) => match store.keys(&query) {
            Ok(keys) => {
                let mut resp = RespBuilder::new();
                resp.add_array(&keys.len());
                keys.iter().for_each(|key| {
                    resp.add_bulk_string(key);
                });
                resp
            }
            Err(err) => {
                let mut resp = RespBuilder::new();
                resp.add_simple_error(err.to_string().as_str());
                resp
            }
        },
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
            resp.add_simple_error("ERR unhandled WAIT request");
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
        Command::ConfigGet(_) => {
            let mut resp = RespBuilder::new();
            resp.add_simple_error("ERR CONFIG not handled");
            resp
        }
        Command::BgSave => {
            let mut resp = RespBuilder::new();
            resp.add_simple_error("ERR BGSAVE not handled");
            resp
        }
        Command::Publish(_, _)
        | Command::Subscribe(_)
        | Command::Quit
        | Command::Psubscribe(_)
        | Command::Unsubscribe(_)
        | Command::Punsubscribe(_) => {
            let mut resp = RespBuilder::new();
            resp.add_simple_error("ERR unhandled subscription command");
            resp
        }
        Command::Reset => {
            *mode = Mode::Normal;
            let mut resp = RespBuilder::new();
            resp.add_simple_error("OK");
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
