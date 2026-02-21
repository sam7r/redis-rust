use rand::{RngExt, distr::Alphanumeric};
use std::{
    fs,
    io::{Read, Write},
    path::Path,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread, time,
};

use crate::command;
use crate::governor::{
    error::GovError,
    traits::{Governor, Master},
    types::{Config, ExpireStrategy, Info, Psync, ReplicaOffsetState},
};
use crate::persistence::encoder::RdbEncoder;
use crate::persistence::types::ValueType;
use crate::resp::RespBuilder;
use crate::store::DataStore;

struct Replica {
    pub status: ReplicaOffsetState,
    pub offset: u64,
    pub stream: Arc<Mutex<std::net::TcpStream>>,
}

pub struct MasterGovernor {
    repl_id: String,
    repl_offset: AtomicU64,
    datastore: Arc<DataStore>,
    cleanup_type: ExpireStrategy,
    replicas: RwLock<Vec<Replica>>,
    db_directory: String,
    db_filename: String,
}

impl MasterGovernor {
    pub fn new(datastore: Arc<DataStore>, cleanup_type: ExpireStrategy, config: Config) -> Self {
        MasterGovernor {
            repl_id: generate_random_id(),
            repl_offset: AtomicU64::new(0),
            datastore,
            cleanup_type,
            replicas: RwLock::new(Vec::new()),
            db_directory: config.db_directory,
            db_filename: config.db_filename,
        }
    }

    fn get_replication_info(&self) -> Vec<(String, String)> {
        vec![
            ("role".to_string(), "master".to_string()),
            ("master_replid".to_string(), self.repl_id.clone()),
            (
                "master_repl_offset".to_string(),
                self.repl_offset.load(Ordering::SeqCst).to_string(),
            ),
        ]
    }

    fn get_psync_mode(&self, replication_id: &str, _offset: i64) -> Psync {
        if self.repl_id != replication_id {
            Psync::FullResync(
                self.repl_id.clone(),
                self.repl_offset.load(Ordering::SeqCst),
            )
        } else {
            Psync::Continue
        }
    }

    fn build_rdb_file(&self) -> Vec<u8> {
        let mut encoder = RdbEncoder::new();

        encoder
            .write_header(7)
            .write_aux("redis-ver", "7.2.0")
            .write_aux_with_int("redis-bits", 64);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        encoder.write_aux_with_int("ctime", now);
        encoder.write_aux_with_int("used-mem", 0);
        encoder.write_aux_with_int("aof-base", 0);

        encoder.write_select_db(0);

        if let Ok(iter) = self.datastore.iter() {
            let hash_size = iter.data.len();
            let expires_size = iter.expires.len();
            encoder.write_resizedb(hash_size, expires_size);

            for (key, value, expiry) in iter.iter() {
                if let Some(expire_time) = expiry {
                    let expire_seconds = expire_time / 1000;
                    encoder.write_expire_seconds(expire_seconds as u32);
                }

                match value {
                    crate::store::Value::String(s) => {
                        encoder.write_value_type(ValueType::String);
                        encoder.write_string(key);
                        encoder.write_string(s);
                    }
                    crate::store::Value::List(list) => {
                        encoder.write_value_type(ValueType::List);
                        encoder.write_string(key);
                        encoder.write_length(list.len());
                        for item in list {
                            encoder.write_string(item);
                        }
                    }
                    crate::store::Value::Stream(_) => {
                        // Streams are type 14 (QuickList), skip for now
                    }
                }
            }
        }

        encoder.write_eof();
        encoder.write_checksum(0);

        encoder.finish().to_vec()
    }

    pub fn register_replica_instance(&self, stream: Arc<Mutex<std::net::TcpStream>>) {
        if let Ok(mut replicas) = self.replicas.write() {
            let status = ReplicaOffsetState::Confirmed;
            let replica = Replica {
                stream,
                status,
                offset: 0,
            };
            replicas.push(replica);
        }
    }

    fn add_repl_offset(&self, n: u64) {
        let prev = self.repl_offset.fetch_add(n, Ordering::SeqCst);
        println!("Updating master offset: {} -> {}", prev, prev + n);
    }

    fn get_replica_ack(&self) {
        if let Ok(mut replicas) = self.replicas.write()
            && self.repl_offset.load(Ordering::SeqCst) > 0
        {
            let mut get_ack_query = RespBuilder::new();
            get_ack_query
                .add_array(&3)
                .add_bulk_string("REPLCONF")
                .add_bulk_string("GETACK")
                .add_bulk_string("*");

            for repl in replicas.iter_mut() {
                let mut stream = repl.stream.lock().unwrap();
                match stream.write(get_ack_query.as_bytes()) {
                    Ok(0) | Err(_) => {
                        println!("GETACK not sent, asumming disconnected");
                        repl.status = ReplicaOffsetState::Unknown;
                        continue;
                    }
                    Ok(_) => {}
                }

                let mut buff = [0; 512];
                let _ = stream.set_read_timeout(Some(time::Duration::from_millis(100)));
                match stream.read(&mut buff) {
                    Ok(0) | Err(_) => {
                        println!("No ACK, assuming disconnected");
                        repl.status = ReplicaOffsetState::Unknown;
                        continue;
                    }
                    Ok(bytes_read) => {
                        let response = String::from_utf8_lossy(&buff[..bytes_read]);
                        let out = command::prepare_command(&response);
                        if let Some(command::Command::ReplConf(_, offset)) = out {
                            repl.offset = offset.parse::<u64>().unwrap();
                            if repl.offset != self.repl_offset.load(Ordering::SeqCst) {
                                repl.status = ReplicaOffsetState::Unconfirmed;
                            } else {
                                repl.status = ReplicaOffsetState::Confirmed;
                            }
                            println!(
                                "ACK Received offset {}, status updated to {}",
                                repl.offset, repl.status
                            )
                        } else {
                            println!("GETACK unexpected response {}", &response);
                        }
                    }
                }
            }
            self.add_repl_offset(get_ack_query.as_bytes().len() as u64);
        }
    }
}

impl Master for MasterGovernor {
    fn get_config(&self) -> Config {
        Config {
            db_filename: self.db_filename.clone(),
            db_directory: self.db_directory.clone(),
        }
    }

    fn bgsave(&self) -> Result<String, Box<dyn std::error::Error>> {
        let db_directory = self.db_directory.clone();
        let db_filename = self.db_filename.clone();

        thread::scope(|s| {
            s.spawn(move || {
                let data = self.build_rdb_file();
                let path = Path::new(&db_directory).join(&db_filename);
                if let Err(e) = fs::write(&path, data) {
                    eprintln!("BGSAVE failed: {}", e);
                } else {
                    println!("BGSAVE completed: {:?}", path);
                }
            });
        });

        Ok("Background saving started".to_string())
    }

    fn confirm_replica_ack(
        &self,
        repl_number: u8,
        max_wait_time_millis: u64,
    ) -> Result<Option<u8>, GovError> {
        {
            if repl_number == 0 || self.replicas.read().iter().len() == 0 {
                return Ok(Some(0));
            }
        }

        let timed_out = Arc::new(AtomicBool::new(false));
        let t = timed_out.clone();

        thread::spawn(move || {
            thread::sleep(time::Duration::from_millis(max_wait_time_millis));
            t.store(true, Ordering::SeqCst);
        });

        thread::scope(move |s| {
            s.spawn(move || {
                self.get_replica_ack();
            });
        });

        loop {
            let received_repl: Vec<ReplicaOffsetState> = {
                self.replicas
                    .read()
                    .unwrap()
                    .iter()
                    .filter(|&r| r.status == ReplicaOffsetState::Confirmed)
                    .map(|r| r.status)
                    .collect()
            };

            let current_acks = received_repl.len() as u8;

            if !timed_out.load(Ordering::SeqCst) {
                if current_acks >= repl_number {
                    return Ok(Some(current_acks));
                }
                thread::sleep(time::Duration::from_millis(
                    max_wait_time_millis / repl_number as u64,
                ));
            } else {
                return Ok(Some(current_acks));
            };
        }
    }

    fn propagate_command(&self, cmd: command::Command) {
        let out = command::serialize_command(cmd.clone());
        if should_propagate_command(cmd.clone())
            && let Ok(mut replicas) = self.replicas.write()
        {
            for repl in replicas.iter_mut() {
                if let Ok(mut stream) = repl.stream.lock() {
                    let _ = stream.write_all(out.as_bytes());
                    repl.status = ReplicaOffsetState::Unconfirmed;
                }
            }
            self.add_repl_offset(out.len() as u64);
        }
    }

    fn handle_psync(
        &self,
        mut stream: std::net::TcpStream,
        replication_id: &str,
        offset: i64,
    ) -> Result<Psync, Box<dyn std::error::Error>> {
        println!(
            "Handling PSYNC with replication_id: {}, offset: {}",
            replication_id, offset
        );
        let mode = self.get_psync_mode(replication_id, offset);

        match mode {
            Psync::FullResync(_, offset) => {
                stream.write_all(
                    RespBuilder::new()
                        .add_simple_string(&format!("FULLRESYNC {} {}", self.repl_id, offset))
                        .as_bytes(),
                )?;
                let rdb_data = self.build_rdb_file();
                let resp = format!("${}\r\n", rdb_data.len());
                println!("Sending FULLRESYNC response: {}", resp.trim());
                stream.write_all(resp.as_bytes())?;
                stream.write_all(&rdb_data)?;
            }
            Psync::Continue => {
                println!("Sending CONTINUE response");
                stream.write_all(RespBuilder::new().add_simple_string("CONTINUE").as_bytes())?;
                // TODO: Implement incremental replication logic here
            }
        }

        let stream = Arc::new(Mutex::new(stream));
        self.register_replica_instance(stream);

        Ok(mode)
    }
}

impl Governor for MasterGovernor {
    fn get_datastore(&self) -> Arc<DataStore> {
        Arc::clone(&self.datastore)
    }

    fn get_expire_strategy(&self) -> Option<ExpireStrategy> {
        Some(self.cleanup_type)
    }

    fn get_info(&self, options: Vec<Info>) -> Result<Vec<(String, String)>, GovError> {
        let mut result = Vec::new();
        for opt in options {
            match opt {
                Info::All | Info::Default | Info::Everything => {
                    result.extend(self.get_replication_info());
                }
                Info::Replication => {
                    result.extend(self.get_replication_info());
                }
                _ => {}
            }
        }
        Ok(result)
    }
}

fn generate_random_id() -> String {
    let mut rng = rand::rng();
    (0..40).map(|_| rng.sample(Alphanumeric) as char).collect()
}

fn should_propagate_command(cmd: command::Command) -> bool {
    matches!(
        cmd,
        command::Command::Set(_, _, _)
            | command::Command::Incr(_)
            | command::Command::Rpush(_, _)
            | command::Command::Lpush(_, _)
            | command::Command::Lpop(_, _)
            | command::Command::Xadd(_, _, _)
    )
}
