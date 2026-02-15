use super::resp::RespBuilder;
use super::store::{DataStore, Event};
use rand::{RngExt, distr::Alphanumeric};
use std::{
    error, fmt,
    io::{Read, Write},
    net::TcpStream,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    thread, time,
};

#[allow(dead_code)]
#[derive(Debug)]
pub enum CleanupType {
    Scheduled(time::Duration),
    Lazy,
}

pub struct Governor {
    role: Role,
    datastore: Arc<DataStore>,
    cleanup_type: CleanupType,
    repl_offset: AtomicU64,
    repl_id: String,
    slave_list: RwLock<Vec<String>>,
}

#[derive(Debug)]
pub enum Role {
    Master,
    Slave,
}

#[derive(Debug)]
pub struct Options {
    pub role: Role,
    pub cleanup_type: CleanupType,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            role: Role::Master,
            cleanup_type: CleanupType::Lazy,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Info {
    All,
    Default,
    Everything,
    Server,
    Clients,
    Memory,
    Persistence,
    Stats,
    Replication,
    Cpu,
    Commandstats,
    Cluster,
    Keyspace,
}

#[derive(Debug)]
pub struct GovError {
    message: String,
}

impl fmt::Display for GovError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl error::Error for GovError {}

impl Governor {
    pub fn new(datastore: Arc<DataStore>, options: Options) -> Self {
        Governor {
            datastore,
            role: options.role,
            cleanup_type: options.cleanup_type,
            repl_id: generate_random_id(),
            repl_offset: AtomicU64::new(0),
            slave_list: RwLock::new(Vec::new()),
        }
    }

    pub fn get_info(&self, options: Vec<Info>) -> Result<Vec<(String, String)>, GovError> {
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

    fn get_replication_info(&self) -> Vec<(String, String)> {
        let mut info = vec![(
            "role".to_string(),
            match self.role {
                Role::Master => "master".to_string(),
                Role::Slave => "slave".to_string(),
            },
        )];

        if let Role::Master = self.role {
            info.push(("master_replid".to_string(), self.repl_id.to_string()));
            info.push((
                "master_repl_offset".to_string(),
                self.repl_offset.load(Ordering::SeqCst).to_string(),
            ));
        }

        info
    }

    pub fn start(&mut self) {
        match self.cleanup_type {
            CleanupType::Scheduled(interval) => {
                let store = Arc::clone(&self.datastore);
                thread::spawn(move || {
                    loop {
                        thread::sleep(interval);
                        store.cleanup();
                    }
                });
            }
            CleanupType::Lazy => {
                let (tx, rx) = std::sync::mpsc::channel();
                let _ = self.datastore.add_channel_subscriber("*", tx);
                let next_tick = Arc::new(RwLock::new(0));

                let nt = Arc::clone(&next_tick);
                thread::spawn(move || {
                    while let Ok(event) = rx.recv() {
                        if let Event::ExpireUpdated(exp_time_millis) = event {
                            let should_update = {
                                let rt = nt.read().unwrap();
                                let now = time::SystemTime::now()
                                    .duration_since(time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis();
                                *rt < now || *rt > exp_time_millis
                            };

                            if should_update {
                                let mut wt = nt.write().unwrap();
                                *wt = exp_time_millis;
                            }
                        }
                    }
                });

                let tt = Arc::clone(&next_tick);
                let store = Arc::clone(&self.datastore);
                thread::spawn(move || {
                    loop {
                        let should_cleanup = {
                            let rt = tt.read().unwrap();
                            if *rt > 0 {
                                let now = time::SystemTime::now()
                                    .duration_since(time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis();
                                *rt <= now
                            } else {
                                false
                            }
                        };
                        if should_cleanup {
                            store.cleanup();
                            let mut wt = next_tick.write().unwrap();
                            *wt = 0;
                        }
                        thread::sleep(time::Duration::from_millis(1));
                    }
                });
            }
        }
    }

    pub fn start_replication(
        &mut self,
        master_addr: &str,
        self_port: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let master_addr_split = master_addr.split(' ').collect::<Vec<&str>>();
        let master_host = master_addr_split[0];
        let master_port = master_addr_split[1].parse::<u16>()?;

        let mut stream = TcpStream::connect(format!("{}:{}", master_host, master_port))?;
        // send ping command
        stream.write_all(
            RespBuilder::new()
                .add_array(&1)
                .add_bulk_string("PING")
                .as_bytes(),
        )?;

        let mut buffer = [0; 512];
        match stream.read(&mut buffer) {
            Ok(0) => {
                return Err(Box::new(GovError {
                    message: "Connection closed by master".to_string(),
                }));
            }
            Ok(bytes_read) => {
                let response = String::from_utf8_lossy(&buffer[..bytes_read]);
                if !response.starts_with("+PONG") {
                    return Err(Box::new(GovError {
                        message: "Unexpected response from master PING".to_string(),
                    }));
                }
                let repl_conf_messages = vec![
                    RespBuilder::new()
                        .add_array(&3)
                        .add_bulk_string("REPLCONF")
                        .add_bulk_string("listening-port")
                        .add_bulk_string(self_port)
                        .as_bytes()
                        .to_vec(),
                    RespBuilder::new()
                        .add_array(&3)
                        .add_bulk_string("REPLCONF")
                        .add_bulk_string("capa")
                        .add_bulk_string("psync2")
                        .as_bytes()
                        .to_vec(),
                ];

                for msg in repl_conf_messages {
                    stream.write_all(&msg)?;
                    let mut repl_buffer = [0; 512];
                    let bytes_read_repl = stream.read(&mut repl_buffer)?;
                    let response = String::from_utf8_lossy(&repl_buffer[..bytes_read_repl]);
                    if !response.starts_with("+OK") {
                        return Err(Box::new(GovError {
                            message: "Unexpected response from master REPLCONF".to_string(),
                        }));
                    }
                }

                stream.write_all(
                    RespBuilder::new()
                        .add_array(&3)
                        .add_bulk_string("PSYNC")
                        .add_bulk_string("?")
                        .add_bulk_string("-1")
                        .as_bytes(),
                )?;
                let mut psync_buffer = [0; 512];
                let bytes_read_psync = stream.read(&mut psync_buffer)?;
                let response = String::from_utf8_lossy(&psync_buffer[..bytes_read_psync]);
                if !response.starts_with("+FULLRESYNC") && !response.starts_with("+CONTINUE") {
                    return Err(Box::new(GovError {
                        message: "Unexpected response from master PSYNC".to_string(),
                    }));
                }
            }
            Err(e) => {
                return Err(Box::new(GovError {
                    message: format!("Failed to read from master: {}", e),
                }));
            }
        }
        Ok(())
    }

    pub fn set_slave_listening_port(&self, port: &str) {
        let mut data = self.slave_list.write().unwrap();
        data.push(port.to_string());
    }

    pub fn handle_psync(&self, replication_id: &str, offset: i64) -> Result<String, GovError> {
        if replication_id != self.repl_id {
            return Ok(format!(
                "FULLRESYNC {} {}",
                self.repl_id,
                self.repl_offset.load(Ordering::SeqCst)
            ));
        }
        if offset as u64 == self.repl_offset.load(Ordering::SeqCst) {
            return Ok("CONTINUE".to_string());
        }
        Ok(format!(
            "FULLRESYNC {} {}",
            self.repl_id,
            self.repl_offset.load(Ordering::SeqCst)
        ))
    }
}

fn generate_random_id() -> String {
    let mut rng = rand::rng();
    (0..40).map(|_| rng.sample(Alphanumeric) as char).collect()
}
