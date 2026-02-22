use std::{
    fs,
    io::{Read, Write},
    path::Path,
    sync::{Arc, RwLock},
    thread, time,
};

use crate::{
    command,
    data::{store::DataStore, types::Event},
    governor::{
        error::GovError,
        types::{Config, ExpireStrategy, Info, Psync},
    },
    persistence::{decoder::RdbDecoder, types::Expiry},
    resp::RespBuilder,
};

pub trait Governor {
    fn get_info(&self, options: Vec<Info>) -> Result<Vec<(String, String)>, GovError>;
    fn get_datastore(&self) -> Arc<DataStore>;
    fn get_expire_strategy(&self) -> Option<ExpireStrategy>;

    fn load_rdb_data(&self, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let mut decoder = RdbDecoder::new(data);
        let rdb = decoder.parse()?;
        let datastore = self.get_datastore();

        println!("Loading RDB data: version {}", rdb.version);
        for db in rdb.databases {
            println!("Loading database {}", db.db_number);
            for entry in db.entries {
                let key = String::from_utf8_lossy(&entry.key).to_string();
                if let Some(expiry) = entry.expiry {
                    let key_expiry: u128 = match expiry {
                        Expiry::Seconds(s) => (s as u128) * 1000,
                        Expiry::Milliseconds(ms) => ms as u128,
                    };

                    if time::SystemTime::now()
                        .duration_since(time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis()
                        >= key_expiry
                    {
                        println!("  Skipping expired key: {} - {}", key, key_expiry);
                        continue;
                    }

                    println!("  Setting expire for key: {} to {} ms", key, key_expiry);

                    datastore
                        .set_expire(&key, key_expiry)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
                }
                match entry.value {
                    crate::persistence::types::Value::String(v) => {
                        let value = String::from_utf8_lossy(&v).to_string();
                        datastore
                            .set(&key, value, vec![])
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
                        println!("  Set string key: {}", key);
                    }
                    crate::persistence::types::Value::List(items) => {
                        let values: Vec<String> = items
                            .iter()
                            .map(|v| String::from_utf8_lossy(v).to_string())
                            .collect();
                        datastore
                            .push(&key, values, false)
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
                        println!("  Set list key: {}", key);
                    }
                    crate::persistence::types::Value::Stream(_) => {
                        // Streams not fully supported yet
                        println!("  Skipping stream key: {}", key);
                    }
                    _ => {}
                }
            }
        }
        println!("RDB data loaded successfully");
        Ok(())
    }

    fn start_expire_manager(&mut self) {
        match self.get_expire_strategy() {
            Some(ExpireStrategy::Scheduled(interval)) => {
                let store = self.get_datastore();
                thread::spawn(move || {
                    loop {
                        thread::sleep(interval);
                        if store.cleanup().is_err() {
                            println!("Error during scheduled cleanup");
                        }
                    }
                });
            }
            Some(ExpireStrategy::Lazy) => {
                let (tx, rx) = std::sync::mpsc::channel();
                let _ = self.get_datastore().add_channel_subscriber("*", tx);
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
                let store = self.get_datastore();
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
                            if store.cleanup().is_err() {
                                println!("Error during lazy cleanup");
                            }
                            let mut wt = next_tick.write().unwrap();
                            *wt = 0;
                        }
                        thread::sleep(time::Duration::from_millis(1));
                    }
                });
            }
            None => {
                println!("No expire strategy configured, skipping");
            }
        }
    }

    fn send_ping(
        &self,
        stream: &mut std::net::TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
                    message: "Connection closed".to_string(),
                }));
            }
            Ok(bytes_read) => {
                let response = String::from_utf8_lossy(&buffer[..bytes_read]);
                if !response.starts_with("+PONG") {
                    return Err(Box::new(GovError {
                        message: "Unexpected response from PING".to_string(),
                    }));
                }
            }
            Err(e) => {
                return Err(Box::new(GovError {
                    message: format!("Failed to read stream: {}", e),
                }));
            }
        }
        Ok(())
    }
}

pub trait Master: Governor {
    fn get_config(&self) -> Config;
    fn propagate_command(&self, prepared_cmd: command::PreparedCommand);
    fn confirm_replica_ack(&self, repl_count: u8, wait_time: u64) -> Result<Option<u8>, GovError>;
    fn bgsave(&self) -> Result<String, Box<dyn std::error::Error>>;

    fn load_rdb_from_file(&self) -> Result<(), Box<dyn std::error::Error>> {
        let config = self.get_config();
        if fs::create_dir_all(&config.db_directory).is_err() {
            println!(
                "Failed to create db directory '{}', proceeding with loading if file exists",
                config.db_directory
            );
        }

        let path = Path::new(&config.db_directory).join(&config.db_filename);
        if !path.exists() {
            println!(
                "RDB file '{}' not found, skipping RDB loading",
                path.to_string_lossy()
            );
            return Ok(());
        }

        let data = std::fs::read(path)?;
        self.load_rdb_data(&data)
    }

    fn handle_psync(
        &self,
        stream: std::net::TcpStream,
        replication_id: &str,
        offset: i64,
    ) -> Result<Psync, Box<dyn std::error::Error>>;
}

pub trait Slave: Governor {
    fn start_replication(
        &mut self,
        master_addr: &str,
        self_port: &str,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
