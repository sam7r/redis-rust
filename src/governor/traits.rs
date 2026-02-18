use std::{
    io::{Read, Write},
    sync::{Arc, RwLock},
    thread, time,
};

use crate::{
    command,
    governor::{
        error::GovError,
        types::{Config, ExpireStrategy, Info, Psync},
    },
    resp::RespBuilder,
    store::{DataStore, Event},
};

pub trait Governor {
    fn get_info(&self, options: Vec<Info>) -> Result<Vec<(String, String)>, GovError>;
    fn get_datastore(&self) -> Arc<DataStore>;
    fn get_expire_strategy(&self) -> Option<ExpireStrategy>;

    fn start_expire_manager(&mut self) {
        match self.get_expire_strategy() {
            Some(ExpireStrategy::Scheduled(interval)) => {
                let store = self.get_datastore();
                thread::spawn(move || {
                    loop {
                        thread::sleep(interval);
                        store.cleanup();
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
                            store.cleanup();
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
    fn propagate_command(&self, command: command::Command);
    fn confirm_replica_ack(&self, repl_count: u8, wait_time: u64) -> Result<Option<u8>, GovError>;

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
