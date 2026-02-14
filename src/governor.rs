use super::store::{DataStore, Event};
use std::{
    sync::{Arc, RwLock},
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

pub struct Error {
    message: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Governor {
    pub fn new(datastore: Arc<DataStore>, options: Options) -> Self {
        Governor {
            datastore,
            role: options.role,
            cleanup_type: options.cleanup_type,
        }
    }

    pub fn get_info(&self, options: Vec<Info>) -> Result<Vec<(String, String)>, Error> {
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
        vec![(
            "role".to_string(),
            match self.role {
                Role::Master => "master".to_string(),
                Role::Slave => "slave".to_string(),
            },
        )]
    }

    pub fn start(&self) {
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
}
