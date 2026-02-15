use crate::{
    resp::RespBuilder,
    store::{DataStore, Event},
};
use rand::{RngExt, distr::Alphanumeric};
use std::{
    io::Write,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    thread, time,
};

use crate::command;
use crate::governor::{
    error::GovError,
    traits::{Governor, Master},
    types::{ExpireStrategy, Info, Psync},
};

pub struct MasterGovernor {
    repl_id: String,
    repl_offset: AtomicU64,
    datastore: Arc<DataStore>,
    cleanup_type: ExpireStrategy,
    slave_instances: RwLock<Vec<Arc<Mutex<std::net::TcpStream>>>>,
}

impl MasterGovernor {
    pub fn new(datastore: Arc<DataStore>, cleanup_type: ExpireStrategy) -> Self {
        MasterGovernor {
            repl_id: generate_random_id(),
            repl_offset: AtomicU64::new(0),
            datastore,
            cleanup_type,
            slave_instances: RwLock::new(Vec::new()),
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
        // Placeholder for RDB file generation logic
        let empty_rdb: Vec<u8> = vec![
            0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, // REDIS0011
            0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, // redis-ver
            0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, // 7.2.0
            0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74,
            0x73, // redis-bits
            0xc0, 0x40, // 64
            0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, // ctime
            0xc2, 0x6d, 0x08, 0xbc, 0x65, // timestamp
            0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, // used-mem
            0xc2, 0xb0, 0xc4, 0x10, 0x00, // memory
            0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, // aof-base
            0xc0, 0x00, // 0
            0xff, // EOF
            0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2, // checksum
        ];

        empty_rdb
    }
}

impl Master for MasterGovernor {
    fn set_slave_instance(&self, stream: Arc<Mutex<std::net::TcpStream>>) {
        if let Ok(mut slave_instances) = self.slave_instances.write() {
            slave_instances.push(stream);
        }
    }

    fn propagate_command(&self, cmd: command::Command) {
        if should_propagate_command(cmd.clone()) {
            self.repl_offset.fetch_add(1, Ordering::SeqCst);
            if let Ok(slave_instances) = self.slave_instances.read() {
                for stream in slave_instances.iter() {
                    if let Ok(mut stream) = stream.lock() {
                        let _ =
                            stream.write_all(command::serialize_command(cmd.clone()).as_bytes());
                    }
                }
            }
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
                stream.write_all(resp.as_bytes())?;
                stream.write_all(&rdb_data)?;
            }
            Psync::Continue => {
                stream.write_all(RespBuilder::new().add_simple_string("CONTINUE").as_bytes())?;
                // TODO: Implement incremental replication logic here
            }
        }

        let stream = Arc::new(Mutex::new(stream));
        self.set_slave_instance(stream);

        Ok(mode)
    }

    fn start_store_manager(&mut self) {
        match self.cleanup_type {
            ExpireStrategy::Scheduled(interval) => {
                let store = Arc::clone(&self.datastore);
                thread::spawn(move || {
                    loop {
                        thread::sleep(interval);
                        store.cleanup();
                    }
                });
            }
            ExpireStrategy::Lazy => {
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

impl Governor for MasterGovernor {
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
