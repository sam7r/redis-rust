use rand::{RngExt, distr::Alphanumeric};
use std::{
    io::Write,
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
    types::{ExpireStrategy, Info, Psync, ReplicaStatus},
};
use crate::resp::RespBuilder;
use crate::store::DataStore;

struct Replica {
    pub status: ReplicaStatus,
    pub stream: Arc<Mutex<std::net::TcpStream>>,
}

pub struct MasterGovernor {
    repl_id: String,
    repl_offset: AtomicU64,
    datastore: Arc<DataStore>,
    cleanup_type: ExpireStrategy,
    connected_replicas: RwLock<Vec<Replica>>,
}

impl MasterGovernor {
    pub fn new(datastore: Arc<DataStore>, cleanup_type: ExpireStrategy) -> Self {
        MasterGovernor {
            repl_id: generate_random_id(),
            repl_offset: AtomicU64::new(0),
            datastore,
            cleanup_type,
            connected_replicas: RwLock::new(Vec::new()),
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

    pub fn set_replica_instance(&self, stream: Arc<Mutex<std::net::TcpStream>>) {
        if let Ok(mut replicas) = self.connected_replicas.write() {
            let status = ReplicaStatus::Connected;
            let replica = Replica { stream, status };
            replicas.push(replica);
        }
    }

    fn get_replica_ack(&self) {
        if let Ok(mut replicas) = self.connected_replicas.write() {
            for repl in replicas.iter_mut() {
                let mut stream = repl.stream.lock().unwrap();
                if let Ok(()) = stream.write_all(
                    RespBuilder::new()
                        .add_array(&3)
                        .add_bulk_string("REPLCONF")
                        .add_bulk_string("GETACK")
                        .add_bulk_string("*")
                        .as_bytes(),
                ) {
                    repl.status = ReplicaStatus::AckReceived;
                }
            }
        }
    }
}

impl Master for MasterGovernor {
    fn confirm_replica_ack(
        &self,
        repl_number: u8,
        max_wait_time_millis: u64,
    ) -> Result<Option<u8>, GovError> {
        {
            if repl_number == 0 || self.connected_replicas.read().iter().len() == 0 {
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
            if !timed_out.load(Ordering::SeqCst) {
                let received_repl: Vec<ReplicaStatus> = {
                    self.connected_replicas
                        .read()
                        .unwrap()
                        .iter()
                        .filter(|&r| r.status == ReplicaStatus::AckReceived)
                        .map(|r| r.status)
                        .collect()
                };

                let current_acks = received_repl.len() as u8;

                if current_acks >= repl_number {
                    return Ok(Some(repl_number));
                }
                thread::sleep(time::Duration::from_millis(
                    max_wait_time_millis / repl_number as u64,
                ));
            } else {
                return Ok(None);
            };
        }
    }

    fn propagate_command(&self, cmd: command::Command) {
        if should_propagate_command(cmd.clone()) {
            self.repl_offset.fetch_add(0, Ordering::SeqCst);
            if let Ok(replicas) = self.connected_replicas.read() {
                for repl in replicas.iter() {
                    if let Ok(mut stream) = repl.stream.lock() {
                        let out = command::serialize_command(cmd.clone());
                        let _ = stream.write_all(out.as_bytes());
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
        self.set_replica_instance(stream);

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
