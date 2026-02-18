use std::time;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum ExpireStrategy {
    Scheduled(time::Duration),
    Lazy,
}

#[derive(Debug)]
pub enum Role {
    Master,
    Slave,
}

#[allow(dead_code)]
#[derive(Eq, PartialEq, Clone, Copy)]
pub enum ReplicaOffsetState {
    Confirmed,
    Unconfirmed,
    Unknown,
}

impl std::fmt::Display for ReplicaOffsetState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ReplicaOffsetState::Confirmed => write!(f, "Confirmed"),
            ReplicaOffsetState::Unconfirmed => write!(f, "Unconfirmed"),
            ReplicaOffsetState::Unknown => write!(f, "Unknown"),
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

#[allow(dead_code)]
pub enum Psync {
    FullResync(String, u64),
    Continue,
}

#[derive(Default)]
pub struct Config {
    pub db_filename: String,
    pub db_directory: String,
}
