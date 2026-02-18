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
pub enum ReplicaStatus {
    Connected,
    Disconnected,
    Syncing,
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
