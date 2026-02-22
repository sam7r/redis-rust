use crate::{
    data::store::DataStore,
    governor::{
        error::GovError,
        master::MasterGovernor,
        slave::SlaveGovernor,
        traits::{Governor, Master, Slave},
        types::{Config, ExpireStrategy, Info, Psync},
    },
};

pub enum GovernorInstance {
    Master(MasterGovernor),
    Slave(SlaveGovernor),
}

impl Governor for GovernorInstance {
    fn load_rdb_data(&self, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            GovernorInstance::Master(m) => m.load_rdb_data(data),
            GovernorInstance::Slave(s) => s.load_rdb_data(data),
        }
    }

    fn get_datastore(&self) -> std::sync::Arc<DataStore> {
        match self {
            GovernorInstance::Master(m) => m.get_datastore(),
            GovernorInstance::Slave(s) => s.get_datastore(),
        }
    }

    fn get_expire_strategy(&self) -> Option<ExpireStrategy> {
        match self {
            GovernorInstance::Master(m) => m.get_expire_strategy(),
            GovernorInstance::Slave(s) => s.get_expire_strategy(),
        }
    }

    fn get_info(&self, options: Vec<Info>) -> Result<Vec<(String, String)>, GovError> {
        match self {
            GovernorInstance::Master(m) => m.get_info(options),
            GovernorInstance::Slave(s) => s.get_info(options),
        }
    }

    fn start_expire_manager(&mut self) {
        match self {
            GovernorInstance::Master(m) => m.start_expire_manager(),
            GovernorInstance::Slave(s) => s.start_expire_manager(),
        }
    }

    fn send_ping(
        &self,
        stream: &mut std::net::TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            GovernorInstance::Master(m) => m.send_ping(stream),
            GovernorInstance::Slave(s) => s.send_ping(stream),
        }
    }
}

impl Master for GovernorInstance {
    fn get_config(&self) -> super::types::Config {
        match self {
            GovernorInstance::Master(m) => m.get_config(),
            GovernorInstance::Slave(_) => Config::default(),
        }
    }

    fn load_rdb_from_file(&self) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            GovernorInstance::Master(m) => m.load_rdb_from_file(),
            GovernorInstance::Slave(_) => Err(Box::new(GovError {
                message: "load_rdb_from_file not available on slave".to_string(),
            })),
        }
    }

    fn bgsave(&self) -> Result<String, Box<dyn std::error::Error>> {
        match self {
            GovernorInstance::Master(m) => m.bgsave(),
            GovernorInstance::Slave(_) => Err(Box::new(GovError {
                message: "BGSAVE not available on slave".to_string(),
            })),
        }
    }

    fn confirm_replica_ack(&self, repl_count: u8, wait_time: u64) -> Result<Option<u8>, GovError> {
        match self {
            GovernorInstance::Master(m) => m.confirm_replica_ack(repl_count, wait_time),
            GovernorInstance::Slave(_) => Ok(None),
        }
    }

    fn propagate_command(&self, prepared_cmd: crate::command::PreparedCommand) {
        match self {
            GovernorInstance::Master(m) => m.propagate_command(prepared_cmd),
            GovernorInstance::Slave(_) => {}
        }
    }

    fn handle_psync(
        &self,
        stream: std::net::TcpStream,
        replication_id: &str,
        offset: i64,
    ) -> Result<Psync, Box<dyn std::error::Error>> {
        match self {
            GovernorInstance::Master(m) => m.handle_psync(stream, replication_id, offset),
            GovernorInstance::Slave(_) => Err(Box::new(GovError {
                message: "handle_psync not available on slave".to_string(),
            })),
        }
    }
}

impl Slave for GovernorInstance {
    fn start_replication(
        &mut self,
        master_addr: &str,
        self_port: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            GovernorInstance::Master(_) => Err(Box::new(GovError {
                message: "start_replication not available on master".to_string(),
            })),
            GovernorInstance::Slave(s) => s.start_replication(master_addr, self_port),
        }
    }
}
