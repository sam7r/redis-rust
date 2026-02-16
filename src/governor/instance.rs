use crate::governor::{
    error::GovError,
    master::MasterGovernor,
    slave::SlaveGovernor,
    traits::{Governor, Master, Slave},
    types::{Info, Psync},
};

pub enum GovernorInstance {
    Master(MasterGovernor),
    Slave(SlaveGovernor),
}

impl Governor for GovernorInstance {
    fn get_info(&self, options: Vec<Info>) -> Result<Vec<(String, String)>, GovError> {
        match self {
            GovernorInstance::Master(m) => m.get_info(options),
            GovernorInstance::Slave(s) => s.get_info(options),
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
    fn start_store_manager(&mut self) {
        match self {
            GovernorInstance::Master(m) => m.start_store_manager(),
            GovernorInstance::Slave(_) => {}
        }
    }

    fn propagate_command(&self, command: crate::command::Command) {
        match self {
            GovernorInstance::Master(m) => m.propagate_command(command),
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
