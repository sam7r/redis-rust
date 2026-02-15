use crate::governor::{
    error::GovError,
    traits::{Governor, Slave},
    types::Info,
};
use crate::resp::RespBuilder;
use crate::store::DataStore;
use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::{Arc, atomic::AtomicU64},
};

#[allow(dead_code)]
pub struct SlaveGovernor {
    datastore: Arc<DataStore>,
    repl_offset: AtomicU64,
    master_repl_id: Option<String>,
}

impl SlaveGovernor {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        SlaveGovernor {
            datastore,
            repl_offset: AtomicU64::new(0),
            master_repl_id: None,
        }
    }

    fn get_replication_info(&self) -> Vec<(String, String)> {
        let info = vec![("role".to_string(), "slave".to_string())];
        info
    }
}

impl Governor for SlaveGovernor {
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

impl Slave for SlaveGovernor {
    fn start_replication(
        &mut self,
        master_addr: &str,
        self_port: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let master_addr_split = master_addr.split(' ').collect::<Vec<&str>>();
        let master_host = master_addr_split[0];
        let master_port = master_addr_split[1].parse::<u16>()?;

        let mut stream = TcpStream::connect(format!("{}:{}", master_host, master_port))?;

        self.send_ping(&mut stream)?;
        self.send_replconf(&mut stream, self_port)?;
        self.request_psync(&mut stream, self.master_repl_id.clone(), -1)?;

        Ok(())
    }

    fn send_replconf(
        &self,
        stream: &mut std::net::TcpStream,
        port: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let commands = vec![
            RespBuilder::new()
                .add_array(&3)
                .add_bulk_string("REPLCONF")
                .add_bulk_string("listening-port")
                .add_bulk_string(port)
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

        for msg in commands {
            stream.write_all(&msg)?;
            let mut buff = [0; 512];
            let bytes_read = stream.read(&mut buff)?;
            let response = String::from_utf8_lossy(&buff[..bytes_read]);
            if !response.starts_with("+OK") {
                return Err(Box::new(GovError {
                    message: "Unexpected response from master REPLCONF".to_string(),
                }));
            }
        }
        Ok(())
    }

    fn request_psync(
        &self,
        stream: &mut std::net::TcpStream,
        replication_id: Option<String>,
        offset: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let id = match replication_id {
            Some(id) => id,
            None => "?".to_string(),
        };
        stream.write_all(
            RespBuilder::new()
                .add_array(&3)
                .add_bulk_string("PSYNC")
                .add_bulk_string(&id)
                .add_bulk_string(&offset.to_string())
                .as_bytes(),
        )?;
        let mut buffer = [0; 512];
        let bytes_read = stream.read(&mut buffer)?;
        let response = String::from_utf8_lossy(&buffer[..bytes_read]);
        if !response.starts_with("+FULLRESYNC") && !response.starts_with("+CONTINUE") {
            return Err(Box::new(GovError {
                message: "Unexpected response from master PSYNC".to_string(),
            }));
        }
        dbg!("Received PSYNC response: {}", response);
        Ok(())
    }
}
