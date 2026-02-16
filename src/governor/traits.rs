use crate::command;
use std::io::{Read, Write};

use crate::{
    governor::{
        error::GovError,
        types::{Info, Psync},
    },
    resp::RespBuilder,
};

pub trait Governor {
    fn get_info(&self, options: Vec<Info>) -> Result<Vec<(String, String)>, GovError>;
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
    fn start_store_manager(&mut self);
    fn propagate_command(&self, command: command::Command);
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
