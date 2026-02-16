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
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};

use crate::command;

#[allow(dead_code)]
#[derive(Clone)]
pub struct SlaveGovernor {
    datastore: Arc<DataStore>,
    repl_offset: Arc<AtomicU64>,
    master_repl_id: Option<String>,
}

impl SlaveGovernor {
    pub fn new(datastore: Arc<DataStore>) -> Self {
        SlaveGovernor {
            datastore,
            repl_offset: Arc::new(AtomicU64::new(0)),
            master_repl_id: None,
        }
    }

    fn get_replication_info(&self) -> Vec<(String, String)> {
        let info = vec![("role".to_string(), "slave".to_string())];
        info
    }

    pub fn send_replconf(
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
            println!(
                "Received REPLCONF response from master: {}",
                response.trim()
            );
        }
        Ok(())
    }

    fn handle_psync(
        &self,
        stream: &mut std::net::TcpStream,
        replication_id: Option<String>,
        offset: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let id = replication_id.unwrap_or_else(|| "?".to_string());

        stream.write_all(
            RespBuilder::new()
                .add_array(&3)
                .add_bulk_string("PSYNC")
                .add_bulk_string(&id)
                .add_bulk_string(&offset.to_string())
                .as_bytes(),
        )?;

        // Read PSYNC response line by line (as string)
        let mut response_line = String::new();
        loop {
            let mut buf = [0u8; 1];
            stream.read_exact(&mut buf)?;
            response_line.push(buf[0] as char);
            if response_line.ends_with("\r\n") {
                break;
            }
        }

        if !response_line.starts_with("+FULLRESYNC") && !response_line.starts_with("+CONTINUE") {
            return Err(Box::new(GovError {
                message: "Unexpected PSYNC response".to_string(),
            }));
        }
        println!("Received PSYNC response: {}", response_line.trim());

        // Read RDB size line
        let mut size_line = String::new();
        loop {
            let mut buf = [0u8; 1];
            stream.read_exact(&mut buf)?;
            size_line.push(buf[0] as char);
            if size_line.ends_with("\r\n") {
                break;
            }
        }

        if size_line.starts_with('$') {
            let size: usize = size_line.strip_prefix('$').unwrap().trim().parse()?;
            let mut rdb_data = vec![0u8; size];
            stream.read_exact(&mut rdb_data)?;
            println!("Received RDB data: {} bytes", rdb_data.len());
        }

        Ok(())
    }

    pub fn handle_incoming(&self, input: &str) -> Option<RespBuilder> {
        let mut mode = crate::Mode::Normal;
        let c = command::prepare_commands(input);
        for cmd in c.iter().flatten() {
            println!("Received command from master: {:?}", cmd);
            if let command::Command::ReplConf(arg, _) = cmd
                && arg == "GETACK"
            {
                let mut resp = RespBuilder::new();
                resp.add_array(&3)
                    .add_bulk_string("REPLCONF")
                    .add_bulk_string("ACK")
                    .add_bulk_string(self.repl_offset.load(Ordering::SeqCst).to_string().as_str());
                return Some(resp);
            } else {
                let _ = crate::perform_command(self.datastore.clone(), cmd.clone(), &mut mode);
            }
        }
        None
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
        self.handle_psync(&mut stream, self.master_repl_id.clone(), -1)?;

        let gov = Arc::new(self.clone());
        let _self = Arc::clone(&gov);

        thread::spawn(move || {
            loop {
                let mut buffer = [0; 512];
                let bytes_read = stream.read(&mut buffer);
                match bytes_read {
                    Ok(0) => {
                        println!("Master closed the connection");
                        break;
                    }
                    Ok(n) => {
                        let input = String::from_utf8_lossy(&buffer[..n]);
                        if let Some(out) = _self.handle_incoming(&input) {
                            stream.write_all(out.as_bytes()).ok();
                        }
                    }
                    Err(e) => {
                        println!("Error reading from master: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(())
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
