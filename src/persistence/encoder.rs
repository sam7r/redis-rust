use crate::persistence::types::*;
use std;

pub struct RdbEncoder {
    data: Vec<u8>,
}

#[allow(dead_code)]
impl RdbEncoder {
    pub fn new() -> Self {
        RdbEncoder { data: Vec::new() }
    }

    pub fn write_header(&mut self, version: u32) -> &mut Self {
        self.data.extend_from_slice(b"REDIS");
        self.data
            .extend_from_slice(format!("{:04}", version).as_bytes());
        self
    }

    pub fn write_aux(&mut self, key: &str, value: &str) -> &mut Self {
        self.data.push(OpCode::Aux.as_u8());
        self.write_string(key);
        self.write_string(value);
        self
    }

    pub fn write_aux_with_int(&mut self, key: &str, value: u64) -> &mut Self {
        self.data.push(OpCode::Aux.as_u8());
        self.write_string(key);
        self.write_string(&value.to_string());
        self
    }

    pub fn write_select_db(&mut self, db_number: usize) -> &mut Self {
        self.data.push(OpCode::SelectDB.as_u8());
        self.write_length(db_number);
        self
    }

    pub fn write_resizedb(&mut self, hash_size: usize, expires_size: usize) -> &mut Self {
        self.data.push(OpCode::ResizeDB.as_u8());
        self.write_length(hash_size);
        self.write_length(expires_size);
        self
    }

    pub fn write_expire_seconds(&mut self, timestamp: u32) -> &mut Self {
        self.data.push(OpCode::ExpiryTime.as_u8());
        self.data.extend_from_slice(&timestamp.to_be_bytes());
        self
    }

    pub fn write_expire_millis(&mut self, timestamp: u64) -> &mut Self {
        self.data.push(OpCode::ExpiryTimeMS.as_u8());
        self.data.extend_from_slice(&timestamp.to_be_bytes());
        self
    }

    pub fn write_string(&mut self, s: &str) -> &mut Self {
        self.write_length(s.len());
        self.data.extend_from_slice(s.as_bytes());
        self
    }

    pub fn write_string_raw(&mut self, s: &[u8]) -> &mut Self {
        self.write_length(s.len());
        self.data.extend_from_slice(s);
        self
    }

    pub fn write_value_type(&mut self, value_type: ValueType) -> &mut Self {
        self.data.push(value_type as u8);
        self
    }

    pub fn write_length(&mut self, len: usize) -> &mut Self {
        if len < 64 {
            self.data.push(len as u8);
        } else if len < 16384 {
            let high = 0x40 | ((len >> 8) as u8);
            let low = (len & 0xFF) as u8;
            self.data.push(high);
            self.data.push(low);
        } else if len < 2usize.pow(32) {
            self.data.push(0x80);
            self.data.extend_from_slice(&(len as u32).to_be_bytes());
        } else {
            panic!("Length too large");
        }
        self
    }

    pub fn write_int8(&mut self, value: i8) -> &mut Self {
        self.data.push(0xC0);
        self.data.push(value as u8);
        self
    }

    pub fn write_int16(&mut self, value: i16) -> &mut Self {
        self.data.push(0xC1);
        self.data.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn write_int32(&mut self, value: i32) -> &mut Self {
        self.data.push(0xC2);
        self.data.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn write_eof(&mut self) -> &mut Self {
        self.data.push(OpCode::Eof.as_u8());
        self
    }

    pub fn write_checksum(&mut self, checksum: u64) -> &mut Self {
        self.data.extend_from_slice(&checksum.to_be_bytes());
        self
    }

    pub fn finish(&self) -> &[u8] {
        &self.data
    }

    pub fn save(&self, path: &str) -> std::io::Result<()> {
        std::fs::write(path, &self.data)
    }
}

#[cfg(test)]
mod encoder_tests {
    use std::time;

    use super::*;

    #[test]
    fn writes_to_file() {
        match RdbEncoder::new()
            .write_header(7)
            .write_aux("redis-ver", "7.2.0")
            .write_aux_with_int("redis-bits", 64)
            .write_select_db(0)
            .write_resizedb(1, 0)
            .write_expire_seconds(
                time::SystemTime::now()
                    .duration_since(time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as u32,
            )
            .write_value_type(ValueType::String)
            .write_string("key")
            .write_string("value")
            .write_eof()
            .write_checksum(0)
            .save("dump.rdb")
        {
            Ok(_) => println!("RDB file saved successfully."),
            Err(e) => eprintln!("Failed to save RDB file: {}", e),
        }
    }
}
