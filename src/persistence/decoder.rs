use crate::persistence::types::*;

pub struct RdbDecoder<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> RdbDecoder<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        RdbDecoder { data, pos: 0 }
    }

    pub fn parse(&mut self) -> Result<RdbFile<'a>, RdbError> {
        let header = self.read_magic()?;
        let version = self.read_version()?;

        let mut metadata = Vec::new();
        let mut databases = Vec::new();

        loop {
            match self.peek() {
                Some(0xFA) => {
                    self.pos += 1;
                    let key = self.read_string()?.to_vec();
                    let value = self.read_string()?.to_vec();
                    metadata.push((key, value));
                }
                Some(0xFE) => {
                    self.pos += 1;
                    let db_num = self.read_length()?;
                    let db = self.parse_database(db_num)?;
                    databases.push(db);
                }
                Some(0xFF) => {
                    self.pos += 1;
                    break;
                }
                Some(b) => {
                    return Err(RdbError::UnknownOpcode(b));
                }
                None => {
                    return Err(RdbError::UnexpectedEnd);
                }
            }
        }

        let checksum = self.read_bytes(8)?;

        Ok(RdbFile {
            header,
            version,
            metadata,
            databases,
            checksum,
        })
    }

    fn read_magic(&mut self) -> Result<String, RdbError> {
        let magic = self.read_bytes(5)?;
        let s = std::str::from_utf8(magic).map_err(|_| RdbError::InvalidMagic)?;
        if s != "REDIS" {
            return Err(RdbError::InvalidMagic);
        }
        Ok(s.to_string())
    }

    fn read_version(&mut self) -> Result<u32, RdbError> {
        let version = self.read_bytes(4)?;
        let s = std::str::from_utf8(version).map_err(|_| RdbError::InvalidVersion)?;
        s.parse().map_err(|_| RdbError::InvalidVersion)
    }

    fn parse_database(&mut self, db_number: usize) -> Result<Database, RdbError> {
        let mut hash_size = 0;
        let mut expires_size = 0;

        if self.peek() == Some(0xFB) {
            self.pos += 1;
            hash_size = self.read_length()?;
            expires_size = self.read_length()?;
        }

        let mut entries = Vec::new();

        loop {
            match self.peek() {
                Some(0xFE) | Some(0xFF) | None => break,
                _ => {
                    let expiry = self.read_expiry()?;
                    let value_type = self.read_value_type()?;
                    let key = self.read_string()?.to_vec();
                    let value = self.read_value(value_type)?;
                    entries.push(KvEntry {
                        expiry,
                        value_type,
                        key,
                        value,
                    });
                }
            }
        }

        Ok(Database {
            db_number,
            hash_size,
            expires_size,
            entries,
        })
    }

    fn read_expiry(&mut self) -> Result<Option<Expiry>, RdbError> {
        match self.peek() {
            Some(0xFD) => {
                self.pos += 1;
                let ts = self.read_bytes(4)?;
                let secs = u32::from_be_bytes(ts.try_into().unwrap());
                Ok(Some(Expiry::Seconds(secs)))
            }
            Some(0xFC) => {
                self.pos += 1;
                let ts = self.read_bytes(8)?;
                let ms = u64::from_be_bytes(ts.try_into().unwrap());
                Ok(Some(Expiry::Milliseconds(ms)))
            }
            _ => Ok(None),
        }
    }

    fn read_value_type(&mut self) -> Result<u8, RdbError> {
        self.read_byte()
    }

    fn read_string(&mut self) -> Result<&'a [u8], RdbError> {
        let (encoding, len) = self.read_length_with_encoding()?;

        match encoding {
            0..=2 => {
                let s = self.read_bytes(len)?;
                Ok(s)
            }
            3 => match len {
                0 => self.read_bytes(1),
                1 => {
                    let val = self.read_bytes(2)?;
                    Ok(val)
                }
                2 => {
                    let val = self.read_bytes(4)?;
                    Ok(val)
                }
                _ => Err(RdbError::UnknownStringEncoding),
            },
            _ => Err(RdbError::UnknownStringEncoding),
        }
    }

    fn read_value(&mut self, value_type: u8) -> Result<Value, RdbError> {
        match value_type {
            0 => {
                let s = self.read_string()?.to_vec();
                Ok(Value::String(s))
            }
            1 => {
                let len = self.read_length()?;
                let mut items = Vec::with_capacity(len);
                for _ in 0..len {
                    let item = self.read_string()?.to_vec();
                    items.push(item);
                }
                Ok(Value::List(items))
            }
            14 => {
                let elements = self.read_quicklist()?;
                Ok(Value::Stream(elements))
            }
            _ => Err(RdbError::UnsupportedValueType(value_type)),
        }
    }

    fn read_quicklist(&mut self) -> Result<Vec<Vec<u8>>, RdbError> {
        let len = self.read_length()?;
        let mut elements = Vec::new();

        for _ in 0..len {
            let ziplist_data = self.read_string()?;
            let ziplist_elements = self.parse_ziplist(ziplist_data)?;
            elements.extend(ziplist_elements);
        }

        Ok(elements)
    }

    fn parse_ziplist(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, RdbError> {
        let mut pos = 0;

        if data.len() < 10 {
            return Err(RdbError::InvalidZiplist);
        }

        let _zlbytes = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let _zltail = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let num_entries = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        let mut elements = Vec::with_capacity(num_entries);

        for _ in 0..num_entries {
            if pos >= data.len() {
                return Err(RdbError::UnexpectedEnd);
            }

            let prev_len = data[pos];
            pos += 1;

            if prev_len == 254 {
                pos += 4;
            }

            if pos >= data.len() {
                return Err(RdbError::UnexpectedEnd);
            }

            let flag = data[pos];
            pos += 1;

            let (_, bytes_to_read) = match flag >> 6 {
                0b00 => {
                    let len = flag & 0x3F;
                    (len as usize, len as usize)
                }
                0b01 => {
                    let low = data[pos];
                    pos += 1;
                    let len = (((flag & 0x3F) as usize) << 8) | (low as usize);
                    (len, len)
                }
                0b10 => {
                    let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    (len, len)
                }
                0b11 => match (flag >> 4) & 0x03 {
                    0b1100 => {
                        let val = i16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
                        pos += 2;
                        elements.push(val.to_string().into_bytes());
                        continue;
                    }
                    0b1101 => {
                        let val = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                        pos += 4;
                        elements.push(val.to_string().into_bytes());
                        continue;
                    }
                    0b1110 => {
                        let val = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                        pos += 8;
                        elements.push(val.to_string().into_bytes());
                        continue;
                    }
                    0b1111 => {
                        let val = i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], 0]);
                        pos += 3;
                        elements.push(val.to_string().into_bytes());
                        continue;
                    }
                    _ => return Err(RdbError::UnknownZiplistEncoding),
                },
                _ => return Err(RdbError::UnknownZiplistEncoding),
            };

            if pos + bytes_to_read > data.len() {
                return Err(RdbError::UnexpectedEnd);
            }

            let element = data[pos..pos + bytes_to_read].to_vec();
            pos += bytes_to_read;
            elements.push(element);
        }

        Ok(elements)
    }

    fn read_length(&mut self) -> Result<usize, RdbError> {
        let (encoding, len) = self.read_length_with_encoding()?;
        if encoding > 2 {
            return Err(RdbError::UnknownLengthEncoding);
        }
        Ok(len)
    }

    fn read_length_with_encoding(&mut self) -> Result<(u8, usize), RdbError> {
        let byte = self.read_byte()?;

        let (encoding, value) = match byte >> 6 {
            0b00 => (0, (byte & 0x3F) as usize),
            0b01 => {
                let low = self.read_byte()?;
                (1, (((byte & 0x3F) as usize) << 8) | (low as usize))
            }
            0b10 => {
                let bytes = self.read_bytes(4)?;
                let len = u32::from_be_bytes(bytes.try_into().unwrap()) as usize;
                (2, len)
            }
            0b11 => (3, (byte & 0x3F) as usize),
            _ => unreachable!(),
        };

        Ok((encoding, value))
    }

    fn peek(&self) -> Option<u8> {
        self.data.get(self.pos).copied()
    }

    fn read_byte(&mut self) -> Result<u8, RdbError> {
        self.data
            .get(self.pos)
            .copied()
            .inspect(|_| {
                self.pos += 1;
            })
            .ok_or(RdbError::UnexpectedEnd)
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], RdbError> {
        if self.pos + n > self.data.len() {
            return Err(RdbError::UnexpectedEnd);
        }
        let bytes = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(bytes)
    }
}
