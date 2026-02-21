use std::collections::HashMap;

#[allow(dead_code)]
pub enum ValueType {
    String,
    List,
    Set,
    ZSet,
    Hash,
    ZipMap,
    ZipList,
    IntSet,
    ZSetZipList,
    HashZipList,
    QuickList,
}

pub enum OpCode {
    Aux,
    SelectDB,
    ResizeDB,
    ExpiryTime,
    ExpiryTimeMS,
    Eof,
}

impl OpCode {
    pub fn as_u8(&self) -> u8 {
        match self {
            OpCode::Aux => 0xFA,
            OpCode::SelectDB => 0xFE,
            OpCode::ResizeDB => 0xFB,
            OpCode::ExpiryTime => 0xFD,
            OpCode::ExpiryTimeMS => 0xFC,
            OpCode::Eof => 0xFF,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct RdbFile<'a> {
    pub header: String,
    pub version: u32,
    pub metadata: Vec<(Vec<u8>, Vec<u8>)>,
    pub databases: Vec<Database>,
    pub checksum: &'a [u8],
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Database {
    pub db_number: usize,
    pub hash_size: usize,
    pub expires_size: usize,
    pub entries: Vec<KvEntry>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct KvEntry {
    pub expiry: Option<Expiry>,
    pub value_type: u8,
    pub key: Vec<u8>,
    pub value: Value,
}

#[derive(Debug)]
pub enum Expiry {
    Seconds(u32),
    Milliseconds(u64),
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum Value {
    String(Vec<u8>),
    List(Vec<Vec<u8>>),
    Set(Vec<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    Stream(Vec<Vec<u8>>),
}

#[derive(Debug)]
pub enum RdbError {
    UnexpectedEnd,
    InvalidMagic,
    InvalidVersion,
    UnknownOpcode(u8),
    UnknownLengthEncoding,
    UnknownStringEncoding,
    UnsupportedValueType(u8),
    InvalidZiplist,
    UnknownZiplistEncoding,
}

impl std::fmt::Display for RdbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdbError::UnexpectedEnd => write!(f, "Unexpected end of data"),
            RdbError::InvalidMagic => write!(f, "Invalid RDB magic number"),
            RdbError::InvalidVersion => write!(f, "Invalid RDB version"),
            RdbError::UnknownOpcode(b) => write!(f, "Unknown opcode: {:#x}", b),
            RdbError::UnknownLengthEncoding => write!(f, "Unknown length encoding"),
            RdbError::UnknownStringEncoding => write!(f, "Unknown string encoding"),
            RdbError::UnsupportedValueType(t) => write!(f, "Unsupported value type: {}", t),
            RdbError::InvalidZiplist => write!(f, "Invalid ziplist"),
            RdbError::UnknownZiplistEncoding => write!(f, "Unknown ziplist encoding"),
        }
    }
}

impl std::error::Error for RdbError {}
