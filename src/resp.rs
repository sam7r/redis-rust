use std::fmt;

pub struct RespParser<'a> {
    pub data: &'a str,
    pub cursor: usize,
}

// RESP2 data types
pub enum DataType {
    SimpleString,
    SimpleError,
    Integer,
    BulkString,
    Array,
    None,
}

impl DataType {
    pub fn from_char(c: char) -> Self {
        match c {
            '*' => DataType::Array,
            '+' => DataType::SimpleString,
            '-' => DataType::SimpleError,
            '$' => DataType::BulkString,
            ':' => DataType::Integer,
            _ => DataType::None,
        }
    }
    pub fn to_char(&self) -> char {
        match self {
            DataType::SimpleString => '+',
            DataType::BulkString => '$',
            DataType::Integer => ':',
            DataType::Array => '*',
            DataType::SimpleError => '-',
            DataType::None => '\0',
        }
    }
}

pub struct RespBuilder {
    data: String,
}

impl fmt::Display for RespBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

impl RespBuilder {
    pub fn new() -> Self {
        RespBuilder {
            data: String::new(),
        }
    }

    pub fn join(&mut self, str: &str) -> &mut Self {
        self.data.push_str(str);
        self
    }

    pub fn add_simple_string(&mut self, value: &str) -> &mut Self {
        self.data.push(DataType::SimpleString.to_char());
        self.data.push_str(value);
        self.data.push_str("\r\n");
        self
    }

    pub fn add_bulk_string(&mut self, value: &str) -> &mut Self {
        self.data.push(DataType::BulkString.to_char());
        self.data.push_str(value.len().to_string().as_str());
        self.data.push_str("\r\n");
        self.data.push_str(value);
        self.data.push_str("\r\n");
        self
    }

    pub fn negative_bulk_string(&mut self) -> &mut Self {
        self.data.push(DataType::BulkString.to_char());
        self.data.push_str("-1");
        self.data.push_str("\r\n");
        self
    }

    pub fn add_integer(&mut self, value: &str) -> &mut Self {
        self.data.push(DataType::Integer.to_char());
        self.data.push_str(value);
        self.data.push_str("\r\n");
        self
    }

    pub fn add_array(&mut self, size: &usize) -> &mut Self {
        self.data.push(DataType::Array.to_char());
        self.data.push_str(&size.to_string());
        self.data.push_str("\r\n");
        self
    }

    pub fn negative_array(&mut self) -> &mut Self {
        self.data.push(DataType::Array.to_char());
        self.data.push_str("-1");
        self.data.push_str("\r\n");
        self
    }

    pub fn add_simple_error(&mut self, message: &str) -> &mut Self {
        self.data.push(DataType::SimpleError.to_char());
        self.data.push_str(message);
        self.data.push_str("\r\n");
        self
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_bytes()
    }
}

impl<'a> RespParser<'a> {
    pub fn new(data: &'a str) -> Self {
        RespParser { data, cursor: 0 }
    }

    pub fn read_char(&mut self) -> Option<char> {
        self.data.chars().nth(self.cursor).inspect(|c| {
            self.cursor += c.len_utf8();
        })
    }

    pub fn read_line(&mut self) -> Option<&'a str> {
        let remaining = &self.data[self.cursor..];

        if let Some(crlf_pos) = remaining.find("\r\n") {
            let line = &remaining[..crlf_pos];
            self.cursor += crlf_pos + 2; // +2 for CRLF
            Some(line)
        } else {
            None
        }
    }

    pub fn read_str(&mut self, len: usize) -> Option<&'a str> {
        let start = self.cursor;
        let end = self.cursor + len;

        if end + 2 <= self.data.len() && &self.data[end..end + 2] == "\r\n" {
            self.cursor = end + 2;
            Some(&self.data[start..end])
        } else {
            None
        }
    }
}
