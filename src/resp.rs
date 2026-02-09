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
