pub struct RespParser<'a> {
    pub data: &'a str,
    pub cursor: usize,
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
