use std::{error, fmt};

#[derive(Debug)]
pub struct GovError {
    pub message: String,
}

impl fmt::Display for GovError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl error::Error for GovError {}
