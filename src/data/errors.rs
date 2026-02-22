use core::fmt;

pub struct Error {
    error: DataStoreError,
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl Error {
    pub fn from(t: DataStoreError) -> Self {
        Error { error: t }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for Error {}

pub enum DataStoreError {
    LockError,
    StringNotNumber,
    InvalidStreamEntryId,
    StreamEntryIdMustBeGreaterThan(String),
    StreamEntryIdLessThanLastEntry,
    UnexpectedEmptyStream,
}

impl fmt::Display for DataStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataStoreError::LockError => write!(f, "ERR Failed to access data"),
            DataStoreError::InvalidStreamEntryId => write!(f, "ERR Invalid stream entry ID"),
            DataStoreError::StringNotNumber => {
                write!(f, "ERR value is not an integer or out of range")
            }
            DataStoreError::UnexpectedEmptyStream => {
                write!(f, "ERR Encountered unexpected empty stream")
            }
            DataStoreError::StreamEntryIdLessThanLastEntry => {
                write!(
                    f,
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                )
            }
            DataStoreError::StreamEntryIdMustBeGreaterThan(v) => {
                write!(f, "ERR The ID specified in XADD must be greater than {v}")
            }
        }
    }
}
