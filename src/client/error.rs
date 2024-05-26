use serde::Serialize;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Serialize)]
pub enum Error {
    Logic { reason: String },
    UnableToConnect { reason: String },
    GenericIo { reason: String },
    InvalidServerResponse { reason: String },
    Generic { reason: String },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::GenericIo {
            reason: value.to_string(),
        }
    }
}

impl From<crate::error::Error> for Error {
    fn from(value: crate::error::Error) -> Self {
        use crate::error::Error as TopLevelError;
        match value {
            TopLevelError::Io { reason } => Error::GenericIo { reason },
            _ => Self::Generic {
                reason: value.to_string(),
            },
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::InvalidServerResponse {
            reason: value.to_string(),
        }
    }
}
