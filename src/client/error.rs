use serde::Serialize;

/// Concrete type for a [`crate::client::Client`]` error
pub type Result<T> = std::result::Result<T, Error>;

/// Enum that represents a [`crate::client::Client`] error
#[derive(Debug, Serialize)]
pub enum Error {
    /// Logic is a bug -> if this happens we have to fix something
    Logic { reason: String },
    /// Variant returned when a client was unable to establish a tcp connection with an rldb node
    UnableToConnect { reason: String },
    /// Generic IO error (automatically converted from [`std::io::Error`])
    GenericIo { reason: String },
    /// Variant returned if the client was unable to interpret the server response
    InvalidServerResponse { reason: String },
    /// truly generic error -> might be dropped soon
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
