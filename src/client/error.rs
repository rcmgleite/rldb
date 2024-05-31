use crate::utils::serde_utf8_bytes;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Concrete type for a [`crate::client::Client`]` error
pub type Result<T> = std::result::Result<T, Error>;

/// Enum that represents a [`crate::client::Client`] error
#[derive(Debug, Serialize, Deserialize)]
pub enum Error {
    /// Variant returned when a client was unable to establish a tcp connection with an rldb node
    UnableToConnect { reason: String },
    /// Server telling the client that the request sent is invalid for some reason
    InvalidRequest { reason: String },
    /// Variant returned if the client was unable to interpret the server response
    InvalidServerResponse { reason: String },
    /// Variant returned when either PUT or GET quorums are not met
    QuorumNotReached { required: usize, got: usize },
    /// Error for GET requests when the key doesn't exist
    NotFound {
        #[serde(with = "serde_utf8_bytes")]
        key: Bytes,
    },
    /// Generic IO error (automatically converted from [`std::io::Error`])
    Io { reason: String },
    /// Generic error - let's drop this ASAP
    Generic { reason: String },
    /// Tells the user of the Client library that it did something wrong (like calling connect twice)
    Logic { reason: String },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io {
            reason: value.to_string(),
        }
    }
}

impl From<crate::error::Error> for Error {
    fn from(value: crate::error::Error) -> Self {
        use crate::error::Error as TopLevelError;
        match value {
            TopLevelError::Io { reason } => Error::Io { reason },
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
