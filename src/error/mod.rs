//! This module defines client/user visible errors that can be returned by rldb.

use std::fmt::Display;

use bytes::Bytes;
use serde::Serialize;

use crate::utils::serde_utf8_bytes;

pub type Result<T> = std::result::Result<T, Error>;

/// Error enum with all possible variants
#[derive(Debug, Serialize)]
pub enum Error {
    NotFound {
        #[serde(with = "serde_utf8_bytes")]
        key: Bytes,
    },
    InvalidRequest(InvalidRequest),
    InvalidServerConfig {
        reason: String,
    },
    Internal(Internal),
    Io {
        reason: String,
    },
    Generic {
        reason: String,
    },
    QuorumNotReached {
        operation: String,
        reason: String,
    },
    Logic {
        reason: String,
    },
    Client(crate::client::error::Error),
    SingleNodeCluster,
}

impl Error {
    /// Returns true if this is an instance of a [`Error::NotFound`] variant
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::NotFound { .. })
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io {
            reason: err.to_string(),
        }
    }
}

impl From<crate::storage_engine::Error> for Error {
    fn from(err: crate::storage_engine::Error) -> Self {
        Self::Internal(Internal::StorageEngine(err))
    }
}

impl From<crate::client::error::Error> for Error {
    fn from(err: crate::client::error::Error) -> Self {
        match err {
            crate::client::error::Error::NotFound { key } => Self::NotFound { key },
            _ => Self::Client(err),
        }
    }
}

#[derive(Debug, Serialize)]
pub enum Internal {
    Logic { reason: String },
    Unknown { reason: String },
    StorageEngine(crate::storage_engine::Error),
}

#[derive(Debug, Serialize)]
pub enum InvalidRequest {
    Generic { reason: String },
    MaxMessageSizeExceeded { max: usize, got: usize },
    StaleContextProvided,
    EmptyContextWhenOverridingKey,
    ReplicationPutMustIncludeContext,

    UnableToConstructCommandFromMessage { expected_id: u32, got: u32 },
    InvalidJsonPayload(String),
    EmptyMessagePayload,
    UnrecognizedCommand { id: u32 },
}
