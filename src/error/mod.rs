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
    InvalidRequest {
        reason: String,
    },
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

impl From<crate::cluster::error::Error> for Error {
    fn from(err: crate::cluster::error::Error) -> Self {
        Self::Internal(Internal::Cluster(err))
    }
}

impl From<crate::client::error::Error> for Error {
    fn from(err: crate::client::error::Error) -> Self {
        match err {
            crate::client::error::Error::UnableToConnect { reason } => Self::Io { reason },
            crate::client::error::Error::InvalidRequest { reason } => {
                Self::InvalidRequest { reason }
            }
            crate::client::error::Error::InvalidServerResponse { reason } => {
                Self::Internal(Internal::Unknown { reason })
            }
            crate::client::error::Error::QuorumNotReached { operation, reason } => {
                Self::QuorumNotReached { operation, reason }
            }
            crate::client::error::Error::NotFound { key } => Self::NotFound { key },
            crate::client::error::Error::Io { reason } => Self::Io { reason },
            crate::client::error::Error::Generic { reason } => Self::Generic { reason },
            crate::client::error::Error::Logic { reason } => {
                Self::Internal(Internal::Unknown { reason })
            }
        }
    }
}

#[derive(Debug, Serialize)]
pub enum Internal {
    Logic { reason: String },
    Unknown { reason: String },
    StorageEngine(crate::storage_engine::Error),
    Cluster(crate::cluster::error::Error),
}
