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

#[derive(Debug, Serialize)]
pub enum Internal {
    Unknown,
    StorageEngine(crate::storage_engine::Error),
    Cluster(crate::cluster::error::Error),
}
