//! This module defines client/user visible errors that can be returned by rldb.

use std::fmt::Display;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::utils::serde_utf8_bytes;

pub type Result<T> = std::result::Result<T, Error>;

/// Error enum with all possible variants
#[derive(Debug, Serialize, Deserialize)]
pub enum Error {
    /// Variant returned for GET requests when the key is not present
    NotFound {
        #[serde(with = "serde_utf8_bytes")]
        key: Bytes,
    },
    /// returned by the server when the request is invalid for any reason -> this means the client has to fix something
    InvalidRequest(InvalidRequest),
    /// returned during server bootstrap if any configuration is invalid
    InvalidServerConfig { reason: String },
    /// Internal error that should be opaque to an external client.. Since today we use the same error type
    /// for internal errors and client errors this is a bit moot
    Internal(Internal),
    /// Self explanatory
    Io { reason: String },
    /// Error returned either in PUT or GET when quorum is not met
    QuorumNotReached {
        operation: String,
        reason: String,
        errors: Vec<Error>,
    },
    /// Logic is a type of error that signifies a bug in the database.
    Logic { reason: String },
    /// Error returned when a cluster has a single node and tries to heartbeat to self
    SingleNodeCluster,
    /// Returned when any invalid payload is returned/received by the server
    InvalidJsonPayload { reason: String },
}

impl Error {
    /// Returns true if this is an instance of a [`Error::NotFound`] variant
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::NotFound { .. })
    }

    /// returnes true if this error is a variant of [`InvalidRequest::StaleContextProvided`]
    pub fn is_stale_context_provided(&self) -> bool {
        match self {
            Error::InvalidRequest(e) => e.is_stale_context_provided(),
            Error::QuorumNotReached {
                operation: _,
                reason: _,
                errors,
            } => errors.iter().all(|e| e.is_stale_context_provided()),
            _ => false,
        }
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

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::InvalidJsonPayload {
            reason: value.to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Internal {
    Logic { reason: String },
    Unknown { reason: String },
    StorageEngine(crate::storage_engine::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum InvalidRequest {
    Generic { reason: String },
    MessageReceivedWithoutRequestId,
    MessageRequestIdMustBeUtf8Encoded,
    MaxMessageSizeExceeded { max: u32, got: u32 },
    MalformedContext,
    StaleContextProvided,
    EmptyContextWhenOverridingKey,
    ReplicationPutMustIncludeContext,

    UnableToConstructCommandFromMessage { expected_id: u32, got: u32 },
    InvalidJsonPayload(String),
    EmptyMessagePayload,
    UnrecognizedCommand { id: u32 },
}

impl InvalidRequest {
    pub fn is_stale_context_provided(&self) -> bool {
        matches!(self, InvalidRequest::StaleContextProvided)
    }
}
