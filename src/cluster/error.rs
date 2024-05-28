//! Error type for cluster related operations
use std::fmt::Display;

use serde::Serialize;

use crate::client::error::Error as ClientError;

/// Enum that represents Errors for cluster operations
#[derive(Debug, Serialize)]
pub enum Error {
    /// Error related to one a rldb cluster has a single node
    ClusterHasOnlySelf,
    /// Internal error -> something not recoverable and not triggered by client errors
    Internal { reason: String },
    /// This is a bug - if it happens there's something to be fixed
    Logic { reason: String },
    /// Cluster nodes talk to other nodes via [`crate::client`]. Any errors from [`crate::client`] are mapped to this variant
    Client(ClientError),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

/// Type alias for [`std::result::Result`] with the cluster [`Error`] type
pub type Result<T> = std::result::Result<T, Error>;

impl From<ClientError> for Error {
    fn from(value: ClientError) -> Self {
        Self::Client(value)
    }
}
