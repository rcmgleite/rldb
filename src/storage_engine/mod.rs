//! This trait represents the interface for a storage engine (Key/Value)
//! Keys and values are opaque bytes and are not interpreted in any way by StorageEngine implementations
use async_trait::async_trait;
use bytes::Bytes;
use serde::Serialize;
use std::fmt::Debug;

pub mod in_memory;

/// StorageEngine concrete implementations must implement the StorageEngine trait
#[async_trait]
pub trait StorageEngine: Debug {
    /// Retrieves [`Bytes`] associated with the provided key.
    /// If the key is not found, Ok(None) is returned
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Stores the provided key/value pair
    async fn put(&self, key: Bytes, value: Bytes) -> Result<()>;

    /// Deletes the [`Bytes`] associated with the provided key
    async fn delete(&self, key: &[u8]) -> Result<()>;

    /// Returns a [`Vec`] of all keys stored in the [`StorageEngine`]
    async fn keys(&self) -> Result<Vec<Bytes>>;
}

/// A concrete [`std::error::Error`] type for [`StorageEngine`] operations
#[derive(Debug, Serialize)]
pub enum Error {
    /// Some unknown internal issue that should not be transparant to client
    Internal,
    /// A logic issue means a bug. if this variant is returned it's because a bug must be fixed
    Logic { reason: String },
}

/// A concrete [`Result`] type binding [`Error`] for [`StorageEngine`] operations
pub type Result<T> = std::result::Result<T, Error>;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}
