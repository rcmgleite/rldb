//! This trait represents the interface for a storage engine (Key/Value)
//! Keys and values are opaque bytes and are not interpreted in any way by StorageEngine implementations
use async_trait::async_trait;
use bytes::Bytes;
use serde::Serialize;
use std::fmt::Debug;

pub mod in_memory;

#[async_trait]
pub trait StorageEngine: Debug {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    async fn put(&self, key: Bytes, value: Bytes) -> Result<()>;
    async fn delete(&self, key: &[u8]) -> Result<()>;
    async fn keys(&self) -> Result<Vec<Bytes>>;
}

#[derive(Debug, Serialize)]
pub enum Error {
    Internal,
    Logic { reason: String },
}

pub type Result<T> = std::result::Result<T, Error>;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}
