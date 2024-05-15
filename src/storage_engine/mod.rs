//! This trait represents the interface for a storage engine (Key/Value)
//! Keys and values are opaque bytes and are not interpreted in any way by StorageEngine implementations
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt::Debug;

pub mod in_memory;

#[async_trait]
pub trait StorageEngine: Debug {
    async fn get(&self, key: &Bytes) -> anyhow::Result<Option<Bytes>>;
    async fn put(&self, key: Bytes, value: Bytes) -> anyhow::Result<()>;
    async fn delete(&self, key: &Bytes) -> anyhow::Result<()>;
    async fn keys(&self) -> anyhow::Result<Vec<Bytes>>;
    // async fn snapshot(&self) -> anyhow::Result<Self>;
}
