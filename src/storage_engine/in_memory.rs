use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::StorageEngine;

#[derive(Clone, Debug, Default)]
pub struct InMemory {
    inner: Arc<Mutex<HashMap<Bytes, Bytes>>>,
}

const LOCK_ERR: &str = "Unable to acquire InMemory lock. This should never happen";

#[async_trait]
impl StorageEngine for InMemory {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        if let Ok(guard) = self.inner.lock() {
            Ok(guard.get(key).map(Clone::clone))
        } else {
            Err(anyhow!(LOCK_ERR))
        }
    }

    async fn put(&self, key: Bytes, value: Bytes) -> anyhow::Result<()> {
        if let Ok(mut guard) = self.inner.lock() {
            guard
                .entry(key)
                .and_modify(|e| *e = value.clone())
                .or_insert(value);
            Ok(())
        } else {
            Err(anyhow!(LOCK_ERR))
        }
    }

    async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        if let Ok(mut guard) = self.inner.lock() {
            guard.remove(key);
            Ok(())
        } else {
            Err(anyhow!(LOCK_ERR))
        }
    }
    async fn keys(&self) -> anyhow::Result<Vec<Bytes>> {
        if let Ok(guard) = self.inner.lock() {
            Ok(guard.keys().map(Clone::clone).collect())
        } else {
            Err(anyhow!(LOCK_ERR))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::InMemory;
    use crate::storage_engine::StorageEngine;
    use bytes::Bytes;

    #[tokio::test]
    async fn put_get_delete() {
        let store = InMemory::default();
        let key = Bytes::from("key");
        let value = Bytes::from("value");

        store.put(key.clone(), value.clone()).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap().unwrap(), value);

        store.delete(&key).await.unwrap();
        assert!(store.get(&key).await.unwrap().is_none());
    }
}
