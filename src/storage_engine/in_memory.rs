use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use super::StorageEngine;

#[derive(Clone, Debug, Default)]
pub struct InMemory {
    inner: Arc<RwLock<HashMap<Bytes, Bytes>>>,
}

const LOCK_ERR: &str = "Unable to acquire InMemory lock. This should never happen";

#[async_trait]
impl StorageEngine for InMemory {
    async fn get(&self, key: &Bytes) -> anyhow::Result<Option<Bytes>> {
        if let Ok(guard) = self.inner.read() {
            Ok(guard.get(key).map(Clone::clone))
        } else {
            Err(anyhow!(LOCK_ERR))
        }
    }

    async fn put(&self, key: Bytes, value: Bytes) -> anyhow::Result<()> {
        if let Ok(mut guard) = self.inner.write() {
            guard
                .entry(key)
                .and_modify(|e| *e = value.clone())
                .or_insert(value);
            Ok(())
        } else {
            Err(anyhow!(LOCK_ERR))
        }
    }

    async fn delete(&self, key: &Bytes) -> anyhow::Result<()> {
        if let Ok(mut guard) = self.inner.write() {
            guard.remove(key);
            Ok(())
        } else {
            Err(anyhow!(LOCK_ERR))
        }
    }
    async fn keys(&self) -> anyhow::Result<Vec<Bytes>> {
        if let Ok(guard) = self.inner.read() {
            Ok(guard.keys().map(Clone::clone).collect())
        } else {
            Err(anyhow!(LOCK_ERR))
        }
    }

    async fn snapshot(&self) -> anyhow::Result<Self> {
        if let Ok(guard) = self.inner.read() {
            Ok(Self {
                inner: Arc::new(RwLock::new(guard.clone())),
            })
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
