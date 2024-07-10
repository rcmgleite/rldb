//! An in-memory [`StorageEngine`] implementation
//!
//! This implementation uses a [`HashMap`] wrapped by a [`Mutex`] and does nothing fency around performance.
//! It's the most straightforward implementation of a [`StorageEngine`] used for development/testing only
use async_trait::async_trait;
use bytes::Bytes;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};
use tracing::instrument;

use super::{Error, Result, StorageEngine};

/// Type alias for the underlying datastructure used to store the key/value pairs
type Store = HashMap<Bytes, Bytes>;

/// The InMemory [`StorageEngine`] definition
#[derive(Clone, Debug, Default)]
pub struct InMemory {
    inner: Arc<Mutex<Store>>,
}

impl InMemory {
    /// private function used to acquire a lock over the [`Store`].
    /// A fail to acquire a lock is considered a [`Error::Logic`] since the only reason why
    /// an [`Error`] should be returned is in case of [`Mutex`] poisoning
    fn acquire_lock(&self) -> Result<MutexGuard<Store>> {
        match self.inner.lock() {
            Ok(guard) => Ok(guard),
            Err(_) => Err(Error::Logic {
                reason: "Unable to acquire lock for InMemory storage engine - poisoned..."
                    .to_string(),
            }),
        }
    }
}

#[async_trait]
impl StorageEngine for InMemory {
    #[instrument(name = "storage_engine::in_memory::get", level = "info", skip(self))]
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let guard = self.acquire_lock()?;
        Ok(guard.get(key).cloned())
    }

    #[instrument(name = "storage_engine::in_memory::put", level = "info", skip(self))]
    async fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        let mut guard = self.acquire_lock()?;
        guard
            .entry(key)
            .and_modify(|e| *e = value.clone())
            .or_insert(value);
        Ok(())
    }

    #[instrument(level = "info", skip(self))]
    async fn delete(&self, key: &[u8]) -> Result<()> {
        let mut guard = self.acquire_lock()?;
        guard.remove(key);
        Ok(())
    }

    #[instrument(level = "info", skip(self))]
    async fn keys(&self) -> Result<Vec<Bytes>> {
        let guard = self.acquire_lock()?;
        Ok(guard.keys().map(Clone::clone).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::InMemory;
    use crate::{storage_engine::StorageEngine, utils::generate_random_ascii_string};
    use bytes::Bytes;
    use quickcheck::Arbitrary;

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

    #[tokio::test]
    async fn override_key() {
        let store = InMemory::default();
        let key = Bytes::from("key");
        let value1 = Bytes::from("value");
        let value2 = Bytes::from("value2");

        store.put(key.clone(), value1.clone()).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap().unwrap(), value1);

        store.put(key.clone(), value2.clone()).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap().unwrap(), value2);
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestInput {
        key_values_thread_1: Vec<String>,
        key_values_thread_2: Vec<String>,
        key_values_thread_3: Vec<String>,
    }

    fn generate_random_deduped_string_keys(n_keys: usize) -> Vec<String> {
        let mut nodes = Vec::with_capacity(n_keys);
        for _ in 0..n_keys {
            nodes.push(generate_random_ascii_string(20))
        }
        nodes.sort();
        nodes.dedup_by(|a, b| a.eq_ignore_ascii_case(&b));
        nodes
    }

    impl Arbitrary for TestInput {
        fn arbitrary(_: &mut quickcheck::Gen) -> Self {
            let keys = generate_random_deduped_string_keys(600);

            Self {
                key_values_thread_1: Vec::from(&keys[0..200]),
                key_values_thread_2: Vec::from(&keys[200..400]),
                key_values_thread_3: Vec::from(&keys[400..600]),
            }
        }
    }

    async fn put_get(store: InMemory, items: Vec<String>) -> anyhow::Result<usize> {
        let mut items_added = 0;

        for key in items.iter() {
            let key = Bytes::from(key.clone());
            store.put(key.clone(), key.clone()).await?;
            assert_eq!(store.get(&key).await?.unwrap(), key);
            items_added += 1;
        }

        Ok(items_added)
    }

    // This is kind of a dumb test.. it just asserts that
    //  1. concurrent puts/get don't hang due to bad mutex usage
    //  2. all items that were supposed to be added are added
    //  3. the key value pairs match
    #[quickcheck_async::tokio]
    async fn concurrency_test_put_get(input: TestInput) {
        let store = InMemory::default();
        let h1 = {
            let store = store.clone();
            let input = input.key_values_thread_1.clone();
            tokio::spawn(put_get(store, input))
        };

        let h2 = {
            let store = store.clone();
            let input = input.key_values_thread_2.clone();
            tokio::spawn(put_get(store, input))
        };

        let h3 = {
            let store = store.clone();
            let input = input.key_values_thread_3.clone();
            tokio::spawn(put_get(store, input))
        };

        let (r1, r2, r3) = tokio::join!(h1, h2, h3);
        let h1_items_added = r1.unwrap().unwrap();
        let h2_items_added = r2.unwrap().unwrap();
        let h3_items_added = r3.unwrap().unwrap();
        let total = h1_items_added + h2_items_added + h3_items_added;

        assert_eq!(
            total,
            input.key_values_thread_1.len()
                + input.key_values_thread_2.len()
                + input.key_values_thread_3.len()
        );
        assert_eq!(store.keys().await.unwrap().len(), total,);
    }
}
