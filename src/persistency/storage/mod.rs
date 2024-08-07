use crate::{
    cmd::types::SerializedContext,
    error::{Error, Internal, InvalidRequest, Result},
    utils::{generate_random_ascii_string, serde_utf8_bytes},
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32c::crc32c;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    mem::size_of,
    sync::{Arc, Mutex, MutexGuard},
};
use tracing::{event, instrument, Level};

use super::versioning::version_vector::{ProcessId, VersionVector, VersionVectorOrd};

#[derive(Debug, Default)]
struct Store {
    data: HashMap<Bytes, Bytes>,
    metadata: HashMap<Bytes, Bytes>,
}

/// [`Storage`] is a facade that provides the APIs for something similar to a multi-map - ie: enables a client to store
/// multiples values associated with a single key. This is required for a database that uses leaderless replication
/// as conflicts might occur due to concurrent puts happening on different nodes.
/// When a conflict is detected, the put still succeeds but both values are stored.
/// A followup PUT with the appropriate [`SerializedContext`] is required to resolve the conflict.
#[derive(Debug, Clone)]
pub struct Storage {
    store: Arc<Mutex<Store>>,
    pid: ProcessId,
}

/// Represents a Value associated with a key in the database.
/// Every value contains:
///  1. Its bytes - the actual bytes a client stored
///  2. a checksum of these bytes
///
/// To make sure no corruptions happened at rest or while fullfilling the request, clients
/// should validate the received bytes against the provided checksum.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct Value {
    #[serde(with = "serde_utf8_bytes")]
    value: Bytes,
    crc32c: u32,
}

impl Value {
    pub fn new(value: Bytes, crc32c: u32) -> Self {
        Self { value, crc32c }
    }

    /// TODO: At some point this function has to be deleted as using it
    /// is unsafe checksum-wise
    pub fn new_unchecked(value: Bytes) -> Self {
        let crc = crc32c(&value);

        Self { value, crc32c: crc }
    }

    pub fn as_value(&self) -> Bytes {
        self.value.clone()
    }

    pub fn random() -> Self {
        Self::new_unchecked(generate_random_ascii_string(10).into())
    }
}

/// A [`StorageEntry`] represents a stored value in the database.
///
/// Currently we only store the [`Value`] and its version (represented by [`VersionVector`]).
/// It's unclear if we will need other type of Metadata in the future so won't bother to
/// add it before we need it.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct StorageEntry {
    pub value: Value,
    pub version: VersionVector,
}

/// This enum represents the result of [`version_evaluation`]. It tells the caller
/// if the lhs version should override the rhs one or if they conflict.
pub(crate) enum VersionEvaluation {
    Override,
    Conflict,
}

pub(crate) fn version_evaluation(
    lhs: &VersionVector,
    rhs: &VersionVector,
) -> Result<VersionEvaluation> {
    match lhs.causality(rhs) {
        VersionVectorOrd::HappenedBefore | VersionVectorOrd::Equals => {
            // the data we are trying to store is actually older than what's already stored.
            // This can happen due to read repair and active anti-entropy processes for instance.
            // We return an error here to make sure the client knows the put was rejected and does not expect
            // the value passed in this request to be stored
            event!(Level::WARN, "StaleContextProvided");
            Err(Error::InvalidRequest(InvalidRequest::StaleContextProvided))
        }
        VersionVectorOrd::HappenedAfter => {
            // This means we are ok to override both data and metadata
            Ok(VersionEvaluation::Override)
        }
        VersionVectorOrd::HappenedConcurrently => {
            // now we have the problem of concurrent writes. Our current approach is to actually
            // store both versions so that the client can deal with the conflict resolution
            Ok(VersionEvaluation::Conflict)
        }
    }
}

impl Storage {
    pub fn new(pid: ProcessId) -> Self {
        Self {
            store: Default::default(),
            pid,
        }
    }

    fn acquire_store_lock(&self) -> Result<MutexGuard<Store>> {
        self.store.lock().map_err(|_| {
            Error::Internal(Internal::Logic {
                reason: "Unable to acquire Storage lock. This should never happen".to_string(),
            })
        })
    }

    /// Implementation notes:
    /// 1. Every PUT is preceded by a GET. This is because PUTs have to verify
    ///  the current Version of the key that is already stored.
    /// 2. The current implementation locks the entire store for the whole put operation. This works
    ///  but is horrible performance-wise
    #[instrument(name = "storage::put", level = "info", skip(self))]
    pub fn put(
        &self,
        key: Bytes,
        value: Value,
        context: SerializedContext,
    ) -> Result<Vec<StorageEntry>> {
        let mut store_guard = self.acquire_store_lock()?;
        let current_versions = match self.synchronized_get(&store_guard, key.clone()) {
            Ok(current_versions) => current_versions,
            Err(Error::NotFound { .. }) => Vec::new(),
            Err(err) => {
                return Err(err);
            }
        };
        event!(
            Level::INFO,
            "current versions - key: {:?} - {:?}",
            key,
            current_versions
        );

        let new_version = context.deserialize(self.pid)?;

        let mut entries_to_store = Vec::new();
        let new_entry = StorageEntry {
            value: value.clone(),
            version: new_version.into(),
        };

        for existing_entry in current_versions {
            if let VersionEvaluation::Conflict =
                version_evaluation(&new_entry.version, &existing_entry.version)?
            {
                entries_to_store.push(existing_entry);
            }
        }

        entries_to_store.push(new_entry);

        event!(
            Level::INFO,
            "versions that will be stored - key: {:?} - {:?}",
            key,
            entries_to_store
        );
        self.synchronized_put(&mut store_guard, key, entries_to_store)
    }

    /// Internal function that requires the store to be locked in other
    /// to actually make updates to the inner [`Store`]
    #[instrument(
        name = "storage::synchronized_put",
        level = "info",
        skip(self, store_guard)
    )]
    fn synchronized_put(
        &self,
        store_guard: &mut MutexGuard<Store>,
        key: Bytes,
        items: Vec<StorageEntry>,
    ) -> Result<Vec<StorageEntry>> {
        let mut data_buf = BytesMut::new();
        let mut version_buf = BytesMut::new();
        data_buf.put_u32(items.len() as u32);
        version_buf.put_u32(items.len() as u32);

        for item in items.iter() {
            let value = item.value.as_value();
            data_buf.put_u32(value.len() as u32);
            data_buf.put_slice(&value[..]);

            let serialized_version = item.version.serialize();
            version_buf.put_u32(serialized_version.len() as u32);
            version_buf.put_slice(&serialized_version[..]);
        }

        // since the metadata is what tells us that data exists, let's store it second, only if the data was stored
        // successfully
        store_guard.data.insert(key.clone(), data_buf.freeze());
        store_guard.metadata.insert(key, version_buf.freeze());

        Ok(items)
    }

    #[instrument(level = "debug")]
    fn unmarshall_entry(item: &mut Bytes) -> Result<Bytes> {
        if item.remaining() < size_of::<u32>() {
            return Err(Error::Internal(Internal::Logic {
                reason: "Buffer too small".to_string(),
            }));
        }

        let item_length = item.get_u32() as usize;
        if item.remaining() < item_length {
            return Err(Error::Internal(Internal::Logic {
                reason: "Buffer too small".to_string(),
            }));
        }

        let mut ret = item.clone();
        ret.truncate(item_length);
        item.advance(item_length);
        Ok(ret)
    }

    #[instrument(level = "debug")]
    fn unmarshall_entries(mut items: Bytes) -> Result<Vec<Bytes>> {
        let n_items = items.get_u32() as usize;
        let mut res = Vec::with_capacity(n_items);
        for _ in 0..n_items {
            res.push(Self::unmarshall_entry(&mut items)?);
        }

        Ok(res)
    }

    #[instrument(level = "info", skip(self))]
    pub fn get(&self, key: Bytes) -> Result<Vec<StorageEntry>> {
        let guard = self.acquire_store_lock()?;
        self.synchronized_get(&guard, key)
    }

    /// Inner get function that requires a lock on the inner [`Store`] for execution.
    /// This is needed for PUT operations as they require a GET prior to the PUT to validate
    /// metadata ([`VersionVector`])
    #[instrument(name = "storage::synchronized_get" level = "info", skip(self, store_guard))]
    fn synchronized_get(
        &self,
        store_guard: &MutexGuard<Store>,
        key: Bytes,
    ) -> Result<Vec<StorageEntry>> {
        let version = store_guard
            .metadata
            .get(&key)
            .ok_or(Error::NotFound { key: key.clone() })?
            .clone();

        let data = store_guard
            .data
            .get(&key)
            .ok_or(Error::Internal(Internal::Logic {
                reason: "Metadata entry found but no data entry found".to_string(),
            }))?
            .clone();

        let metadata_items = Self::unmarshall_entries(version)?;
        let data_items = Self::unmarshall_entries(data)?;

        if metadata_items.len() != data_items.len() {
            return Err(Error::Internal(Internal::Logic {
                reason: "Data and Metadata items must have the same length".to_string(),
            }));
        }

        let data_and_metadata_items: Vec<StorageEntry> = std::iter::zip(metadata_items, data_items)
            .map(|(m, d)| StorageEntry {
                value: Value::new_unchecked(d), // TODO: the crc should be stored, not computed on the fly
                version: VersionVector::deserialize(self.pid, m).unwrap(), // TODO: unwrap()
            })
            .collect();

        Ok(data_and_metadata_items)
    }
}

#[cfg(test)]
mod tests {
    use super::Storage;
    use crate::{
        cmd::types::Context,
        persistency::{storage::Value, versioning::version_vector::VersionVector},
    };
    use bytes::Bytes;

    // stores the same key twice with conflicting versions and makes sure both are stored
    #[test]
    fn test_storage_conflict() {
        let store = Storage::new(0);

        let key = Bytes::from("key");
        let value_pid_0 = Value::new_unchecked(Bytes::from("value 0"));

        let mut version_pid_0 = VersionVector::new(0);

        version_pid_0.increment();

        store
            .put(
                key.clone(),
                value_pid_0.clone(),
                Context::from(version_pid_0.clone()).serialize(),
            )
            .unwrap();

        let mut version_pid_1 = VersionVector::new(1);

        version_pid_1.increment();

        let value_pid_1 = Value::new_unchecked(Bytes::from("value 1"));

        store
            .put(
                key.clone(),
                value_pid_1.clone(),
                Context::from(version_pid_1.clone()).serialize(),
            )
            .unwrap();

        let get_entries = store.get(key).unwrap();

        assert_eq!(get_entries.len(), 2);
        for entry in get_entries {
            if entry.version == version_pid_0 {
                assert_eq!(entry.value, value_pid_0);
            } else if entry.version == version_pid_1 {
                assert_eq!(entry.value, value_pid_1);
            } else {
                panic!("should never happen");
            }
        }
    }
}
