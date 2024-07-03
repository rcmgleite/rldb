use crate::{
    error::{Error, InvalidRequest, Result},
    storage_engine::{in_memory::InMemory, StorageEngine as StorageEngineTrait},
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{collections::HashMap, mem::size_of, sync::Arc};
use tracing::{event, Level};

use super::{
    versioning::version_vector::{ProcessId, VersionVectorOrd},
    Metadata, MetadataEvaluation,
};

/// type alias to the [`StorageEngine`] that makes it clonable and [`Send`]
pub type StorageEngine = Arc<dyn StorageEngineTrait + Send + Sync + 'static>;

/// Since I don't know what the inner [`StorageEngine`] API should look like to enable easy integration with [`crate::persistency::VersionVector`],
/// this intermediate type works as a facade between [`StorageEngine`] and [`crate::persistency::Db`]. This might disappear once I finally
/// come up with a proper [`StorageEngine`] API.
///
/// [`Storage`] is a facade that provides the APIs for something similar to a multi-map - ie: enables a client to store
/// multiples values associated with a single key. This is required for a database that uses leaderless replication
/// as conflicts might occur due to concurrent puts happening on different nodes.
/// When a conflict is detected, the put still succeeds but both values are stored.
/// A followup PUT with the appropriate [`Metadata`] is required to resolve the conflict.
#[derive(Debug)]
pub struct Storage {
    data_engine: StorageEngine,
    /// Metadata Storage engine - currently hardcoded to be in-memory
    /// The idea of having a separate storage engine just for metadata is do that we can avoid
    /// adding framing layers to the data we want to store to append/prepend the metadata.
    /// Also, when we do PUTs, we have to validate metadata (version) prior to storing the data,
    /// and without a separate storage engine for metadata, we would always require a GET before a PUT,
    /// which introduces more overhead than needed.
    metadata_engine: InMemory,
    pid: ProcessId,
}

///
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct StorageEntry {
    pub value: Bytes,
    pub metadata: Metadata,
}

pub(crate) fn metadata_evaluation(lhs: &Metadata, rhs: &Metadata) -> Result<MetadataEvaluation> {
    match lhs.versions.causality(&rhs.versions) {
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
            Ok(MetadataEvaluation::Override)
        }
        VersionVectorOrd::HappenedConcurrently => {
            // now we have the problem of concurrent writes. Our current approach is to actually
            // store both versions so that the client can deal with the conflict resolution
            Ok(MetadataEvaluation::Conflict)
        }
    }
}

impl Storage {
    pub fn new(data_engine: StorageEngine, pid: ProcessId) -> Self {
        Self {
            data_engine,
            metadata_engine: InMemory::default(),
            pid,
        }
    }

    pub async fn put(&self, key: Bytes, entry: StorageEntry) -> Result<Vec<StorageEntry>> {
        let current_versions = match self.get(key.clone()).await {
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

        let mut version_map = HashMap::<Metadata, StorageEntry>::new();
        version_map.insert(
            entry.metadata.clone(),
            StorageEntry {
                metadata: entry.metadata.clone(),
                value: entry.value,
            },
        );

        for existing_entry in current_versions {
            if let super::MetadataEvaluation::Conflict =
                metadata_evaluation(&entry.metadata, &existing_entry.metadata)?
            {
                version_map.insert(existing_entry.metadata.clone(), existing_entry);
            }
        }

        let entries_to_store = version_map.into_iter().map(|e| e.1).collect();

        event!(
            Level::INFO,
            "versions that will be stored - key: {:?} - {:?}",
            key,
            entries_to_store
        );
        self.do_put(key, entries_to_store).await
    }

    async fn do_put(&self, key: Bytes, items: Vec<StorageEntry>) -> Result<Vec<StorageEntry>> {
        let mut data_buf = BytesMut::new();
        let mut metadata_buf = BytesMut::new();
        data_buf.put_u32(items.len() as u32);
        metadata_buf.put_u32(items.len() as u32);

        for item in items.iter() {
            data_buf.put_u32(item.value.len() as u32);
            data_buf.put_slice(&item.value[..]);

            let serialized_metadata = item.metadata.serialize();
            metadata_buf.put_u32(serialized_metadata.len() as u32);
            metadata_buf.put_slice(&serialized_metadata[..]);
        }

        // since the metadata is what tells us that data exists, let's store it second, only if the data was stored
        // successfully
        self.data_engine.put(key.clone(), data_buf.freeze()).await?;
        self.metadata_engine.put(key, metadata_buf.freeze()).await?;

        Ok(items)
    }

    fn unmarshall_entry(item: &mut Bytes) -> Result<Bytes> {
        if item.remaining() < size_of::<u32>() {
            return Err(Error::Logic {
                reason: "Buffer too small".to_string(),
            });
        }

        let item_length = item.get_u32() as usize;
        if item.remaining() < item_length {
            return Err(Error::Logic {
                reason: "Buffer too small".to_string(),
            });
        }

        let mut ret = item.clone();
        ret.truncate(item_length);
        item.advance(item_length);
        Ok(ret)
    }

    fn unmarshall_entries(mut items: Bytes) -> Result<Vec<Bytes>> {
        let n_items = items.get_u32() as usize;
        let mut res = Vec::with_capacity(n_items);
        for _ in 0..n_items {
            res.push(Self::unmarshall_entry(&mut items)?);
        }

        Ok(res)
    }

    pub async fn get(&self, key: Bytes) -> Result<Vec<StorageEntry>> {
        let metadata = self
            .metadata_engine
            .get(&key)
            .await?
            .ok_or(Error::NotFound { key: key.clone() })?;
        let data = self.data_engine.get(&key).await?.ok_or(Error::Logic {
            reason: "Metadata entry found but no data entry found".to_string(),
        })?;

        let metadata_items = Self::unmarshall_entries(metadata)?;
        let data_items = Self::unmarshall_entries(data)?;

        if metadata_items.len() != data_items.len() {
            return Err(Error::Logic {
                reason: "Data and Metadata items must have the same length".to_string(),
            });
        }

        let data_and_metadata_items: Vec<StorageEntry> = std::iter::zip(metadata_items, data_items)
            .map(|(m, d)| StorageEntry {
                value: d,
                metadata: Metadata::deserialize(self.pid, m).unwrap(), // TODO: unwrap()
            })
            .collect();

        Ok(data_and_metadata_items)
    }
}

#[cfg(test)]
mod tests {
    use super::Storage;
    use crate::{
        persistency::{storage::StorageEntry, versioning::version_vector::VersionVector, Metadata},
        storage_engine::in_memory::InMemory,
    };
    use bytes::Bytes;
    use std::sync::Arc;

    // stores the same key twice with conflicting versions and makes sure both are stored
    #[tokio::test]
    async fn test_storage_conflict() {
        let store = Storage::new(Arc::new(InMemory::default()), 0);

        let key = Bytes::from("key");
        let value_pid_0 = Bytes::from("value 0");

        let mut metadata_pid_0 = Metadata {
            versions: VersionVector::new(0),
        };

        metadata_pid_0.versions.increment();

        let entry_pid_0 = StorageEntry {
            value: value_pid_0.clone(),
            metadata: metadata_pid_0.clone(),
        };

        store.put(key.clone(), entry_pid_0).await.unwrap();

        let mut metadata_pid_1 = Metadata {
            versions: VersionVector::new(1),
        };

        metadata_pid_1.versions.increment();

        let value_pid_1 = Bytes::from("value 1");
        let entry_pid_1 = StorageEntry {
            value: value_pid_1.clone(),
            metadata: metadata_pid_1.clone(),
        };

        store.put(key.clone(), entry_pid_1).await.unwrap();

        let mut get_entries = store.get(key).await.unwrap();

        assert_eq!(get_entries.len(), 2);
        let entry_0 = get_entries.remove(0);
        assert_eq!(entry_0.value, value_pid_0);
        assert_eq!(entry_0.metadata, metadata_pid_0);

        let entry_1 = get_entries.remove(0);
        assert_eq!(entry_1.value, value_pid_1);
        assert_eq!(entry_1.metadata, metadata_pid_1);
    }
}
