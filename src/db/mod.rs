use bytes::Bytes;
use std::sync::Arc;

use crate::cluster::ring_state::{Node, RingState};

pub type StorageEngine = Arc<dyn crate::storage_engine::StorageEngine + Send + Sync + 'static>;

/// Db is the abstraction that connects storage_engine and overall database state
/// in a single interface.
/// It exists mainly to hide [`StorageEngine`] and [`PartitioningScheme`] details so that they can
/// be updated later on..
#[derive(Debug)]
pub struct Db {
    /// the underlaying storage engine
    storage_engine: StorageEngine,
    /// the partition scheme (if any)
    /// This will be present if this is configured as a cluster node
    partitioning_scheme: Option<Arc<PartitioningScheme>>,
}

pub enum OwnsKeyResponse {
    True,
    False { addr: Bytes },
}

impl Db {
    pub fn new(
        storage_engine: StorageEngine,
        partitioning_scheme: Option<Arc<PartitioningScheme>>,
    ) -> Self {
        Self {
            storage_engine,
            partitioning_scheme,
        }
    }

    pub async fn put(&self, key: Bytes, value: Bytes) -> anyhow::Result<()> {
        self.storage_engine.put(key, value).await
    }

    pub async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>> {
        self.storage_engine.get(key).await
    }

    pub fn owns_key(&self, key: &[u8]) -> OwnsKeyResponse {
        if let Some(partitioning_scheme) = self.partitioning_scheme.clone() {
            let PartitioningScheme::ConsistentHashing(ring_state) = partitioning_scheme.as_ref();
            if ring_state.owns_key(key) {
                OwnsKeyResponse::True
            } else {
                OwnsKeyResponse::False {
                    addr: ring_state.key_owner(key).unwrap().addr.clone(),
                }
            }
        } else {
            OwnsKeyResponse::True
        }
    }

    pub fn update_ring_state(&self, nodes: Vec<Node>) -> anyhow::Result<()> {
        if let Some(partitioning_scheme) = self.partitioning_scheme.clone() {
            let PartitioningScheme::ConsistentHashing(ring_state) = partitioning_scheme.as_ref();
            ring_state.merge_nodes(nodes)
        } else {
            Ok(())
        }
    }
}

/// TODO: [`RingState`] is 100% coupled with ConsistentHashing. We should remodel that
/// and create a common interface so that when we decide to add new Partitioning schemes we don't
/// have to refactor the entire thing.
#[derive(Debug)]
pub enum PartitioningScheme {
    ConsistentHashing(RingState),
}

#[cfg(test)]
mod tests {
    // TODO
}
