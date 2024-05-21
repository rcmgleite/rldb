use bytes::Bytes;
use std::sync::Arc;

use crate::{
    cluster::state::{Node, State},
    error::Result,
};

pub type StorageEngine = Arc<dyn crate::storage_engine::StorageEngine + Send + Sync + 'static>;

/// Db is the abstraction that connects storage_engine and overall database state
/// in a single interface.
/// It exists mainly to hide [`StorageEngine`] and [`PartitioningScheme`] details so that they can
/// be updated later on..
#[derive(Debug)]
pub struct Db {
    /// The underlaying storage engine
    storage_engine: StorageEngine,
    /// Cluster state.
    /// This will be present if this is configured as a cluster node
    cluster_state: Option<Arc<State>>,
}

pub enum OwnsKeyResponse {
    True,
    False { addr: Bytes },
}

impl Db {
    pub fn new(storage_engine: StorageEngine, cluster_state: Option<Arc<State>>) -> Self {
        Self {
            storage_engine,
            cluster_state,
        }
    }

    pub async fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        Ok(self.storage_engine.put(key, value).await?)
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.storage_engine.get(key).await?)
    }

    pub fn owns_key(&self, key: &[u8]) -> Result<OwnsKeyResponse> {
        if let Some(cluster_state) = self.cluster_state.as_ref() {
            if cluster_state.owns_key(key)? {
                Ok(OwnsKeyResponse::True)
            } else {
                Ok(OwnsKeyResponse::False {
                    addr: cluster_state.key_owner(key).unwrap().addr.clone(),
                })
            }
        } else {
            Ok(OwnsKeyResponse::True)
        }
    }

    pub fn update_cluster_state(&self, nodes: Vec<Node>) -> Result<()> {
        if let Some(cluster_state) = self.cluster_state.as_ref() {
            Ok(cluster_state.merge_nodes(nodes)?)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO
}
