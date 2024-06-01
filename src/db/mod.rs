//! Module that contains the abstraction connecting the [`StorageEngine`] and [`State`] into a single interface.
//!
//! This interface is what a [`crate::cmd::Command`] has access to in order to execute its functionality.
use bytes::Bytes;
use std::sync::Arc;

use crate::{
    cluster::state::{Node, State},
    error::{Error, Result},
    server::config::Quorum,
};

/// type alias to the [`StorageEngine`] that makes it clonable and [`Send`]
pub type StorageEngine = Arc<dyn crate::storage_engine::StorageEngine + Send + Sync + 'static>;

/// Db is the abstraction that connects storage_engine and overall database state
/// in a single interface.
/// It exists mainly to hide [`StorageEngine`] and [`State`] details so that they can
/// be updated later on..
#[derive(Debug)]
pub struct Db {
    /// The underlaying storage engine
    storage_engine: StorageEngine,
    /// Cluster state.
    /// This will be present if this is configured as a cluster node
    cluster_state: Option<Arc<State>>,
    /// Quorum configuration
    /// TODO: Should this really be here?
    quorum: Option<Quorum>,
}

/// Possibly a bad idea, but using an enum instead of a boolean to determine if a key is owned by a node or not.
/// This is mostly useful because the [`OwnsKeyResponse::False`] variant contains the addrs of the node
/// that actually holds the key, which is sent back to the client as part of the TCP response.
pub enum OwnsKeyResponse {
    /// The node provided to [`Db::owns_key`] owns the key
    True,
    /// The node provided to [`Db::owns_key`] does not own the key. The actual owner is returned in the addr field.
    False { addr: Bytes },
}

impl Db {
    /// Returns a new instance of [`Db`] with the provided [`StorageEngine`] and [`State`].
    pub fn new(
        storage_engine: StorageEngine,
        cluster_state: Option<Arc<State>>,
        quorum: Option<Quorum>,
    ) -> Self {
        Self {
            storage_engine,
            cluster_state,
            quorum,
        }
    }

    /// Stores the given key and value into the underlying [`StorageEngine`]
    ///
    /// TODO: Should the checks regarding ownership of keys/partitions be moved to this function
    /// instead of delegated to the Put [`crate::cmd::Command`]
    pub async fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        Ok(self.storage_engine.put(key, value).await?)
    }

    /// Retrieves the [`Bytes`] associated with the given key.
    ///
    /// If the key is not found, [Option::None] is returned
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.storage_engine.get(key).await?)
    }

    /// Verifies if the key provided is owned by self.
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

    /// Updates the cluster state based on the nodes provided.
    ///
    /// This is used as part of the Gossip protocol to propagate cluster changes across all nodes
    pub fn update_cluster_state(&self, nodes: Vec<Node>) -> Result<()> {
        if let Some(cluster_state) = self.cluster_state.as_ref() {
            Ok(cluster_state.merge_nodes(nodes)?)
        } else {
            Ok(())
        }
    }

    /// returns the quorum config
    pub fn quorum_config(&self) -> Option<Quorum> {
        self.quorum.clone()
    }

    pub fn preference_list(&self, key: &[u8]) -> Result<Vec<Bytes>> {
        if let Some(cluster_state) = self.cluster_state.as_ref() {
            // TODO: quorum config must be tied to cluster state otherwise the API is really weird
            Ok(cluster_state.preference_list(key, self.quorum.as_ref().unwrap().replicas)?)
        } else {
            // TODO: There must be a way to make sure a caller is not allowed to call methods that only make sense
            // in cluster mode instead of a runtime failure.
            Err(Error::Generic {
                reason: "preference_list is meaningless for rldb not in cluster mode".to_string(),
            })
        }
    }

    pub fn cluster_state(&self) -> Result<Vec<Node>> {
        if let Some(cluster_state) = self.cluster_state.as_ref() {
            Ok(cluster_state.get_nodes()?)
        } else {
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO
}
