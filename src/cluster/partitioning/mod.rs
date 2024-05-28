//! Module that contains different partitioning schemes
use crate::cluster::error::Result;
use bytes::Bytes;

pub mod consistent_hashing;
pub mod mock;

/// This trait defines a PartitioningScheme (ie: how should data be split amongst cluster nodes)
///
/// For the 2 mutating operations: `add_node` and `remove_node`, data has to be moved between nodes.
/// This operation is called resharding and it is expensive. For this reason, the more stable the cluster
/// configuration is, the better.
pub trait PartitioningScheme {
    /// adds a new node to the partition state
    fn add_node(&mut self, key: Bytes) -> Result<()>;

    /// removes a node from the partition state
    fn remove_node(&mut self, key: &[u8]) -> Result<()>;

    /// returns the owner of a given key
    fn key_owner(&self, key: &[u8]) -> Result<Bytes>;
}
