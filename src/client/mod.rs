//! Module that contains the Client API for all public commands implemented by rldb.
use crate::cluster::state::Node;

use async_trait::async_trait;
use bytes::Bytes;

pub mod db_client;
pub mod error;
pub mod mock;

/// Trait that defines which functions a RLDB client needs to implement
#[async_trait]
pub trait Client {
    /// Starts a TCP connection with an rldb node
    async fn connect(&mut self) -> error::Result<()>;
    /// Ping command interface
    async fn ping(&mut self) -> error::Result<serde_json::Value>;
    /// Get command interface
    async fn get(&mut self, key: Bytes) -> error::Result<serde_json::Value>;
    /// Put command interface
    async fn put(&mut self, key: Bytes, value: Bytes) -> error::Result<serde_json::Value>;
    /// Heartbeat command interface
    async fn heartbeat(&mut self, known_nodes: Vec<Node>) -> error::Result<serde_json::Value>;
    /// JoinCluster command interface
    async fn join_cluster(
        &mut self,
        known_cluster_node_addr: String,
    ) -> error::Result<serde_json::Value>;
}

/// Factory is the abstraction that allows different [`Client`] to be used by the cluster [crate::cluster::state]
#[async_trait]
pub trait Factory {
    /// the factory method that receiving an addr String and returns a trait object for [`Client`]
    async fn get(&self, addr: String) -> crate::client::error::Result<Box<dyn Client + Send>>;
}
