use crate::cluster::state::Node;

use async_trait::async_trait;
use bytes::Bytes;

pub mod db_client;
pub mod error;
pub mod mock;

/// This trait defines which functions a RLDB client needs to implement
/// It's mostly used for tests...
#[async_trait]
pub trait Client {
    async fn connect(&mut self) -> error::Result<()>;
    async fn ping(&mut self) -> error::Result<serde_json::Value>;
    async fn get(&mut self, key: Bytes) -> error::Result<serde_json::Value>;
    async fn put(&mut self, key: Bytes, value: Bytes) -> error::Result<serde_json::Value>;
    async fn heartbeat(&mut self, known_nodes: Vec<Node>) -> error::Result<serde_json::Value>;
    async fn join_cluster(
        &mut self,
        known_cluster_node_addr: String,
    ) -> error::Result<serde_json::Value>;
}

#[async_trait]
pub trait Factory {
    async fn get(&self, addr: String) -> crate::client::error::Result<Box<dyn Client + Send>>;
}
