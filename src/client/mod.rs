//! Module that contains the Client API for all public commands implemented by rldb.
use crate::{
    cluster::state::Node,
    cmd::{
        cluster::{
            cluster_state::ClusterStateResponse, heartbeat::HeartbeatResponse,
            join_cluster::JoinClusterResponse,
        },
        get::GetResponse,
        ping::PingResponse,
        put::PutResponse,
    },
    error::Result,
};

use async_trait::async_trait;
use bytes::Bytes;

pub mod db_client;
pub mod mock;

/// Trait that defines which functions a RLDB client needs to implement
#[async_trait]
pub trait Client {
    /// Starts a TCP connection with an rldb node
    async fn connect(&mut self) -> Result<()>;
    /// Ping command interface
    async fn ping(&mut self) -> Result<PingResponse>;
    /// Get command interface
    async fn get(&mut self, key: Bytes, replica: bool) -> Result<GetResponse>;
    /// Put command interface
    async fn put(
        &mut self,
        key: Bytes,
        value: Bytes,
        metadata: Option<String>,
        replication: bool,
    ) -> Result<PutResponse>;
    /// Heartbeat command interface
    async fn heartbeat(&mut self, known_nodes: Vec<Node>) -> Result<HeartbeatResponse>;
    /// JoinCluster command interface
    async fn join_cluster(
        &mut self,
        known_cluster_node_addr: String,
    ) -> Result<JoinClusterResponse>;
    /// ClusterState command interface
    async fn cluster_state(&mut self) -> Result<ClusterStateResponse>;
}

/// Factory is the abstraction that allows different [`Client`] to be used by the cluster [crate::cluster::state]
#[async_trait]
pub trait Factory {
    /// the factory method that receiving an addr String and returns a trait object for [`Client`]
    async fn get(&self, addr: String) -> Result<Box<dyn Client + Send>>;
}
