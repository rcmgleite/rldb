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
        replication_get::ReplicationGetResponse,
    },
    error::Result,
    persistency::storage::Value,
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
    ///
    /// This is the normal GET interface the users of the database should use to issue GET requests.
    /// For information on the return type, see [`GetResponse`].
    async fn get(&mut self, key: Bytes) -> Result<GetResponse>;
    /// ReplicationGet command interface
    ///
    /// This API was created to be used only when nodes within the cluster need to retrieve data from other nodes.
    /// It's possible that this will be moved to another Trait in the future.
    async fn replication_get(&mut self, key: Bytes) -> Result<ReplicationGetResponse>;
    /// Put command interface
    ///
    /// # Notes
    ///  1. [`Value`] forces the client to provide a crc of the bytes being stored, which is key for durability
    ///  2. the context argument should be filled by whatever context string is returned by GET.
    ///   This is used internally to manage versioning/conflicts and it's not meant to be interpreted by the user/normal clients.
    ///  3. the replication flag might be dropped in favor of 2 different puts - one for users and one for internal usage
    ///   (similar to [`Client::get`] and [`Client::replication_get`])
    async fn put(
        &mut self,
        key: Bytes,
        value: Value,
        context: Option<String>,
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
