//! A concrete [`Client`] implementation for rldb
use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::{event, Level};

use crate::cluster::state::Node;
use crate::cmd;
use crate::cmd::cluster::cluster_state::ClusterStateResponse;
use crate::cmd::cluster::heartbeat::HeartbeatResponse;
use crate::cmd::cluster::join_cluster::JoinClusterResponse;
use crate::cmd::get::GetResponse;
use crate::cmd::ping::PingResponse;
use crate::cmd::put::PutResponse;
use crate::server::message::Message;

use super::{Client, Factory};
use crate::error::{Error, Result};

/// DbClient handle
pub struct DbClient {
    /// state stores the [`DbClientState`] of this implementation
    state: DbClientState,
}

/// A [`DbClient`] can either be Connected or Disconnected
enum DbClientState {
    Disconnected { addr: String },
    Connected { connection: TcpStream },
}

impl DbClient {
    pub fn new(addr: String) -> Self {
        Self {
            state: DbClientState::Disconnected { addr },
        }
    }

    fn get_conn_mut(&mut self) -> Result<&mut TcpStream> {
        match &mut self.state {
            DbClientState::Connected { connection } => Ok(connection),
            DbClientState::Disconnected { .. } => Err(Error::Logic {
                reason: "You must call `connect` before any other method for DbClient".to_string(),
            }),
        }
    }
}

#[async_trait]
impl Client for DbClient {
    async fn connect(&mut self) -> Result<()> {
        match &self.state {
            DbClientState::Disconnected { addr } => {
                self.state = DbClientState::Connected {
                    connection: TcpStream::connect(addr).await?,
                };
            }
            DbClientState::Connected { .. } => {
                return Err(Error::Logic {
                    reason: "called `connect` twice on a DbClient".to_string(),
                });
            }
        }

        Ok(())
    }

    async fn ping(&mut self) -> Result<PingResponse> {
        let ping_cmd = cmd::ping::Ping;
        let req = Message::from(ping_cmd).serialize();

        let conn = self.get_conn_mut()?;
        conn.write_all(&req).await?;

        let response = Message::try_from_async_read(conn).await?;
        event!(Level::DEBUG, "{:?}", response.payload.as_ref().unwrap());
        serde_json::from_slice(&response.payload.unwrap())?
    }

    async fn get(&mut self, key: Bytes, replica: bool) -> Result<GetResponse> {
        let get_cmd = if replica {
            cmd::get::Get::new_replica(key)
        } else {
            cmd::get::Get::new(key)
        };

        let req = Message::from(get_cmd).serialize();

        let conn = self.get_conn_mut()?;
        conn.write_all(&req).await?;

        let response = Message::try_from_async_read(conn).await?;
        serde_json::from_slice(&response.payload.unwrap())?
    }

    async fn put(
        &mut self,
        key: Bytes,
        value: Bytes,
        metadata: Option<String>,
        replication: bool,
    ) -> Result<PutResponse> {
        let put_cmd = if replication {
            cmd::put::Put::new_replication(key, value, metadata)
        } else {
            cmd::put::Put::new(key, value, metadata)
        };
        let req = Message::from(put_cmd).serialize();

        let conn = self.get_conn_mut()?;
        conn.write_all(&req).await?;

        let response = Message::try_from_async_read(conn).await?;

        event!(
            Level::DEBUG,
            "put response: {:?}",
            response.payload.as_ref().unwrap()
        );

        serde_json::from_slice(&response.payload.unwrap())?
    }

    async fn heartbeat(&mut self, known_nodes: Vec<Node>) -> Result<HeartbeatResponse> {
        let cmd = cmd::cluster::heartbeat::Heartbeat::new(known_nodes);
        let req = Message::from(cmd).serialize();

        let conn = self.get_conn_mut()?;
        conn.write_all(&req).await?;

        let response = Message::try_from_async_read(conn).await?;
        serde_json::from_slice(&response.payload.unwrap())?
    }

    async fn join_cluster(
        &mut self,
        known_cluster_node_addr: String,
    ) -> Result<JoinClusterResponse> {
        let cmd = cmd::cluster::join_cluster::JoinCluster::new(known_cluster_node_addr);
        let req = Message::from(cmd).serialize();

        let conn = self.get_conn_mut()?;
        conn.write_all(&req).await?;

        let response = Message::try_from_async_read(conn).await?;
        serde_json::from_slice(&response.payload.unwrap())?
    }

    async fn cluster_state(&mut self) -> Result<ClusterStateResponse> {
        let cmd = cmd::cluster::cluster_state::ClusterState;
        let req = Message::from(cmd).serialize();

        let conn = self.get_conn_mut()?;
        conn.write_all(&req).await?;

        let response = Message::try_from_async_read(conn).await?;
        serde_json::from_slice(&response.payload.unwrap())?
    }
}

pub struct DbClientFactory;

#[async_trait]
impl Factory for DbClientFactory {
    async fn get(&self, addr: String) -> Result<Box<dyn Client + Send>> {
        let mut client = DbClient::new(addr);
        client.connect().await?;

        Ok(Box::new(client))
    }
}
