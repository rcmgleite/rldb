//! A concrete [`Client`] implementation for rldb
use async_trait::async_trait;
use bytes::Bytes;
use futures::Future;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::{event, instrument, Level};

use crate::cmd;
use crate::cmd::cluster::cluster_state::ClusterStateResponse;
use crate::cmd::cluster::heartbeat::HeartbeatResponse;
use crate::cmd::cluster::join_cluster::JoinClusterResponse;
use crate::cmd::get::GetResponse;
use crate::cmd::ping::PingResponse;
use crate::cmd::put::PutResponse;
use crate::persistency::storage::Value;
use crate::server::message::Message;
use crate::server::REQUEST_ID;
use crate::utils::generate_random_ascii_string;
use crate::{cluster::state::Node, cmd::replication_get::ReplicationGetResponse};

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

    async fn with_request_id<F>(f: F) -> F::Output
    where
        F: Future,
    {
        if let Ok(_) = REQUEST_ID.try_with(|_| ()) {
            f.await
        } else {
            REQUEST_ID.scope(generate_request_id(), f).await
        }
    }
}

#[async_trait]
impl Client for DbClient {
    #[instrument(name = "db_client::connect", level = "info", skip(self))]
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

    #[instrument(name = "db_client::ping", level = "info", skip(self))]
    async fn ping(&mut self) -> Result<PingResponse> {
        Self::with_request_id(async move {
            let ping_cmd = cmd::ping::Ping;
            let req = Message::from(ping_cmd).serialize();

            let conn = self.get_conn_mut()?;
            conn.write_all(&req).await?;

            let response = Message::try_from_async_read(conn).await?;
            event!(Level::INFO, "DEBUG {:?}", response.payload);
            serde_json::from_slice(&response.payload.unwrap())?
        })
        .await
    }

    #[instrument(name = "db_client::get", level = "info", skip(self))]
    async fn get(&mut self, key: Bytes) -> Result<GetResponse> {
        Self::with_request_id(async move {
            let get_cmd = cmd::get::Get::new(key);

            let req = Message::from(get_cmd).serialize();

            let conn = self.get_conn_mut()?;
            conn.write_all(&req).await?;

            let response = Message::try_from_async_read(conn).await?;
            serde_json::from_slice(&response.payload.unwrap())?
        })
        .await
    }

    #[instrument(name = "db_client::replication_get", level = "info", skip(self))]
    async fn replication_get(&mut self, key: Bytes) -> Result<ReplicationGetResponse> {
        Self::with_request_id(async move {
            let replication_get_cmd = cmd::replication_get::ReplicationGet::new(key);
            let req = Message::from(replication_get_cmd).serialize();
            let conn = self.get_conn_mut()?;
            conn.write_all(&req).await?;
            let response = Message::try_from_async_read(conn).await?;
            serde_json::from_slice(&response.payload.unwrap())?
        })
        .await
    }

    #[instrument(name = "db_client::put", level = "info", skip(self))]
    async fn put(
        &mut self,
        key: Bytes,
        value: Value,
        context: Option<String>,
        replication: bool,
    ) -> Result<PutResponse> {
        Self::with_request_id(async move {
            let put_cmd = if replication {
                cmd::put::Put::new_replication(key, value, context)
            } else {
                cmd::put::Put::new(key, value, context)
            };
            let req = Message::from(put_cmd).serialize();

            let conn = self.get_conn_mut()?;
            conn.write_all(&req).await?;

            let response = Message::try_from_async_read(conn).await?;

            event!(
                Level::TRACE,
                "put response: {:?}",
                response.payload.as_ref().unwrap()
            );

            serde_json::from_slice(&response.payload.unwrap())?
        })
        .await
    }

    #[instrument(name = "db_client::heartbeat", level = "info", skip(self))]
    async fn heartbeat(&mut self, known_nodes: Vec<Node>) -> Result<HeartbeatResponse> {
        Self::with_request_id(async move {
            let cmd = cmd::cluster::heartbeat::Heartbeat::new(known_nodes);
            let req = Message::from(cmd).serialize();

            let conn = self.get_conn_mut()?;
            conn.write_all(&req).await?;

            let response = Message::try_from_async_read(conn).await?;
            serde_json::from_slice(&response.payload.unwrap())?
        })
        .await
    }

    #[instrument(name = "db_client::join_cluster", level = "info", skip(self))]
    async fn join_cluster(
        &mut self,
        known_cluster_node_addr: String,
    ) -> Result<JoinClusterResponse> {
        Self::with_request_id(async move {
            let cmd = cmd::cluster::join_cluster::JoinCluster::new(known_cluster_node_addr);
            let req = Message::from(cmd).serialize();

            let conn = self.get_conn_mut()?;
            conn.write_all(&req).await?;

            let response = Message::try_from_async_read(conn).await?;
            serde_json::from_slice(&response.payload.unwrap())?
        })
        .await
    }

    #[instrument(name = "db_client::cluster_state", level = "info", skip(self))]
    async fn cluster_state(&mut self) -> Result<ClusterStateResponse> {
        Self::with_request_id(async move {
            let cmd = cmd::cluster::cluster_state::ClusterState;
            let req = Message::from(cmd).serialize();

            let conn = self.get_conn_mut()?;
            conn.write_all(&req).await?;

            let response = Message::try_from_async_read(conn).await?;
            serde_json::from_slice(&response.payload.unwrap())?
        })
        .await
    }
}

pub struct DbClientFactory;

#[async_trait]
impl Factory for DbClientFactory {
    #[instrument(name = "db_client::factory::get", level = "info", skip(self))]
    async fn get(&self, addr: String) -> Result<Box<dyn Client + Send>> {
        let mut client = DbClient::new(addr);
        client.connect().await?;

        Ok(Box::new(client))
    }
}

// dummy function to generate request ids.. probably better to change this to uuid or some other good
// requestid type
pub fn generate_request_id() -> String {
    generate_random_ascii_string(10)
}
