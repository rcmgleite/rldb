//! This file contains 2 things
//!  1. the TCP listener implementation
//!    - It accepts tcp connections
//!    - tries to parse a [`Request`] our of the connection
//!    - tries to construct a [`Command`] out of the parsed Request
//!    - executes the command
//!    - writes the response back to the client
//!  2. The Request protocol
//!    - currently a simple header (cmd,length) and a json encoded payload
use crate::cluster::heartbeat::start_heartbeat;
use crate::cluster::ring_state::RingState;
use crate::cmd::Command;
use crate::db::{Db, PartitioningScheme};
use crate::error::{Error, Result};
use crate::storage_engine::in_memory::InMemory;
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tracing::{event, instrument, Level};

use self::config::{ClusterConfig, Config, StandaloneConfig};
use self::message::Message;

pub mod config;
pub mod message;

pub struct Server {
    /// listener for incoming client requests
    client_listener: TcpListener,
    /// listener for incoming cluster (gossip) requests
    /// Only exists in cluster configuration
    cluster_listener: Option<TcpListener>,
    db: Arc<Db>,
}

impl Server {
    pub async fn from_config(path: PathBuf) -> Result<Self> {
        let c = tokio::fs::read_to_string(path).await?;
        let config: Config = serde_json::from_str(&c).map_err(|e| Error::InvalidServerConfig {
            reason: e.to_string(),
        })?;

        match config.cluster_type {
            config::ClusterType::Standalone(StandaloneConfig {
                port,
                storage_engine,
            }) => {
                let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

                let storage_engine = match storage_engine {
                    config::StorageEngine::InMemory => Arc::new(InMemory::default()),
                };

                Ok(Self {
                    client_listener: listener,
                    cluster_listener: None,
                    db: Arc::new(Db::new(storage_engine, None)),
                })
            }

            config::ClusterType::Cluster(ClusterConfig {
                port,
                storage_engine,
                partitioning_scheme,
                gossip,
                // TODO: wire quorum configs once this feature is implemented
                quorum: _,
            }) => {
                let client_listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

                let storage_engine = match storage_engine {
                    config::StorageEngine::InMemory => Arc::new(InMemory::default()),
                };

                // configure the partitioning_scheme. This step already
                // includes the node itself to the ring.
                let partitioning_scheme = match partitioning_scheme {
                    config::PartitioningScheme::ConsistentHashing => {
                        // let own_addr = Bytes::from(local_ip().unwrap().to_string());
                        let own_addr = Bytes::from(format!("127.0.0.1:{}", gossip.port));
                        let ring_state = RingState::new(own_addr);
                        Arc::new(PartitioningScheme::ConsistentHashing(ring_state))
                    }
                };

                let cluster_listener =
                    TcpListener::bind(format!("127.0.0.1:{}", gossip.port)).await?;

                // FIXME: There's a big problem here - if this task exists how will
                // the node know that it has to shutdown? Something to be fixed soon...
                tokio::spawn(start_heartbeat(partitioning_scheme.clone()));

                Ok(Self {
                    client_listener,
                    cluster_listener: Some(cluster_listener),
                    db: Arc::new(Db::new(storage_engine, Some(partitioning_scheme))),
                })
            }
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        event!(Level::INFO, "Listener started");

        // if we have a cluster listener, accept connection from both client and cluster listeners.
        if let Some(cluster_listener) = self.cluster_listener.take() {
            loop {
                let conn = tokio::select! {
                    Ok((conn, _)) = self.client_listener.accept() => {
                       conn
                    }
                    Ok((conn, _)) = cluster_listener.accept() => {
                        conn
                    }
                };

                let db = self.db.clone();
                tokio::spawn(handle_connection(conn, db));
            }
        } else {
            // otherwise only listen to client connections
            loop {
                let (conn, _) = self.client_listener.accept().await?;
                let db = self.db.clone();
                tokio::spawn(handle_connection(conn, db));
            }
        }
    }
}

#[instrument(level = "debug")]
async fn handle_connection(mut conn: TcpStream, db: Arc<Db>) -> Result<()> {
    loop {
        // Since [`Error`] is Serialize, in case of an error we can write it back to the client
        match handle_message(&mut conn, db.clone()).await {
            Ok(message) => {
                conn.write_all(&message.serialize()).await?;
            }
            Err(err) => {
                let msg = Message::new(0, Some(Bytes::from(serde_json::to_string(&err).unwrap())));
                conn.write_all(&msg.serialize()).await?;
            }
        }
    }
}

/// There are some oddities here still
///  1. parsing a message and a command from a message can return an error
///    when that happens, the response will be a message with id `0`. This is odd.
///    makes me believe that maybe the protocol is not great still
///  2. On the other hand, cmd.execute return a specific Response object per command which includes failure/success cases.
async fn handle_message(conn: &mut TcpStream, db: Arc<Db>) -> Result<Message> {
    let message = Message::try_from_async_read(conn).await?;
    let cmd = Command::try_from_message(message)?;
    Ok(cmd.execute(db.clone()).await)
}
