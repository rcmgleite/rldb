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
use crate::error::{Error, Result};
use crate::storage_engine::in_memory::InMemory;
use crate::{cmd::Command, storage_engine::StorageEngine};
use bytes::Bytes;
use std::sync::Arc;
use std::{fmt::Debug, path::PathBuf};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tracing::{event, instrument, Level};

use self::config::{ClusterConfig, Config, StandaloneConfig};
use self::message::Message;

pub mod config;
pub mod message;

pub type SyncStorageEngine = Arc<dyn StorageEngine + Send + Sync + 'static>;

pub struct Server {
    client_listener: TcpListener,
    db: Arc<Db>,
}

// TODO: This Db struct is very cumbersome... rework it..
#[derive(Debug)]
pub struct Db {
    pub storage_engine: SyncStorageEngine,
    pub partitioning_scheme: Option<Arc<PartitioningScheme>>,
    pub state: ServerState,
}

#[derive(Debug)]
pub enum PartitioningScheme {
    ConsistentHashing(RingState),
}

#[derive(Debug)]
pub enum ServerMode {
    Standalone,
    Cluster { gossip_listener: TcpListener },
}

#[derive(Debug)]
pub struct ServerState {
    pub mode: ServerMode,
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
                    db: Arc::new(Db {
                        storage_engine,
                        partitioning_scheme: None,
                        state: ServerState {
                            mode: ServerMode::Standalone,
                        },
                    }),
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

                let gossip_listener =
                    TcpListener::bind(format!("127.0.0.1:{}", gossip.port)).await?;

                // FIXME: There's a big problem here - if this task exists how will
                // the node know that it has to shutdown? Something to be fixed soon...
                tokio::spawn(start_heartbeat(partitioning_scheme.clone()));

                Ok(Self {
                    client_listener,
                    db: Arc::new(Db {
                        storage_engine,
                        partitioning_scheme: Some(partitioning_scheme),
                        state: ServerState {
                            mode: ServerMode::Cluster { gossip_listener },
                        },
                    }),
                })
            }
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        event!(Level::INFO, "Listener started");
        if let ServerMode::Cluster { gossip_listener } = &self.db.state.mode {
            // for now let's accept new connections from both the client_listener and gossip_listener
            // in the same server instance.. this might become very odd very quickly
            loop {
                let conn = tokio::select! {
                    Ok((conn, _)) = self.client_listener.accept() => {
                       conn
                    }
                    Ok((conn, _)) = gossip_listener.accept() => {
                        conn
                    }
                };

                let db = self.db.clone();
                tokio::spawn(handle_connection(conn, db));
            }
        } else {
            loop {
                let (conn, _) = self.client_listener.accept().await?;
                let db = self.db.clone();
                tokio::spawn(handle_connection(conn, db));
            }
        }
    }
}

#[instrument(level = "debug")]
async fn handle_connection(mut tcp_stream: TcpStream, db: Arc<Db>) -> Result<()> {
    loop {
        let request = Message::try_from_async_read(&mut tcp_stream).await?;
        let cmd = Command::try_from_request(request)?;
        let response = cmd.execute(db.clone()).await.serialize();

        tcp_stream.write_all(&response).await?;
    }
}
