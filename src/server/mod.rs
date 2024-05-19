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
use crate::cmd::ClusterCommand;
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
    storage_engine: SyncStorageEngine,
    state: ServerState,
}

#[derive(Debug)]
pub enum PartitioningScheme {
    ConsistentHashing(RingState),
}

pub enum ServerMode {
    Standalone,
    Cluster {
        gossip_listener: TcpListener,
        partitioning_scheme: Arc<PartitioningScheme>,
    },
}

pub struct ServerState {
    mode: ServerMode,
}

impl Server {
    pub async fn from_config(path: PathBuf) -> anyhow::Result<Self> {
        let c = tokio::fs::read_to_string(path).await?;
        let config: Config = serde_json::from_str(&c)?;

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
                    storage_engine,
                    state: ServerState {
                        mode: ServerMode::Standalone,
                    },
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
                    storage_engine,
                    state: ServerState {
                        mode: ServerMode::Cluster {
                            gossip_listener,
                            partitioning_scheme: partitioning_scheme,
                        },
                    },
                })
            }
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        event!(Level::INFO, "Listener started");
        if let ServerMode::Cluster {
            gossip_listener,
            partitioning_scheme,
        } = &self.state.mode
        {
            // for now let's accept new connections from both the client_listener and gossip_listener
            // in the same server instance.. this might become very odd very quickly
            loop {
                tokio::select! {
                    Ok((tcp_stream, _)) = self.client_listener.accept() => {
                        let storage_engine = self.storage_engine.clone();
                        tokio::spawn(handle_client_connection(tcp_stream, storage_engine));
                    }
                    Ok((tcp_stream, _)) = gossip_listener.accept() => {
                        tokio::spawn(handle_gossip_connection(tcp_stream, partitioning_scheme.clone()));
                    }
                }
            }
        } else {
            loop {
                let (tcp_stream, _) = self.client_listener.accept().await?;
                let storage_engine = self.storage_engine.clone();
                tokio::spawn(handle_client_connection(tcp_stream, storage_engine));
            }
        }
    }
}

#[instrument(level = "debug")]
async fn handle_client_connection(
    mut tcp_stream: TcpStream,
    storage_engine: SyncStorageEngine,
) -> anyhow::Result<()> {
    loop {
        let request = Message::try_from_async_read(&mut tcp_stream).await?;
        let cmd = Command::try_from_request(request)?;
        let response = cmd.execute(storage_engine.clone()).await.serialize();

        tcp_stream.write_all(&response).await?;
    }
}

#[instrument(level = "debug")]
async fn handle_gossip_connection(
    mut tcp_stream: TcpStream,
    partition_scheme: Arc<PartitioningScheme>,
) -> anyhow::Result<()> {
    loop {
        let request = Message::try_from_async_read(&mut tcp_stream).await?;
        let cmd = ClusterCommand::try_from_request(request)?;
        let response = cmd.execute(partition_scheme.clone()).await.serialize();

        tcp_stream.write_all(&response).await?;
    }
}
