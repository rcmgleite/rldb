//! This file contains 2 things
//!  1. the TCP listener implementation
//!    - It accepts tcp connections
//!    - tries to parse a [`Request`] our of the connection
//!    - tries to construct a [`Command`] out of the parsed Request
//!    - executes the command
//!    - writes the response back to the client
//!  2. The Request protocol
//!    - currently a simple header (cmd,length) and a binary payload
//!
//! The Request protocol in this mod is agnostic to the serialization format of the payload.
use crate::cluster::heartbeat::start_heartbeat;
use crate::cluster::ring_state::RingState;
use crate::cmd::ClusterCommand;
use crate::storage_engine::in_memory::InMemory;
use crate::{cmd::Command, storage_engine::StorageEngine};
use anyhow::anyhow;
use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;
use std::mem::size_of;
use std::sync::Arc;
use std::{fmt::Debug, path::PathBuf};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{event, instrument, Level};

use self::config::{ClusterConfig, Config, StandaloneConfig};

pub mod config;

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

/// Kind of arbitrary but let's make sure a single connection can't consume more
/// than 1Mb of memory...
const MAX_MESSAGE_SIZE: usize = 1 * 1024 * 1024;

/// A Request is the unit of the protocol built on top of TCP
/// that this server uses.
#[derive(Debug)]
pub struct Message {
    /// Message id -> used as a way of identifying the format of the payload for deserialization
    pub id: u32,
    /// length of the payload
    pub length: u32,
    /// the Request payload
    pub payload: Option<Bytes>,
}

impl Message {
    pub fn new(id: u32, payload: Option<Bytes>) -> Self {
        Self {
            id,
            length: payload.clone().map_or(0, |elem| elem.len() as u32),
            payload,
        }
    }

    pub async fn try_from_async_read<R: AsyncRead + Unpin>(reader: &mut R) -> anyhow::Result<Self> {
        let id = reader.read_u32().await?;
        let length = reader.read_u32().await?;

        let payload = if length > 0 {
            if length > MAX_MESSAGE_SIZE as u32 {
                return Err(anyhow!(
                    "Request length too long {} - max accepted value: {}",
                    length,
                    MAX_MESSAGE_SIZE
                ));
            }
            let mut buf = vec![0u8; length as usize];
            reader.read_exact(&mut buf).await?;
            Some(buf.into())
        } else {
            None
        };

        Ok(Self {
            id,
            length,
            payload,
        })
    }
}

/// Trait used so that specific serialization protocls can be implemented by the Message consumers
pub trait IntoRequest: Serialize {
    fn id(&self) -> u32;
    fn payload(&self) -> Option<Bytes> {
        None
    }
}

impl Message {
    pub fn serialize_into_request<T: IntoRequest>(value: T) -> Bytes {
        let id = value.id();
        let mut buf;
        if let Some(payload) = value.payload() {
            buf = BytesMut::with_capacity(payload.len() + 2 * size_of::<u32>());
            buf.put_u32(id);
            buf.put_u32(payload.len() as u32);
            buf.put(payload);
        } else {
            buf = BytesMut::with_capacity(2 * size_of::<u32>());
            buf.put_u32(id);
            buf.put_u32(0);
        };

        buf.freeze()
    }

    pub fn serialize(self) -> Bytes {
        let payload = self.payload;
        let payload_len = payload.clone().map_or(0, |payload| payload.len());
        let mut buf = BytesMut::with_capacity(payload_len + 2 * size_of::<u32>());

        buf.put_u32(self.id);
        buf.put_u32(payload_len as u32);
        if let Some(payload) = payload {
            buf.put(payload);
        }

        buf.freeze()
    }
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
