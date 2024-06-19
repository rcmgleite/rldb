//! A TCP based server that receives incoming requests and dispatches [`Command`]
//!
//! This file contains 2 things
//!  1. the TCP listener implementation
//!    - It accepts tcp connections
//!    - tries to parse a [`Message`] our of the connection
//!    - tries to construct a [`Command`] out of the parsed Request
//!    - executes the command
//!    - writes the response back to the client
//!  2. The Request protocol
//!    - currently a simple header (cmd,length) and a json encoded payload
use crate::client::db_client::DbClientFactory;
use crate::cluster::heartbeat::start_heartbeat;
use crate::cluster::state::State;
use crate::cmd::Command;
use crate::error::{Error, Result};
use crate::persistency::partitioning::consistent_hashing::ConsistentHashing;
use crate::persistency::Db;
use crate::storage_engine::in_memory::InMemory;
use bytes::Bytes;
use futures::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};
use tracing::{event, span, Instrument, Level};

use self::config::Config;
use self::message::Message;

pub mod config;
pub mod message;

/// The server struct definition
pub struct Server {
    /// listener for incoming client requests
    client_listener: TcpListener,
    /// The underlying [`Db`] used to store data and manage cluster state
    db: Arc<Db>,
}

impl Server {
    /// This functions constructs a server based on the provided json configuration
    /// Once this function returns, all required listeners will already be bound to the requested ports
    /// and the [`Server`] will be ready to start receiving requests.
    ///
    /// # Errors
    /// This function will return errors in broadly 2 scenarios
    /// 1. the configuration provided is malformed
    /// 2. the listener(s) fail to bind to the provided ports
    pub async fn from_config(path: PathBuf) -> Result<Self> {
        let c = tokio::fs::read_to_string(path).await?;
        let config: Config = serde_json::from_str(&c).map_err(|e| Error::InvalidServerConfig {
            reason: e.to_string(),
        })?;

        {
            let client_listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).await?;

            let storage_engine = match config.storage_engine {
                config::StorageEngine::InMemory => Arc::new(InMemory::default()),
            };

            // configure the partitioning_scheme. This step already
            // includes the node itself to the ring.
            let cluster_state = match config.partitioning_scheme {
                config::PartitioningScheme::ConsistentHashing => Arc::new(State::new(
                    Box::<ConsistentHashing>::default(),
                    client_listener.local_addr().unwrap().to_string().into(),
                    config.quorum,
                )?),
            };

            // FIXME: There's a big problem here - if this task exists how will
            // the node know that it has to shutdown? Something to be fixed soon...
            tokio::spawn(start_heartbeat(cluster_state.clone()));

            let own_addr = Bytes::from(client_listener.local_addr().unwrap().to_string());
            Ok(Self {
                client_listener,
                db: Arc::new(Db::new(
                    own_addr,
                    storage_engine,
                    cluster_state,
                    Box::new(DbClientFactory),
                )),
            })
        }
    }

    pub fn client_listener_local_addr(&self) -> std::io::Result<SocketAddr> {
        self.client_listener.local_addr()
    }

    /// This is the main loop of [`Server`]. When this is called, new TCP connections
    /// will be accepted and requests handled.
    ///
    /// TODO: shutdown is not fully implemented yet.. we are not waiting for inflight
    /// requests to finish/drain for example..
    pub async fn run(&mut self, shutdown: impl Future) -> Result<()> {
        event!(Level::INFO, "Listener started");

        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                Ok((conn, _)) = self.client_listener.accept() => {
                    let db = self.db.clone();
                    let own_addr = conn.local_addr().unwrap().to_string();
                    let span = span!(Level::INFO, "handle_connection", node = %own_addr);
                    tokio::spawn(handle_connection(conn, db).instrument(span));
                }
                _ = &mut shutdown => {
                    event!(Level::WARN, "shutting down");
                    return Ok(());
                }
            }
        }
    }
}

/// Helper function spanwed on a new [`tokio`] task for every newly accepted tcp connection.
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

/// Function that reads a [`Message`] out of the [TcpStream], construct a [`Command`] and executes it.
///
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
