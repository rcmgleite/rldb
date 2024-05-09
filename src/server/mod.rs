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
use anyhow::anyhow;
use bytes::{BufMut, Bytes, BytesMut};
use serde::Serialize;
use std::fmt::Debug;
use std::mem::size_of;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{event, instrument, Level};

use crate::{cmd::Command, storage_engine::StorageEngine};

struct Listener<S: StorageEngine> {
    listener: TcpListener,
    storage_engine: S,
}

/// Kind of arbitrary but let's make sure a single connection can't consume more
/// than 1Mb of memory...
const MAX_MESSAGE_SIZE: usize = 1 * 1024 * 1024;

/// A Request is the unit of the protocol built on top of TCP
/// that this server uses.
#[derive(Debug)]
pub struct Request {
    /// Request id -> used as a way of identifying the format of the payload for deserialization
    pub id: u32,
    /// length of the payload
    pub length: u32,
    /// the Request payload
    pub payload: Option<Bytes>,
}

impl Request {
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

impl Request {
    pub fn serialize<T: IntoRequest>(value: T) -> Bytes {
        let id = value.id();
        event!(Level::DEBUG, "Will serialize cmd: {}", id);
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
}

/// Similar to [`Request`], [`Response`] is also agnostic to the payload serialization format
#[derive(Debug)]
pub struct Response {
    id: u32,
    payload: Bytes,
}

impl Response {
    pub fn new(id: u32, payload: Bytes) -> Self {
        Self { id, payload }
    }
}

impl Response {
    fn serialize(self) -> Bytes {
        let payload = self.payload;
        let mut buf = BytesMut::with_capacity(payload.len() + 2 * size_of::<u32>());

        buf.put_u32(self.id);
        buf.put_u32(payload.len() as u32);
        buf.put(payload.clone());

        buf.freeze()
    }
}

impl<S: StorageEngine + Send + 'static> Listener<S> {
    async fn run(&self) -> anyhow::Result<()> {
        event!(Level::INFO, "Listener started");

        loop {
            let (tcp_stream, _) = self.listener.accept().await?;
            event!(
                Level::DEBUG,
                "accepted new tcp connection: {:?}",
                tcp_stream
            );
            let storage_engine = self.storage_engine.clone();
            tokio::spawn(handle_connection(tcp_stream, storage_engine));
        }
    }
}

#[instrument(level = "debug")]
async fn handle_connection<S: StorageEngine>(
    mut tcp_stream: TcpStream,
    storage_engine: S,
) -> anyhow::Result<()> {
    loop {
        let request = Request::try_from_async_read(&mut tcp_stream).await?;
        let cmd = Command::try_from_request(request)?;
        let response = cmd.execute(storage_engine.clone()).await.serialize();

        event!(Level::DEBUG, "going to write response: {:?}", response);

        tcp_stream.write_all(&response).await?;
    }
}

pub async fn run<S: StorageEngine + Send + 'static>(
    listener: TcpListener,
    storage_engine: S,
) -> anyhow::Result<()> {
    let server = Listener {
        listener,
        storage_engine,
    };
    server.run().await
}
