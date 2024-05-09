pub mod get;
pub mod ping;
pub mod put;

use anyhow::anyhow;
use bytes::Bytes;
use serde::Serialize;
use tokio::{
    io::{AsyncBufRead, AsyncWrite, BufStream},
    net::TcpStream,
};
use tracing::{event, Level};

use crate::{
    cmd::get::GET_CMD,
    cmd::ping::PING_CMD,
    server::{Message, Response},
    storage_engine::StorageEngine,
};

// TODO: remove
pub trait AsyncBufReadWrite: AsyncBufRead + AsyncWrite + Send + Unpin {}
impl AsyncBufReadWrite for BufStream<TcpStream> {}

pub enum Command {
    Ping(ping::Ping),
    Get(get::Get),
}

impl Command {
    pub async fn execute<S: StorageEngine>(self, storage_engine: S) -> Response {
        match self {
            Command::Ping(cmd) => {
                let payload = cmd.execute().await;

                Response::new(PING_CMD, Self::serialize_response_payload(payload))
            }
            Command::Get(cmd) => {
                let payload = cmd.execute(storage_engine).await;

                Response::new(GET_CMD, Self::serialize_response_payload(payload))
            }
        }
    }

    pub fn try_from_message(message: Message) -> anyhow::Result<Command> {
        event!(Level::DEBUG, "trying to parse cmd: {}", message.id);
        match message.id {
            PING_CMD => Ok(Command::Ping(ping::Ping)),
            GET_CMD => Ok(Command::Get(get::Get::try_from_message(message)?)),
            _ => {
                return Err(anyhow!("Unrecognized command: {}", message.id));
            }
        }
    }

    pub(crate) fn serialize_response_payload<T: Serialize>(payload: T) -> Bytes {
        Bytes::from(serde_json::to_string(&payload).unwrap())
    }
}
