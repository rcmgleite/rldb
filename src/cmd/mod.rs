pub mod ping;

use anyhow::anyhow;
use bytes::Bytes;
use serde::Serialize;
use tokio::{
    io::{AsyncBufRead, AsyncWrite, BufStream},
    net::TcpStream,
};
use tracing::{event, Level};

use crate::server::{Message, Response};

use self::ping::PING_CMD;

// TODO: remove
pub trait AsyncBufReadWrite: AsyncBufRead + AsyncWrite + Send + Unpin {}
impl AsyncBufReadWrite for BufStream<TcpStream> {}

pub enum Command {
    Ping(ping::Ping),
}

impl Command {
    pub async fn execute(self) -> Response {
        let response_payload = match self {
            Command::Ping(cmd) => cmd.execute().await,
        };

        Response::new(PING_CMD, response_payload)
    }

    pub async fn try_from_message(message: Message) -> anyhow::Result<Command> {
        event!(Level::DEBUG, "trying to parse cmd: {}", message.id);
        match message.id {
            PING_CMD => Ok(Command::Ping(ping::Ping)),
            _ => {
                return Err(anyhow!("Unrecognized command: {}", message.id));
            }
        }
    }

    pub(crate) fn serialize_response_payload<T: Serialize>(payload: T) -> Bytes {
        Bytes::from(serde_json::to_string(&payload).unwrap())
    }
}
