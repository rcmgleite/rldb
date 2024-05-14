pub mod cluster;
pub mod get;
pub mod ping;
pub mod put;

use anyhow::anyhow;
use bytes::Bytes;
use serde::Serialize;
use tracing::{event, Level};

use crate::{
    cmd::{get::GET_CMD, ping::PING_CMD, put::PUT_CMD},
    server::{Request, Response},
    storage_engine::StorageEngine,
};

pub enum Command {
    Ping(ping::Ping),
    Get(get::Get),
    Put(put::Put),
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
            Command::Put(cmd) => {
                let payload = cmd.execute(storage_engine).await;
                Response::new(PUT_CMD, Self::serialize_response_payload(payload))
            }
        }
    }

    pub fn try_from_request(request: Request) -> anyhow::Result<Command> {
        event!(Level::DEBUG, "trying to parse cmd: {}", request.id);
        match request.id {
            PING_CMD => Ok(Command::Ping(ping::Ping)),
            GET_CMD => Ok(Command::Get(get::Get::try_from_request(request)?)),
            PUT_CMD => Ok(Command::Put(put::Put::try_from_request(request)?)),
            _ => {
                event!(Level::ERROR, "Unrecognized command: {}", request.id);
                return Err(anyhow!("Unrecognized command: {}", request.id));
            }
        }
    }

    pub(crate) fn serialize_response_payload<T: Serialize>(payload: T) -> Bytes {
        Bytes::from(serde_json::to_string(&payload).unwrap())
    }
}
