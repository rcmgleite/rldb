pub mod cluster;
pub mod get;
pub mod ping;
pub mod put;

use std::sync::Arc;

use anyhow::anyhow;
use bytes::Bytes;
use serde::Serialize;
use tracing::{event, Level};

use crate::{
    cmd::{get::GET_CMD, ping::PING_CMD, put::PUT_CMD},
    server::{PartitioningScheme, Request, Response, SyncStorageEngine},
};

use self::cluster::heartbeat::{Heartbeat, CMD_CLUSTER_HEARTBEAT};

pub enum Command {
    Ping(ping::Ping),
    Get(get::Get),
    Put(put::Put),
}

impl Command {
    pub async fn execute(self, storage_engine: SyncStorageEngine) -> Response {
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

pub enum ClusterCommand {
    Heartbeat(Heartbeat),
    AddNode,
    RemoveNode,
}

impl ClusterCommand {
    pub async fn execute(self, partition_scheme: Arc<PartitioningScheme>) -> Response {
        match self {
            ClusterCommand::Heartbeat(cmd) => {
                let payload = cmd.execute(partition_scheme).await;
                Response::new(
                    CMD_CLUSTER_HEARTBEAT,
                    Self::serialize_response_payload(payload),
                )
            }
            ClusterCommand::AddNode => {
                todo!()
            }
            ClusterCommand::RemoveNode => {
                todo!()
            }
        }
    }

    pub fn try_from_request(request: Request) -> anyhow::Result<ClusterCommand> {
        event!(Level::DEBUG, "trying to parse cmd: {}", request.id);
        match request.id {
            CMD_CLUSTER_HEARTBEAT => Ok(ClusterCommand::Heartbeat(
                cluster::heartbeat::Heartbeat::try_from_request(request)?,
            )),
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
