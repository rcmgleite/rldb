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
    cmd::{
        cluster::join_cluster::CMD_CLUSTER_JOIN_CLUSTER, get::GET_CMD, ping::PING_CMD, put::PUT_CMD,
    },
    server::{Message, PartitioningScheme, SyncStorageEngine},
};

use self::cluster::{
    heartbeat::{Heartbeat, CMD_CLUSTER_HEARTBEAT},
    join_cluster::JoinCluster,
};

pub enum Command {
    Ping(ping::Ping),
    Get(get::Get),
    Put(put::Put),
}

impl Command {
    pub async fn execute(self, storage_engine: SyncStorageEngine) -> Message {
        match self {
            Command::Ping(cmd) => {
                let payload = cmd.execute().await;
                Message::new(PING_CMD, Self::serialize_response_payload(payload))
            }
            Command::Get(cmd) => {
                let payload = cmd.execute(storage_engine).await;
                Message::new(GET_CMD, Self::serialize_response_payload(payload))
            }
            Command::Put(cmd) => {
                let payload = cmd.execute(storage_engine).await;
                Message::new(PUT_CMD, Self::serialize_response_payload(payload))
            }
        }
    }

    pub fn try_from_request(request: Message) -> anyhow::Result<Command> {
        match request.id {
            PING_CMD => Ok(Command::Ping(ping::Ping)),
            GET_CMD => Ok(Command::Get(get::Get::try_from_request(request)?)),
            PUT_CMD => Ok(Command::Put(put::Put::try_from_request(request)?)),
            _ => {
                event!(Level::WARN, "Unrecognized command: {}", request.id);
                return Err(anyhow!("Unrecognized command: {}", request.id));
            }
        }
    }

    pub(crate) fn serialize_response_payload<T: Serialize>(payload: T) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(&payload).unwrap()))
    }
}

pub enum ClusterCommand {
    Heartbeat(Heartbeat),
    JoinCluster(JoinCluster),
    RemoveNode,
}

impl ClusterCommand {
    pub async fn execute(self, partition_scheme: Arc<PartitioningScheme>) -> Message {
        match self {
            ClusterCommand::Heartbeat(cmd) => {
                let payload = cmd.execute(partition_scheme).await;
                Message::new(
                    CMD_CLUSTER_HEARTBEAT,
                    Self::serialize_response_payload(payload),
                )
            }
            ClusterCommand::JoinCluster(cmd) => {
                let payload = cmd.execute(partition_scheme).await;
                Message::new(
                    CMD_CLUSTER_JOIN_CLUSTER,
                    Self::serialize_response_payload(payload),
                )
            }
            ClusterCommand::RemoveNode => {
                todo!()
            }
        }
    }

    pub fn try_from_request(request: Message) -> anyhow::Result<ClusterCommand> {
        match request.id {
            CMD_CLUSTER_HEARTBEAT => Ok(ClusterCommand::Heartbeat(
                cluster::heartbeat::Heartbeat::try_from_request(request)?,
            )),
            CMD_CLUSTER_JOIN_CLUSTER => Ok(ClusterCommand::JoinCluster(
                cluster::join_cluster::JoinCluster::try_from_request(request)?,
            )),
            _ => {
                event!(Level::WARN, "Unrecognized command: {}", request.id);
                return Err(anyhow!("Unrecognized command: {}", request.id));
            }
        }
    }

    pub(crate) fn serialize_response_payload<T: Serialize>(payload: T) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(&payload).unwrap()))
    }
}
