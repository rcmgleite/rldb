pub mod cluster;
pub mod get;
pub mod ping;
pub mod put;

use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;
use tracing::{event, Level};

use crate::{
    cmd::{
        cluster::join_cluster::CMD_CLUSTER_JOIN_CLUSTER, get::GET_CMD, ping::PING_CMD, put::PUT_CMD,
    },
    error::{Error, Result},
    server::{message::Message, Db},
};

use self::cluster::{
    heartbeat::{Heartbeat, CMD_CLUSTER_HEARTBEAT},
    join_cluster::JoinCluster,
};

// TODO: Note - we are mixing cluster and client commands here... it might be better to split them in the future.
// right now a cluster command issued against the client port will run normally which is a bit weird...
pub enum Command {
    Ping(ping::Ping),
    Get(get::Get),
    Put(put::Put),
    Heartbeat(Heartbeat),
    JoinCluster(JoinCluster),
}

impl Command {
    pub async fn execute(self, db: Arc<Db>) -> Message {
        match self {
            Command::Ping(cmd) => {
                let payload = cmd.execute().await;
                Message::new(PING_CMD, Self::serialize_response_payload(payload))
            }
            Command::Get(cmd) => {
                let payload = cmd.execute(db).await;
                Message::new(GET_CMD, Self::serialize_response_payload(payload))
            }
            Command::Put(cmd) => {
                let payload = cmd.execute(db).await;
                Message::new(PUT_CMD, Self::serialize_response_payload(payload))
            }
            Command::Heartbeat(cmd) => {
                let payload = cmd.execute(db).await;
                Message::new(
                    CMD_CLUSTER_HEARTBEAT,
                    Self::serialize_response_payload(payload),
                )
            }
            Command::JoinCluster(cmd) => {
                let payload = cmd.execute(db).await;
                Message::new(
                    CMD_CLUSTER_JOIN_CLUSTER,
                    Self::serialize_response_payload(payload),
                )
            }
        }
    }

    pub fn try_from_request(request: Message) -> Result<Command> {
        match request.id {
            PING_CMD => Ok(Command::Ping(ping::Ping)),
            GET_CMD => Ok(Command::Get(get::Get::try_from_request(request)?)),
            PUT_CMD => Ok(Command::Put(put::Put::try_from_request(request)?)),
            CMD_CLUSTER_HEARTBEAT => Ok(Command::Heartbeat(
                cluster::heartbeat::Heartbeat::try_from_request(request)?,
            )),
            CMD_CLUSTER_JOIN_CLUSTER => Ok(Command::JoinCluster(
                cluster::join_cluster::JoinCluster::try_from_request(request)?,
            )),
            _ => {
                event!(Level::WARN, "Unrecognized command: {}", request.id);
                return Err(Error::InvalidRequest {
                    reason: format!("Unrecognized command: {}", request.id),
                });
            }
        }
    }

    pub(crate) fn serialize_response_payload<T: Serialize>(payload: T) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(&payload).unwrap()))
    }
}
