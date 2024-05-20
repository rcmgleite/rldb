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

macro_rules! try_from_message_with_payload {
    ($message:expr, $cmd:expr, $t:ty) => {{
        (|| {
            if $message.id != $cmd {
                return Err(Error::InvalidRequest {
                    reason: format!(
                        "Unable to construct Get Command from Request. Expected id {} got {}",
                        $cmd, $message.id
                    ),
                });
            }

            if let Some(payload) = $message.payload {
                let s: $t =
                    serde_json::from_slice(&payload).map_err(|e| Error::InvalidRequest {
                        reason: format!("Invalid json payload for request {}", e.to_string()),
                    })?;
                Ok(s)
            } else {
                return Err(Error::InvalidRequest {
                    reason: "Message payload can't be None".to_string(),
                });
            }
        })()
    }};
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

    pub fn try_from_message(message: Message) -> Result<Command> {
        match message.id {
            PING_CMD => Ok(Command::Ping(ping::Ping)),
            GET_CMD => Ok(Command::Get(try_from_message_with_payload!(
                message,
                GET_CMD,
                get::Get
            )?)),
            PUT_CMD => Ok(Command::Put(try_from_message_with_payload!(
                message,
                PUT_CMD,
                put::Put
            )?)),
            CMD_CLUSTER_HEARTBEAT => Ok(Command::Heartbeat(try_from_message_with_payload!(
                message,
                CMD_CLUSTER_HEARTBEAT,
                cluster::heartbeat::Heartbeat
            )?)),
            CMD_CLUSTER_JOIN_CLUSTER => Ok(Command::JoinCluster(try_from_message_with_payload!(
                message,
                CMD_CLUSTER_JOIN_CLUSTER,
                cluster::join_cluster::JoinCluster
            )?)),
            _ => {
                event!(Level::WARN, "Unrecognized command: {}", message.id);
                return Err(Error::InvalidRequest {
                    reason: format!("Unrecognized command: {}", message.id),
                });
            }
        }
    }

    pub(crate) fn serialize_response_payload<T: Serialize>(payload: T) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(&payload).unwrap()))
    }
}
