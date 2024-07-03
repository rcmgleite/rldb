//! Module that contains all commands implemented by rldb.
//!
//! Commands have 2 responsibilities:
//!  1. Parse request params (basically serde_json calls)
//!  2. Construct responses that are sent back to callers
//!
//! Everything else should be delegated to the [`crate::persistency`] layer or other modules.
pub mod cluster;
pub mod get;
pub mod ping;
pub mod put;

use std::sync::Arc;

use bytes::Bytes;
use cluster::cluster_state::ClusterState as ClusterStateCommand;
use cluster::heartbeat::Heartbeat as HeartbeatCommand;
use cluster::join_cluster::JoinCluster as JoinClusterCommand;
use get::Get as GetCommand;
use ping::Ping as PingCommand;
use put::Put as PutCommand;
use serde::Serialize;
use tracing::{event, Level};

use crate::{
    cmd::{
        cluster::{heartbeat::CMD_CLUSTER_HEARTBEAT, join_cluster::CMD_CLUSTER_JOIN_CLUSTER},
        get::GET_CMD,
        ping::PING_CMD,
        put::PUT_CMD,
    },
    error::{Error, InvalidRequest, Result},
    persistency::Db,
    server::message::{IntoMessage, Message},
};

use self::cluster::cluster_state::CMD_CLUSTER_CLUSTER_STATE;

/// Command definition - this enum contains all commands implemented by rldb.
///
/// TODO: Note - we are mixing cluster and client commands here... it might be better to split them in the future.
/// right now a cluster command issued against the client port will run normally which is a bit weird...
pub enum Command {
    Ping(PingCommand),
    Get(GetCommand),
    Put(PutCommand),
    Heartbeat(HeartbeatCommand),
    JoinCluster(JoinClusterCommand),
    ClusterState(ClusterStateCommand),
}

/// macro that tries to construct a specific [`Command`] from a [`Message`]
macro_rules! try_from_message_with_payload {
    ($message:expr, $t:ident) => {{
        (|| {
            if $message.id != $t::cmd_id() {
                return Err(Error::InvalidRequest(
                    InvalidRequest::UnableToConstructCommandFromMessage {
                        expected_id: $t::cmd_id(),
                        got: $message.id,
                    },
                ));
            }

            if let Some(payload) = $message.payload {
                let s: $t = serde_json::from_slice(&payload).map_err(|e| {
                    Error::InvalidRequest(InvalidRequest::InvalidJsonPayload(e.to_string()))
                })?;
                Ok(s)
            } else {
                return Err(Error::InvalidRequest(InvalidRequest::EmptyMessagePayload));
            }
        })()
    }};
}

impl Command {
    /// Executes a given command by forwarding the [`Db`] instance provided
    pub async fn execute(self, db: Arc<Db>) -> Message {
        match self {
            Command::Ping(cmd) => {
                let request_id = cmd.request_id();
                let payload = cmd.execute().await;
                Message::new(
                    PING_CMD,
                    request_id,
                    Self::serialize_response_payload(payload),
                )
            }
            Command::Get(cmd) => {
                let request_id = cmd.request_id();
                let payload = cmd.execute(db).await;
                Message::new(
                    GET_CMD,
                    request_id,
                    Self::serialize_response_payload(payload),
                )
            }
            Command::Put(cmd) => {
                let request_id = cmd.request_id();
                let payload = cmd.execute(db).await;
                Message::new(
                    PUT_CMD,
                    request_id,
                    Self::serialize_response_payload(payload),
                )
            }
            Command::Heartbeat(cmd) => {
                let request_id = cmd.request_id();
                let payload = cmd.execute(db).await;
                Message::new(
                    CMD_CLUSTER_HEARTBEAT,
                    request_id,
                    Self::serialize_response_payload(payload),
                )
            }
            Command::JoinCluster(cmd) => {
                let request_id = cmd.request_id();
                let payload = cmd.execute(db).await;
                let res = Message::new(
                    CMD_CLUSTER_JOIN_CLUSTER,
                    request_id,
                    Self::serialize_response_payload(payload),
                );

                res
            }
            Command::ClusterState(cmd) => {
                let request_id = cmd.request_id();
                let payload = cmd.execute(db).await;
                Message::new(
                    CMD_CLUSTER_CLUSTER_STATE,
                    request_id,
                    Self::serialize_response_payload(payload),
                )
            }
        }
    }

    /// Tries to construct a [`Command`] from the provided [`Message`]
    ///
    /// # Errors
    /// returns an error if the payload doesn't conform with the specified [`Command`]
    pub fn try_from_message(message: Message) -> Result<Command> {
        match message.id {
            PING_CMD => Ok(Command::Ping(ping::Ping::new(message.request_id))),
            GET_CMD => Ok(Command::Get(try_from_message_with_payload!(
                message, GetCommand
            )?)),
            PUT_CMD => Ok(Command::Put(try_from_message_with_payload!(
                message, PutCommand
            )?)),
            CMD_CLUSTER_HEARTBEAT => Ok(Command::Heartbeat(try_from_message_with_payload!(
                message,
                HeartbeatCommand
            )?)),
            CMD_CLUSTER_JOIN_CLUSTER => Ok(Command::JoinCluster(try_from_message_with_payload!(
                message,
                JoinClusterCommand
            )?)),
            CMD_CLUSTER_CLUSTER_STATE => Ok(Command::ClusterState(try_from_message_with_payload!(
                message,
                ClusterStateCommand
            )?)),
            _ => {
                event!(Level::WARN, "Unrecognized command: {}", message.id);
                Err(Error::InvalidRequest(InvalidRequest::UnrecognizedCommand {
                    id: message.id,
                }))
            }
        }
    }

    /// Serializes the given payload into json
    pub(crate) fn serialize_response_payload<T: Serialize>(payload: Result<T>) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(&payload).unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::Command;
    use crate::cmd::get::Get;
    use crate::cmd::put::Put;
    use crate::error::{Error, InvalidRequest};
    use crate::server::message::Message;
    use bytes::Bytes;

    #[test]
    fn invalid_request_mixed_request_id() {
        let put_cmd = Put::new(
            Bytes::from("foo"),
            Bytes::from("bar"),
            None,
            "requestId".to_string(),
        );
        let mut message = Message::from(put_cmd);
        message.id = Get::cmd_id();

        let err = Command::try_from_message(message).err().unwrap();
        match err {
            Error::InvalidRequest(InvalidRequest::InvalidJsonPayload(_)) => {}
            _ => {
                panic!("Unexpected error: {}", err);
            }
        }
    }

    #[test]
    fn invalid_request_unrecognized_command() {
        let put_cmd = Put::new(
            Bytes::from("foo"),
            Bytes::from("bar"),
            None,
            "requestId".to_string(),
        );
        let mut message = Message::from(put_cmd);
        message.id = 99999;

        let err = Command::try_from_message(message).err().unwrap();
        match err {
            Error::InvalidRequest(InvalidRequest::UnrecognizedCommand { id }) => {
                assert_eq!(id, 99999);
            }
            _ => {
                panic!("Unexpected error: {}", err);
            }
        }
    }

    #[test]
    fn invalid_request_empty_payload() {
        let put_cmd = Put::new(
            Bytes::from("foo"),
            Bytes::from("bar"),
            None,
            "requestId".to_string(),
        );
        let mut message = Message::from(put_cmd);
        message.payload = None;

        let err = Command::try_from_message(message).err().unwrap();
        match err {
            Error::InvalidRequest(InvalidRequest::EmptyMessagePayload) => {}
            _ => {
                panic!("Unexpected error: {}", err);
            }
        }
    }
}
