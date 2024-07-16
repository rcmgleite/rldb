//! Module that contains all commands implemented by rldb.
//!
//! # Design principals
//! Commands have 2 responsibilities:
//!  1. Parse request params (basically serde_json calls)
//!  2. Construct responses that are sent back to callers
//!
//! Everything else should be delegated to the [`crate::persistency`] layer or other modules.
pub mod cluster;
pub mod get;
pub mod ping;
pub mod put;
pub mod replication_get;
pub mod types;

use std::sync::Arc;

use cluster::cluster_state::ClusterState as ClusterStateCommand;
use cluster::heartbeat::Heartbeat as HeartbeatCommand;
use cluster::join_cluster::JoinCluster as JoinClusterCommand;
use get::Get as GetCommand;
use ping::Ping as PingCommand;
use put::Put as PutCommand;
use replication_get::ReplicationGet as ReplicationGetCommand;
use strum_macros::FromRepr;
use tracing::{event, instrument, Level};

use crate::{
    error::{Error, InvalidRequest, Result},
    persistency::Db,
    server::message::Message,
};

#[derive(Debug, FromRepr, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandId {
    NotSet = 0,
    Ping = 1,
    Get = 2,
    Put = 3,
    ReplicationGet = 4,

    // cluster commands
    Heartbeat = 5,
    JoinCluster = 6,
    ClusterState = 7,
}

impl TryFrom<u8> for CommandId {
    type Error = Error;

    fn try_from(v: u8) -> std::result::Result<Self, Self::Error> {
        if let Some(s) = Self::from_repr(v) {
            Ok(s)
        } else {
            Err(Error::InvalidRequest(InvalidRequest::UnrecognizedCommand {
                id: v,
            }))
        }
    }
}

/// Command definition - this enum contains all commands implemented by rldb.
///
/// TODO: Note - we are mixing cluster and client commands here... it might be better to split them in the future.
/// right now a cluster command issued against the client port will run normally which is a bit weird...
#[derive(Debug)]
pub enum Command {
    Ping(PingCommand),
    Get(GetCommand),
    ReplicationGet(ReplicationGetCommand),
    Put(PutCommand),
    Heartbeat(HeartbeatCommand),
    JoinCluster(JoinClusterCommand),
    ClusterState(ClusterStateCommand),
}

/// macro that tries to construct a specific [`Command`] from a [`Message`]
macro_rules! try_from_message_with_payload {
    ($message:expr, $t:ident) => {{
        (|| {
            if $message.cmd_id != $t::cmd_id() {
                return Err(Error::InvalidRequest(
                    InvalidRequest::UnableToConstructCommandFromMessage {
                        expected_id: $t::cmd_id() as u8,
                        got: $message.cmd_id as u8,
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
    #[instrument(name = "cmd::execute", level = "info", skip(db))]
    pub async fn execute(self, db: Arc<Db>) -> Message {
        match self {
            Command::Ping(cmd) => cmd.execute().await.into(),
            Command::Get(cmd) => cmd.execute(db).await.into(),
            Command::ReplicationGet(cmd) => cmd.execute(db).await.into(),
            Command::Put(cmd) => cmd.execute(db).await.into(),
            Command::Heartbeat(cmd) => cmd.execute(db).await.into(),
            Command::JoinCluster(cmd) => cmd.execute(db).await.into(),
            Command::ClusterState(cmd) => cmd.execute(db).await.into(),
        }
    }

    /// Tries to construct a [`Command`] from the provided [`Message`]
    ///
    /// # Errors
    /// returns an error if the payload doesn't conform with the specified [`Command`]
    #[instrument(level = "info")]
    pub fn try_from_message(message: Message) -> Result<Command> {
        match message.cmd_id {
            CommandId::Ping => Ok(Command::Ping(ping::Ping)),
            CommandId::Get => Ok(Command::Get(try_from_message_with_payload!(
                message, GetCommand
            )?)),
            CommandId::ReplicationGet => Ok(Command::ReplicationGet(
                try_from_message_with_payload!(message, ReplicationGetCommand)?,
            )),
            CommandId::Put => Ok(Command::Put(try_from_message_with_payload!(
                message, PutCommand
            )?)),
            CommandId::Heartbeat => Ok(Command::Heartbeat(try_from_message_with_payload!(
                message,
                HeartbeatCommand
            )?)),
            CommandId::JoinCluster => Ok(Command::JoinCluster(try_from_message_with_payload!(
                message,
                JoinClusterCommand
            )?)),
            CommandId::ClusterState => Ok(Command::ClusterState(try_from_message_with_payload!(
                message,
                ClusterStateCommand
            )?)),
            CommandId::NotSet => {
                event!(Level::WARN, "CommandId not set (or set to zero)");
                Err(Error::InvalidRequest(InvalidRequest::CommandIdNotSet))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Command, CommandId};
    use crate::cmd::get::Get;
    use crate::cmd::put::Put;
    use crate::error::{Error, InvalidRequest};
    use crate::persistency::storage::Value;
    use crate::server::message::Message;
    use bytes::Bytes;

    #[test]
    fn invalid_request_mixed_message_id() {
        let put_cmd = Get::new(Bytes::from("foo"));
        let mut message = Message::from(put_cmd);
        message.cmd_id = Put::cmd_id();

        let err = Command::try_from_message(message).err().unwrap();
        match err {
            Error::InvalidRequest(InvalidRequest::InvalidJsonPayload(_)) => {}
            _ => {
                panic!("Unexpected error: {}", err);
            }
        }
    }

    #[test]
    fn invalid_request_command_id_not_set() {
        let put_cmd = Put::new(
            Bytes::from("foo"),
            Value::new_unchecked(Bytes::from("bar")),
            None,
        );
        let mut message = Message::from(put_cmd);
        message.cmd_id = CommandId::NotSet;

        let err = Command::try_from_message(message).err().unwrap();
        assert!(matches!(
            err,
            Error::InvalidRequest(InvalidRequest::CommandIdNotSet)
        ));
    }

    #[test]
    fn invalid_request_empty_payload() {
        let put_cmd = Put::new(
            Bytes::from("foo"),
            Value::new_unchecked(Bytes::from("bar")),
            None,
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
