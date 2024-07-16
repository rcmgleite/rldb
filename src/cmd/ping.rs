//! Ping [`crate::cmd::Command`]
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{error::Result, server::message::IntoMessage};

use super::CommandId;

#[derive(Debug, Serialize)]
pub struct Ping;

impl Ping {
    #[instrument(name = "cmd::ping", level = "info")]
    pub async fn execute(self) -> Result<PingResponse> {
        Ok(PingResponse {
            message: "PONG".to_string(),
        })
    }
}

impl IntoMessage for Ping {
    fn cmd_id(&self) -> CommandId {
        CommandId::Ping
    }
}

/// [`Ping`] response payload
#[derive(Serialize, Deserialize)]
pub struct PingResponse {
    pub message: String,
}

impl IntoMessage for Result<PingResponse> {
    fn cmd_id(&self) -> CommandId {
        CommandId::Ping
    }

    fn payload(&self) -> Option<bytes::Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}
