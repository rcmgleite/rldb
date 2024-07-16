//! Put [`crate::cmd::Command`]
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::error::Result;
use crate::persistency::storage::Value;
use crate::persistency::Db;
use crate::server::message::IntoMessage;
use crate::utils::serde_utf8_bytes;

use super::types::SerializedContext;
use super::CommandId;

/// Struct that represents a deserialized Put payload
#[derive(Debug, Serialize, Deserialize)]
pub struct Put {
    #[serde(with = "serde_utf8_bytes")]
    key: Bytes,
    value: Value,
    replication: bool,
    context: Option<String>,
}

impl Put {
    /// Constructs a new [`Put`] [`crate::cmd::Command`]
    pub fn new(key: Bytes, value: Value, context: Option<String>) -> Self {
        Self::new_private(key, value, context, false)
    }

    /// constructs a new [`Put`] [`crate::cmd::Command`] for replication
    pub fn new_replication(key: Bytes, value: Value, context: Option<String>) -> Self {
        Self::new_private(key, value, context, true)
    }

    fn new_private(key: Bytes, value: Value, context: Option<String>, replication: bool) -> Self {
        Self {
            key,
            value,
            replication,
            context,
        }
    }

    /// Executes a [`Put`] [`crate::cmd::Command`]
    #[instrument(name = "cmd::put", level = "info", skip(db))]
    pub async fn execute(self, db: Arc<Db>) -> Result<PutResponse> {
        db.put(
            self.key,
            self.value,
            self.replication,
            self.context.map(SerializedContext::from),
        )
        .await?;
        Ok(PutResponse {
            message: "Ok".to_string(),
        })
    }

    pub fn cmd_id() -> CommandId {
        CommandId::Put
    }
}

impl IntoMessage for Put {
    fn cmd_id(&self) -> CommandId {
        Self::cmd_id()
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}

/// [`Put`] response payload in its deserialized form.
#[derive(Debug, Serialize, Deserialize)]
pub struct PutResponse {
    pub message: String,
}

impl IntoMessage for Result<PutResponse> {
    fn cmd_id(&self) -> CommandId {
        Put::cmd_id()
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}
