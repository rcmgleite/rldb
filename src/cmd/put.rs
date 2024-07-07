//! Put [`crate::cmd::Command`]
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::persistency::storage::Value;
use crate::persistency::Db;
use crate::server::message::IntoMessage;
use crate::utils::serde_utf8_bytes;

use super::types::SerializedContext;

pub const PUT_CMD: u32 = 3;

/// Struct that represents a deserialized Put payload
#[derive(Serialize, Deserialize)]
pub struct Put {
    #[serde(with = "serde_utf8_bytes")]
    key: Bytes,
    value: Value,
    replication: bool,
    context: Option<String>,
    request_id: String,
}

impl Put {
    /// Constructs a new [`Put`] [`crate::cmd::Command`]
    pub fn new(key: Bytes, value: Value, context: Option<String>, request_id: String) -> Self {
        Self::new_private(key, value, context, false, request_id)
    }

    /// constructs a new [`Put`] [`crate::cmd::Command`] for replication
    pub fn new_replication(
        key: Bytes,
        value: Value,
        context: Option<String>,
        request_id: String,
    ) -> Self {
        Self::new_private(key, value, context, true, request_id)
    }

    fn new_private(
        key: Bytes,
        value: Value,
        context: Option<String>,
        replication: bool,
        request_id: String,
    ) -> Self {
        Self {
            key,
            value,
            replication,
            context,
            request_id,
        }
    }

    /// Executes a [`Put`] [`crate::cmd::Command`]
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

    pub fn cmd_id() -> u32 {
        PUT_CMD
    }
}

impl IntoMessage for Put {
    fn id(&self) -> u32 {
        Self::cmd_id()
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }

    fn request_id(&self) -> String {
        self.request_id.clone()
    }
}

/// [`Put`] response payload in its deserialized form.
#[derive(Debug, Serialize, Deserialize)]
pub struct PutResponse {
    pub message: String,
}
