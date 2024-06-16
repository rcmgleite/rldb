//! Put [`crate::cmd::Command`]
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::persistency::Db;
use crate::server::message::IntoMessage;
use crate::utils::{serde_hex_bytes, serde_utf8_bytes};

pub const PUT_CMD: u32 = 3;

/// Struct that represents a deserialized Put payload
#[derive(Serialize, Deserialize)]
pub struct Put {
    #[serde(with = "serde_utf8_bytes")]
    key: Bytes,
    #[serde(with = "serde_utf8_bytes")]
    value: Bytes,
    replication: bool,
    #[serde(with = "serde_hex_bytes")]
    metadata: Option<Bytes>,
}

impl Put {
    /// Constructs a new [`Put`] [`crate::cmd::Command`]
    pub fn new(key: Bytes, value: Bytes, metadata: Option<String>) -> Self {
        Self::new_private(key, value, metadata, false)
    }

    /// constructs a new [`Put`] [`crate::cmd::Command`] for replication
    pub fn new_replication(key: Bytes, value: Bytes, metadata: Option<String>) -> Self {
        Self::new_private(key, value, metadata, true)
    }

    fn new_private(key: Bytes, value: Bytes, metadata: Option<String>, replication: bool) -> Self {
        let metadata: Option<Bytes> = metadata.map(|m| hex::decode(m).unwrap().into());

        Self {
            key,
            value,
            replication,
            metadata,
        }
    }

    /// Executes a [`Put`] [`crate::cmd::Command`]
    pub async fn execute(self, db: Arc<Db>) -> Result<PutResponse> {
        db.put(self.key, self.value, self.replication, self.metadata)
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
}

/// [`Put`] response payload in its deserialized form.
#[derive(Debug, Serialize, Deserialize)]
pub struct PutResponse {
    pub message: String,
}
