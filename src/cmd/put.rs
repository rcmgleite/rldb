//! Put [`crate::cmd::Command`]
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::db::{Db, OwnsKeyResponse};
use crate::error::{Error, Result};
use crate::server::message::IntoMessage;
use crate::utils::serde_utf8_bytes;

pub const PUT_CMD: u32 = 3;

/// Struct that represents a deserialized Put payload
#[derive(Serialize, Deserialize)]
pub struct Put {
    #[serde(with = "serde_utf8_bytes")]
    key: Bytes,
    #[serde(with = "serde_utf8_bytes")]
    value: Bytes,
}

impl Put {
    /// Constructs a new [`Put`] [`crate::cmd::Command`]
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self { key, value }
    }

    /// Executes a [`Put`] [`crate::cmd::Command`]
    pub async fn execute(self, db: Arc<Db>) -> Result<PutResponse> {
        if let OwnsKeyResponse::False { addr } = db.owns_key(&self.key)? {
            return Err(Error::InvalidRequest {
                reason: format!(
                    "Key owned by another node. Redirect request to node {:?}",
                    addr
                ),
            });
        }

        db.put(self.key.into(), self.value.into()).await?;
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
#[derive(Serialize, Deserialize)]
pub struct PutResponse {
    message: String,
}
