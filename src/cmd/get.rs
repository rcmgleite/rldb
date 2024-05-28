//! Get [`crate::cmd::Command`]
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::db::{Db, OwnsKeyResponse};
use crate::error::{Error, Result};
use crate::server::message::IntoMessage;
use crate::utils::serde_utf8_bytes;

pub const GET_CMD: u32 = 2;

#[derive(Serialize, Deserialize)]
pub struct Get {
    #[serde(with = "serde_utf8_bytes")]
    key: Bytes,
}

impl Get {
    /// Constructs a new [`Get`] instance
    pub fn new(key: Bytes) -> Self {
        Self { key }
    }

    /// Executes the [`Get`] command using the specified [`Db`] instance
    pub async fn execute(self, db: Arc<Db>) -> Result<GetResponse> {
        if let OwnsKeyResponse::False { addr } = db.owns_key(&self.key)? {
            return Err(Error::InvalidRequest {
                reason: format!(
                    "Key owned by another node. Redirect request to node {:?}",
                    addr
                ),
            });
        }

        let value = db.get(&self.key).await?;
        if let Some(value) = value {
            return Ok(GetResponse { value });
        } else {
            return Err(Error::NotFound { key: self.key });
        }
    }

    /// returns the cmd id for [`Get`]
    pub fn cmd_id() -> u32 {
        GET_CMD
    }
}

impl IntoMessage for Get {
    fn id(&self) -> u32 {
        Self::cmd_id()
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}

/// The struct that represents a [`Get`] response payload
#[derive(Serialize, Deserialize)]
pub struct GetResponse {
    #[serde(with = "serde_utf8_bytes")]
    value: Bytes,
}
