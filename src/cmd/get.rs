//! Get [`crate::cmd::Command`]
use std::hash::Hash;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::persistency::Db;
use crate::server::message::IntoMessage;
use crate::utils::serde_utf8_bytes;

pub const GET_CMD: u32 = 2;

#[derive(Serialize, Deserialize)]
pub struct Get {
    #[serde(with = "serde_utf8_bytes")]
    key: Bytes,
    replica: bool,
}

impl Get {
    /// Constructs a new [`Get`] instance
    pub fn new(key: Bytes) -> Self {
        Self {
            key,
            replica: false,
        }
    }

    pub fn new_replica(key: Bytes) -> Self {
        Self { key, replica: true }
    }

    /// Executes the [`Get`] command using the specified [`Db`] instance
    pub async fn execute(self, db: Arc<Db>) -> Result<GetResponse> {
        if let Some(resp) = db.get(self.key.clone(), self.replica).await? {
            Ok(GetResponse { value: resp })
        } else {
            Err(Error::NotFound { key: self.key })
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
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct GetResponse {
    #[serde(with = "serde_utf8_bytes")]
    pub value: Bytes,
}
