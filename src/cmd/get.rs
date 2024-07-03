//! Get [`crate::cmd::Command`]
use std::hash::Hash;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::persistency::{Db, Metadata};
use crate::server::message::IntoMessage;
use crate::utils::serde_utf8_bytes;

pub const GET_CMD: u32 = 2;

#[derive(Serialize, Deserialize)]
pub struct Get {
    #[serde(with = "serde_utf8_bytes")]
    key: Bytes,
    replica: bool,
    request_id: String,
}

impl Get {
    /// Constructs a new [`Get`] instance
    pub fn new(key: Bytes, request_id: String) -> Self {
        Self {
            key,
            replica: false,
            request_id,
        }
    }

    pub fn new_replica(key: Bytes, request_id: String) -> Self {
        Self {
            key,
            replica: true,
            request_id,
        }
    }

    /// Executes the [`Get`] command using the specified [`Db`] instance
    pub async fn execute(self, db: Arc<Db>) -> Result<Vec<GetResponse>> {
        if let Some(resp) = db.get(self.key.clone(), self.replica).await? {
            Ok(resp
                .into_iter()
                .map(|entry| GetResponse {
                    value: entry.value,
                    metadata: hex::encode(entry.metadata.serialize()),
                })
                .collect())
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

    fn request_id(&self) -> String {
        self.request_id.clone()
    }
}

/// The struct that represents a [`Get`] response payload
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GetResponse {
    #[serde(with = "serde_utf8_bytes")]
    pub value: Bytes,
    /// A hex encoded representation of the object metadata
    pub metadata: String,
}

impl std::fmt::Debug for GetResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let meta =
            Metadata::deserialize(0, hex::decode(self.metadata.clone()).unwrap().into()).unwrap();
        f.debug_struct("GetResponse")
            .field("value", &self.value)
            .field("metadata", &meta)
            .finish()
    }
}
