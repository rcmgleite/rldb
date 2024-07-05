//! ReplicationGet [`crate::cmd::Command`]
use std::hash::Hash;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::persistency::storage::StorageEntry;
use crate::persistency::Db;
use crate::server::message::IntoMessage;
use crate::utils::serde_utf8_bytes;

pub const REPLICATION_GET_CMD: u32 = 4;

#[derive(Serialize, Deserialize)]
pub struct ReplicationGet {
    #[serde(with = "serde_utf8_bytes")]
    key: Bytes,
    request_id: String,
}

impl ReplicationGet {
    pub fn new(key: Bytes, request_id: String) -> Self {
        Self { key, request_id }
    }

    pub async fn execute(self, db: Arc<Db>) -> Result<ReplicationGetResponse> {
        Ok(ReplicationGetResponse {
            values: db.get(self.key.clone(), true).await?,
        })
    }

    pub fn cmd_id() -> u32 {
        REPLICATION_GET_CMD
    }
}

impl IntoMessage for ReplicationGet {
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

/// The struct that represents a [`ReplicationGet`] response payload
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Debug)]
pub struct ReplicationGetResponse {
    pub values: Vec<StorageEntry>,
}
