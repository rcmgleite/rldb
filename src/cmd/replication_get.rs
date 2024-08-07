//! ReplicationGet [`crate::cmd::Command`]
//!
//! This command is to be used internally by the Coordinator node during a GET.
//! The difference between [`crate::cmd::get::Get`] and [`crate::cmd::replication_get::ReplicationGet`]
//! is 2 fold:
//!  1. A replication GET instructs the [`crate::persistency::storage`] layer to simply return whatever value it has locally
//!   (as opposed to having to perform a quorum read on multiple nodes)
//!  2. The response of a ReplicationGet includes 1..n [`crate::persistency::versioning::version_vector::VersionVector`]
//!  instead of a [`crate::cmd::types::Context`] object. This is because the coordinator node needs to know
//!  all existing versions of a key to be able to handle the request, as opposed to a client that only needs
//!  the merged version vector instance to be able to resolve conflicts if they exist.
//!
//! # TODOs
//!  1. It might be important in the future to make this method protected somehow - ie: make sure regular clients can't call it.
use std::hash::Hash;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::error::Result;
use crate::persistency::storage::StorageEntry;
use crate::persistency::Db;
use crate::server::message::IntoMessage;
use crate::utils::serde_utf8_bytes;

use super::CommandId;

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicationGet {
    #[serde(with = "serde_utf8_bytes")]
    key: Bytes,
}

impl ReplicationGet {
    pub fn new(key: Bytes) -> Self {
        Self { key }
    }

    #[instrument(name = "cmd::replication_get", level = "info")]
    pub async fn execute(self, db: Arc<Db>) -> Result<ReplicationGetResponse> {
        Ok(ReplicationGetResponse {
            values: db.get(self.key.clone(), true).await?,
        })
    }

    pub fn cmd_id() -> CommandId {
        CommandId::ReplicationGet
    }
}

impl IntoMessage for ReplicationGet {
    fn cmd_id(&self) -> CommandId {
        Self::cmd_id()
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}

/// The struct that represents a [`ReplicationGet`] response payload
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Debug)]
pub struct ReplicationGetResponse {
    pub values: Vec<StorageEntry>,
}

impl IntoMessage for Result<ReplicationGetResponse> {
    fn cmd_id(&self) -> CommandId {
        ReplicationGet::cmd_id()
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}
