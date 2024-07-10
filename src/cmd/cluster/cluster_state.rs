//! [`ClusterState`] [`crate::cmd::Command`]
//!
//! Returns the current cluster state for the requested node
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    cluster::state::Node, cmd::CLUSTER_CLUSTER_STATE_CMD, error::Result, persistency::Db,
    server::message::IntoMessage,
};

/// ClusterState deserialized [`crate::cmd::Command`]
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ClusterState;

impl ClusterState {
    pub fn new() -> Self {
        Self
    }

    /// Executes a [`ClusterState`] command.
    pub async fn execute(self, db: Arc<Db>) -> Result<ClusterStateResponse> {
        let cluster_state = db.cluster_state()?;

        Ok(ClusterStateResponse {
            nodes: cluster_state,
        })
    }

    pub fn cmd_id() -> u32 {
        CLUSTER_CLUSTER_STATE_CMD
    }
}

impl IntoMessage for ClusterState {
    fn id(&self) -> u32 {
        Self::cmd_id()
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}

/// Deserialized [`ClusterState`] response payload.
#[derive(Serialize, Deserialize)]
pub struct ClusterStateResponse {
    pub nodes: Vec<Node>,
}

impl IntoMessage for Result<ClusterStateResponse> {
    fn id(&self) -> u32 {
        ClusterState::cmd_id()
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}
