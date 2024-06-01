//! [`ClusterState`] [`crate::cmd::Command`]
//!
//! Returns the current cluster state for the requested node
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{cluster::state::Node, db::Db, error::Result, server::message::IntoMessage};

pub const CMD_CLUSTER_CLUSTER_STATE: u32 = 102;

/// ClusterState deserialized [`crate::cmd::Command`]
#[derive(Default, Serialize, Deserialize)]
pub struct ClusterState;

impl ClusterState {
    /// Executes a [`ClusterState`] command.
    pub async fn execute(self, db: Arc<Db>) -> Result<ClusterStateResponse> {
        let cluster_state = db.cluster_state()?;

        Ok(ClusterStateResponse {
            nodes: cluster_state,
        })
    }

    pub fn cmd_id() -> u32 {
        CMD_CLUSTER_CLUSTER_STATE
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
