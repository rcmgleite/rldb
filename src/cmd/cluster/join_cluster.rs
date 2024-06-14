//! [`JoinCluster`] [`crate::cmd::Command`]
//!
//! Every newly bootstrapped node that needs to join a cluster must receive this [`crate::server::message::Message`].
//! it will receive one existing node cluster so that it can establish a TCP connection to it and start receiving
//! cluster information from it.
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{cluster::state::Node, error::Result, persistency::Db, server::message::IntoMessage};

pub const CMD_CLUSTER_JOIN_CLUSTER: u32 = 101;

/// JoinCluster deserialized [`crate::cmd::Command`]
#[derive(Serialize, Deserialize)]
pub struct JoinCluster {
    known_cluster_node_addr: String,
}

impl JoinCluster {
    pub fn new(known_cluster_node_addr: String) -> Self {
        Self {
            known_cluster_node_addr,
        }
    }

    /// Executes a [`JoinCluster`] command.
    ///
    /// This command simply adds the provided target node to the cluster state.
    /// the background heartbeat process will take care of receiving ring state info
    /// from this node (eventually). See [`crate::cluster::heartbeat`] docs for more information.
    pub async fn execute(self, db: Arc<Db>) -> Result<JoinClusterResponse> {
        let target_node = Node::new(Bytes::from(self.known_cluster_node_addr));

        db.update_cluster_state(vec![target_node])?;

        Ok(JoinClusterResponse {
            message: "Ok".to_string(),
        })
    }

    pub fn cmd_id() -> u32 {
        CMD_CLUSTER_JOIN_CLUSTER
    }
}

impl IntoMessage for JoinCluster {
    fn id(&self) -> u32 {
        Self::cmd_id()
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}

/// Deserialized [`JoinCluster`] response payload.
#[derive(Serialize, Deserialize)]
pub struct JoinClusterResponse {
    message: String,
}
