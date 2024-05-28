//! Heartbeat [`crate::cmd::Command`]
//!
//! This command is issued as part of the Gossip protocol that propagates
//! cluster states to all cluster nodes.
//! Every heartbeat request marshalls the node's own view of the cluster and sends it to X other nodes.
//! The receiving end of the command will merge its view of the cluster with received view and consolidate it
//! by checking each individual [`crate::cluster::state::Node::tick`] field and always favoring the highest one.
//! See [`crate::cluster::state`] docs for more information.
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{cluster::state::Node, db::Db, error::Result, server::message::IntoMessage};

pub const CMD_CLUSTER_HEARTBEAT: u32 = 100;

#[derive(Serialize, Deserialize)]
pub struct Heartbeat {
    nodes: Vec<Node>,
}

impl Heartbeat {
    /// Constructs a new heartbeat [`crate::cmd::Command`]
    pub fn new(nodes: Vec<Node>) -> Self {
        Self { nodes }
    }

    /// Executes a [`Heartbeat`] command

    /// Heartbeat flow
    /// 1. receive a heartbeat (possibly from a node that it doesn't know yet)
    /// 2. update it's view of the ring state including the possibly new node
    /// 3. responde to the heartbeat with an ACK response
    pub async fn execute(self, db: Arc<Db>) -> Result<HeartbeatResponse> {
        db.update_cluster_state(self.nodes)?;

        Ok(HeartbeatResponse {
            message: "Ok".to_string(),
        })
    }

    pub fn cmd_id() -> u32 {
        CMD_CLUSTER_HEARTBEAT
    }
}

impl IntoMessage for Heartbeat {
    fn id(&self) -> u32 {
        Self::cmd_id()
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}

/// [Heartbeat] deserialized response payload
#[derive(Serialize, Deserialize)]
pub struct HeartbeatResponse {
    message: String,
}
