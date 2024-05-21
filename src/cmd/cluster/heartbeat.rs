use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    cluster::{heartbeat::JsonSerializableNode, state::Node},
    db::Db,
    error::Result,
    server::message::IntoMessage,
};

pub const CMD_CLUSTER_HEARTBEAT: u32 = 100;

#[derive(Serialize, Deserialize)]
pub struct Heartbeat {
    nodes: Vec<JsonSerializableNode>,
}

impl Heartbeat {
    pub fn new(nodes: Vec<JsonSerializableNode>) -> Self {
        Self { nodes }
    }

    // Heartbeat flow
    // 1. receive a heartbeat (possibly from a node that it doesn't know yet)
    // 2. update it's view of the ring state including the possibly new node
    // 3. responde to the heartbeat with an ACK response
    //
    // FIXME: The data types here are bad.. a lot of memcpys happening here for no good reason.
    // main problem is the json format not being able to serialize bytes::Bytes
    pub async fn execute(self, db: Arc<Db>) -> Result<HeartbeatResponse> {
        let nodes: Vec<Node> = self.nodes.iter().map(|e| Node::from(e.clone())).collect();
        db.update_cluster_state(nodes)?;

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

#[derive(Serialize, Deserialize)]
pub struct HeartbeatResponse {
    message: String,
}
