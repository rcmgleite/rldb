use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{cluster::ring_state::Node, db::Db, server::message::IntoMessage};

pub const CMD_CLUSTER_JOIN_CLUSTER: u32 = 101;

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

    // This cmd simply adds the provided target node to the ring_state.
    // the background heartbeat process will take care of receiving ring state info
    // from this node (eventually)
    pub async fn execute(self, db: Arc<Db>) -> JoinClusterResponse {
        let target_node = Node::new(Bytes::from(self.known_cluster_node_addr));

        db.update_ring_state(vec![target_node]);

        JoinClusterResponse::Success {
            message: "Ok".to_string(),
        }
    }
}

impl IntoMessage for JoinCluster {
    fn id(&self) -> u32 {
        CMD_CLUSTER_JOIN_CLUSTER
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}

#[derive(Serialize, Deserialize)]
pub enum JoinClusterResponse {
    Success { message: String },
    Failure { message: String },
}
