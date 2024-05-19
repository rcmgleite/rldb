use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    cluster::ring_state::Node,
    error::{Error, Result},
    server::{
        message::{IntoMessage, Message},
        PartitioningScheme,
    },
};

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
    pub async fn execute(
        self,
        partitioning_scheme: Arc<PartitioningScheme>,
    ) -> JoinClusterResponse {
        let PartitioningScheme::ConsistentHashing(ring_state) = partitioning_scheme.as_ref();
        let target_node = Node::new(Bytes::from(self.known_cluster_node_addr));

        ring_state.merge_nodes(vec![target_node]);

        JoinClusterResponse::Success {
            message: "Ok".to_string(),
        }
    }

    pub fn try_from_request(request: Message) -> Result<Self> {
        if request.id != CMD_CLUSTER_JOIN_CLUSTER {
            return Err(Error::InvalidRequest {
                reason: format!(
                    "Unable to construct JoinCluster Command from Request. Expected id {} got {}",
                    CMD_CLUSTER_JOIN_CLUSTER, request.id
                ),
            });
        }

        if let Some(payload) = request.payload {
            let s: Self = serde_json::from_slice(&payload).map_err(|e| Error::InvalidRequest {
                reason: format!(
                    "Invalid json payload for JoinCluster request {}",
                    e.to_string()
                ),
            })?;
            Ok(s)
        } else {
            return Err(Error::InvalidRequest {
                reason: "JoinCluster message payload can't be None".to_string(),
            });
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
