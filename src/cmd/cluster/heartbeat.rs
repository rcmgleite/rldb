use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    cluster::{heartbeat::JsonSerializableNode, ring_state::Node},
    error::{Error, Result},
    server::{
        message::{IntoMessage, Message},
        Db, PartitioningScheme,
    },
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
    pub async fn execute(self, db: Arc<Db>) -> HeartbeatResponse {
        if let Some(partitioning_scheme) = &db.partitioning_scheme {
            let PartitioningScheme::ConsistentHashing(ring_state) = partitioning_scheme.as_ref();

            let nodes: Vec<Node> = self.nodes.iter().map(|e| Node::from(e.clone())).collect();
            ring_state.merge_nodes(nodes);

            HeartbeatResponse::Success {
                message: "ACK".to_string(),
            }
        } else {
            todo!()
        }
    }

    pub fn try_from_request(request: Message) -> Result<Self> {
        if request.id != CMD_CLUSTER_HEARTBEAT {
            return Err(Error::InvalidRequest {
                reason: format!(
                    "Unable to construct Heartbeat Command from Request. Expected id {} got {}",
                    CMD_CLUSTER_HEARTBEAT, request.id
                ),
            });
        }

        if let Some(payload) = request.payload {
            let s: Self = serde_json::from_slice(&payload).map_err(|e| Error::InvalidRequest {
                reason: format!(
                    "Invalid json payload for Heartbeat request {}",
                    e.to_string()
                ),
            })?;
            Ok(s)
        } else {
            return Err(Error::InvalidRequest {
                reason: "Heartbeat message payload can't be None".to_string(),
            });
        }
    }
}

impl IntoMessage for Heartbeat {
    fn id(&self) -> u32 {
        CMD_CLUSTER_HEARTBEAT
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}

#[derive(Serialize, Deserialize)]
pub enum HeartbeatResponse {
    Success { message: String },
    Failure { message: String },
}
