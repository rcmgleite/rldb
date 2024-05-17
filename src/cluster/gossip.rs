//! This file contains the logic related to the gossip protocol
//! used between rldb-server nodes in cluster mode. This is a TCP based protocol
//! that will be used to:
//!   1. Node discovery
//!     Every node that joins a running rldb cluster will learn about the other nodes via this protocol
//!     Nodes exchange they IP/PORT part, which is currently used as key to determine which part of the
//!     partition ring every node holds.
//!     With this information, every node can determine the correct owner of any given key.
//!     This can also be used to determine which nodes in the ring will contain replicas of the key being added.
//!     For now, we replica the data to the 2 next nodes on the ring.
//!   2. Evaluate nodes health
//!     This happens by each node randomly choosing another 2 nodes to ping every 1 second (configurable)
//!     Hosts that are unreachable or respond with anything other than success are marked as
//!     [`NodeStatus::PossiblyOffline`]
use super::ring_state::{Node, NodeStatus};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct RingStateMessagePayload {
    nodes: Vec<JsonSerializableNode>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct JsonSerializableNode {
    pub addr: String,
    status: NodeStatus,
    tick: u128,
}

impl From<Node> for JsonSerializableNode {
    fn from(node: Node) -> Self {
        Self {
            addr: String::from_utf8(node.addr.into()).unwrap(),
            status: node.status,
            tick: node.tick,
        }
    }
}

impl From<JsonSerializableNode> for Node {
    fn from(node: JsonSerializableNode) -> Self {
        Self {
            addr: node.addr.into(),
            status: node.status,
            tick: node.tick,
        }
    }
}
