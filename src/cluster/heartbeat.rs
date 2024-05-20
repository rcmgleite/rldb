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
use std::{collections::HashMap, sync::Arc};

use crate::{client, db::PartitioningScheme};

use super::ring_state::{Node, NodeStatus};
use serde::{Deserialize, Serialize};
use tracing::{event, Level};

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

/// Start heartbeat will
/// 1. create a tcp connection with the required target_addr (If one doesn't exist yet)
/// 2. send a heartbeat message to the target node (which includes the current node view of the ring)
/// 3. Receive a heartbeat response (ACK OR FAILURE)
/// 4. loop forever picking a random node of the ring every X seconds and performing steps 2 through 3 again
pub async fn start_heartbeat(partitioning_scheme: Arc<PartitioningScheme>) {
    let PartitioningScheme::ConsistentHashing(ring_state) = partitioning_scheme.as_ref();
    let mut cluster_connections = HashMap::new();

    // Now we loop every X seconds to hearbeat to one node in the cluster
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        event!(Level::DEBUG, "heartbeat loop starting: {:?}", ring_state);

        // tick (ie: update your own state)
        ring_state.tick();

        let target_node = ring_state.get_random_node();

        // It doesn't make sense to heartbeat to self... let's pick some other node...
        if target_node.addr == ring_state.own_addr() {
            event!(Level::DEBUG, "skipping heatbeat to self");
            continue;
        }
        // let's re-use an exisiting connection to the picked random node if one exists.. otherwise create a new one
        let conn = if let Some(conn) = cluster_connections.get_mut(&target_node.addr) {
            conn
        } else {
            // unwrap is safe because we construct the addrs and we know they are valid utf8 strings...
            let client = match client::DbClient::connect(
                String::from_utf8(target_node.addr.clone().into()).unwrap(),
            )
            .await
            {
                Ok(conn) => conn,
                Err(err) => {
                    event!(
                        Level::WARN,
                        "Unable to connect to node {:?} - err {}",
                        target_node,
                        err
                    );

                    ring_state.mark_node_as_possibly_offline(target_node);

                    continue;
                }
            };
            cluster_connections.insert(target_node.addr.clone(), client);

            // unwrap is unsafe because we just constructed this hashmap
            cluster_connections.get_mut(&target_node.addr).unwrap()
        };

        if let Err(err) = conn.heartbeat(ring_state.get_nodes()).await {
            event!(
                Level::WARN,
                "Unable to connect to node {:?} - err {:?}",
                target_node,
                err
            );
            cluster_connections.remove(&target_node.addr);
            ring_state.mark_node_as_possibly_offline(target_node);
        } else {
            event!(Level::DEBUG, "heartbeat cycle finished {:?}", ring_state);
        }
    }
}
