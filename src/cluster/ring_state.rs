use anyhow::anyhow;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::consistent_hashing::ConsistentHashing;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
    // Host is reachable
    Ok,
    // Host is unreachable - could be transient
    PossiblyOffline,
    // Host is offline - triggered by manual operator action
    Offline,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Node {
    // the IP/PORT pair formatted as <ip>:<port>
    // it's wrapped around Bytes for now to avoid allocating the String multiple times...
    addr: Bytes,
    // Node status, see [`NodeStatus`]
    status: NodeStatus,
    // On every update, this counter is incremented.
    // This is how a node knows if it's data about a specific node is stale or not
    tick: u128,
}

#[derive(Clone, Debug)]
pub struct RingState {
    // Which nodes are part of the ring
    nodes: HashMap<Bytes, Node>,
    // Partitioning scheme
    // TODO: Find a way to inject this instead of hardcoding it..
    partitioning_scheme: ConsistentHashing,
    #[allow(dead_code)]
    // Node's own address
    own_addr: Bytes,
    // Counter incremented every time a ping request from the cluster protocol is processed.
    tick: u128,
}

impl RingState {
    pub fn new(own_addr: Bytes) -> Self {
        let mut partitioning_scheme = ConsistentHashing::default();
        // the first insertion is always successful. unwrap() is safe
        partitioning_scheme.add_node(own_addr.clone()).unwrap();

        Self {
            nodes: Default::default(),
            partitioning_scheme,
            own_addr,
            tick: 0,
        }
    }

    pub fn tick(&mut self) {
        self.tick += 1;
    }
    /// Merges the current ring state view with the one passed as rhs.
    /// This is used as part of the gossip protocol exchange for cluster state synchronization
    pub fn merge_nodes(&mut self, nodes: Vec<Node>) -> anyhow::Result<()> {
        for node in nodes {
            if let Some(node_current_view) = self.nodes.get_mut(&node.addr) {
                // current view is stale, let's update it
                if node_current_view.tick < node.tick {
                    match node.status {
                        // Node was actually removed. Let's drop it from our view of the cluster
                        NodeStatus::Offline => {
                            self.nodes.remove(&node.addr);
                            self.partitioning_scheme.remove_node(&node.addr);
                        }
                        // Otherwise, let's update tick and status
                        NodeStatus::PossiblyOffline | NodeStatus::Ok => {
                            node_current_view.status = node.status;
                            node_current_view.tick = node.tick;
                        }
                    }
                }
            } else {
                self.partitioning_scheme.add_node(node.addr.clone())?;
                self.nodes.insert(node.addr.clone(), node);
            }
        }

        Ok(())
    }

    /// Whenever a PUT request is received, this API is called to understand which node owns the given key.
    /// If the current node is the owner, it accepts the PUT and acts like the coordinator node (eg: triggers vector clock update,
    /// replication etc...). If not, it will forward the request to the actual owner, wait for a response and forward the reply
    /// to the client.
    pub fn key_owner(&self, key: &[u8]) -> anyhow::Result<&Node> {
        let node_key = self.partitioning_scheme.key_owner(key)?;
        self.nodes.get(&node_key).ok_or(anyhow!(
            "Unable to find node inside RingState. This should never happen."
        ))
    }

    /// Creates a snapshot of the current [`RingState`] for this node.`
    /// This is serialized and sent to peer nodes via gossip.
    pub fn snapshot(&self) -> Self {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    // TODO
}
