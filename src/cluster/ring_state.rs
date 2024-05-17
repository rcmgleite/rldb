use anyhow::anyhow;
use bytes::Bytes;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

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
    pub addr: Bytes,
    // Node status, see [`NodeStatus`]
    pub status: NodeStatus,
    // On every update, this counter is incremented.
    // This is how a node knows if it's data about a specific node is stale or not
    pub tick: u128,
}

// Note: This interior mutability pattern is useful but the mutex usage in this file is horrible..
#[derive(Clone, Debug)]
pub struct RingState {
    inner: Arc<Mutex<RingStateInner>>,
}

#[derive(Debug)]
struct RingStateInner {
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
            inner: Arc::new(Mutex::new(RingStateInner {
                nodes: Default::default(),
                partitioning_scheme,
                own_addr,
                tick: 0,
            })),
        }
    }

    pub fn knows_node(&self, key: &[u8]) -> bool {
        self.inner.lock().unwrap().nodes.contains_key(key)
    }

    pub fn tick(&self) {
        // TODO: Remove unwrap()
        self.inner.lock().unwrap().tick += 1;
    }
    /// Merges the current ring state view with the one passed as rhs.
    /// This is used as part of the gossip protocol exchange for cluster state synchronization
    pub fn merge_nodes(&self, nodes: Vec<Node>) {
        let mut inner_guard = self.inner.lock().unwrap();
        for node in nodes {
            if let Some(node_current_view) = inner_guard.nodes.get_mut(&node.addr) {
                // current view is stale, let's update it
                if node_current_view.tick < node.tick {
                    match node.status {
                        // Node was actually removed. Let's drop it from our view of the cluster
                        NodeStatus::Offline => {
                            inner_guard.nodes.remove(&node.addr);
                            inner_guard.partitioning_scheme.remove_node(&node.addr);
                        }
                        // Otherwise, let's update tick and status
                        NodeStatus::PossiblyOffline | NodeStatus::Ok => {
                            node_current_view.status = node.status;
                            node_current_view.tick = node.tick;
                        }
                    }
                }
            } else {
                inner_guard
                    .partitioning_scheme
                    .add_node(node.addr.clone())
                    .unwrap();
                inner_guard.nodes.insert(node.addr.clone(), node);
            }
        }
    }

    /// Whenever a PUT request is received, this API is called to understand which node owns the given key.
    /// If the current node is the owner, it accepts the PUT and acts like the coordinator node (eg: triggers vector clock update,
    /// replication etc...). If not, it will forward the request to the actual owner, wait for a response and forward the reply
    /// to the client.
    pub fn key_owner(&self, key: &[u8]) -> anyhow::Result<Node> {
        let inner_guard = self.inner.lock().unwrap();
        let node_key = inner_guard.partitioning_scheme.key_owner(key)?;
        inner_guard
            .nodes
            .get(&node_key)
            .ok_or(anyhow!(
                "Unable to find node inside RingState. This should never happen."
            ))
            .map(|node| node.clone())
    }

    pub fn get_nodes(&self) -> Vec<Node> {
        let guard = self.inner.lock().unwrap();
        guard.nodes.iter().map(|(_, v)| v.clone()).collect()
    }

    pub fn get_random_node(&self) -> Node {
        let guard = self.inner.lock().unwrap();
        let keys: Vec<&Bytes> = guard.nodes.keys().collect();
        let rnd = rand::thread_rng().gen_range(0..keys.len());

        let key = keys[rnd];
        guard.nodes[key].clone()
    }
}

#[cfg(test)]
mod tests {
    // TODO
}
