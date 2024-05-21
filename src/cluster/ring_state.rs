use bytes::Bytes;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use super::{
    consistent_hashing::ConsistentHashing,
    error::{Error, Result},
};

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

impl Node {
    pub fn new(addr: Bytes) -> Self {
        Self {
            addr,
            status: NodeStatus::PossiblyOffline,
            tick: 0,
        }
    }
}

// Note: This interior mutability pattern is useful but the mutex usage in this file is horrible..
#[derive(Clone, Debug)]
pub struct RingState {
    own_addr: Bytes,
    inner: Arc<Mutex<RingStateInner>>,
}

#[derive(Debug)]
struct RingStateInner {
    // Which nodes are part of the ring
    nodes: HashMap<Bytes, Node>,
    // Partitioning scheme
    // TODO: Find a way to inject this instead of hardcoding it..
    partitioning_scheme: ConsistentHashing,
}

impl RingState {
    pub fn new(own_addr: Bytes) -> Result<Self> {
        let mut partitioning_scheme = ConsistentHashing::default();
        partitioning_scheme.add_node(own_addr.clone())?;
        let mut nodes = HashMap::new();
        nodes.insert(
            own_addr.clone(),
            Node {
                addr: own_addr.clone(),
                status: NodeStatus::Ok,
                tick: 0,
            },
        );

        Ok(Self {
            own_addr,
            inner: Arc::new(Mutex::new(RingStateInner {
                nodes,
                partitioning_scheme,
            })),
        })
    }

    fn acquire_lock(&self) -> Result<MutexGuard<RingStateInner>> {
        if let Ok(guard) = self.inner.lock() {
            Ok(guard)
        } else {
            Err(Error::Logic {
                reason: "Unable to acquire lock".to_string(),
            })
        }
    }

    pub fn knows_node(&self, key: &[u8]) -> Result<bool> {
        let guard = self.acquire_lock()?;
        Ok(guard.nodes.contains_key(key))
    }

    pub fn tick(&self) -> Result<()> {
        let own_addr = self.own_addr.clone();
        let mut guard = self.acquire_lock()?;
        let own_node = guard.nodes.get_mut(&own_addr).unwrap();
        own_node.tick += 1;

        Ok(())
    }
    /// Merges the current ring state view with the one passed as rhs.
    /// This is used as part of the gossip protocol exchange for cluster state synchronization
    pub fn merge_nodes(&self, nodes: Vec<Node>) -> Result<()> {
        let own_addr = self.own_addr.clone();
        let mut inner_guard = self.acquire_lock()?;
        for node in nodes {
            if let Some(node_current_view) = inner_guard.nodes.get_mut(&node.addr) {
                // Edge case: If a node was offline and came back, it's tick will be set to 0
                // while other nodes in the cluster will have the node register at a much greater tick.
                // so here we check for own node and increase our own tick to make sure our version is the most up
                // to date
                if node.addr == own_addr {
                    if node.tick > node_current_view.tick {
                        node_current_view.tick = node.tick + 1000;
                        continue;
                    }
                }

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
                    .add_node(node.addr.clone())?;
                inner_guard.nodes.insert(node.addr.clone(), node);
            }
        }

        Ok(())
    }

    pub fn mark_node_as_possibly_offline(&self, node: Node) -> Result<()> {
        let mut inner_guard = self.acquire_lock()?;
        inner_guard.nodes.entry(node.addr).and_modify(|entry| {
            entry.status = NodeStatus::PossiblyOffline;
            entry.tick += 1;
        });

        Ok(())
    }

    /// Whenever a PUT request is received, this API is called to understand which node owns the given key.
    /// If the current node is the owner, it accepts the PUT and acts like the coordinator node (eg: triggers vector clock update,
    /// replication etc...). If not, it will forward the request to the actual owner, wait for a response and forward the reply
    /// to the client.
    pub fn key_owner(&self, key: &[u8]) -> Result<Node> {
        let inner_guard = self.acquire_lock()?;
        let node_key = inner_guard.partitioning_scheme.key_owner(key)?;
        inner_guard
            .nodes
            .get(&node_key)
            .ok_or(Error::Logic {
                reason: "Unable to find node inside RingState. This should never happen."
                    .to_string(),
            })
            .map(|node| node.clone())
    }

    pub fn get_nodes(&self) -> Result<Vec<Node>> {
        let guard = self.acquire_lock()?;
        Ok(guard.nodes.iter().map(|(_, v)| v.clone()).collect())
    }

    pub fn get_random_node(&self) -> Result<Node> {
        let guard = self.acquire_lock()?;
        let keys: Vec<&Bytes> = guard.nodes.keys().collect();
        let rnd = if keys.len() == 1 {
            0
        } else {
            rand::thread_rng().gen_range(0..keys.len())
        };

        let key = keys[rnd];
        Ok(guard.nodes[key].clone())
    }

    pub fn own_addr(&self) -> Bytes {
        self.own_addr.clone()
    }

    pub fn owns_key(&self, key: &[u8]) -> Result<bool> {
        Ok(self.own_addr == self.key_owner(key)?.addr)
    }
}

#[cfg(test)]
mod tests {
    // TODO
}
