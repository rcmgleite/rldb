//! This file contains the [`State`] data structure.
//! This structure is responsible for holding the cluster state at any given time.
//! It owns the [`PartitioningScheme`] provided during construction and delegates
//! queries like: Which node owns a given key to it.
//!
//! This structure is also responsible for storing the state for each node in the cluster.
//! By querying this structure, a caller is able to know if a node is healthy or not and, therefore,
//! decide to skip faulty hosts entirely.
//!
//! Currently, the state is updated via a gossip protocol in which nodes exchange their view of the cluster
//! to every other host. For more info, see the docs for [`super::heartbeat`].
use bytes::Bytes;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use crate::utils::serde_utf8_bytes;

use super::{
    error::{Error, Result},
    partitioning::PartitioningScheme,
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

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Node {
    // the IP/PORT pair formatted as <ip>:<port>
    // it's wrapped around Bytes for now to avoid allocating the String multiple times...
    #[serde(with = "serde_utf8_bytes")]
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
#[derive(Clone)]
pub struct State {
    own_addr: Bytes,
    inner: Arc<Mutex<StateInner>>,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.inner.try_lock() {
            Ok(inner) => {
                write!(f, "State: {:?}", inner)
            }
            Err(_) => {
                write!(f, "Unable to acquire lock for logging at this time...")
            }
        }
    }
}

struct StateInner {
    // Which nodes are part of the ring
    nodes: HashMap<Bytes, Node>,
    // Partitioning scheme
    partitioning_scheme: Box<dyn PartitioningScheme + Send>,
}

impl std::fmt::Debug for StateInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (_, v) in self.nodes.iter() {
            write!(f, "\n{:?}", v)?;
        }

        Ok(())
    }
}

impl State {
    pub fn new(
        mut partitioning_scheme: Box<dyn PartitioningScheme + Send>,
        own_addr: Bytes,
    ) -> Result<Self> {
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
            inner: Arc::new(Mutex::new(StateInner {
                nodes,
                partitioning_scheme,
            })),
        })
    }

    fn acquire_lock(&self) -> Result<MutexGuard<StateInner>> {
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
                            inner_guard.partitioning_scheme.remove_node(&node.addr)?;
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

    /// Returns a random not that is not self
    pub fn get_random_node(&self) -> Result<Node> {
        let guard = self.acquire_lock()?;
        let keys: Vec<&Bytes> = guard.nodes.keys().collect();
        let rnd = if keys.len() == 1 {
            return Err(Error::ClusterHasOnlySelf);
        } else {
            rand::thread_rng().gen_range(0..keys.len())
        };

        let key = keys[rnd];
        if *key == self.own_addr {
            // repick if you got self
            drop(guard);
            return self.get_random_node();
        }

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
