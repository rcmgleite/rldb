//! This file contains the logic related to the gossip protocol used between rldb-server nodes in cluster mode.
//!
//! This is a TCP based protocol
//! that will be used to:
//!   1. Node discovery
//!     Every node that joins a running rldb cluster will learn about the other nodes via this protocol
//!     Nodes exchange they IP/PORT part, which is currently used as key to determine which which partition it owns.
//!     With this information, every node can determine the correct owner of any given key.
//!     This can also be used to determine which nodes in the ring will contain replicas of the key being added.
//!   2. Evaluate nodes health
//!     This happens by each node randomly choosing X nodes to ping every Y seconds (configurable).
//!     Hosts that are unreachable or respond with anything other than success are marked as
//!     [`crate::cluster::state::NodeStatus::PossiblyOffline`]
//!     Note: Node are never automatically removed from the cluster. This requires an operator to manually intervene.
//!     The reason for that is that reshuffling partitions can be quite expensive (even when using schemes like consistent hashing).
//!     So we deliberately chose to let an operator make that decision instead of having automatic detection.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    client::{db_client::DbClientFactory, Client, Factory as ClientFactory},
    error::{Error, Result},
    server::config::Heartbeat as HeartbeatConfig,
};

use super::state::{Node, State};
use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::{event, span, Instrument, Level};

/// Struct that represents the deserialized payload used on [`crate::cmd::Command::Heartbeat`] command
#[derive(Serialize, Deserialize)]
pub struct RingStateMessagePayload {
    /// Nodes that are part of the cluster state
    nodes: Vec<Node>,
}

/// function responsible for the heartbeat process that enables rldb cluster nodes to propagete cluster state changes / failures.
///
/// This operations does the following:
/// 1. create a tcp connection with the required target_addr (If one doesn't exist yet)
/// 2. send a heartbeat message to the target node (which includes the current node view of the ring)
/// 3. Receive a heartbeat response (ACK OR FAILURE)
/// 4. loop forever picking a random node of the ring every X seconds and performing steps 2 through 3 again
pub async fn start_heartbeat(cluster_state: Arc<State>, config: HeartbeatConfig) {
    let cluster_connections = Arc::new(Mutex::new(HashMap::new()));

    // Now we loop every X seconds to hearbeat to one node in the cluster
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(config.interval as u64)).await;

        let span = span!(Level::DEBUG, "heartbeat_loop", addr=?cluster_state.own_addr());
        do_heartbeat(
            config.fanout,
            cluster_state.clone(),
            Arc::new(DbClientFactory),
            cluster_connections.clone(),
        )
        .instrument(span)
        .await;

        event!(Level::DEBUG, "heartbeat cycle finished {:?}", cluster_state);
    }
}

#[derive(Debug, PartialEq)]
enum HeartbeatResult {
    Success,
}

type ClusterConnectionsMap = Arc<Mutex<HashMap<Bytes, Box<dyn Client + Send>>>>;

async fn do_heartbeat_to_node(
    target_node: Node,
    cluster_state: Arc<State>,
    client_factory: Arc<dyn ClientFactory + Send + Sync + 'static>,
    cluster_connections: ClusterConnectionsMap,
) -> Result<HeartbeatResult> {
    event!(Level::DEBUG, "heartbeating to node: {:?}", target_node);

    let client = cluster_connections
        .lock()
        .unwrap()
        .remove(&target_node.addr);

    let mut client = if let Some(client) = client {
        client
    } else {
        let addr: String = String::from_utf8_lossy(&target_node.addr).into();

        match client_factory.get(addr).await {
            Ok(conn) => conn,
            Err(err) => {
                if let Err(err) = cluster_state.mark_node_as_possibly_offline(target_node.clone()) {
                    event!(
                        Level::WARN,
                        "Unable to mark node {:?} as offline: {}",
                        target_node.addr,
                        err
                    );
                }

                return Err(err.into());
            }
        }
    };

    let known_nodes = cluster_state.get_nodes()?;

    if let Err(err) = client.heartbeat(known_nodes).await {
        event!(
            Level::WARN,
            "Unable to heartbeat to node {:?} - err {:?}",
            target_node,
            err
        );

        if let Err(err) = cluster_state.mark_node_as_possibly_offline(target_node) {
            // TODO: we are swallowing this error and only logging it.
            // this is a bit odd because we should never actually fail to mark a node as possibly offline (unless there's a logic issue in the code)
            // Makes me wonder if the API for cluster state should be changed...
            event!(Level::WARN, "Unable to mark node as offline: {}", err);
        }

        Err(err.into())
    } else {
        event!(
            Level::DEBUG,
            "Successfully heartbeated to node {:?}",
            target_node,
        );
        let mut guard = cluster_connections.lock().unwrap();
        guard.insert(target_node.addr.clone(), client);
        Ok(HeartbeatResult::Success)
    }
}

async fn do_heartbeat(
    fanout: usize,
    cluster_state: Arc<State>,
    client_factory: Arc<dyn ClientFactory + Send + Sync + 'static>,
    cluster_connections: ClusterConnectionsMap,
) -> Vec<Result<HeartbeatResult>> {
    event!(Level::DEBUG, "heartbeat loop starting: {:?}", cluster_state);

    // tick (ie: update your own state)
    if let Err(err) = cluster_state.tick() {
        event!(
            Level::ERROR,
            "Unable to call cluster_state.tick() - {}",
            err
        );

        return vec![Err(err)];
    }

    let mut target_nodes = Vec::new();
    for _ in 0..fanout {
        match cluster_state.get_random_node() {
            Ok(target_node) => target_nodes.push(target_node),
            Err(err) => {
                if let Error::SingleNodeCluster = err {
                    event!(Level::DEBUG, "skipping heatbeat to self");
                }

                event!(Level::ERROR, "Unable to get node to heartbeat to: {}", err);
            }
        }
    }

    let mut heartbeats = FuturesUnordered::new();
    for target_node in target_nodes {
        heartbeats.push(do_heartbeat_to_node(
            target_node,
            cluster_state.clone(),
            client_factory.clone(),
            cluster_connections.clone(),
        ));
    }

    let mut result = Vec::new();
    while let Some(res) = heartbeats.next().await {
        result.push(res)
    }

    result
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use crate::{
        client::{error::Error, mock::MockClientFactoryBuilder},
        cluster::{
            heartbeat::{do_heartbeat, HeartbeatResult},
            state::{Node, NodeStatus, State},
        },
        persistency::partitioning::mock::MockPartitioningScheme,
        server::config::Quorum,
        test_utils::fault::When,
    };
    use bytes::Bytes;

    /// Tests that the heartbeat success flow works as expected.
    /// The invariants are:
    ///  1. No errors are returned
    ///  2. at the end of the execution, there's a single cached connection (in cluster_connections)
    ///  3. at the end, the cluster state contains 2 nodes(self and remote) both with NodeStatus::Ok and tick == 1
    #[tokio::test]
    async fn success() {
        let fanout = 1;
        let own_addr = Bytes::from("fake addr");
        let state = Arc::new(
            State::new(
                Box::new(MockPartitioningScheme::default()),
                own_addr.clone(),
                Quorum::default(),
            )
            .unwrap(),
        );

        let remote_node_addr = Bytes::from("A");
        state
            .merge_nodes(vec![Node {
                addr: remote_node_addr.clone(),
                status: NodeStatus::Ok,
                tick: 1,
            }])
            .unwrap();

        let cluster_connections = Arc::new(Mutex::new(HashMap::new()));

        let mut res = do_heartbeat(
            fanout,
            state.clone(),
            Arc::new(
                MockClientFactoryBuilder::new()
                    .with_connection_fault(When::Never)
                    .with_heartbeat_fault(When::Never)
                    .build(),
            ),
            cluster_connections.clone(),
        )
        .await;

        assert_eq!(res.len(), 1);
        assert_eq!(res.remove(0).unwrap(), HeartbeatResult::Success);
        assert_eq!(cluster_connections.lock().unwrap().len(), 1);

        let nodes = state.get_nodes().unwrap();
        assert_eq!(nodes.len(), 2);
        for node in nodes {
            if node.addr == own_addr {
                assert_eq!(node.status, NodeStatus::Ok);
                assert_eq!(node.tick, 1);
            } else if node.addr == remote_node_addr {
                assert_eq!(node.status, NodeStatus::Ok);
                assert_eq!(node.tick, 1);
            } else {
                panic!("This else clause should never be reached. Either the node is itself or the other node included in the state");
            }
        }
    }

    /// Tests that heartbeats to self are skipped.
    /// The invariants are:
    ///  1. The response of the heartbeat call is Skipped
    ///  2. no connections are cached (as none are created)
    ///  3. the state has a single node (self)
    ///  4. this single node has a tick of 1 and is marked as Ok
    #[tokio::test]
    async fn skip_heartbeat_to_self() {
        let own_addr = Bytes::from("fake addr");
        let fanout = 1;
        let state = Arc::new(
            State::new(
                Box::new(MockPartitioningScheme::default()),
                own_addr.clone(),
                Quorum::default(),
            )
            .unwrap(),
        );
        let cluster_connections = Arc::new(Mutex::new(HashMap::new()));

        let result = do_heartbeat(
            fanout,
            state.clone(),
            Arc::new(MockClientFactoryBuilder::new().without_faults().build()),
            cluster_connections.clone(),
        )
        .await;
        assert_eq!(result.len(), 0);

        assert_eq!(cluster_connections.lock().unwrap().len(), 0);
        let nodes = state.get_nodes().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].addr, own_addr);
        assert_eq!(nodes[0].tick, 1);
        assert_eq!(nodes[0].status, NodeStatus::Ok);
    }

    /// [`failure_on_connect`] and [`failure_on_heartbeat`] are very similar. the ONLY difference is where the error happens.
    /// Both have the same invariants though
    ///  1. An error response must be returned
    ///  2. At the end of the execution, no connections are cached
    ///  3. at the end of the execution, the state has the remote node marked as PossiblyOffline with a tick of 2
    #[tokio::test]
    async fn failure_on_connect() {
        let own_addr = Bytes::from("fake addr");
        let state = Arc::new(
            State::new(
                Box::new(MockPartitioningScheme::default()),
                own_addr.clone(),
                Quorum::default(),
            )
            .unwrap(),
        );

        let remote_node_addr = Bytes::from("A");
        state
            .merge_nodes(vec![Node {
                addr: remote_node_addr.clone(),
                status: NodeStatus::Ok,
                tick: 1,
            }])
            .unwrap();

        let cluster_connections = Arc::new(Mutex::new(HashMap::new()));

        let mut result = do_heartbeat(
            1,
            state.clone(),
            Arc::new(
                MockClientFactoryBuilder::new()
                    .with_connection_fault(When::Always)
                    .build(),
            ),
            cluster_connections.clone(),
        )
        .await;

        assert_eq!(result.len(), 1);
        let err = result.remove(0).err().unwrap();

        match err {
            crate::error::Error::Client(Error::UnableToConnect { .. }) => {}
            _ => {
                panic!("Unexpected error: {}", err)
            }
        }

        assert_eq!(cluster_connections.lock().unwrap().len(), 0);
        let nodes = state.get_nodes().unwrap();
        assert_eq!(nodes.len(), 2);
        for node in nodes {
            if node.addr == own_addr {
                // own node must be ok and have a tick equals to 1
                assert_eq!(node.status, NodeStatus::Ok);
                assert_eq!(node.tick, 1);
            } else if node.addr == remote_node_addr {
                // since connection failed, we should've increased the tick of the node by 1 and set it to PossiblyOffline
                assert_eq!(node.status, NodeStatus::PossiblyOffline);
                assert_eq!(node.tick, 2);
            } else {
                panic!("This else clause should never be reached. Either the node is itself or the other node included in the state");
            }
        }
    }

    /// see [`failure_on_connect`]
    #[tokio::test]
    async fn failure_on_heartbeat() {
        let own_addr = Bytes::from("fake addr");
        let fanout = 1;
        let state = Arc::new(
            State::new(
                Box::new(MockPartitioningScheme::default()),
                own_addr.clone(),
                Quorum::default(),
            )
            .unwrap(),
        );

        let remote_node_addr = Bytes::from("A");
        state
            .merge_nodes(vec![Node {
                addr: remote_node_addr.clone(),
                status: NodeStatus::Ok,
                tick: 1,
            }])
            .unwrap();

        let cluster_connections = Arc::new(Mutex::new(HashMap::new()));

        let mut result = do_heartbeat(
            fanout,
            state.clone(),
            Arc::new(
                MockClientFactoryBuilder::new()
                    .with_connection_fault(When::Never)
                    .with_heartbeat_fault(When::Always)
                    .build(),
            ),
            cluster_connections.clone(),
        )
        .await;

        assert_eq!(result.len(), 1);
        let err = result.remove(0).err().unwrap();

        match err {
            crate::error::Error::Client(Error::Io { .. }) => {}
            _ => {
                panic!("Unexpected error: {}", err)
            }
        }

        assert_eq!(cluster_connections.lock().unwrap().len(), 0);
        let nodes = state.get_nodes().unwrap();
        assert_eq!(nodes.len(), 2);
        for node in nodes {
            if node.addr == own_addr {
                // own node must be ok and have a tick equals to 1
                assert_eq!(node.status, NodeStatus::Ok);
                assert_eq!(node.tick, 1);
            } else if node.addr == remote_node_addr {
                // since heartbeat failed, we should've increased the tick of the node by 1 and set it to PossiblyOffline
                assert_eq!(node.status, NodeStatus::PossiblyOffline);
                assert_eq!(node.tick, 2);
            } else {
                panic!("This else clause should never be reached. Either the node is itself or the other node included in the state");
            }
        }
    }
}
