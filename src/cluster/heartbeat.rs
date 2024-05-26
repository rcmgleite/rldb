//! This file contains the logic related to the gossip protocol
//! used between rldb-server nodes in cluster mode. This is a TCP based protocol
//! that will be used to:
//!   1. Node discovery
//!     Every node that joins a running rldb cluster will learn about the other nodes via this protocol
//!     Nodes exchange they IP/PORT part, which is currently used as key to determine which which partition it owns.
//!     With this information, every node can determine the correct owner of any given key.
//!     This can also be used to determine which nodes in the ring will contain replicas of the key being added.
//!   2. Evaluate nodes health
//!     This happens by each node randomly choosing X nodes to ping every Y seconds (configurable).
//!     Hosts that are unreachable or respond with anything other than success are marked as
//!     [`NodeStatus::PossiblyOffline`]
//!     Note: Node are never automatically removed from the cluster. This requires an operator to manually intervene.
//!     The reason for that is that reshuffling partitions can be quite expensive (even when using schemes like consistent hashing).
//!     So we deliberately chose to let an operator make that decision instead of having automatic detection.
use std::{collections::HashMap, sync::Arc};

use crate::{
    client::{db_client::DbClientFactory, Client, Factory as ClientFactory},
    cluster::error::Error,
};

use super::{
    error::Result,
    state::{Node, State},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::{event, Level};

#[derive(Serialize, Deserialize)]
pub struct RingStateMessagePayload {
    nodes: Vec<Node>,
}

/// Start heartbeat will
/// 1. create a tcp connection with the required target_addr (If one doesn't exist yet)
/// 2. send a heartbeat message to the target node (which includes the current node view of the ring)
/// 3. Receive a heartbeat response (ACK OR FAILURE)
/// 4. loop forever picking a random node of the ring every X seconds and performing steps 2 through 3 again
///
/// TODO: This is gonna be a tough one to write good tests for
pub async fn start_heartbeat(cluster_state: Arc<State>) {
    let mut cluster_connections = HashMap::new();

    // Now we loop every X seconds to hearbeat to one node in the cluster
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        if let Err(err) = do_heartbeat(
            cluster_state.clone(),
            Box::new(DbClientFactory),
            &mut cluster_connections,
        )
        .await
        {
            match err {
                Error::ClusterHasOnlySelf => {
                    event!(Level::DEBUG, "Skipping heartbeat to self");
                }
                _ => {
                    event!(Level::WARN, "{}", err);
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum HeartbeatResult {
    Skipped,
    Success,
}

// TODO: Inject config on how many nodes to heartbeat to
async fn do_heartbeat(
    cluster_state: Arc<State>,
    client_factory: Box<dyn ClientFactory + Send>,
    cluster_connections: &mut HashMap<Bytes, Box<dyn Client + Send>>,
) -> Result<HeartbeatResult> {
    event!(Level::DEBUG, "heartbeat loop starting: {:?}", cluster_state);

    // tick (ie: update your own state)
    cluster_state.tick()?;

    let target_node = match cluster_state.get_random_node() {
        Ok(target_node) => target_node,
        Err(err) => {
            if let Error::ClusterHasOnlySelf = err {
                event!(Level::DEBUG, "skipping heatbeat to self");
                return Ok(HeartbeatResult::Skipped);
            }

            return Err(err);
        }
    };

    // let's re-use an exisiting connection to the picked random node if one exists.. otherwise create a new one
    let client = if let Some(client) = cluster_connections.get_mut(&target_node.addr) {
        client
    } else {
        let addr: String = String::from_utf8_lossy(&target_node.addr).into();

        let client = match client_factory.get(addr).await {
            Ok(conn) => conn,
            Err(err) => {
                if let Err(err) = cluster_state.mark_node_as_possibly_offline(target_node) {
                    event!(Level::WARN, "Unable to mark node as offline: {}", err);
                }

                return Err(err.into());
            }
        };

        // cache the connection
        cluster_connections.insert(target_node.addr.clone(), client);
        // unwrap is unsafe because we just constructed this hashmap
        cluster_connections.get_mut(&target_node.addr).unwrap()
    };

    let known_nodes = cluster_state.get_nodes()?;

    if let Err(err) = client.heartbeat(known_nodes).await {
        event!(
            Level::WARN,
            "Unable to connect to node {:?} - err {:?}",
            target_node,
            err
        );

        cluster_connections.remove(&target_node.addr);
        if let Err(err) = cluster_state.mark_node_as_possibly_offline(target_node) {
            event!(Level::WARN, "Unable to mark node as offline: {}", err);
        }

        Err(err.into())
    } else {
        event!(Level::DEBUG, "heartbeat cycle finished {:?}", cluster_state);
        Ok(HeartbeatResult::Success)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::{
        client::{error::Error, mock::MockClientFactoryBuilder},
        cluster::{
            heartbeat::{do_heartbeat, HeartbeatResult},
            partitioning::mock::MockPartitioningScheme,
            state::{Node, NodeStatus, State},
        },
        test_utils::fault::When,
    };
    use bytes::Bytes;

    #[tokio::test]
    async fn success() {
        let own_addr = Bytes::from("fake addr");
        let state =
            Arc::new(State::new(Box::new(MockPartitioningScheme::default()), own_addr).unwrap());

        state
            .merge_nodes(vec![Node {
                addr: Bytes::from("A"),
                status: NodeStatus::Ok,
                tick: 1,
            }])
            .unwrap();

        let mut cluster_connections = HashMap::new();

        let res = do_heartbeat(
            state,
            Box::new(
                MockClientFactoryBuilder::new()
                    .with_connection_fault(When::Never)
                    .with_heartbeat_fault(When::Never)
                    .build(),
            ),
            &mut cluster_connections,
        )
        .await
        .unwrap();

        assert_eq!(res, HeartbeatResult::Success);
        assert_eq!(cluster_connections.len(), 1);
    }

    #[tokio::test]
    async fn skip_heartbeat_to_self() {
        let own_addr = Bytes::from("fake addr");
        let state =
            Arc::new(State::new(Box::new(MockPartitioningScheme::default()), own_addr).unwrap());
        let mut cluster_connections = HashMap::new();

        assert_eq!(
            do_heartbeat(
                state,
                Box::new(MockClientFactoryBuilder::new().without_faults().build()),
                &mut cluster_connections
            )
            .await
            .unwrap(),
            HeartbeatResult::Skipped
        );

        assert_eq!(cluster_connections.len(), 0);
    }

    #[tokio::test]
    async fn failure_on_connect() {
        let own_addr = Bytes::from("fake addr");
        let state =
            Arc::new(State::new(Box::new(MockPartitioningScheme::default()), own_addr).unwrap());

        state
            .merge_nodes(vec![Node {
                addr: Bytes::from("A"),
                status: NodeStatus::Ok,
                tick: 1,
            }])
            .unwrap();

        let mut cluster_connections = HashMap::new();

        let err = do_heartbeat(
            state,
            Box::new(
                MockClientFactoryBuilder::new()
                    .with_connection_fault(When::Always)
                    .build(),
            ),
            &mut cluster_connections,
        )
        .await
        .err()
        .unwrap();

        match err {
            crate::cluster::error::Error::Client(Error::UnableToConnect { .. }) => {}
            _ => {
                panic!("Uexpected error: {}", err)
            }
        }

        assert_eq!(cluster_connections.len(), 0);
    }

    #[tokio::test]
    async fn failure_on_heartbeat() {
        let own_addr = Bytes::from("fake addr");
        let state =
            Arc::new(State::new(Box::new(MockPartitioningScheme::default()), own_addr).unwrap());

        state
            .merge_nodes(vec![Node {
                addr: Bytes::from("A"),
                status: NodeStatus::Ok,
                tick: 1,
            }])
            .unwrap();

        let mut cluster_connections = HashMap::new();

        let err = do_heartbeat(
            state,
            Box::new(
                MockClientFactoryBuilder::new()
                    .with_connection_fault(When::Never)
                    .with_heartbeat_fault(When::Always)
                    .build(),
            ),
            &mut cluster_connections,
        )
        .await
        .err()
        .unwrap();

        match err {
            crate::cluster::error::Error::Client(Error::GenericIo { .. }) => {}
            _ => {
                panic!("Uexpected error: {}", err)
            }
        }

        assert_eq!(cluster_connections.len(), 0);
    }
}
