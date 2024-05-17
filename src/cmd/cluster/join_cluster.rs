use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    client,
    server::{IntoRequest, PartitioningScheme, Request},
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

    // TODO: It's odd that we start heartbeating at this point..
    // how will we actually start heartbeating for the first node in the cluster?
    // The hearbeat loop should likely be initiated somewhere else once
    // a cluster node is started (based on the cluster config)
    pub async fn execute(
        self,
        partitioning_scheme: Arc<PartitioningScheme>,
    ) -> JoinClusterResponse {
        tokio::spawn(start_heartbeat(
            self.known_cluster_node_addr,
            partitioning_scheme,
        ));

        JoinClusterResponse::Success {
            message: "Ok".to_string(),
        }
    }

    pub fn try_from_request(request: Request) -> anyhow::Result<Self> {
        if request.id != CMD_CLUSTER_JOIN_CLUSTER {
            return Err(anyhow!(
                "Unable to construct JoinCluster Command from Request. Expected id {} got {}",
                CMD_CLUSTER_JOIN_CLUSTER,
                request.id
            ));
        }

        if let Some(payload) = request.payload {
            let s: Self = serde_json::from_slice(&payload)?;
            Ok(s)
        } else {
            return Err(anyhow!("JoinCluster message payload can't be None"));
        }
    }
}

/// Start heartbeat will
/// 1. create a tcp connection with the required target_addr
/// 2. send a heartbeat message to the target node
/// 3. Receive a heartbeat response (ACK OR FAILURE)
/// 4. loop forever picking a random node of the ring every X seconds and performing steps 2 through 3 again
async fn start_heartbeat(
    target_addr: String,
    partitioning_scheme: Arc<PartitioningScheme>,
) -> anyhow::Result<()> {
    let PartitioningScheme::ConsistentHashing(ring_state) = partitioning_scheme.as_ref();
    let mut cluster_connections = HashMap::new();

    // connect to the provided target and send first heartbeat
    let mut client = client::DbClient::connect(&target_addr).await?;
    client.heartbeat(ring_state.get_nodes()).await?;

    // store the client in our view of the connected clients
    cluster_connections.insert(target_addr.into(), client);

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    // Now we loop every X seconds to hearbeat to one node in the cluster
    loop {
        let target_node = ring_state.get_random_node();
        // let's re-use an exisiting connection to the picked random node if one exists.. otherwise create a new one
        let conn = if let Some(conn) = cluster_connections.get_mut(&target_node.addr) {
            conn
        } else {
            let client =
                client::DbClient::connect(String::from_utf8(target_node.addr.clone().into())?)
                    .await?;
            cluster_connections.insert(target_node.addr.clone(), client);
            cluster_connections.get_mut(&target_node.addr).unwrap()
        };

        conn.heartbeat(ring_state.get_nodes()).await?;
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

impl IntoRequest for JoinCluster {
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
