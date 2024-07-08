//! Mock implementation for [`Client`]
//!
//! Still WIP... might be completely removed in favor of a package like mockall
use crate::{
    cluster::state::{Node, State},
    cmd::{
        cluster::{
            cluster_state::ClusterStateResponse, heartbeat::HeartbeatResponse,
            join_cluster::JoinClusterResponse,
        },
        get::GetResponse,
        ping::PingResponse,
        put::PutResponse,
        replication_get::ReplicationGetResponse,
        types::{Context, SerializedContext},
    },
    error::{Error, Result},
    persistency::{partitioning::consistent_hashing::ConsistentHashing, storage::Value, Db},
    server::config::Quorum,
    storage_engine::in_memory::InMemory,
    test_utils::fault::{Fault, When},
};

use async_trait::async_trait;
use bytes::Bytes;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::{Client, Factory as ClientFactory};

#[derive(Debug, Default)]
pub struct Stats {
    pub n_calls: usize,
}

#[derive(Debug, Default)]
pub struct MockClientStats {
    pub connect: Stats,
    pub heartbeat: Stats,
}

#[derive(Debug, Clone, Default)]
pub struct MockClientFaults {
    pub connect: Fault,
    pub heartbeat: Fault,
}

#[derive(Debug)]
pub struct MockClient {
    pub faults: MockClientFaults,
    pub stats: MockClientStats,
    pub db: Db,
}

impl MockClient {
    pub fn new(faults: MockClientFaults, db: Db) -> Self {
        Self {
            faults,
            stats: Default::default(),
            db,
        }
    }
}

/// the only 2 methods required here are connect and heartbeat.. the rest we leave as todo!()
#[async_trait]
impl Client for MockClient {
    async fn connect(&mut self) -> Result<()> {
        self.stats.connect.n_calls += 1;
        let fault = &self.faults.connect;
        match fault.when {
            When::Always => {
                return Err(Error::Io {
                    reason: "Mocked error on connect".to_string(),
                });
            }
            When::Never => { /* noop */ }
        }

        Ok(())
    }
    async fn ping(&mut self) -> Result<PingResponse> {
        todo!()
    }
    async fn get(&mut self, key: Bytes) -> Result<GetResponse> {
        let res = self.db.get(key, false).await?;
        let values = res.iter().map(|e| e.value.clone()).collect();
        let context = res.iter().fold(Context::default(), |mut acc, e| {
            acc.merge_version(&e.version);
            acc
        });

        Ok(GetResponse {
            values,
            context: context.serialize().into(),
        })
    }

    async fn replication_get(&mut self, key: Bytes) -> Result<ReplicationGetResponse> {
        let res = self.db.get(key, true).await?;
        Ok(ReplicationGetResponse { values: res })
    }

    async fn put(
        &mut self,
        key: Bytes,
        value: Value,
        context: Option<String>,
        replication: bool,
    ) -> Result<PutResponse> {
        self.db
            .put(
                key,
                value,
                replication,
                context.map(SerializedContext::from),
            )
            .await?;
        Ok(PutResponse {
            message: "Ok".to_string(),
        })
    }

    async fn heartbeat(&mut self, _: Vec<Node>) -> Result<HeartbeatResponse> {
        self.stats.heartbeat.n_calls += 1;
        let fault = &self.faults.heartbeat;
        match fault.when {
            When::Always => {
                return Err(Error::Io {
                    reason: "Mocked error on heartbeat".to_string(),
                });
            }
            When::Never => { /* noop */ }
        }

        Ok(HeartbeatResponse {
            message: "Ok".to_string(),
        })
    }
    async fn join_cluster(
        &mut self,
        _known_cluster_node_addr: String,
    ) -> Result<JoinClusterResponse> {
        todo!()
    }

    async fn cluster_state(&mut self) -> Result<ClusterStateResponse> {
        todo!()
    }
}

pub struct MockClientFactory {
    pub faults: MockClientFaults,
    pub dbs: Arc<Mutex<HashMap<String, Db>>>,
}

#[async_trait]
impl ClientFactory for MockClientFactory {
    async fn get(&self, addr: String) -> Result<Box<dyn Client + Send>> {
        let db = {
            let own_addr: Bytes = addr.clone().into();
            let cluster_state = Arc::new(
                State::new(
                    Box::<ConsistentHashing>::default(),
                    own_addr.clone(),
                    Quorum::default(),
                )
                .unwrap(),
            );
            cluster_state
                .merge_nodes(vec![
                    Node {
                        addr: Bytes::from("127.0.0.1:3001"),
                        status: crate::cluster::state::NodeStatus::Ok,
                        tick: 0,
                    },
                    Node {
                        addr: Bytes::from("127.0.0.1:3002"),
                        status: crate::cluster::state::NodeStatus::Ok,
                        tick: 0,
                    },
                ])
                .unwrap();

            let storage_engine = Arc::new(InMemory::default());

            let mut guard = self.dbs.lock().unwrap();
            let db = guard.entry(addr.clone()).or_insert(Db::new(
                own_addr,
                storage_engine,
                cluster_state,
                Arc::new(MockClientFactoryBuilder::new().build()),
            ));

            db.clone()
        };

        let mut client = MockClient::new(self.faults.clone(), db);
        client.connect().await?;
        Ok(Box::new(client))
    }
}

pub struct MockClientFactoryBuilder {
    faults: MockClientFaults,
}

impl Default for MockClientFactoryBuilder {
    fn default() -> Self {
        let faults = MockClientFaults {
            connect: Fault { when: When::Never },
            heartbeat: Fault { when: When::Never },
        };

        Self { faults }
    }
}

impl MockClientFactoryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_connection_fault(mut self, when: When) -> Self {
        self.faults.connect = Fault { when };
        self
    }

    pub fn with_heartbeat_fault(mut self, when: When) -> Self {
        self.faults.heartbeat = Fault { when };
        self
    }

    pub fn without_faults(mut self) -> Self {
        self.faults = Default::default();
        self
    }

    pub fn build(self) -> MockClientFactory {
        MockClientFactory {
            faults: self.faults,
            dbs: Default::default(),
        }
    }
}
