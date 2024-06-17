//! Mock implementation for [`Client`]
//!
//! Still WIP... might be completely removed in favor of a package like mockall
use crate::{
    cluster::state::Node,
    cmd::{
        cluster::{
            cluster_state::ClusterStateResponse, heartbeat::HeartbeatResponse,
            join_cluster::JoinClusterResponse,
        },
        get::GetResponse,
        ping::PingResponse,
        put::PutResponse,
    },
    persistency::Db,
    storage_engine::{in_memory::InMemory, StorageEngine},
    test_utils::fault::{Fault, When},
};

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use super::{error::Error, error::Result, Client, Factory as ClientFactory};

type StorageEngineType = Arc<dyn StorageEngine + Send + Sync>;

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
    pub storage_engine: StorageEngineType,
}

impl MockClient {
    pub fn new(
        faults: MockClientFaults,
        storage_engine: Arc<dyn StorageEngine + Send + Sync + 'static>,
    ) -> Self {
        Self {
            faults,
            stats: Default::default(),
            storage_engine,
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
                return Err(Error::UnableToConnect {
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
    async fn get(&mut self, key: Bytes, _replica: bool) -> Result<GetResponse> {
        let response = Db::into_metadata_and_data(self.storage_engine.get(&key).await.unwrap())?;

        if let Some((metadata, data)) = response {
            Ok(GetResponse {
                value: data,
                metadata: String::from_utf8(hex::encode(metadata).into_bytes()).unwrap(),
            })
        } else {
            Err(Error::NotFound { key })
        }
    }
    async fn put(
        &mut self,
        key: Bytes,
        value: Bytes,
        metadata: Option<String>,
        _replication: bool,
    ) -> Result<PutResponse> {
        if let Some(metadata) = metadata {
            let mut value_and_metadata = BytesMut::new();
            value_and_metadata.put_slice(&hex::decode(metadata).unwrap());
            value_and_metadata.put_slice(&value);
            self.storage_engine
                .put(key, value_and_metadata.freeze())
                .await
                .unwrap();
        } else {
            self.storage_engine.put(key, value).await.unwrap();
        }

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
    pub storage_engines: Arc<Mutex<HashMap<String, StorageEngineType>>>,
}

#[async_trait]
impl ClientFactory for MockClientFactory {
    async fn get(&self, addr: String) -> Result<Box<dyn Client + Send>> {
        let storage_engine = {
            let mut guard = self.storage_engines.lock().unwrap();
            let storage_engine = guard.entry(addr).or_insert(Arc::new(InMemory::default()));
            storage_engine.clone()
        };

        let mut client = MockClient::new(self.faults.clone(), storage_engine);
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
            storage_engines: Default::default(),
        }
    }
}
