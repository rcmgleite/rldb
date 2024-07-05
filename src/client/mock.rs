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
        replication_get::ReplicationGetResponse,
        Context, SerializedContext, Value,
    },
    error::{Error, Result},
    persistency::{
        storage::{metadata_evaluation, StorageEntry},
        Metadata, MetadataEvaluation,
    },
    storage_engine::{in_memory::InMemory, StorageEngine},
    test_utils::fault::{Fault, When},
};

use async_trait::async_trait;
use bytes::Bytes;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::Mutex as AsyncMutex;

use super::{Client, Factory as ClientFactory};

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
    pub storage: Arc<AsyncMutex<Storage>>,
}

#[derive(Debug)]
pub struct Storage {
    storage_engine: StorageEngineType,
    metadata_engine: StorageEngineType,
}

impl MockClient {
    pub fn new(
        faults: MockClientFaults,

        storage_engine: StorageEngineType,
        metadata_engine: StorageEngineType,
    ) -> Self {
        Self {
            faults,
            stats: Default::default(),
            storage: Arc::new(AsyncMutex::new(Storage {
                storage_engine,
                metadata_engine,
            })),
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
        let storage_guard = self.storage.lock().await;
        let metadata = storage_guard.metadata_engine.get(&key).await.unwrap();
        let data = storage_guard.storage_engine.get(&key).await.unwrap();
        match (metadata, data) {
            (None, None) => Err(Error::NotFound { key }),
            (None, Some(_)) | (Some(_), None) => panic!("should never happen"),
            (Some(metadata), Some(data)) => {
                let metadata = Metadata::deserialize(0, metadata).unwrap();
                let mut context = Context::default();
                context.merge_metadata(&metadata);

                Ok(GetResponse {
                    values: vec![Value::new(data, 0)],
                    context: context.serialize().into(),
                })
            }
        }
    }
    async fn replication_get(&mut self, key: Bytes) -> Result<ReplicationGetResponse> {
        let storage_guard = self.storage.lock().await;
        let metadata = storage_guard.metadata_engine.get(&key).await.unwrap();
        let data = storage_guard.storage_engine.get(&key).await.unwrap();
        match (metadata, data) {
            (None, None) => Err(Error::NotFound { key }),
            (None, Some(_)) | (Some(_), None) => panic!("should never happen"),
            (Some(metadata), Some(data)) => {
                let metadata = Metadata::deserialize(0, metadata).unwrap();

                Ok(ReplicationGetResponse {
                    values: vec![StorageEntry {
                        value: data,
                        metadata,
                    }],
                })
            }
        }
    }
    async fn put(
        &mut self,
        key: Bytes,
        value: Bytes,
        context: Option<SerializedContext>,
        replication: bool,
    ) -> Result<PutResponse> {
        // only support replication for now
        assert!(replication);
        let metadata = context.unwrap().deserialize(0).unwrap().into_metadata();
        let storage_guard = self.storage.lock().await;

        let existing_metadata = storage_guard.metadata_engine.get(&key).await.unwrap();
        if let Some(existing_metadata) = existing_metadata {
            let deserialized_existing_metadata =
                Metadata::deserialize(0, existing_metadata).unwrap();
            if let MetadataEvaluation::Conflict =
                metadata_evaluation(&metadata, &deserialized_existing_metadata)?
            {
                return Err(Error::Internal(crate::error::Internal::Unknown {
                    reason: "Puts with conflicts are currently not implemented".to_string(),
                }));
            }
        }

        storage_guard
            .metadata_engine
            .put(key.clone(), metadata.serialize())
            .await
            .unwrap();
        storage_guard.storage_engine.put(key, value).await.unwrap();
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
    pub metadata_engines: Arc<Mutex<HashMap<String, StorageEngineType>>>,
}

#[async_trait]
impl ClientFactory for MockClientFactory {
    async fn get(&self, addr: String) -> Result<Box<dyn Client + Send>> {
        let storage_engine = {
            let mut guard = self.storage_engines.lock().unwrap();
            let storage_engine = guard
                .entry(addr.clone())
                .or_insert(Arc::new(InMemory::default()));
            storage_engine.clone()
        };

        let metadata_engine = {
            let mut guard = self.metadata_engines.lock().unwrap();
            let metadata_engine = guard.entry(addr).or_insert(Arc::new(InMemory::default()));
            metadata_engine.clone()
        };

        let mut client = MockClient::new(self.faults.clone(), storage_engine, metadata_engine);
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
            metadata_engines: Default::default(),
        }
    }
}
