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
    test_utils::fault::{Fault, When},
};

use async_trait::async_trait;
use bytes::Bytes;

use super::{error::Error, error::Result, Client, Factory as ClientFactory};

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
}

impl MockClient {
    pub fn new(faults: MockClientFaults) -> Self {
        Self {
            faults,
            stats: Default::default(),
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
    async fn get(&mut self, _key: Bytes, _replica: bool) -> Result<GetResponse> {
        todo!()
    }
    async fn put(
        &mut self,
        _key: Bytes,
        _value: Bytes,
        _metadata: Option<String>,
        _replication: bool,
    ) -> Result<PutResponse> {
        todo!()
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
}

#[async_trait]
impl ClientFactory for MockClientFactory {
    async fn get(&self, _: String) -> Result<Box<dyn Client + Send>> {
        let mut client = MockClient::new(self.faults.clone());
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
        }
    }
}
