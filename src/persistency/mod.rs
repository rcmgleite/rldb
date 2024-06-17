//! Module that contains the abstraction connecting the [`StorageEngine`] and [`ClusterState`] into a single interface.
//!
//! This interface is what a [`crate::cmd::Command`] has access to in order to execute its functionality.
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{stream::FuturesUnordered, StreamExt};
use partitioning::consistent_hashing::murmur3_hash;
use quorum::{min_required_replicas::MinRequiredReplicas, Evaluation, OperationStatus, Quorum};
use std::sync::Arc;
use tracing::{event, Level};
use versioning::version_vector::{ProcessId, VersionVector};

pub mod partitioning;
pub mod quorum;
pub mod versioning;

use crate::{
    client::{db_client::DbClient, Client},
    cluster::state::{Node as ClusterNode, State as ClusterState},
    error::{Error, Result},
};

/// type alias to the [`StorageEngine`] that makes it clonable and [`Send`]
pub type StorageEngine = Arc<dyn crate::storage_engine::StorageEngine + Send + Sync + 'static>;

/// Db is the abstraction that connects storage_engine and overall database state
/// in a single interface.
/// It exists mainly to hide [`StorageEngine`] and [`ClusterState`] details so that they can
/// be updated later on..
#[derive(Debug)]
pub struct Db {
    /// The underlaying storage engine
    storage_engine: StorageEngine,
    /// Cluster state.
    cluster_state: Arc<ClusterState>,
    /// Own state
    own_state: State,
}

/// State of the current node
#[derive(Debug)]
enum State {
    /// Node is running and actively handling incoming requests
    Active { shared: Arc<Shared> },
    /// Node is synchronizing from another host.
    /// This happens when a node either was just added to the cluster
    /// or was offline and is now catching up for any reason
    #[allow(dead_code)]
    Synchronizing {
        shared: Arc<Shared>,
        synchonization_src: Bytes,
    },
}

impl State {
    fn pid(&self) -> ProcessId {
        match self {
            State::Active { shared } => shared.pid(),
            State::Synchronizing { shared, .. } => shared.pid(),
        }
    }
}

#[derive(Debug)]
struct Shared {
    pid: ProcessId,
}

impl Shared {
    fn new(node: &[u8]) -> Self {
        Self {
            pid: murmur3_hash(node),
        }
    }

    fn pid(&self) -> ProcessId {
        self.pid
    }
}

#[derive(Debug)]
pub struct Metadata {
    pub versions: VersionVector,
}

impl Metadata {
    /// Serializes [`Metadata`] into [`Bytes`]
    ///
    /// FIXME: This impl is really inefficient due to the amount of memory copies.
    /// The operating of adding "framing"/metadata to [`Bytes`] is something that we do a lot...
    /// we might need a better abstratction/data structure to deal with the properly
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32(self.versions.serialized_size() as u32);
        buf.put_slice(&self.versions.serialize());

        buf.freeze()
    }

    pub fn deserialize(self_pid: u128, serialized: Bytes) -> Result<Metadata> {
        Ok(Metadata {
            versions: VersionVector::deserialize(self_pid, serialized)?,
        })
    }
}

impl Db {
    /// Returns a new instance of [`Db`] with the provided [`StorageEngine`] and [`ClusterState`].
    pub fn new(
        own_addr: Bytes,
        storage_engine: StorageEngine,
        cluster_state: Arc<ClusterState>,
    ) -> Self {
        Self {
            storage_engine,
            cluster_state,
            own_state: State::Active {
                shared: Arc::new(Shared::new(&own_addr)),
            },
        }
    }

    /// Stores the given key and value into the underlying [`StorageEngine`]
    ///
    /// TODO: Should the checks regarding ownership of keys/partitions be moved to this function
    /// instead of delegated to the Put [`crate::cmd::Command`]
    pub async fn put(
        &self,
        key: Bytes,
        value: Bytes,
        replication: bool,
        metadata: Option<Bytes>,
    ) -> Result<()> {
        if replication {
            event!(Level::DEBUG, "Executing a replication Put");
            // if this is a replication PUT, we don't have to deal with quorum
            if let Some(metadata) = metadata {
                let mut value_and_metadata = BytesMut::new();
                value_and_metadata.put_slice(&metadata);
                value_and_metadata.put_slice(&value);

                self.storage_engine
                    .put(key, value_and_metadata.freeze())
                    .await?;
            } else {
                return Err(Error::InvalidRequest {
                    reason: "Replication PUT MUST include metadata".to_string(),
                });
            }
        } else {
            event!(Level::DEBUG, "Executing a coordinator Put");

            let mut metadata = if let Some(metadata) = metadata {
                Metadata::deserialize(self.own_state.pid(), metadata)?
            } else {
                Metadata {
                    versions: VersionVector::new(self.own_state.pid()),
                }
            };

            metadata.versions.increment();
            let serialized_metadata = metadata.serialize();

            // if this is not a replication PUT, we have to honor quorum before returning a success
            // if we have a quorum config, we have to make sure we wrote to enough replicas
            let quorum_config = self.cluster_state.quorum_config();
            let mut quorum =
                MinRequiredReplicas::new(quorum_config.replicas, quorum_config.writes)?;
            let mut futures = FuturesUnordered::new();

            // the preference list will contain the node itself (otherwise there's a bug).
            // How can we assert that here? Maybe this should be guaranteed by the Db API instead...
            let preference_list = self.preference_list(&key)?;
            for node in preference_list {
                futures.push(self.do_put(
                    key.clone(),
                    value.clone(),
                    serialized_metadata.clone(),
                    node,
                ))
            }

            // for now, let's wait til all futures either succeed or fail.
            // we don't strictly need that since we are quorum based..
            // doing it this way now for simplicity but this will have a latency impact
            //
            // For more info, see similar comment on [`crate::cmd::get::Get`]
            while let Some(res) = futures.next().await {
                match res {
                    Ok(_) => {
                        let _ = quorum.update(OperationStatus::Success(()));
                    }
                    Err(err) => {
                        event!(Level::WARN, "Failed a PUT: {:?}", err);
                        let _ = quorum.update(OperationStatus::Failure(err));
                    }
                }
            }

            let quorum_result = quorum.finish();

            match quorum_result.evaluation {
                Evaluation::Reached => {}
                Evaluation::NotReached | Evaluation::Unreachable => {
                    event!(
                        Level::WARN,
                        "quorum not reached: successes: {}, failures: {}",
                        quorum_result.successes.len(),
                        quorum_result.failures.len(),
                    );
                    return Err(Error::QuorumNotReached {
                        operation: "Put".to_string(),
                        reason: format!("Unable to execute a min of {} PUTs", quorum_config.writes),
                    });
                }
            }
        }

        Ok(())
    }

    /// Wrapper function that allows the same inteface to put locally (using [`Db`]) or remotely (using [`DbClient`])
    async fn do_put(
        &self,
        key: Bytes,
        value: Bytes,
        serialized_metadata: Bytes,
        dst_addr: Bytes,
    ) -> Result<()> {
        // FIXME: A lot of duplication in this operation
        let mut value_and_metadata = BytesMut::new();
        value_and_metadata.put_slice(&serialized_metadata);
        value_and_metadata.put_slice(&value);
        let value_and_metadata = value_and_metadata.freeze();
        if self.owns_key(&dst_addr)? {
            event!(Level::DEBUG, "Storing key : {:?} locally", key);
            self.storage_engine.put(key, value_and_metadata).await?;
        } else {
            event!(
                Level::DEBUG,
                "will store key : {:?} in node: {:?}",
                key,
                dst_addr
            );
            let mut client =
                DbClient::new(String::from_utf8(dst_addr.clone().into()).map_err(|e| {
                    event!(
                        Level::ERROR,
                        "Unable to parse addr as utf8 {}",
                        e.to_string()
                    );
                    Error::Internal(crate::error::Internal::Unknown {
                        reason: e.to_string(),
                    })
                })?);

            event!(Level::DEBUG, "connecting to node node: {:?}", dst_addr);
            client.connect().await?;

            // TODO: assert the response
            let _ = client
                .put(
                    key.clone(),
                    value,
                    Some(hex::encode(serialized_metadata)),
                    true,
                )
                .await?;

            event!(Level::DEBUG, "stored key : {:?} locally", key);
        }

        Ok(())
    }

    /// Retrieves the [`Bytes`] associated with the given key.
    ///
    /// If the key is not found, [Option::None] is returned
    pub async fn get(&self, key: Bytes, replica: bool) -> Result<Option<(Bytes, Bytes)>> {
        if replica {
            event!(Level::DEBUG, "Executing a replica GET");

            // FIXME: Horrible logic
            let value_and_metadata = self.storage_engine.get(&key).await?;
            let metadata_value_option = value_and_metadata.map(|mut value_and_metadata| {
                let metadata_size = value_and_metadata.get_u32();
                let metadata =
                    Bytes::copy_from_slice(&value_and_metadata[0..metadata_size as usize]);
                let value = Bytes::copy_from_slice(&value_and_metadata[metadata_size as usize..]);
                (metadata, value)
            });

            Ok(metadata_value_option)
        } else {
            event!(Level::INFO, "executing a non-replica GET");
            let quorum_config = self.cluster_state.quorum_config();
            let mut quorum = MinRequiredReplicas::new(quorum_config.replicas, quorum_config.reads)?;

            let mut futures = FuturesUnordered::new();
            let preference_list = self.preference_list(&key)?;
            event!(Level::INFO, "GET preference_list {:?}", preference_list);
            for node in preference_list {
                futures.push(self.do_get(key.clone(), node))
            }

            // TODO: we are waiting for all nodes on the preference list to return either error or success
            // this will cause latency issues and it's no necessary.. fix it later
            // Note: The [`crate::cluster::quorum::Quorum`] API already handles early evaluation
            // in case quorum is not reachable. The change to needed here is fairly small in this regard.
            // What's missing is deciding on:
            //  1. what do we do with inflight requests in case of an early success?
            //  2. (only for the PUT case) what do we do with successful PUT requests? rollback? what does that look like?
            while let Some(res) = futures.next().await {
                match res {
                    Ok(res) => {
                        let _ = quorum.update(OperationStatus::Success(res));
                    }
                    Err(err) => {
                        event!(
                            Level::WARN,
                            "Got a failed GET from a remote host: {:?}",
                            err
                        );

                        let _ = quorum.update(OperationStatus::Failure(err));
                    }
                }
            }

            event!(Level::INFO, "quorum: {:?}", quorum);

            let quorum_result = quorum.finish();
            match quorum_result.evaluation {
                Evaluation::Reached => {
                    let value_and_metadata = quorum_result.successes[0].clone();
                    let metadata_value_option = value_and_metadata.map(|mut value_and_metadata| {
                        let metadata_size = value_and_metadata.get_u32();
                        let metadata =
                            Bytes::copy_from_slice(&value_and_metadata[0..metadata_size as usize]);
                        let value =
                            Bytes::copy_from_slice(&value_and_metadata[metadata_size as usize..]);
                        (metadata, value)
                    });

                    Ok(metadata_value_option)
                }
                Evaluation::NotReached | Evaluation::Unreachable => {
                    if quorum_result.failures.iter().all(|err| err.is_not_found()) {
                        return Err(Error::NotFound { key: key.clone() });
                    }

                    Err(Error::QuorumNotReached {
                        operation: "Get".to_string(),
                        reason: format!(
                            "Unable to execute {} successful GETs",
                            quorum_config.reads
                        ),
                    })
                }
            }
        }
    }

    async fn do_get(&self, key: Bytes, src_addr: Bytes) -> Result<Option<Bytes>> {
        if self.owns_key(&src_addr)? {
            event!(
                Level::INFO,
                "node is part of preference_list {:?}",
                src_addr
            );
            Ok(self.storage_engine.get(&key).await?)
        } else {
            event!(
                Level::INFO,
                "node is NOT part of preference_list {:?} - will have to do a remote call",
                src_addr
            );
            let mut client =
                DbClient::new(String::from_utf8(src_addr.clone().into()).map_err(|e| {
                    event!(
                        Level::ERROR,
                        "Unable to parse addr as utf8 {}",
                        e.to_string()
                    );
                    Error::Internal(crate::error::Internal::Unknown {
                        reason: e.to_string(),
                    })
                })?);

            event!(Level::INFO, "connecting to node node: {:?}", src_addr);
            client.connect().await?;

            let resp = client.get(key.clone(), true).await?;
            Ok(Some(resp.value))
        }
    }

    /// Verifies if the key provided is owned by self.
    pub fn owns_key(&self, key: &[u8]) -> Result<bool> {
        self.cluster_state.owns_key(key)
    }

    /// Updates the cluster state based on the nodes provided.
    ///
    /// This is used as part of the Gossip protocol to propagate cluster changes across all nodes
    pub fn update_cluster_state(&self, nodes: Vec<ClusterNode>) -> Result<()> {
        Ok(self.cluster_state.merge_nodes(nodes)?)
    }

    pub fn preference_list(&self, key: &[u8]) -> Result<Vec<Bytes>> {
        Ok(self
            .cluster_state
            .preference_list(key, self.cluster_state.quorum_config().replicas)?)
    }

    pub fn cluster_state(&self) -> Result<Vec<ClusterNode>> {
        Ok(self.cluster_state.get_nodes()?)
    }
}

#[cfg(test)]
mod tests {
    // TODO
}
