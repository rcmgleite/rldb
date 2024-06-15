//! Module that contains the abstraction connecting the [`StorageEngine`] and [`ClusterState`] into a single interface.
//!
//! This interface is what a [`crate::cmd::Command`] has access to in order to execute its functionality.
use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use quorum::{min_required_replicas::MinRequiredReplicas, Evaluation, OperationStatus, Quorum};
use std::sync::Arc;
use tracing::{event, Level};

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
    /// This will be present if this is configured as a cluster node
    cluster_state: Option<Arc<ClusterState>>,
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
    Synchronizing {
        shared: Arc<Shared>,
        synchonization_src: Bytes,
    },
}

#[derive(Debug)]
struct Shared;

/// Possibly a bad idea, but using an enum instead of a boolean to determine if a key is owned by a node or not.
/// This is mostly useful because the [`OwnsKeyResponse::False`] variant contains the addrs of the node
/// that actually holds the key, which is sent back to the client as part of the TCP response.
pub enum OwnsKeyResponse {
    /// The node provided to [`Db::owns_key`] owns the key
    True,
    /// The node provided to [`Db::owns_key`] does not own the key. The actual owner is returned in the addr field.
    False { addr: Bytes },
}

impl Db {
    /// Returns a new instance of [`Db`] with the provided [`StorageEngine`] and [`ClusterState`].
    pub fn new(storage_engine: StorageEngine, cluster_state: Option<Arc<ClusterState>>) -> Self {
        Self {
            storage_engine,
            cluster_state,
            own_state: State::Active {
                shared: Arc::new(Shared),
            },
        }
    }

    /// Stores the given key and value into the underlying [`StorageEngine`]
    ///
    /// TODO: Should the checks regarding ownership of keys/partitions be moved to this function
    /// instead of delegated to the Put [`crate::cmd::Command`]
    pub async fn put(&self, key: Bytes, value: Bytes, replication: bool) -> Result<()> {
        if let Some(cluster_state) = self.cluster_state.as_ref() {
            if replication {
                event!(Level::DEBUG, "Executing a replication Put");
                // if this is a replication PUT, we don't have to deal with quorum
                self.storage_engine.put(key, value).await?;
            } else {
                event!(Level::DEBUG, "Executing a coordinator Put");
                // if this is not a replication PUT, we have to honor quorum before returning a success
                // if we have a quorum config, we have to make sure we wrote to enough replicas
                let quorum_config = cluster_state.quorum_config();
                let mut quorum =
                    MinRequiredReplicas::new(quorum_config.replicas, quorum_config.writes)?;
                let mut futures = FuturesUnordered::new();

                // the preference list will contain the node itself (otherwise there's a bug).
                // How can we assert that here? Maybe this should be guaranteed by the Db API instead...
                let preference_list = self.preference_list(&key)?;
                for node in preference_list {
                    futures.push(self.do_put(key.clone(), value.clone(), node))
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
                            reason: format!(
                                "Unable to execute a min of {} PUTs",
                                quorum_config.writes
                            ),
                        });
                    }
                }
            }
        } else {
            event!(Level::DEBUG, "Executing non-cluster PUT");
            self.storage_engine.put(key, value).await?;
        }

        Ok(())
    }

    /// Wrapper function that allows the same inteface to put locally (using [`Db`]) or remotely (using [`DbClient`])
    async fn do_put(&self, key: Bytes, value: Bytes, dst_addr: Bytes) -> Result<()> {
        if let OwnsKeyResponse::True = self.owns_key(&dst_addr)? {
            event!(Level::DEBUG, "Storing key : {:?} locally", key);
            self.storage_engine.put(key, value).await?;
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
            let _ = client.put(key.clone(), value, true).await?;

            event!(Level::DEBUG, "stored key : {:?} locally", key);
        }

        Ok(())
    }

    /// Retrieves the [`Bytes`] associated with the given key.
    ///
    /// If the key is not found, [Option::None] is returned
    pub async fn get(&self, key: Bytes, replica: bool) -> Result<Option<Bytes>> {
        if let Some(cluster_state) = self.cluster_state.as_ref() {
            event!(Level::DEBUG, "Executing cluster GET");
            if replica {
                event!(Level::DEBUG, "Executing a replica GET");
                Ok(self.storage_engine.get(&key).await?)
            } else {
                event!(Level::INFO, "executing a non-replica GET");
                let quorum_config = cluster_state.quorum_config();
                let mut quorum =
                    MinRequiredReplicas::new(quorum_config.replicas, quorum_config.reads)?;

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
                    Evaluation::Reached => Ok(quorum_result.successes[0].clone()),
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
        } else {
            event!(Level::INFO, "executing a non-cluster GET");
            Ok(self.storage_engine.get(&key).await?)
        }
    }

    async fn do_get(&self, key: Bytes, src_addr: Bytes) -> Result<Option<Bytes>> {
        if let OwnsKeyResponse::True = self.owns_key(&src_addr)? {
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
    pub fn owns_key(&self, key: &[u8]) -> Result<OwnsKeyResponse> {
        if let Some(cluster_state) = self.cluster_state.as_ref() {
            if cluster_state.owns_key(key)? {
                Ok(OwnsKeyResponse::True)
            } else {
                Ok(OwnsKeyResponse::False {
                    addr: cluster_state.key_owner(key).unwrap().addr.clone(),
                })
            }
        } else {
            Ok(OwnsKeyResponse::True)
        }
    }

    /// Updates the cluster state based on the nodes provided.
    ///
    /// This is used as part of the Gossip protocol to propagate cluster changes across all nodes
    pub fn update_cluster_state(&self, nodes: Vec<ClusterNode>) -> Result<()> {
        if let Some(cluster_state) = self.cluster_state.as_ref() {
            Ok(cluster_state.merge_nodes(nodes)?)
        } else {
            Ok(())
        }
    }

    pub fn preference_list(&self, key: &[u8]) -> Result<Vec<Bytes>> {
        if let Some(cluster_state) = self.cluster_state.as_ref() {
            Ok(cluster_state.preference_list(key, cluster_state.quorum_config().replicas)?)
        } else {
            // TODO: There must be a way to make sure a caller is not allowed to call methods that only make sense
            // in cluster mode instead of a runtime failure.
            Err(Error::Generic {
                reason: "preference_list is meaningless for rldb not in cluster mode".to_string(),
            })
        }
    }

    pub fn cluster_state(&self) -> Result<Vec<ClusterNode>> {
        if let Some(cluster_state) = self.cluster_state.as_ref() {
            Ok(cluster_state.get_nodes()?)
        } else {
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO
}
