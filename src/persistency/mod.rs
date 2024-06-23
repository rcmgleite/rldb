//! Module that contains the abstraction connecting the [`StorageEngine`] and [`ClusterState`] into a single interface.
//!
//! This interface is what a [`crate::cmd::Command`] has access to in order to execute its functionality.
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{stream::FuturesUnordered, StreamExt};
use partitioning::consistent_hashing::murmur3_hash;
use quorum::{min_required_replicas::MinRequiredReplicas, Evaluation, OperationStatus, Quorum};
use std::sync::Arc;
use tracing::{event, Level};
use versioning::version_vector::{ProcessId, VersionVector, VersionVectorOrd};

pub mod partitioning;
pub mod quorum;
pub mod versioning;

use crate::{
    client::Factory,
    cluster::state::{Node as ClusterNode, State as ClusterState},
    error::{Error, Internal, InvalidRequest, Result},
    storage_engine::{in_memory::InMemory, StorageEngine as StorageEngineTrait},
};

/// type alias to the [`StorageEngine`] that makes it clonable and [`Send`]
pub type StorageEngine = Arc<dyn StorageEngineTrait + Send + Sync + 'static>;
pub type ClientFactory = Box<dyn Factory + Send + Sync>;

/// Db is the abstraction that connects storage_engine and overall database state
/// in a single interface.
/// It exists mainly to hide [`StorageEngine`] and [`ClusterState`] details so that they can
/// be updated later on..
pub struct Db {
    /// The underlaying storage engine
    storage_engine: StorageEngine,
    /// Metadata Storage engine - currently hardcoded to be in-memory
    /// The idea of having a separate storage engine just for metadata is do that we can avoid
    /// adding framing layers to the data we want to store to append/prepend the metadata.
    /// Also, when we do PUTs, we have to validate metadata (version) prior to storing the data,
    /// and without a separate storage engine for metadata, we would always require a GET before a PUT,
    /// which introduces more overhead than needed.
    metadata_engine: InMemory,
    /// Cluster state.
    cluster_state: Arc<ClusterState>,
    /// Own state
    own_state: State,
    /// Used to construct [`crate::client::Client`] instances
    client_factory: ClientFactory,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("storage_engine", &self.storage_engine)
            .field("cluster_state", &self.cluster_state)
            .field("own_state", &self.own_state)
            .finish()
    }
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
            pid: process_id(node),
        }
    }

    fn pid(&self) -> ProcessId {
        self.pid
    }
}

pub(crate) fn process_id(addr: &[u8]) -> ProcessId {
    murmur3_hash(addr)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Metadata {
    // version vector of this object
    pub versions: VersionVector,
}

enum MetadataEvaluation {
    Override,
    Conflict,
}

fn metadata_evaluation(lhs: &Metadata, rhs: &Metadata) -> Result<MetadataEvaluation> {
    match lhs.versions.causality(&rhs.versions) {
        VersionVectorOrd::HappenedBefore | VersionVectorOrd::Equals => {
            // the data we are trying to store is actually older than what's already stored.
            // This can happen due to read repair and active anti-entropy processes for instance.
            // We return an error here to make sure the client knows the put was rejected and does not expect
            // the value passed in this request to be stored
            event!(Level::WARN, "StaleContextProvided");
            Err(Error::InvalidRequest(InvalidRequest::StaleContextProvided))
        }
        VersionVectorOrd::HappenedAfter => {
            // This means we are ok to override both data and metadata
            Ok(MetadataEvaluation::Override)
        }
        VersionVectorOrd::HappenedConcurrently => {
            // now we have the problem of concurrent writes. Our current approach is to actually
            // store both versions so that the client can deal with the conflict resolution
            Ok(MetadataEvaluation::Conflict)
        }
    }
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

    pub fn deserialize(self_pid: u128, mut serialized: Bytes) -> Result<Metadata> {
        let serialized_size = serialized.get_u32() as usize;
        serialized.truncate(serialized_size);
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
        client_factory: ClientFactory,
    ) -> Self {
        Self {
            storage_engine,
            metadata_engine: InMemory::default(),
            cluster_state,
            own_state: State::Active {
                shared: Arc::new(Shared::new(&own_addr)),
            },
            client_factory,
        }
    }

    /// Stores the given key and value into the underlying [`StorageEngine`]
    ///
    /// # Note on the format passed down to the storage engine:
    ///  |      u32        |      u32    | variable |     u32     | variable | ...
    ///  |Number of records| Data 1 size | Data 1   | Data 2 size |  Data    | ...
    ///
    /// # TODOs
    ///  1. Handle partial successes
    ///   - what do we do if a PUT failed but some nodes successfully wrote the data?
    ///  2. Properly handle version / version conflicts
    ///   - We first have to decide if they can actually happen - might not be possible if we don't allow nodes outside the preference list
    ///     to accept puts.
    ///  3. Include integrity checks (checksums) on both data and metadata
    pub async fn put(
        &self,
        key: Bytes,
        value: Bytes,
        replication: bool,
        metadata: Option<Bytes>,
    ) -> Result<()> {
        let preference_list = self.cluster_state.preference_list(&key)?;
        if replication {
            event!(Level::INFO, "Executing a replication Put");
            if !preference_list.contains(&self.cluster_state.own_addr()) {
                return Err(Error::Internal(Internal::Logic {
                    reason: format!(
                        "Node {:?} Received a replication PUT for a key that it doesn't own",
                        self.cluster_state.own_addr()
                    ),
                }));
            }

            // if this is a replication PUT, we don't have to deal with quorum
            if let Some(metadata) = metadata {
                let existing_metadata_for_key = self.metadata_engine.get(&key).await?;
                if let Some(existing_metadata_for_key) = existing_metadata_for_key {
                    // If we already have a metadata for the key being PUT, we have to understand if this new PUT
                    // descends from what we have stored or if this is a concurrent put (see [`crate::persistency::versioning::VersionVector`])
                    // for nomenclature description.
                    let existing_metadata_for_key =
                        Metadata::deserialize(self.own_state.pid(), existing_metadata_for_key)?;

                    let deserialized_metadata =
                        Metadata::deserialize(self.own_state.pid(), metadata.clone())?;

                    match metadata_evaluation(&deserialized_metadata, &existing_metadata_for_key)? {
                        MetadataEvaluation::Conflict => {
                            return Err(Error::Internal(crate::error::Internal::Unknown {
                                reason: "Puts with conflicts are currently not implemented"
                                    .to_string(),
                            }));
                        }
                        MetadataEvaluation::Override => {
                            // fallthrough
                        }
                    }
                }
                // if, on the other hand, there's no pre-existing metadata for this key, this is the first PUT
                // for the given key. just executed it.
                self.storage_engine.put(key.clone(), value).await?;
                self.metadata_engine.put(key, metadata).await?;
            } else {
                return Err(Error::InvalidRequest(
                    InvalidRequest::ReplicationPutMustIncludeContext,
                ));
            }
        } else if !preference_list.contains(&self.cluster_state.own_addr()) {
            let dst_node = preference_list[0].clone();
            event!(
                Level::INFO,
                "Executing a forward proxy PUT to node: {:?}",
                dst_node
            );
            // let's just forward the PUT (not as replication) to a node that is part of the preference list
            let mut client = self
                .client_factory
                .get(String::from_utf8(dst_node.into()).map_err(|e| {
                    event!(
                        Level::ERROR,
                        "Unable to parse addr as utf8 {}",
                        e.to_string()
                    );
                    Error::Internal(crate::error::Internal::Unknown {
                        reason: e.to_string(),
                    })
                })?)
                .await?;

            let _ = client
                .put(key.clone(), value, metadata.map(hex::encode), false)
                .await?;
        } else {
            event!(Level::INFO, "Executing a coordinator Put");
            // FIXME: There's a bug here - If the node acting as coordinator is not part of the preference list
            // or it simply doesn't have the given key for any reason, the metadata check is worthless and a lot
            // of conflicts would arise on the nodes that are indeed part of the preference list
            let existing_metadata_for_key = self.metadata_engine.get(&key).await?;
            let mut metadata = if let Some(metadata) = metadata {
                Metadata::deserialize(self.own_state.pid(), metadata)?
            } else {
                // if we have a metadata stored but we didn't receive one via PUT, we know the client messed up. let's fail
                if existing_metadata_for_key.is_some() {
                    return Err(Error::InvalidRequest(
                        InvalidRequest::EmptyContextWhenOverridingKey,
                    ));
                }
                Metadata {
                    versions: VersionVector::new(self.own_state.pid()),
                }
            };

            metadata.versions.increment();

            if let Some(existing_metadata_for_key) = existing_metadata_for_key {
                let existing_metadata_for_key =
                    Metadata::deserialize(self.own_state.pid(), existing_metadata_for_key)?;
                match metadata_evaluation(&metadata, &existing_metadata_for_key)? {
                    MetadataEvaluation::Conflict => {
                        return Err(Error::Internal(crate::error::Internal::Unknown {
                            reason: "Puts with conflicts are currently not implemented".to_string(),
                        }));
                    }
                    MetadataEvaluation::Override => {
                        // noop - fallthrough and just continue the execution
                    }
                }
            }

            // if this is not a replication PUT, we have to honor quorum before returning a success
            // if we have a quorum config, we have to make sure we wrote to enough replicas
            let quorum_config = self.cluster_state.quorum_config();
            let mut quorum =
                MinRequiredReplicas::new(quorum_config.replicas, quorum_config.writes)?;
            let mut futures = FuturesUnordered::new();

            // the preference list will contain the node itself (otherwise there's a bug).
            // How can we assert that here? Maybe this should be guaranteed by the Db API instead...
            let preference_list = self.preference_list(&key)?;
            event!(Level::INFO, "preference_list: {:?}", preference_list);
            for node in preference_list {
                futures.push(self.do_put(key.clone(), value.clone(), metadata.clone(), node))
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
                Evaluation::Reached(_) => {}
                Evaluation::NotReached | Evaluation::Unreachable => {
                    event!(
                        Level::WARN,
                        "quorum not reached: failures: {:?} out of {}",
                        quorum_result.failures,
                        quorum_result.total
                    );
                    return Err(Error::QuorumNotReached {
                        operation: "Put".to_string(),
                        reason: format!("Unable to execute a min of {} PUTs", quorum_config.writes),
                        errors: quorum_result.failures,
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
        metadata: Metadata,
        dst_addr: Bytes,
    ) -> Result<()> {
        if self.owns_key(&dst_addr)? {
            event!(Level::INFO, "Storing key : {:?} locally", key);
            self.storage_engine.put(key.clone(), value).await?;
            self.metadata_engine.put(key, metadata.serialize()).await?;
        } else {
            event!(
                Level::INFO,
                "will store key : {:?} in remote node: {:?}",
                key,
                dst_addr
            );
            let mut client = self
                .client_factory
                .get(String::from_utf8(dst_addr.into()).map_err(|e| {
                    event!(
                        Level::ERROR,
                        "Unable to parse addr as utf8 {}",
                        e.to_string()
                    );
                    Error::Internal(crate::error::Internal::Unknown {
                        reason: e.to_string(),
                    })
                })?)
                .await?;

            // TODO: assert the response
            let _ = client
                .put(
                    key.clone(),
                    value,
                    Some(hex::encode(metadata.serialize())),
                    true,
                )
                .await?;
        }

        Ok(())
    }

    /// Retrieves the [`Bytes`] associated with the given key.
    ///
    /// If the key is not found, [Option::None] is returned
    ///
    /// # TODOs
    ///  1. Handle version conflicts
    ///    - we have to deal with the case in which we have multiple versions of the same data encountered
    ///    - this will mean merging [`VersionVector`]s so that the client receives the merged version alongside an array of objects
    ///  2. Handle integrity checks properly
    ///  3. Implement Read Repair
    pub async fn get(&self, key: Bytes, replica: bool) -> Result<Option<(Bytes, Bytes)>> {
        if replica {
            event!(Level::INFO, "Executing a replica GET");
            if let Some(metadata) = self.metadata_engine.get(&key).await? {
                if let Some(data) = self.storage_engine.get(&key).await? {
                    Ok(Some((metadata, data)))
                } else {
                    Err(Error::Internal(crate::error::Internal::Logic {
                        reason:
                            "got a metadata entry without a corresponding data entry. This is a bug"
                                .to_string(),
                    }))
                }
            } else {
                Ok(None)
            }
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

            event!(Level::DEBUG, "quorum: {:?}", quorum);

            let quorum_result = quorum.finish();
            event!(Level::ERROR, "Get quorum result: {:?}", quorum_result);
            match quorum_result.evaluation {
                Evaluation::Reached(value) => Ok(value),
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
                        errors: quorum_result.failures,
                    })
                }
            }
        }
    }

    async fn do_get(&self, key: Bytes, src_addr: Bytes) -> Result<Option<(Bytes, Bytes)>> {
        if self.owns_key(&src_addr)? {
            event!(Level::INFO, "Getting data from local storage");
            if let Some(metadata) = self.metadata_engine.get(&key).await? {
                if let Some(data) = self.storage_engine.get(&key).await? {
                    Ok(Some((metadata, data)))
                } else {
                    Err(Error::Internal(crate::error::Internal::Logic {
                        reason:
                            "got a metadata entry without a corresponding data entry. This is a bug"
                                .to_string(),
                    }))
                }
            } else {
                Ok(None)
            }
        } else {
            event!(
                Level::DEBUG,
                "Scheduling replication GET on node {:?} ",
                src_addr
            );

            let mut client = self
                .client_factory
                .get(String::from_utf8(src_addr.clone().into()).map_err(|e| {
                    event!(
                        Level::ERROR,
                        "Unable to parse addr as utf8 {}",
                        e.to_string()
                    );
                    Error::Internal(crate::error::Internal::Unknown {
                        reason: e.to_string(),
                    })
                })?)
                .await?;

            let resp = client.get(key.clone(), true).await?;
            // FIXME: remove unwrap()
            Ok(Some((
                hex::decode(resp.metadata).unwrap().into(),
                resp.value,
            )))
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
        self.cluster_state.merge_nodes(nodes)
    }

    pub fn preference_list(&self, key: &[u8]) -> Result<Vec<Bytes>> {
        self.cluster_state.preference_list(key)
    }

    pub fn cluster_state(&self) -> Result<Vec<ClusterNode>> {
        self.cluster_state.get_nodes()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rand::{distributions::Alphanumeric, Rng};
    use std::sync::Arc;

    use crate::{
        client::mock::MockClientFactoryBuilder,
        cluster::state::{Node, State},
        error::{Error, InvalidRequest},
        persistency::{
            partitioning::consistent_hashing::ConsistentHashing, process_id,
            versioning::version_vector::VersionVectorOrd, Db, Metadata, VersionVector,
        },
        server::config::Quorum,
        storage_engine::in_memory::InMemory,
    };

    /// Initializes a [`Db`] instance with 2 [`crate::client::mock::MockClient`]
    fn initialize_state() -> (Bytes, Arc<Db>) {
        let own_local_addr = Bytes::from("127.0.0.1:3000");
        let cluster_state = Arc::new(
            State::new(
                Box::<ConsistentHashing>::default(),
                own_local_addr.clone(),
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

        (
            own_local_addr.clone(),
            Arc::new(Db::new(
                own_local_addr,
                storage_engine,
                cluster_state,
                Box::new(MockClientFactoryBuilder::new().build()),
            )),
        )
    }

    #[tokio::test]
    async fn test_db_put_get_simple() {
        let (local_addr, db) = initialize_state();
        let key = Bytes::from("a key");
        let value = Bytes::from("a value");
        let replication = false;
        let metadata = None;
        db.put(key.clone(), value.clone(), replication, metadata)
            .await
            .unwrap();

        let (metadata, data) = db.get(key, replication).await.unwrap().unwrap();

        assert_eq!(data, value);

        let self_pid = process_id(&local_addr);
        let deserialized_metadata = Metadata::deserialize(self_pid, metadata).unwrap();
        let mut expected_metadata = Metadata {
            versions: VersionVector::new(self_pid),
        };
        expected_metadata.versions.increment();

        assert_eq!(deserialized_metadata, expected_metadata);
    }

    #[tokio::test]
    async fn test_db_replication_put_no_metadata() {
        let (_, db) = initialize_state();
        let key = Bytes::from("a key");
        let value = Bytes::from("a value");
        let replication = true;
        let metadata = None;
        let err = db
            .put(key.clone(), value.clone(), replication, metadata)
            .await
            .err()
            .unwrap();

        match err {
            Error::InvalidRequest(InvalidRequest::ReplicationPutMustIncludeContext) => {}
            _ => {
                panic!("Unexpected error {}", err);
            }
        }
    }

    #[tokio::test]
    async fn test_db_put_with_existing_metadata() {
        let (local_addr, db) = initialize_state();
        let key = Bytes::from("a key");
        let value = Bytes::from("a value");
        let replication = false;
        let mut initial_metadata = Metadata {
            versions: VersionVector::new(1), // any random number would do
        };
        initial_metadata.versions.increment();

        db.put(
            key.clone(),
            value.clone(),
            replication,
            Some(initial_metadata.serialize()),
        )
        .await
        .unwrap();

        let (received_metadata, data) = db.get(key, replication).await.unwrap().unwrap();

        assert_eq!(data, value);

        let self_pid = process_id(&local_addr);
        let deserialized_metadata = Metadata::deserialize(self_pid, received_metadata).unwrap();

        let mut expected_metadata =
            Metadata::deserialize(self_pid, initial_metadata.serialize()).unwrap();

        expected_metadata.versions.increment();

        assert_eq!(deserialized_metadata, expected_metadata);
    }

    #[tokio::test]
    async fn test_db_put_no_quorum() {
        // Initializes DB with a only one node registered in State.
        // This means that no Put operation can succeed due to quorum requirements (at least 2 successes)
        let own_local_addr = Bytes::from("127.0.0.1:3000");
        let cluster_state = Arc::new(
            State::new(
                Box::<ConsistentHashing>::default(),
                own_local_addr.clone(),
                Quorum::default(),
            )
            .unwrap(),
        );

        let storage_engine = Arc::new(InMemory::default());

        let db = Arc::new(Db::new(
            own_local_addr,
            storage_engine,
            cluster_state,
            Box::new(MockClientFactoryBuilder::new().build()),
        ));
        let key = Bytes::from("a key");
        let value = Bytes::from("a value");
        let replication = false;
        let metadata = None;
        let err = db
            .put(key.clone(), value.clone(), replication, metadata)
            .await
            .err()
            .unwrap();

        match err {
            Error::QuorumNotReached { operation, .. } => {
                assert_eq!(operation, *"Put");
            }
            _ => {
                panic!("Unexpected err: {}", err);
            }
        }
    }

    #[tokio::test]
    async fn test_db_put_new_metadata_descends_from_original_metadata() {
        let (local_addr, db) = initialize_state();
        let key = Bytes::from("a key");
        let value = Bytes::from("a value");
        let replication = false;

        // This put without metadata will generate the first metadata
        db.put(key.clone(), value.clone(), replication, None)
            .await
            .unwrap();

        let (first_metadata, data) = db.get(key.clone(), replication).await.unwrap().unwrap();

        assert_eq!(data, value);

        let new_value = Bytes::from("a new value");
        // Note that we are piping the metadata received on GET to the PUT request.
        // This is what guarantees that the new_value will override the previous value
        db.put(
            key.clone(),
            new_value.clone(),
            replication,
            Some(first_metadata.clone()),
        )
        .await
        .unwrap();

        let (second_metadata, data) = db.get(key, replication).await.unwrap().unwrap();
        assert_eq!(data, new_value);
        let self_pid = process_id(&local_addr);
        let deserialized_first_metadata = Metadata::deserialize(self_pid, first_metadata).unwrap();
        let deserialized_second_metadata =
            Metadata::deserialize(self_pid, second_metadata).unwrap();

        assert_eq!(
            deserialized_second_metadata
                .versions
                .causality(&deserialized_first_metadata.versions),
            VersionVectorOrd::HappenedAfter
        );
    }

    #[tokio::test]
    async fn test_db_put_stale_version_with_empty_metadata() {
        let (_, db) = initialize_state();
        let key = Bytes::from("a key");
        let value = Bytes::from("a value");
        let replication = false;

        // This put without metadata will generate the first metadata
        db.put(key.clone(), value.clone(), replication, None)
            .await
            .unwrap();

        let (_, data) = db.get(key.clone(), replication).await.unwrap().unwrap();

        assert_eq!(data, value);

        let new_value = Bytes::from("a new value");
        // Now we do another put to the same key and again we don't pass any metadata.
        // this means that this put should fail since it's trying to put a stale value
        let err = db
            .put(key.clone(), new_value.clone(), replication, None)
            .await
            .err()
            .unwrap();

        match err {
            Error::InvalidRequest(InvalidRequest::EmptyContextWhenOverridingKey) => {}
            _ => {
                panic!("Unexpected err: {}", err);
            }
        }
    }

    #[tokio::test]
    async fn test_db_put_stale_version() {
        let (local_addr, db) = initialize_state();
        let key = Bytes::from("a key");
        let value = Bytes::from("a value");
        let replication = false;

        let self_pid = process_id(&local_addr);
        let mut first_metadata = Metadata {
            versions: VersionVector::new(self_pid),
        };

        first_metadata.versions.increment();
        first_metadata.versions.increment();

        // This put without metadata will generate the first metadata
        db.put(
            key.clone(),
            value.clone(),
            replication,
            Some(first_metadata.serialize()),
        )
        .await
        .unwrap();

        let (_, data) = db.get(key.clone(), replication).await.unwrap().unwrap();

        assert_eq!(data, value);

        let new_value = Bytes::from("a new value");
        // Now we do another put to the same key and again we don't pass any metadata.
        // this means that this put should fail since it's trying to put a stale value
        let err = db
            .put(
                key.clone(),
                new_value.clone(),
                replication,
                Some(first_metadata.serialize()),
            )
            .await
            .err()
            .unwrap();

        match err {
            Error::InvalidRequest(InvalidRequest::StaleContextProvided) => {}
            _ => {
                panic!("Unexpected err: {}", err);
            }
        }
    }

    // This is a very specific regression test. At commit https://github.com/rcmgleite/rldb/commit/8fd3edd4c5a8234a5994c352877a447c0c502bed
    // we introduced a separate [`StorageEngine`] for data and metadata. The implementation created a race condition
    // since on a PUT request, we were
    //  1. reading local metadata to check for conflicts
    //  2. update metadata as required
    // in a non-atomic way. when we have concurrency, if 2 clients try to write at the same time
    // and for any reason they both read the metadata at the same time, only one of them will actually have its
    // correct value stored
    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn regression_test_db_concurrent_puts() {
        for _ in 0..100 {
            let (_, db) = initialize_state();
            let key: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(20)
                .map(char::from)
                .collect();
            let key: Bytes = key.into();

            let first_value = Bytes::from("a value");
            let replication = false;

            db.put(key.clone(), first_value.clone(), replication, None)
                .await
                .unwrap();

            let (first_get_metadata, first_get_data) =
                db.get(key.clone(), replication).await.unwrap().unwrap();

            assert_eq!(first_get_data, first_value);

            // let self_pid = process_id(&local_addr);

            enum PutResult {
                Success,
                Failure(Error),
            }

            let c1_value = Bytes::from("c1_value");
            let c2_value = Bytes::from("c2_value");
            let c1_handle = {
                let db = db.clone();
                let key = key.clone();
                let first_get_metadata = first_get_metadata.clone();
                let c1_value = c1_value.clone();
                tokio::spawn(async move {
                    match db
                        .put(
                            key.clone(),
                            c1_value.clone(),
                            replication,
                            Some(first_get_metadata),
                        )
                        .await
                    {
                        Ok(_) => {
                            let get_result = db.get(key, false).await.unwrap().unwrap();
                            assert_eq!(get_result.1, c1_value);
                            PutResult::Success
                        }
                        Err(err) => PutResult::Failure(err),
                    }
                })
            };

            let c2_handle = {
                let db = db.clone();
                let key = key.clone();
                let first_get_metadata = first_get_metadata.clone();
                let c2_value = c2_value.clone();
                tokio::spawn(async move {
                    match db
                        .put(
                            key.clone(),
                            c2_value.clone(),
                            replication,
                            Some(first_get_metadata),
                        )
                        .await
                    {
                        Ok(_) => {
                            let get_result = db.get(key, false).await.unwrap().unwrap();
                            assert_eq!(get_result.1, c2_value);
                            PutResult::Success
                        }
                        Err(err) => PutResult::Failure(err),
                    }
                })
            };

            let c1_res = c1_handle.await.unwrap();
            let c2_res = c2_handle.await.unwrap();
            match (c1_res, c2_res) {
                (PutResult::Success, PutResult::Success) => {
                    panic!("If both puts succeeded, we have a bug");
                }
                (PutResult::Failure(err1), PutResult::Failure(err2)) => {
                    panic!(
                        "Both Puts failed. should never happen: err1: {:?}, err2: {:?}",
                        err1, err2
                    );
                }
                (PutResult::Success, PutResult::Failure(_))
                | (PutResult::Failure(_), PutResult::Success) => {
                    // expected result
                }
            }
        }
    }
}
