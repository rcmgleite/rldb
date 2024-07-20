//! Module that contains the abstraction connecting [`Storage`] and [`ClusterState`] into a single interface.
//!
//! This interface is what a [`crate::cmd::Command`] has access to in order to execute its functionality.
//!
//! # PUT behavior
//! Every node in the cluster is able to accept PUTs for any keys. 2 cases can happen:
//!  1. The node is part of the [`crate::cluster::state::State::preference_list`] for the given key, in which case it executes the PUT as a coordinator node.
//!   The coordinator node is responsible for handling the version bump for the key using [`VersionVector`] and tracking
//!   quorum (using the [`Quorum`] abstraction). This means that a coordinator Node is the one that actually executes all
//!   the replication logic.
//!  2. THe node is NOT part of the [`crate::cluster::state::State::preference_list`] for the given key. In this case, the node works as a proxy.
//!   It will check its own view of the cluster state and forward the PUT to the correct node.
//!
//! # Get behavior
//! Similar to PUT, every node in the cluster can receive requests for any keys. If the node owns the key
//! (ie: it's part of the key [`crate::cluster::state::State::preference_list`]),
//! it will read from local storage plus as many remote nodes as needed to meet [`Quorum`].
//! If it doesn't own the key, the behavior is very similar with one difference - there's
//! no local copy to be read, so all reads are forwarded to the correct nodes based on its view of the cluster
use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt};
use partitioning::consistent_hashing::murmur3_hash;
use quorum::{min_required_replicas::MinRequiredReplicas, Evaluation, OperationStatus, Quorum};
use std::sync::Arc;
use storage::{Storage, StorageEntry, Value};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{event, instrument, Level};
use versioning::version_vector::{ProcessId, VersionVector};

pub mod partitioning;
pub mod quorum;
pub mod storage;
pub mod versioning;

use crate::{
    client::Factory,
    cluster::state::{Node as ClusterNode, State as ClusterState},
    cmd::types::{Context, SerializedContext},
    error::{Error, Internal, InvalidRequest, Result},
};

pub type ClientFactory = Arc<dyn Factory + Send + Sync>;

/// Db is the abstraction that connects storage_engine and overall database state
/// in a single interface.
/// It exists mainly to hide [`Storage`] and [`ClusterState`] details so that they can
/// be updated later on..
#[derive(Clone)]
pub struct Db {
    /// TODO/FIXME: This is bad because it means that every operations locks
    /// the entire Storage.
    storage: Arc<AsyncMutex<Storage>>,
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
            .field("storage_engine", &self.storage)
            .field("cluster_state", &self.cluster_state)
            .field("own_state", &self.own_state)
            .finish()
    }
}

/// State of the current node
#[derive(Clone, Debug)]
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

impl Db {
    /// Returns a new instance of [`Db`].
    pub fn new(
        own_addr: Bytes,
        cluster_state: Arc<ClusterState>,
        client_factory: ClientFactory,
    ) -> Self {
        let own_state = State::Active {
            shared: Arc::new(Shared::new(&own_addr)),
        };
        Self {
            storage: Arc::new(AsyncMutex::new(Storage::new(own_state.pid()))),
            cluster_state,
            own_state,
            client_factory,
        }
    }

    /// Stores the given key and value into the underlying [`crate::persistency::storage::Storage`]
    #[instrument(name = "persistency::put", level = "info", skip(self))]
    pub async fn put(
        &self,
        key: Bytes,
        value: Value,
        replication: bool,
        context: Option<SerializedContext>,
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

            let context = context.ok_or(Error::InvalidRequest(
                InvalidRequest::ReplicationPutMustIncludeContext,
            ))?;

            // if this is a replication PUT, we don't have to deal with quorum. Just store it locally and be done.
            self.local_put(key, value, context).await?;
        } else if !preference_list.contains(&self.cluster_state.own_addr()) {
            let dst_node = preference_list[0].clone();
            event!(
                Level::DEBUG,
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
                .put(key.clone(), value, context.map(Into::into), false)
                .await?;
        } else {
            event!(Level::DEBUG, "Executing a coordinator Put");
            let mut version = if let Some(context) = context {
                context.deserialize(self.own_state.pid())?.into()
            } else {
                VersionVector::new(self.own_state.pid())
            };

            version.increment();
            let mut updated_context = Context::default();
            updated_context.merge_version(&version);
            let updated_context = updated_context.serialize();

            // if this is not a replication PUT, we have to honor quorum before returning a success
            // if we have a quorum config, we have to make sure we wrote to enough replicas
            let quorum_config = self.cluster_state.quorum_config();
            let mut quorum = MinRequiredReplicas::new(quorum_config.writes)?;
            let mut futures = FuturesUnordered::new();

            // the preference list will contain the node itself (otherwise there's a bug).
            // How can we assert that here? Maybe this should be guaranteed by the Db API instead...
            let preference_list = self.preference_list(&key)?;
            event!(Level::DEBUG, "preference_list: {:?}", preference_list);
            for node in preference_list {
                futures.push(self.do_put(key.clone(), value.clone(), updated_context.clone(), node))
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
                Evaluation::NotReached => {
                    event!(
                        Level::WARN,
                        "quorum not reached: failures: {:?}",
                        quorum_result.failures,
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

    #[instrument(name = "persistency::local_put", level = "info", skip(self))]
    async fn local_put(&self, key: Bytes, value: Value, context: SerializedContext) -> Result<()> {
        let mut storage_guard = self.storage.lock().await;
        storage_guard.put(key, value, context).await?;

        // FIXME: return type
        Ok(())
    }

    /// Wrapper function that allows the same inteface to put locally (using [`Db`]) or remotely (using [`DbClient`])
    #[instrument(level = "info", skip(self))]
    async fn do_put(
        &self,
        key: Bytes,
        value: Value,
        context: SerializedContext,
        dst_addr: Bytes,
    ) -> Result<()> {
        if self.owns_key(&dst_addr)? {
            event!(Level::DEBUG, "Storing key : {:?} locally", key);
            self.local_put(key, value, context).await?;
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
                .put(key.clone(), value, Some(context.into()), true)
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
    #[instrument(level = "info")]
    pub async fn get(&self, key: Bytes, replica: bool) -> Result<Vec<StorageEntry>> {
        if replica {
            event!(Level::DEBUG, "Executing a replica GET");
            let storage_guard = self.storage.lock().await;
            Ok(storage_guard.get(key.clone()).await?)
        } else {
            event!(Level::DEBUG, "executing a non-replica GET");
            let quorum_config = self.cluster_state.quorum_config();
            let mut quorum = MinRequiredReplicas::new(quorum_config.reads)?;

            let mut futures = FuturesUnordered::new();
            let preference_list = self.preference_list(&key)?;
            event!(Level::DEBUG, "GET preference_list {:?}", preference_list);
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
                        event!(Level::INFO, "GET Got result for quorum: {:?}", res);

                        for entry in res {
                            let _ = quorum.update(OperationStatus::Success(entry));
                        }
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

            let quorum_result = quorum.finish();
            event!(Level::DEBUG, "Get quorum result: {:?}", quorum_result);
            match quorum_result.evaluation {
                Evaluation::Reached(value) => Ok(value),
                Evaluation::NotReached => {
                    let failure_iter = quorum_result.failures.iter();
                    if failure_iter.size_hint().0 > 0
                        && quorum_result.failures.iter().all(|err| err.is_not_found())
                    {
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

    async fn do_get(&self, key: Bytes, src_addr: Bytes) -> Result<Vec<StorageEntry>> {
        if self.owns_key(&src_addr)? {
            event!(Level::DEBUG, "Getting data from local storage");
            let storage_guard = self.storage.lock().await;
            Ok(storage_guard.get(key.clone()).await?)
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

            Ok(client.replication_get(key.clone()).await?.values)
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
    use std::sync::Arc;

    use crate::{
        client::mock::MockClientFactoryBuilder,
        cluster::state::{Node, State},
        cmd::types::{Context, SerializedContext},
        error::{Error, InvalidRequest},
        persistency::{
            partitioning::consistent_hashing::ConsistentHashing, process_id, storage::Value,
            versioning::version_vector::VersionVectorOrd, Db, VersionVector,
        },
        server::config::Quorum,
        utils::generate_random_ascii_string,
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

        (
            own_local_addr.clone(),
            Arc::new(Db::new(
                own_local_addr,
                cluster_state,
                Arc::new(MockClientFactoryBuilder::new().build()),
            )),
        )
    }

    #[tokio::test]
    async fn test_db_put_get_simple() {
        let (local_addr, db) = initialize_state();
        let key = Bytes::from("a key");
        let value = Value::random();

        let replication = false;
        let context = None;
        db.put(key.clone(), value.clone(), replication, context)
            .await
            .unwrap();

        let mut get_result = db.get(key, replication).await.unwrap();
        assert_eq!(get_result.len(), 1);

        let entry = get_result.remove(0);
        assert_eq!(entry.value, value);

        let self_pid = process_id(&local_addr);
        let deserialized_version = entry.version;
        let mut expected_version = VersionVector::new(self_pid);
        expected_version.increment();

        assert_eq!(deserialized_version, expected_version);
    }

    #[tokio::test]
    async fn test_db_replication_put_no_context() {
        let (_, db) = initialize_state();
        let key = Bytes::from("a key");
        let value = Value::random();
        let replication = true;
        let context = None;
        let err = db
            .put(key.clone(), value.clone(), replication, context)
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
    async fn test_db_put_with_existing_version() {
        let (addr, db) = initialize_state();
        let key = Bytes::from("a key");
        let value = Value::random();
        let replication = false;
        let pid = process_id(&addr);
        let mut initial_version = VersionVector::new(pid);

        initial_version.increment();

        db.put(
            key.clone(),
            value.clone(),
            replication,
            Some(Context::from(initial_version.clone()).serialize()),
        )
        .await
        .unwrap();

        let mut get_result = db.get(key, replication).await.unwrap();

        let entry = get_result.remove(0);
        assert_eq!(entry.value, value);

        let deserialized_version = entry.version;
        let mut expected_version = initial_version;

        expected_version.increment();

        assert_eq!(deserialized_version, expected_version);
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

        let db = Arc::new(Db::new(
            own_local_addr,
            cluster_state,
            Arc::new(MockClientFactoryBuilder::new().build()),
        ));
        let key = Bytes::from("a key");
        let value = Value::random();
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
    async fn test_db_put_new_metadata_descends_from_original_version() {
        let (_, db) = initialize_state();
        let key = Bytes::from("a key");
        let value = Value::random();
        let replication = false;

        // This put without metadata will generate the first metadata
        db.put(key.clone(), value.clone(), replication, None)
            .await
            .unwrap();

        let mut entries = db.get(key.clone(), replication).await.unwrap();

        assert_eq!(entries.len(), 1);

        let first_entry = entries.remove(0);
        assert_eq!(first_entry.value, value);

        let new_value = Value::random();
        // Note that we are piping the metadata received on GET to the PUT request.
        // This is what guarantees that the new_value will override the previous value
        db.put(
            key.clone(),
            new_value.clone(),
            replication,
            // Some(first_entry.metadata.serialize()),
            Some(Context::from(first_entry.version.clone()).serialize()),
        )
        .await
        .unwrap();

        let mut second_read_entries = db.get(key, replication).await.unwrap();
        assert_eq!(second_read_entries.len(), 1);
        let deserialized_first_version = first_entry.version;
        let second_entry = second_read_entries.remove(0);
        let deserialized_second_version = second_entry.version;

        assert_eq!(
            deserialized_second_version.causality(&deserialized_first_version),
            VersionVectorOrd::HappenedAfter
        );
    }

    #[tokio::test]
    async fn test_db_put_stale_version_with_empty_metadata() {
        let (_, db) = initialize_state();
        let key = Bytes::from("a key");
        let value = Value::random();
        let replication = false;

        // This put without metadata will generate the first metadata
        db.put(key.clone(), value.clone(), replication, None)
            .await
            .unwrap();

        let mut first_entries = db.get(key.clone(), replication).await.unwrap();

        assert_eq!(first_entries.len(), 1);
        let first_entry = first_entries.remove(0);
        assert_eq!(first_entry.value, value);

        let new_value = Value::random();
        // Now we do another put to the same key and again we don't pass any metadata.
        // this means that this put should fail since it's trying to put a stale value
        let err = db
            .put(key.clone(), new_value.clone(), replication, None)
            .await
            .err()
            .unwrap();

        assert!(err.is_stale_context_provided());
    }

    #[tokio::test]
    async fn test_db_put_stale_version() {
        let (local_addr, db) = initialize_state();
        let key = Bytes::from("a key");
        let value = Value::random();
        let replication = false;

        let self_pid = process_id(&local_addr);
        let mut first_version = VersionVector::new(self_pid);

        first_version.increment();
        first_version.increment();

        // This put without metadata will generate the first metadata
        db.put(
            key.clone(),
            value.clone(),
            replication,
            Some(Context::from(first_version.clone()).serialize()),
        )
        .await
        .unwrap();

        let mut first_entries = db.get(key.clone(), replication).await.unwrap();

        assert_eq!(first_entries.len(), 1);
        let first_entry = first_entries.remove(0);
        assert_eq!(first_entry.value, value);

        let new_value = Value::random();
        // Now we do another put to the same key and again we don't pass any metadata.
        // this means that this put should fail since it's trying to put a stale value
        let err = db
            .put(
                key.clone(),
                new_value.clone(),
                replication,
                Some(Context::from(first_version).serialize()),
            )
            .await
            .err()
            .unwrap();

        assert!(err.is_stale_context_provided());
    }

    // This is a very specific regression test. At commit https://github.com/rcmgleite/rldb/commit/8fd3edd4c5a8234a5994c352877a447c0c502bed
    // we introduced a separate storage for data and metadata. The implementation created a race condition
    // since on a PUT request, we were
    //  1. reading local metadata to check for conflicts
    //  2. update metadata as required
    // IN A NON-ATOMIC WAY. For that reason, when we had concurrency, if 2 clients tried to write at the same time
    // and for any reason they both read the metadata at the same time, only one of them would actually have its
    // correct value stored and therefore would simply try to override the data that the other possibly just PUT.
    // This test spawns 2 clients and make them both write to the same key. It asserts that a single one of them
    // succeeds in each run (as it should).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn regression_test_db_concurrent_puts() {
        for _ in 0..100 {
            let (_, db) = initialize_state();
            let key: Bytes = generate_random_ascii_string(20).into();

            let first_value = Value::random();
            let replication = false;

            db.put(key.clone(), first_value.clone(), replication, None)
                .await
                .unwrap();

            let mut first_get_result = db.get(key.clone(), replication).await.unwrap();

            assert_eq!(first_get_result.len(), 1);
            let first_get_value = first_get_result.remove(0);
            assert_eq!(first_get_value.value, first_value);

            enum PutResult {
                Success,
                Failure(Error),
            }

            // A client function does a PUT followed by a GET.
            // If the PUT was successful, we have to be able to retrieve the exact same value just put
            // by issuing a GET.
            async fn client(
                db: Arc<Db>,
                key: Bytes,
                value: Value,
                context: Option<SerializedContext>,
            ) -> PutResult {
                match db.put(key.clone(), value.clone(), false, context).await {
                    Ok(_) => {
                        let get_result = db.get(key, false).await.unwrap();
                        assert_eq!(get_result.len(), 1);
                        assert_eq!(get_result[0].value, value);
                        PutResult::Success
                    }
                    Err(err) => PutResult::Failure(err),
                }
            }

            let c1_value = Value::random();
            let c2_value = Value::random();
            let context = Context::from(first_get_value.version.clone()).serialize();
            assert_ne!(c1_value, first_value);
            assert_ne!(c2_value, first_value);
            assert_ne!(c1_value, c2_value);

            let c1_handle = tokio::spawn(client(
                db.clone(),
                key.clone(),
                c1_value,
                Some(context.clone()),
            ));
            let c2_handle = tokio::spawn(client(db, key, c2_value, Some(context)));

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
