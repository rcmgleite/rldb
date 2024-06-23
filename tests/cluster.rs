use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use client::error::Error as ClientError;
use error::Error as ServerError;
use rand::{distributions::Alphanumeric, Rng};
use rldb::{
    client::{self, db_client::DbClient, Client},
    cluster::state::NodeStatus,
    error::{self, InvalidRequest},
    persistency::Metadata,
    server::Server,
};
use tokio::{
    sync::oneshot::{channel, Sender},
    task::JoinHandle,
};

struct ServerHandle {
    task_handle: JoinHandle<()>,
    shutdown: Sender<()>,
    client_listener_addr: String,
}

async fn start_servers(configs: Vec<PathBuf>) -> (Vec<ServerHandle>, Vec<DbClient>) {
    let mut handles = Vec::new();
    for config in configs {
        let mut server = Server::from_config(config)
            .await
            .expect("Unable to construct server from config");
        let client_listener_addr = server.client_listener_local_addr().unwrap().to_string();
        let (shutdown_sender, shutdown_receiver) = channel();
        let server_handle = tokio::spawn(async move {
            server.run(shutdown_receiver).await.unwrap();
        });

        handles.push(ServerHandle {
            task_handle: server_handle,
            shutdown: shutdown_sender,
            client_listener_addr,
        });
    }

    let mut client = DbClient::new(handles[0].client_listener_addr.clone());
    client.connect().await.unwrap();
    for i in 1..handles.len() {
        client
            .join_cluster(handles[i].client_listener_addr.clone())
            .await
            .unwrap();
    }

    let mut clients = Vec::new();
    for i in 0..handles.len() {
        let mut client = DbClient::new(handles[i].client_listener_addr.clone());
        client.connect().await.unwrap();

        wait_cluster_ready(&mut client, handles.len()).await;
        clients.push(client);
    }

    (handles, clients)
}

async fn wait_cluster_ready(client: &mut DbClient, n_nodes: usize) {
    // loops until the cluster state is properly propageted through gossip for the given client
    let sleep_for = tokio::time::Duration::from_millis(100);
    loop {
        let cluster_state = client.cluster_state().await.unwrap();
        if cluster_state.nodes.len() != n_nodes {
            tokio::time::sleep(sleep_for).await;
            continue;
        } else {
            for node in cluster_state.nodes {
                if node.status != NodeStatus::Ok {
                    tokio::time::sleep(sleep_for).await;
                    continue;
                }
            }

            break;
        }
    }
}

async fn stop_servers(handles: Vec<ServerHandle>) {
    for handle in handles {
        drop(handle.shutdown);
        handle.task_handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_cluster_ping() {
    let (handles, mut clients) =
        start_servers(vec!["tests/conf/test_node_config.json".into()]).await;

    let resp = clients[0].ping().await.unwrap();

    assert_eq!(resp.message, *"PONG");
    stop_servers(handles).await;
}

// By design, any node can receive puts for any keys.
// If they do not own the given key, they act as coordinators but store data remotely by forwarding the requests.
//
// The inveriants for this test are:
//  1. the correct data is stored for the provided key
//  2. the metadata (VersionVector) is properly created no matter what cluster node is used as coordinator for PUT
//  3. The version vector has to have a single (node/version) pair since each key is put only once
#[tokio::test]
async fn test_cluster_put_get_success() {
    let (handles, mut clients) = start_servers(vec![
        "tests/conf/test_node_config.json".into();
        rand::thread_rng().gen_range(3..=10)
    ])
    .await;

    let mut used_keys = HashSet::new();
    for _ in 0..100 {
        let key: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(20)
            .map(char::from)
            .collect();
        let key: Bytes = key.into();
        if used_keys.contains(&key) {
            continue;
        }
        used_keys.insert(key.clone());
        // the data stored doesn't matter.. what changes behavior is the key, hence only keys are randomized
        let value = Bytes::from("bar");

        let client = &mut clients[0];
        let response = client
            .put(key.clone(), value.clone(), None, false)
            .await
            .unwrap();
        assert_eq!(response.message, "Ok".to_string());

        let response = client.get(key, false).await.unwrap();
        assert_eq!(response.value, value);

        // TODO: unclear if we should assert on the metadata at this level..
        // might be better to just make sure we can deserialize it and leave specific tests for
        // the Metadata component itself
        let hex_decoded_metadata = hex::decode(response.metadata).unwrap();
        let metadata = Metadata::deserialize(0, hex_decoded_metadata.into()).unwrap();
        assert_eq!(metadata.versions.n_versions(), 1);
    }

    stop_servers(handles).await;
}

// Now let's test a scenario in which we do a put (initially without providing metadata)
// then we do:
//  1. A get - to retreive the Context/Metadata stored in the previous PUT
//  2. A second put, mutating the key and passing the Metadata retreived in the previous step as PUT argument
//
// This is the intented use of the GET/PUT API - a client always has to provide the context retrieved on GET
// in order to properly update a key.
//
// The invariants of this test are:
//  1. Every PUT must successfully update the key
//    - Note that for this specific setup, no conflicts can happen since we are applying the changes sequentially.
//  2. Every node of the cluster will accept a PUT once - the caveat here is: Only quorum_config.replica nodes
//   will actually act as coordinators. So we always have to assert for this specific case and NOT all nodes
#[tokio::test]
async fn test_cluster_update_key_using_every_node_as_proxy_once() {
    let (handles, mut clients) =
        start_servers(vec!["tests/conf/test_node_config.json".into(); 10]).await;

    let key: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(20)
        .map(char::from)
        .collect();
    let key: Bytes = key.into();

    let value = Bytes::from("bar");

    let client = &mut clients[0];
    let response = client
        .put(key.clone(), value.clone(), None, false)
        .await
        .unwrap();

    assert_eq!(response.message, "Ok".to_string());

    for i in 1..clients.len() {
        let client = &mut clients[i];
        let get_response = client.get(key.clone(), false).await.unwrap();

        client
            .put(
                key.clone(),
                value.clone(),
                Some(get_response.metadata),
                false,
            )
            .await
            .unwrap();
    }

    let client = &mut clients[0];
    let get_response = client.get(key.clone(), false).await.unwrap();
    let hex_decoded_metadata = hex::decode(get_response.metadata).unwrap();
    let metadata = Metadata::deserialize(0, hex_decoded_metadata.into()).unwrap();
    // FIXME: don't hardcode
    assert_eq!(metadata.versions.n_versions(), 3);

    stop_servers(handles).await;
}

/// For now I just want to make sure the implementation so far deals with conflicts in a sane manner.
/// That is:
///  1. Stale metadata actually returns an error
///  2. Conflicts create errors
///
/// FIXME: it's very likely that this test will be flaky...
#[tokio::test]
async fn test_cluster_update_key_concurrently() {
    tracing_subscriber::fmt::init();
    let (handles, mut clients) =
        start_servers(vec!["tests/conf/test_node_config.json".into(); 20]).await;

    let key: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(20)
        .map(char::from)
        .collect();
    let key: Bytes = key.into();

    let value = Bytes::from("bar");

    let client = &mut clients[0];
    let response = client
        .put(key.clone(), value.clone(), None, false)
        .await
        .unwrap();

    assert_eq!(response.message, "Ok".to_string());

    let mut client_handles = Vec::new();

    #[derive(Default)]
    struct ErrorsSeen {
        conflict: usize,
        stale_context: usize,
    }

    let saw_failure = Arc::new(Mutex::new(ErrorsSeen::default()));
    for _ in 0..clients.len() {
        // add random string as value so that we can assert on the final value PUT at the end
        let value: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let value: Bytes = value.into();
        let key = key.clone();
        let saw_failure = saw_failure.clone();
        let mut client = clients.remove(0);
        client_handles.push(tokio::spawn(async move {
            let get_response = client.get(key.clone(), false).await.unwrap();

            match client
                .put(
                    key.clone(),
                    value.clone(),
                    Some(get_response.metadata),
                    false,
                )
                .await
            {
                Err(err) => match err {
                    ClientError::QuorumNotReached {
                        operation,
                        reason: _,
                        errors,
                    } => {
                        assert_eq!(operation, *"Put");
                        for err in errors {
                            match err {
                                ServerError::Client(ClientError::Internal(
                                    error::Internal::Unknown { reason },
                                )) => {
                                    assert!(
                                        reason.contains("conflicts are currently not implemented")
                                    );
                                    saw_failure.lock().unwrap().conflict += 1;
                                }
                                _ => {
                                    panic!("Unexpected error: {err}")
                                }
                            }
                        }
                    }
                    ClientError::InvalidRequest(InvalidRequest::StaleContextProvided) => {
                        saw_failure.lock().unwrap().stale_context += 1;
                    }
                    _ => {
                        panic!("Invalid error {err}");
                    }
                },

                Ok(_) => {}
            }
        }));
    }

    for client_handle in client_handles {
        client_handle.await.unwrap();
    }

    // finally, since we are not allowing sloppy quorums and this test is configured for strong consistency (r: 2, w:2 n: 3), every
    // node that we use to query should return the same data.
    let mut returned_values = HashSet::new();
    for i in 0..handles.len() {
        let mut client = DbClient::new(handles[i].client_listener_addr.clone());
        client.connect().await.unwrap();

        let get_result = client.get(key.clone(), false).await.unwrap();
        returned_values.insert(get_result.value);
    }

    println!("DEBUG: {:?}", returned_values);
    assert_eq!(returned_values.len(), 1);

    assert!(saw_failure.lock().unwrap().conflict > 0);
    assert!(saw_failure.lock().unwrap().stale_context > 0);
    stop_servers(handles).await;
}

#[tokio::test]
async fn test_cluster_stale_context_provided() {
    let (handles, mut clients) =
        start_servers(vec!["tests/conf/test_node_config.json".into(); 3]).await;

    let key: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(20)
        .map(char::from)
        .collect();
    let key: Bytes = key.into();

    let value_for_first_put = Bytes::from("First Put");

    let client = &mut clients[0];
    let response = client
        .put(key.clone(), value_for_first_put.clone(), None, false)
        .await
        .unwrap();

    assert_eq!(response.message, "Ok".to_string());
    let first_get_response = client.get(key.clone(), false).await.unwrap();
    assert_eq!(first_get_response.value, value_for_first_put);

    let value_for_second_put = Bytes::from("Second Put");
    client
        .put(
            key.clone(),
            value_for_second_put.clone(),
            Some(first_get_response.metadata.clone()),
            false,
        )
        .await
        .unwrap();

    let second_get_response = client.get(key.clone(), false).await.unwrap();
    assert_eq!(second_get_response.value, value_for_second_put);

    // now we try a third update with the first_get_resposne metadata - this must fail
    let value_for_third_put = Bytes::from("Third Put");
    let err = client
        .put(
            key.clone(),
            value_for_third_put.clone(),
            Some(first_get_response.metadata),
            false,
        )
        .await
        .err()
        .unwrap();

    assert!(matches!(
        err,
        ClientError::InvalidRequest(InvalidRequest::StaleContextProvided)
    ));

    let final_get_result = client.get(key, false).await.unwrap();
    assert_eq!(final_get_result.value, value_for_second_put);

    stop_servers(handles).await;
}

#[tokio::test]
async fn test_cluster_key_not_found() {
    let (handles, mut clients) = start_servers(vec![
        "tests/conf/test_node_config.json".into(),
        "tests/conf/test_node_config.json".into(),
        "tests/conf/test_node_config.json".into(),
    ])
    .await;

    let key = Bytes::from("foo");

    let client = &mut clients[0];
    let err = client.get(key, false).await.err().unwrap();

    match err {
        client::error::Error::NotFound { key: _ } => {}
        _ => {
            panic!("Unexpected error: {}", err);
        }
    }

    stop_servers(handles).await;
}

#[tokio::test]
async fn test_cluster_put_no_quorum() {
    let (handles, mut clients) =
        start_servers(vec!["tests/conf/test_node_config.json".into()]).await;

    let key = Bytes::from("foo");
    let value = Bytes::from("bar");

    let err = clients[0]
        .put(key.clone(), value.clone(), None, false)
        .await
        .err()
        .unwrap();

    match err {
        client::error::Error::QuorumNotReached {
            operation,
            reason: _,
            errors: _,
        } => {
            assert_eq!(&operation, "Put");
        }
        _ => {
            panic!("Unexpected error: {}", err);
        }
    }

    stop_servers(handles).await;
}

#[tokio::test]
async fn test_cluster_get_no_quorum() {
    let (mut handles, mut clients) = start_servers(vec![
        "tests/conf/test_node_config.json".into(),
        "tests/conf/test_node_config.json".into(),
        "tests/conf/test_node_config.json".into(),
    ])
    .await;

    let key = Bytes::from("foo");
    let value = Bytes::from("bar");

    {
        let client = &mut clients[0];
        let response = client
            .put(key.clone(), value.clone(), None, false)
            .await
            .unwrap();
        assert_eq!(response.message, "Ok".to_string());
    }

    // By stopping 2 nodes, we make it impossible for quorum to be reached
    let handle = handles.remove(1);
    clients.remove(1);
    drop(handle.shutdown);
    handle.task_handle.await.unwrap();
    let handle = handles.remove(1);
    clients.remove(1);
    drop(handle.shutdown);
    handle.task_handle.await.unwrap();

    let client = &mut clients[0];
    let err = client.get(key, false).await.err().unwrap();
    match err {
        client::error::Error::QuorumNotReached {
            operation,
            reason: _,
            errors: _,
        } => {
            assert_eq!(&operation, "Get");
        }
        _ => {
            panic!("Unexpected error: {}", err);
        }
    }

    stop_servers(handles).await;
}
