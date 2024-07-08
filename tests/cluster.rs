use std::{
    collections::HashSet,
    path::PathBuf,
    sync::atomic::Ordering::SeqCst,
    sync::{atomic::AtomicUsize, Arc},
};

use bytes::Bytes;
use error::Error;
use rand::Rng;
use rldb::{
    client::{db_client::DbClient, Client},
    cluster::state::NodeStatus,
    error,
    persistency::storage::Value,
    server::Server,
    utils::generate_random_ascii_string,
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
        let key: Bytes = generate_random_ascii_string(20).into();
        if used_keys.contains(&key) {
            continue;
        }
        used_keys.insert(key.clone());
        let value = Value::random();

        let client = &mut clients[0];
        let response = client
            .put(key.clone(), value.clone(), None, false)
            .await
            .unwrap();
        assert_eq!(response.message, "Ok".to_string());

        let mut resp = client.get(key).await.unwrap();
        assert_eq!(resp.values.len(), 1);
        let first_entry = resp.values.remove(0);
        assert_eq!(first_entry, value);
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

    let key: Bytes = generate_random_ascii_string(20).into();

    let value = Value::random();

    let client = &mut clients[0];
    let response = client
        .put(key.clone(), value.clone(), None, false)
        .await
        .unwrap();

    assert_eq!(response.message, "Ok".to_string());

    for i in 1..clients.len() {
        let client = &mut clients[i];
        let resp = client.get(key.clone()).await.unwrap();
        let mut values = resp.values.clone();

        assert_eq!(values.len(), 1);
        let entry = values.remove(0);
        assert_eq!(entry, value);
        client
            .put(key.clone(), value.clone(), Some(resp.context.into()), false)
            .await
            .unwrap();
    }

    let client = &mut clients[0];
    let resp = client.get(key.clone()).await.unwrap();
    let mut values = resp.values.clone();
    assert_eq!(values.len(), 1);
    let entry = values.remove(0);
    assert_eq!(entry, value);

    stop_servers(handles).await;
}

// For now I just want to make sure the implementation so far deals with conflicts in a sane manner.
// That is:
//  1. Stale metadata actually returns an error
//  2. Conflicts store multiple versions of the data as expected
#[ignore]
#[tokio::test]
async fn test_cluster_update_key_concurrently() {
    let n_nodes = 20;
    let (handles, mut clients) =
        start_servers(vec!["tests/conf/test_node_config.json".into(); n_nodes]).await;

    let key: Bytes = generate_random_ascii_string(20).into();

    let value = Value::random();

    let client = &mut clients[0];
    let response = client
        .put(key.clone(), value.clone(), None, false)
        .await
        .unwrap();

    assert_eq!(response.message, "Ok".to_string());

    let mut client_handles = Vec::new();

    let errors_seen: Arc<AtomicUsize> = Default::default();
    // start the counter with 1 because we executed one PUT before looping through all clients
    let successes_seen: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(1));
    for _ in 0..clients.len() {
        let value = Value::random();
        let key = key.clone();
        let errors_seen = errors_seen.clone();
        let successes_seen = successes_seen.clone();
        let mut client = clients.remove(0);

        // adding some random jitter to make sure the test is coupled with timing.
        let sleep_jitter = rand::thread_rng().gen_range(1..=10);
        client_handles.push(tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(sleep_jitter)).await;
            let get_response = client.get(key.clone()).await;

            // This error scenario is not super easy to understand.
            //  1. an initial PUT stores a value for the required key
            //  2. One of the 20 concurrent PUTs we are doing start executing and overrides the value
            //   only locally and delays the replication (time sensitive)
            //  3. Another node does the same thing, updating only the its own local storage
            //  4. yet Another node tries a GET while the cluster is in this inconsistent state:
            //    [Node A: X] [Node B: Y] [Node C: Z] -> While in this state, all GETs will fail with NoQuorum
            //  5. By the end of these 3 concurrent PUTs, we will have every value on every node - so 3 conflicts
            //    [Node A: X,Y,Z] [Node B: X,Y,Z] [Node C: X,Y,Z]
            let context = if get_response.is_err() {
                errors_seen.fetch_add(1, SeqCst);
                return;
            } else {
                Some(get_response.unwrap().context)
            };

            match client.put(key.clone(), value.clone(), context, false).await {
                Err(err) => match err {
                    Error::QuorumNotReached {
                        operation,
                        reason: _,
                        errors,
                    } => {
                        assert_eq!(operation, *"Put");
                        for err in errors {
                            assert!(err.is_stale_context_provided())
                        }

                        errors_seen.fetch_add(1, SeqCst);
                    }
                    _ => {
                        panic!("Invalid error {err}");
                    }
                },

                Ok(_) => {
                    successes_seen.fetch_add(1, SeqCst);
                }
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

        let get_result = client.get(key.clone()).await.unwrap();
        // We can have 3 scenarios:
        //  1, due to the jitter, only a single PUT succeeds and all the others fail. In this case velues.len() == 1;
        //  2, due to the jitter, only 2 PUTs can succeed (concurrently - causing a conflict)
        //  3. due to the jitter (or lack thereof), at most 3 puts can be successful and 3 conflicting versions will be generated.
        //    3 is the max number of conflicts in this case because only 3 nodes in the cluster can accept puts for a given key.
        assert_eq!(get_result.values.len(), successes_seen.load(SeqCst));
        for v in get_result.values {
            returned_values.insert(v);
        }
    }

    println!(
        "DEBUG: success: {:?}, failures: {:?} - {:?}",
        successes_seen.load(SeqCst),
        errors_seen.load(SeqCst),
        returned_values
    );

    assert_eq!(returned_values.len(), successes_seen.load(SeqCst));
    assert_eq!(
        successes_seen.load(SeqCst) + errors_seen.load(SeqCst),
        n_nodes + 1
    );

    stop_servers(handles).await;
}

#[tokio::test]
async fn test_cluster_stale_context_provided() {
    let (handles, mut clients) =
        start_servers(vec!["tests/conf/test_node_config.json".into(); 3]).await;

    let key: Bytes = generate_random_ascii_string(20).into();

    let value_for_first_put = Value::random();

    let client = &mut clients[0];
    let response = client
        .put(key.clone(), value_for_first_put.clone(), None, false)
        .await
        .unwrap();

    assert_eq!(response.message, "Ok".to_string());
    let mut first_get_response = client.get(key.clone()).await.unwrap();
    assert_eq!(first_get_response.values.len(), 1);
    let first_entry = first_get_response.values.remove(0);
    assert_eq!(first_entry, value_for_first_put);

    let value_for_second_put = Value::random();
    client
        .put(
            key.clone(),
            value_for_second_put.clone(),
            Some(first_get_response.context.clone().into()),
            false,
        )
        .await
        .unwrap();

    let mut second_get_response = client.get(key.clone()).await.unwrap();
    assert_eq!(second_get_response.values.len(), 1);
    let second_entry = second_get_response.values.remove(0);
    assert_eq!(second_entry, value_for_second_put);

    // now we try a third update with the first_get_resposne metadata - this must fail
    let value_for_third_put = Value::random();
    let err = client
        .put(
            key.clone(),
            value_for_third_put.clone(),
            Some(first_get_response.context.into()),
            false,
        )
        .await
        .err()
        .unwrap();

    assert!(err.is_stale_context_provided());

    let mut final_get_response = client.get(key).await.unwrap();
    assert_eq!(final_get_response.values.len(), 1);
    let final_get_entry = final_get_response.values.remove(0);
    assert_eq!(final_get_entry, value_for_second_put);

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
    let err = client.get(key).await.err().unwrap();

    match err {
        Error::NotFound { key: _ } => {}
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
    let value = Value::random();

    let err = clients[0]
        .put(key.clone(), value.clone(), None, false)
        .await
        .err()
        .unwrap();

    match err {
        Error::QuorumNotReached {
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
    let value = Value::random();

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
    let err = client.get(key).await.err().unwrap();
    match err {
        Error::QuorumNotReached {
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
