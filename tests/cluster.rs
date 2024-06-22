use std::{collections::HashSet, path::PathBuf};

use bytes::Bytes;
use rand::{distributions::Alphanumeric, Rng};
use rldb::{
    client::{self, db_client::DbClient, Client},
    cluster::state::NodeStatus,
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

async fn start_servers(configs: Vec<PathBuf>) -> Vec<(ServerHandle, DbClient)> {
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

    std::iter::zip(handles, clients).collect()
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

async fn stop_servers(handles: Vec<(ServerHandle, DbClient)>) {
    for handle in handles {
        drop(handle.0.shutdown);
        handle.0.task_handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_cluster_ping() {
    let mut handles = start_servers(vec!["tests/conf/test_node_config.json".into()]).await;

    let resp = handles[0].1.ping().await.unwrap();

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
    let mut handles = start_servers(vec![
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

        let client = &mut handles[0].1;
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
//  2. Every node of the cluster will be used as coordinator once - this means the version vector at the end has to have
//   the same number of versions as the number of nodes in the cluster
#[tokio::test]
async fn test_cluster_update_key_using_every_node_as_coorinator_once() {
    let mut handles = start_servers(vec!["tests/conf/test_node_config.json".into(); 10]).await;

    let key: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(20)
        .map(char::from)
        .collect();
    let key: Bytes = key.into();

    let value = Bytes::from("bar");

    let client = &mut handles[0].1;
    let response = client
        .put(key.clone(), value.clone(), None, false)
        .await
        .unwrap();

    assert_eq!(response.message, "Ok".to_string());

    for i in 1..handles.len() {
        let client = &mut handles[i].1;
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

    let client = &mut handles[0].1;
    let get_response = client.get(key.clone(), false).await.unwrap();
    let hex_decoded_metadata = hex::decode(get_response.metadata).unwrap();
    let metadata = Metadata::deserialize(0, hex_decoded_metadata.into()).unwrap();
    assert_eq!(metadata.versions.n_versions(), handles.len());

    stop_servers(handles).await;
}

#[tokio::test]
async fn test_cluster_key_not_found() {
    let mut handles = start_servers(vec![
        "tests/conf/test_node_config.json".into(),
        "tests/conf/test_node_config.json".into(),
        "tests/conf/test_node_config.json".into(),
    ])
    .await;

    let key = Bytes::from("foo");

    let client = &mut handles[0].1;
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
    let mut handles = start_servers(vec!["tests/conf/test_node_config.json".into()]).await;

    let key = Bytes::from("foo");
    let value = Bytes::from("bar");

    let err = handles[0]
        .1
        .put(key.clone(), value.clone(), None, false)
        .await
        .err()
        .unwrap();

    match err {
        client::error::Error::QuorumNotReached {
            operation,
            reason: _,
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
    let mut handles = start_servers(vec![
        "tests/conf/test_node_config.json".into(),
        "tests/conf/test_node_config.json".into(),
        "tests/conf/test_node_config.json".into(),
    ])
    .await;

    let key = Bytes::from("foo");
    let value = Bytes::from("bar");

    {
        let client = &mut handles[0].1;
        let response = client
            .put(key.clone(), value.clone(), None, false)
            .await
            .unwrap();
        assert_eq!(response.message, "Ok".to_string());
    }

    // By stopping 2 nodes, we make it impossible for quorum to be reached
    let handle = handles.remove(1);
    drop(handle.0.shutdown);
    handle.0.task_handle.await.unwrap();
    let handle = handles.remove(1);
    drop(handle.0.shutdown);
    handle.0.task_handle.await.unwrap();

    let client = &mut handles[0].1;
    let err = client.get(key, false).await.err().unwrap();
    match err {
        client::error::Error::QuorumNotReached {
            operation,
            reason: _,
        } => {
            assert_eq!(&operation, "Get");
        }
        _ => {
            panic!("Unexpected error: {}", err);
        }
    }

    stop_servers(handles).await;
}
