use std::path::PathBuf;

use bytes::Bytes;
use rldb::{
    client::{self, db_client::DbClient, Client},
    cluster::state::NodeStatus,
    persistency::Metadata,
    server::Server,
};
use tokio::{
    sync::oneshot::{channel, Receiver, Sender},
    task::JoinHandle,
};
// TODO - extract to utils
async fn shutdown_future(receiver: Receiver<()>) {
    let _ = receiver.await;
}

struct ServerHandle {
    task_handle: JoinHandle<()>,
    shutdown: Sender<()>,
    client_listener_addr: String,
}

async fn start_servers(configs: Vec<PathBuf>) -> Vec<ServerHandle> {
    let mut handles = Vec::new();
    for config in configs {
        let mut server = Server::from_config(config)
            .await
            .expect("Unable to construct server from config");
        let client_listener_addr = server.client_listener_local_addr().unwrap().to_string();
        let (shutdown_sender, shutdown_receiver) = channel();
        let server_handle = tokio::spawn(async move {
            server
                .run(shutdown_future(shutdown_receiver))
                .await
                .unwrap();
        });

        handles.push(ServerHandle {
            task_handle: server_handle,
            shutdown: shutdown_sender,
            client_listener_addr,
        });
    }

    handles
}

async fn wait_cluster_ready(client: &mut DbClient, n_nodes: usize) {
    // loops until the cluster state is properly propageted through gossip
    loop {
        let cluster_state = client.cluster_state().await.unwrap();
        if cluster_state.nodes.len() != n_nodes {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            continue;
        } else {
            for node in cluster_state.nodes {
                if node.status != NodeStatus::Ok {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                }
            }

            break;
        }
    }
}

#[tokio::test]
async fn test_cluster_put_get_success() {
    let handles = start_servers(vec![
        "tests/conf/test_cluster_node_1.json".into(),
        "tests/conf/test_cluster_node_2.json".into(),
        "tests/conf/test_cluster_node_3.json".into(),
    ])
    .await;

    let mut client = DbClient::new(handles[0].client_listener_addr.clone());
    client.connect().await.unwrap();
    client
        .join_cluster(handles[1].client_listener_addr.clone())
        .await
        .unwrap();
    client
        .join_cluster(handles[2].client_listener_addr.clone())
        .await
        .unwrap();

    // loops until the cluster state is properly propageted through gossip
    wait_cluster_ready(&mut client, 3).await;

    let key = Bytes::from("foo");
    let value = Bytes::from("bar");

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

    for handle in handles {
        drop(handle.shutdown);
        handle.task_handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_cluster_key_not_found() {
    let handles = start_servers(vec![
        "tests/conf/test_cluster_node_1.json".into(),
        "tests/conf/test_cluster_node_2.json".into(),
        "tests/conf/test_cluster_node_3.json".into(),
    ])
    .await;

    let mut client = DbClient::new(handles[0].client_listener_addr.clone());
    client.connect().await.unwrap();
    client
        .join_cluster(handles[1].client_listener_addr.clone())
        .await
        .unwrap();
    client
        .join_cluster(handles[2].client_listener_addr.clone())
        .await
        .unwrap();

    wait_cluster_ready(&mut client, 3).await;

    let key = Bytes::from("foo");

    let err = client.get(key, false).await.err().unwrap();

    match err {
        client::error::Error::NotFound { key: _ } => {}
        _ => {
            panic!("Unexpected error: {}", err);
        }
    }

    for handle in handles {
        drop(handle.shutdown);
        handle.task_handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_cluster_put_no_quorum() {
    let handles = start_servers(vec!["tests/conf/test_cluster_node_1.json".into()]).await;

    let mut client = DbClient::new(handles[0].client_listener_addr.clone());
    client.connect().await.unwrap();

    let key = Bytes::from("foo");
    let value = Bytes::from("bar");

    let err = client
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

    for handle in handles {
        drop(handle.shutdown);
        handle.task_handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_cluster_get_no_quorum() {
    let mut handles = start_servers(vec![
        "tests/conf/test_cluster_node_1.json".into(),
        "tests/conf/test_cluster_node_2.json".into(),
        "tests/conf/test_cluster_node_3.json".into(),
    ])
    .await;

    let mut client = DbClient::new(handles[0].client_listener_addr.clone());
    client.connect().await.unwrap();
    client
        .join_cluster(handles[1].client_listener_addr.clone())
        .await
        .unwrap();
    client
        .join_cluster(handles[2].client_listener_addr.clone())
        .await
        .unwrap();

    // loops until the cluster state is properly propageted through gossip
    wait_cluster_ready(&mut client, 3).await;

    let key = Bytes::from("foo");
    let value = Bytes::from("bar");

    let response = client
        .put(key.clone(), value.clone(), None, false)
        .await
        .unwrap();
    assert_eq!(response.message, "Ok".to_string());

    // kill 2 of the serves
    let handle = handles.remove(1);
    drop(handle.shutdown);
    handle.task_handle.await.unwrap();
    let handle = handles.remove(1);
    drop(handle.shutdown);
    handle.task_handle.await.unwrap();

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

    for handle in handles {
        drop(handle.shutdown);
        handle.task_handle.await.unwrap();
    }
}
