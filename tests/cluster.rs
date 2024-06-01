use std::path::PathBuf;

use bytes::Bytes;
use rldb::{
    client::{self, db_client::DbClient, Client},
    cluster::state::NodeStatus,
    server::Server,
};
use serial_test::serial;
use tokio::{
    sync::oneshot::{channel, Receiver, Sender},
    task::JoinHandle,
};
// TODO - extract to utils
async fn shutdown_future(receiver: Receiver<()>) {
    let _ = receiver.await;
}

async fn start_servers(configs: Vec<PathBuf>) -> Vec<(JoinHandle<()>, Sender<()>)> {
    let mut handles = Vec::new();
    for config in configs {
        let mut server = Server::from_config(config)
            .await
            .expect("Unable to construct server from config");
        let (shutdown_sender, shutdown_receiver) = channel();
        let server_handle = tokio::spawn(async move {
            server
                .run(shutdown_future(shutdown_receiver))
                .await
                .unwrap();
        });

        handles.push((server_handle, shutdown_sender));
    }

    handles
}

async fn wait_cluster_ready(client: &mut DbClient, n_nodes: usize) {
    // loops until the cluster state is properly propageted through gossip
    loop {
        let cluster_state = client.cluster_state().await.unwrap();
        if cluster_state.nodes.len() != n_nodes {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        } else {
            for node in cluster_state.nodes {
                if node.status != NodeStatus::Ok {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                }
            }

            break;
        }
    }
}

#[tokio::test]
#[serial]
async fn test_cluster_put_get_success() {
    let handles = start_servers(vec![
        "tests/conf/test_cluster_node_1.json".into(),
        "tests/conf/test_cluster_node_2.json".into(),
        "tests/conf/test_cluster_node_3.json".into(),
    ])
    .await;

    let mut client = DbClient::new("127.0.0.1:3002".to_string());
    client.connect().await.unwrap();
    client.join_cluster("127.0.0.1:3001".into()).await.unwrap();

    client.join_cluster("127.0.0.1:3003".into()).await.unwrap();

    // loops until the cluster state is properly propageted through gossip
    wait_cluster_ready(&mut client, 3).await;

    let key = Bytes::from("foo");
    let value = Bytes::from("bar");

    let response = client.put(key.clone(), value.clone(), false).await.unwrap();
    assert_eq!(response.message, "Ok".to_string());

    let response = client.get(key, false).await.unwrap();
    assert_eq!(response.value, value);

    for handle in handles {
        drop(handle.1);
        handle.0.await.unwrap();
    }
}

#[tokio::test]
#[serial]
async fn test_cluster_key_not_found() {
    let handles = start_servers(vec![
        "tests/conf/test_cluster_node_1.json".into(),
        "tests/conf/test_cluster_node_2.json".into(),
        "tests/conf/test_cluster_node_3.json".into(),
    ])
    .await;

    let mut client = DbClient::new("127.0.0.1:3002".to_string());
    client.connect().await.unwrap();
    client.join_cluster("127.0.0.1:3001".into()).await.unwrap();

    client.join_cluster("127.0.0.1:3003".into()).await.unwrap();

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
        drop(handle.1);
        handle.0.await.unwrap();
    }
}

#[tokio::test]
#[serial]
async fn test_cluster_put_no_quorum() {
    let handles = start_servers(vec!["tests/conf/test_cluster_node_1.json".into()]).await;

    let mut client = DbClient::new("127.0.0.1:3001".to_string());
    client.connect().await.unwrap();

    let key = Bytes::from("foo");
    let value = Bytes::from("bar");

    let err = client
        .put(key.clone(), value.clone(), false)
        .await
        .err()
        .unwrap();

    match err {
        client::error::Error::QuorumNotReached {
            operation,
            required,
            got,
        } => {
            assert_eq!(&operation, "Put");
            // 2 are required to succeed the put
            assert_eq!(required, 2);
            // only one will actually have succeeded (self)
            // Note that this behavior is still kind of odd.. a cluster with a single node could possibly
            // be considered in bad state and requests should likely be rejected at this point...
            assert_eq!(got, 1);
        }
        _ => {
            panic!("Unexpected error: {}", err);
        }
    }

    for handle in handles {
        drop(handle.1);
        handle.0.await.unwrap();
    }
}

#[tokio::test]
#[serial]
async fn test_cluster_get_no_quorum() {
    let mut handles = start_servers(vec![
        "tests/conf/test_cluster_node_1.json".into(),
        "tests/conf/test_cluster_node_2.json".into(),
        "tests/conf/test_cluster_node_3.json".into(),
    ])
    .await;

    let mut client = DbClient::new("127.0.0.1:3002".to_string());
    client.connect().await.unwrap();
    client.join_cluster("127.0.0.1:3001".into()).await.unwrap();

    client.join_cluster("127.0.0.1:3003".into()).await.unwrap();

    // loops until the cluster state is properly propageted through gossip
    wait_cluster_ready(&mut client, 3).await;

    let key = Bytes::from("foo");
    let value = Bytes::from("bar");

    let response = client.put(key.clone(), value.clone(), false).await.unwrap();
    assert_eq!(response.message, "Ok".to_string());

    // kill 2 of the serves
    let handle_3001 = handles.remove(0);
    drop(handle_3001.1);
    handle_3001.0.await.unwrap();
    let handle_3003 = handles.remove(1);
    drop(handle_3003.1);
    handle_3003.0.await.unwrap();

    let err = client.get(key, false).await.err().unwrap();
    match err {
        client::error::Error::QuorumNotReached {
            operation,
            required,
            got,
        } => {
            assert_eq!(&operation, "Get");
            assert_eq!(required, 2);
            // one Get (self on local Db) succeeded
            assert_eq!(got, 1);
        }
        _ => {
            panic!("Unexpected error: {}", err);
        }
    }

    for handle in handles {
        drop(handle.1);
        handle.0.await.unwrap();
    }
}
