use bytes::Bytes;
use rldb::{
    client::{db_client::DbClient, Client},
    server::Server,
};
use tokio::sync::oneshot::{channel, Receiver};

// TODO: extract to utils
async fn shutdown(receiver: Receiver<()>) {
    let _ = receiver.await;
}

/// Simple PUT followed by GET in standalone mode
#[tokio::test]
async fn test_standalone_put_get_success() {
    let mut server = Server::from_config("tests/conf/test_standalone.json".into())
        .await
        .expect("Unable to construct server from config");
    let server_client_listener_addr = server.client_listener_local_addr().unwrap().to_string();
    let (shutdown_sender, shutdown_receiver) = channel();
    let server_handle = tokio::spawn(async move {
        server.run(shutdown(shutdown_receiver)).await.unwrap();
    });

    let mut client = DbClient::new(server_client_listener_addr);
    client.connect().await.unwrap();

    let key = Bytes::from("A key");
    let value = Bytes::from("A value");

    let response = client.put(key.clone(), value.clone(), false).await.unwrap();
    assert_eq!(response.message, "Ok".to_string());

    let response = client.get(key, false).await.unwrap();
    assert_eq!(response.value, value);

    drop(shutdown_sender);
    server_handle.await.unwrap();
}

/// Get error case -> NotFound
#[tokio::test]
async fn test_standalone_get_not_found() {
    let mut server = Server::from_config("tests/conf/test_standalone.json".into())
        .await
        .expect("Unable to construct server from config");
    let server_client_listener_addr = server.client_listener_local_addr().unwrap().to_string();
    let (shutdown_sender, shutdown_receiver) = channel();
    let server_handle = tokio::spawn(async move {
        server.run(shutdown(shutdown_receiver)).await.unwrap();
    });

    let mut client = DbClient::new(server_client_listener_addr);
    client.connect().await.unwrap();

    let key_to_lookup = Bytes::from("A key");

    let err = client
        .get(key_to_lookup.clone(), false)
        .await
        .err()
        .unwrap();

    match err {
        rldb::client::error::Error::NotFound { key } => {
            assert_eq!(key, key_to_lookup);
        }
        _ => {
            panic!("Unexpected error {:?}", err);
        }
    }

    drop(shutdown_sender);
    server_handle.await.unwrap();
}
