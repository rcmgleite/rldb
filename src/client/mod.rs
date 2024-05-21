use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, ToSocketAddrs},
};

use crate::{
    cluster::{heartbeat::JsonSerializableNode, state::Node},
    cmd::{self},
    server::message::Message,
};

pub struct DbClient {
    stream: TcpStream,
}

impl DbClient {
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> anyhow::Result<Self> {
        Ok(Self {
            stream: TcpStream::connect(addr).await?,
        })
    }

    pub async fn ping(&mut self) -> anyhow::Result<serde_json::Value> {
        let ping_cmd = cmd::ping::Ping;
        let req = Message::from(ping_cmd).serialize();

        self.stream.write_all(&req).await?;

        let response = Message::try_from_async_read(&mut self.stream).await?;
        Ok(serde_json::from_slice(&response.payload.unwrap())?)
    }

    pub async fn get(&mut self, key: String) -> anyhow::Result<serde_json::Value> {
        let get_cmd = cmd::get::Get::new(key);
        let req = Message::from(get_cmd).serialize();

        self.stream.write_all(&req).await?;

        let response = Message::try_from_async_read(&mut self.stream).await?;
        Ok(serde_json::from_slice(&response.payload.unwrap())?)
    }

    pub async fn put(&mut self, key: String, value: String) -> anyhow::Result<serde_json::Value> {
        let put_cmd = cmd::put::Put::new(key, value);
        let req = Message::from(put_cmd).serialize();

        self.stream.write_all(&req).await?;

        let response = Message::try_from_async_read(&mut self.stream).await?;
        Ok(serde_json::from_slice(&response.payload.unwrap())?)
    }

    pub async fn heartbeat(&mut self, known_nodes: Vec<Node>) -> anyhow::Result<serde_json::Value> {
        let serializible_nodes = known_nodes
            .into_iter()
            .map(JsonSerializableNode::from)
            .collect();

        let cmd = cmd::cluster::heartbeat::Heartbeat::new(serializible_nodes);
        let req = Message::from(cmd).serialize();

        self.stream.write_all(&req).await?;

        let response = Message::try_from_async_read(&mut self.stream).await?;
        Ok(serde_json::from_slice(&response.payload.unwrap())?)
    }

    pub async fn join_cluster(
        &mut self,
        known_cluster_node_addr: String,
    ) -> anyhow::Result<serde_json::Value> {
        let cmd = cmd::cluster::join_cluster::JoinCluster::new(known_cluster_node_addr);
        let req = Message::from(cmd).serialize();

        self.stream.write_all(&req).await?;

        let response = Message::try_from_async_read(&mut self.stream).await?;
        Ok(serde_json::from_slice(&response.payload.unwrap())?)
    }
}
