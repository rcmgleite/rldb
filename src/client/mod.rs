use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, ToSocketAddrs},
};

use crate::{
    cluster::{gossip::JsonSerializableNode, ring_state::Node},
    cmd::{
        self,
        cluster::{
            heartbeat::{HeartbeatResponse, CMD_CLUSTER_HEARTBEAT},
            join_cluster::{JoinClusterResponse, CMD_CLUSTER_JOIN_CLUSTER},
        },
        get::{GetResponse, GET_CMD},
        ping::{PingResponse, PING_CMD},
        put::{PutResponse, PUT_CMD},
    },
    server::Request,
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

    pub async fn ping(&mut self) -> anyhow::Result<PingResponse> {
        let ping_cmd = cmd::ping::Ping;
        let req = Request::serialize(ping_cmd);

        self.stream.write_all(&req).await?;

        let response = Request::try_from_async_read(&mut self.stream).await?;
        assert_eq!(response.id, PING_CMD);
        Ok(serde_json::from_slice(&response.payload.unwrap())?)
    }

    pub async fn get(&mut self, key: String) -> anyhow::Result<GetResponse> {
        let get_cmd = cmd::get::Get::new(key);
        let req = Request::serialize(get_cmd);

        self.stream.write_all(&req).await?;

        let response = Request::try_from_async_read(&mut self.stream).await?;
        assert_eq!(response.id, GET_CMD);
        Ok(serde_json::from_slice(&response.payload.unwrap())?)
    }

    pub async fn put(&mut self, key: String, value: String) -> anyhow::Result<PutResponse> {
        let put_cmd = cmd::put::Put::new(key, value);
        let req = Request::serialize(put_cmd);

        self.stream.write_all(&req).await?;

        let response = Request::try_from_async_read(&mut self.stream).await?;
        assert_eq!(response.id, PUT_CMD);
        Ok(serde_json::from_slice(&response.payload.unwrap())?)
    }

    pub async fn heartbeat(&mut self, known_nodes: Vec<Node>) -> anyhow::Result<HeartbeatResponse> {
        let serializible_nodes = known_nodes
            .into_iter()
            .map(JsonSerializableNode::from)
            .collect();

        let cmd = cmd::cluster::heartbeat::Heartbeat::new(serializible_nodes);
        let req = Request::serialize(cmd);

        self.stream.write_all(&req).await?;

        let response = Request::try_from_async_read(&mut self.stream).await?;
        assert_eq!(response.id, CMD_CLUSTER_HEARTBEAT);
        Ok(serde_json::from_slice(&response.payload.unwrap())?)
    }

    pub async fn join_cluster(
        &mut self,
        known_cluster_node_addr: String,
    ) -> anyhow::Result<JoinClusterResponse> {
        let cmd = cmd::cluster::join_cluster::JoinCluster::new(known_cluster_node_addr);
        let req = Request::serialize(cmd);

        self.stream.write_all(&req).await?;

        let response = Request::try_from_async_read(&mut self.stream).await?;
        assert_eq!(response.id, CMD_CLUSTER_JOIN_CLUSTER);
        Ok(serde_json::from_slice(&response.payload.unwrap())?)
    }
}
