use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, ToSocketAddrs},
};

use crate::{
    cmd::{
        self,
        get::{GetResponse, GET_CMD},
        ping::{PingResponse, PING_CMD},
    },
    server::Message,
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
        let msg = Message::serialize(ping_cmd);

        self.stream.write_all(&msg).await?;

        let response = Message::try_from_async_read(&mut self.stream).await?;
        assert_eq!(response.id, PING_CMD);
        Ok(serde_json::from_slice(&response.payload.unwrap())?)
    }

    pub async fn get(&mut self, key: String) -> anyhow::Result<GetResponse> {
        let get_cmd = cmd::get::Get::new(key);
        let msg = Message::serialize(get_cmd);

        self.stream.write_all(&msg).await?;

        let response = Message::try_from_async_read(&mut self.stream).await?;
        assert_eq!(response.id, GET_CMD);
        Ok(serde_json::from_slice(&response.payload.unwrap())?)
    }
}
