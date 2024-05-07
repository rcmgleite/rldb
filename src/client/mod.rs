use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, ToSocketAddrs},
};

use crate::{
    cmd::{self, ping::PingResponse, ping::PING_CMD},
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
}
