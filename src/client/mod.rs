use bytes::Bytes;
use tokio::{
    io::{AsyncWriteExt, BufStream},
    net::{TcpStream, ToSocketAddrs},
};

use crate::cmd;

pub struct DbClient {
    stream: BufStream<TcpStream>,
}

impl DbClient {
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> anyhow::Result<Self> {
        Ok(Self {
            stream: BufStream::new(TcpStream::connect(addr).await?),
        })
    }

    pub async fn ping(&mut self) -> anyhow::Result<Bytes> {
        let ping_cmd = cmd::ping::Ping;
        let serialized = ping_cmd.serialize();

        self.stream.write_all(&serialized).await?;
        self.stream.flush().await?;

        let response = cmd::ping::Ping::parse_response(&mut self.stream).await?;

        Ok(response)
    }
}
