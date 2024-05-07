pub mod ping;

use anyhow::anyhow;
use std::fmt::Debug;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, BufStream},
    net::TcpStream,
};

use self::ping::CMD_NAME as PING_CMD_NAME;

pub trait AsyncBufReadWrite: AsyncBufRead + AsyncWrite + Send + Unpin {}
impl AsyncBufReadWrite for BufStream<TcpStream> {}

pub enum Command {
    Ping(ping::Ping),
}

impl Command {
    pub async fn execute<RW>(self, stream: &mut RW) -> anyhow::Result<()>
    where
        RW: AsyncBufReadWrite,
    {
        match self {
            Command::Ping(cmd) => cmd.execute(stream).await,
        }
    }

    pub async fn try_from_buf_read<R>(reader: &mut R) -> anyhow::Result<Command>
    where
        R: AsyncBufRead + Unpin + Debug,
    {
        loop {
            let buf = reader.fill_buf().await?;

            if buf.is_empty() {
                return Err(anyhow!(
                    "Unable to construct valid command from the provided reader {:?}",
                    reader
                ));
            }

            if buf.starts_with(&PING_CMD_NAME) {
                return Ok(Command::Ping(
                    ping::Ping::try_from_async_read(reader).await?,
                ));
            }
        }
    }
}
