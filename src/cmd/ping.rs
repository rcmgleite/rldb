use anyhow::anyhow;
use bytes::Bytes;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWriteExt};

use super::AsyncBufReadWrite;

pub const CMD_NAME: &[u8] = b"PING ";

pub struct Ping;

impl Ping {
    pub async fn try_from_async_read<R>(reader: &mut R) -> anyhow::Result<Self>
    where
        R: AsyncRead + Unpin,
    {
        let mut cmd_name = vec![0u8; CMD_NAME.len()];
        reader.read_exact(&mut cmd_name[..]).await?;

        if cmd_name.as_slice() != CMD_NAME {
            return Err(anyhow!(
                "Invalid command name {:?}, Should be: {:?}",
                cmd_name,
                CMD_NAME
            ));
        }

        Ok(Self)
    }

    pub async fn execute<RW>(self, read_writer: &mut RW) -> anyhow::Result<()>
    where
        RW: AsyncBufReadWrite,
    {
        read_writer.write_all(b"PONG").await?;
        Ok(())
    }

    pub fn serialize(&self) -> Bytes {
        Bytes::from(CMD_NAME)
    }

    pub async fn parse_response<R>(reader: &mut R) -> anyhow::Result<Bytes>
    where
        R: AsyncBufRead + Unpin,
    {
        let expected_response = b"PONG";
        let mut buf = vec![0u8; expected_response.len()];
        reader.read_exact(&mut buf[..]).await?;

        if buf.as_slice() != expected_response {
            return Err(anyhow!(
                "Received unexpected response from PING cmd {:?}",
                buf
            ));
        }

        Ok(buf.into())
    }
}
