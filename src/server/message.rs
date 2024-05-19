use std::mem::size_of;

use anyhow::anyhow;
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Kind of arbitrary but let's make sure a single connection can't consume more
/// than 1Mb of memory...
const MAX_MESSAGE_SIZE: usize = 1 * 1024 * 1024;

/// A Request is the unit of the protocol built on top of TCP
/// that this server uses.
#[derive(Debug)]
pub struct Message {
    /// Message id -> used as a way of identifying the format of the payload for deserialization
    pub id: u32,
    /// the Request payload
    pub payload: Option<Bytes>,
}

pub trait IntoMessage {
    fn id(&self) -> u32;
    fn payload(&self) -> Option<Bytes> {
        None
    }
}

impl Message {
    pub fn new(id: u32, payload: Option<Bytes>) -> Self {
        Self { id, payload }
    }

    pub async fn try_from_async_read<R: AsyncRead + Unpin>(reader: &mut R) -> anyhow::Result<Self> {
        let id = reader.read_u32().await?;
        let length = reader.read_u32().await?;

        let payload = if length > 0 {
            if length > MAX_MESSAGE_SIZE as u32 {
                return Err(anyhow!(
                    "Request length too long {} - max accepted value: {}",
                    length,
                    MAX_MESSAGE_SIZE
                ));
            }
            let mut buf = vec![0u8; length as usize];
            reader.read_exact(&mut buf).await?;
            Some(buf.into())
        } else {
            None
        };

        Ok(Self { id, payload })
    }

    pub fn serialize(self) -> Bytes {
        let payload = self.payload.clone();
        let payload_len = payload.clone().map_or(0, |payload| payload.len());
        let mut buf = BytesMut::with_capacity(payload_len + 2 * size_of::<u32>());

        buf.put_u32(self.id);
        buf.put_u32(payload_len as u32);
        if let Some(payload) = payload {
            buf.put(payload);
        }

        buf.freeze()
    }
}

impl<M: IntoMessage> From<M> for Message {
    fn from(v: M) -> Self {
        Self {
            id: v.id(),
            payload: v.payload(),
        }
    }
}
