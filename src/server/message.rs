//! This module contains the definition of a [`Message`] - the smallest unit of parseable bytes built for the rldb [`crate::server::Server`].
//!
//! When serialized, a [`Message`] looks like the following:
//!
//! [4 bytes - ID][4 bytes - length of payload][payload (dynamic size)]
use std::mem::size_of;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::Result;

/// Kind of arbitrary but let's make sure a single connection can't consume more
/// than 1Mb of memory...
pub const MAX_MESSAGE_SIZE: usize = 1024 * 1024;

/// The unit of the protocol built on top of TCP
/// that this server uses.
#[derive(Debug)]
pub struct Message {
    /// Message id -> used as a way of identifying the format of the payload for deserialization
    pub id: u32,
    /// the Request payload
    pub payload: Option<Bytes>,
}

/// A trait that has to be implemented for any structs/enums that can be transformed into a [`Message`]
pub trait IntoMessage {
    /// Same as [`Message::id`]
    fn id(&self) -> u32;
    /// Same as [`Message::payload`]
    fn payload(&self) -> Option<Bytes> {
        None
    }
}

impl Message {
    /// Constructs a new [`Message`] with the given id and payload
    pub fn new(id: u32, payload: Option<Bytes>) -> Self {
        Self { id, payload }
    }

    /// This function tries to construct a [`Message`] by reading bytes from the provided [`AsyncRead`] source
    /// # Errors
    /// This functions returns errors in the following cases
    ///  1. The message size is bigger than [`MAX_MESSAGE_SIZE`]
    ///  2. The message is somehow malformed (eg: less bytes were provided than the length received)
    pub async fn try_from_async_read<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self> {
        let id = reader.read_u32().await?;
        let length = reader.read_u32().await?;

        let payload = if length > 0 {
            if length > MAX_MESSAGE_SIZE as u32 {
                return Err(crate::error::Error::InvalidRequest {
                    reason: format!(
                        "Request length too long {} - max accepted value: {}",
                        length, MAX_MESSAGE_SIZE
                    ),
                });
            }
            let mut buf = vec![0u8; length as usize];
            reader.read_exact(&mut buf).await?;
            Some(buf.into())
        } else {
            None
        };

        Ok(Self { id, payload })
    }

    /// Serializes a [`Message`] struct into it's serialized format (see top level comment for format)
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

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use tokio::io::AsyncRead;

    use crate::{
        error::Error,
        server::message::{Message, MAX_MESSAGE_SIZE},
    };

    struct MaxMessageSizeExceededAsyncRead;

    // Writes u32s, one at a time with value MAX_MESSAGE_SIZE + 1
    // that's really all we need for the test `test_max_message_size_exceeded`
    impl AsyncRead for MaxMessageSizeExceededAsyncRead {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            buf.put_u32(MAX_MESSAGE_SIZE as u32 + 1);
            std::task::Poll::Ready(std::io::Result::Ok(()))
        }
    }

    #[tokio::test]
    async fn test_max_message_size_exceeded() {
        let mut reader = MaxMessageSizeExceededAsyncRead;
        let err = Message::try_from_async_read(&mut reader)
            .await
            .err()
            .unwrap();

        match err {
            Error::InvalidRequest { .. } => {}
            _ => {
                panic!("Unexpected error: {}", err);
            }
        }
    }
}
