//! This module contains the definition of a [`Message`] - the smallest unit of parseable bytes built for the rldb [`crate::server::Server`].
//!
//! When serialized, a [`Message`] looks like the following:
//!
//! [4 bytes - ID][4 bytes - length of payload][payload (dynamic size)]
use std::mem::size_of;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{event, Level};

use crate::error::{Error, InvalidRequest, Result};

/// Kind of arbitrary but let's make sure a single connection can't consume more
/// than 1Mb of memory...
pub const MAX_MESSAGE_SIZE: u32 = 1024 * 1024;

/// The unit of the protocol built on top of TCP
/// that this server uses.
#[derive(Debug)]
pub struct Message {
    /// Message id -> used as a way of identifying the format of the payload for deserialization
    pub id: u32,
    /// A unique request identifier - used for request tracing and debugging
    /// Note that this has to be encoded as utf8 otherwise parsing the message will fail
    pub request_id: String,
    /// the Request payload
    pub payload: Option<Bytes>,
}

/// A trait that has to be implemented for any structs/enums that can be transformed into a [`Message`]
pub trait IntoMessage {
    /// Same as [`Message::id`]
    fn id(&self) -> u32;
    /// Same as [Message::request_id]
    fn request_id(&self) -> String;
    /// Same as [`Message::payload`]
    fn payload(&self) -> Option<Bytes> {
        None
    }
}

impl Message {
    /// Constructs a new [`Message`] with the given id and payload
    pub fn new(id: u32, request_id: String, payload: Option<Bytes>) -> Self {
        Self {
            id,
            request_id,
            payload,
        }
    }

    /// This function tries to construct a [`Message`] by reading bytes from the provided [`AsyncRead`] source
    /// # Errors
    /// This functions returns errors in the following cases
    ///  1. The message size is bigger than [`MAX_MESSAGE_SIZE`]
    ///  2. The message is somehow malformed (eg: less bytes were provided than the length received)
    pub async fn try_from_async_read<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self> {
        event!(Level::TRACE, "Will read id");
        let id = reader.read_u32().await?;

        event!(Level::TRACE, "Will read request_id_len");
        let request_id_length = reader.read_u32().await?;
        if request_id_length == 0 {
            return Err(Error::InvalidRequest(
                InvalidRequest::MessageReceivedWithoutRequestId,
            ));
        }

        if request_id_length > MAX_MESSAGE_SIZE {
            return Err(Error::InvalidRequest(
                InvalidRequest::MaxMessageSizeExceeded {
                    max: MAX_MESSAGE_SIZE,
                    got: request_id_length,
                },
            ));
        }

        let request_id = {
            let mut buf = vec![0u8; request_id_length as usize];
            event!(
                Level::TRACE,
                "Will read request id of size: {}",
                request_id_length
            );
            reader.read_exact(&mut buf).await?;
            event!(Level::TRACE, "Did read request id");
            String::from_utf8(buf).map_err(|_| {
                Error::InvalidRequest(InvalidRequest::MessageRequestIdMustBeUtf8Encoded)
            })?
        };

        event!(Level::TRACE, "will read payload length");
        let payload_length = reader.read_u32().await?;

        let payload = if payload_length > 0 {
            if payload_length + request_id_length > MAX_MESSAGE_SIZE {
                return Err(crate::error::Error::InvalidRequest(
                    InvalidRequest::MaxMessageSizeExceeded {
                        max: MAX_MESSAGE_SIZE,
                        got: payload_length,
                    },
                ));
            }
            let mut buf = vec![0u8; payload_length as usize];
            event!(Level::TRACE, "Will read payload of len: {}", payload_length);
            reader.read_exact(&mut buf).await?;
            event!(Level::TRACE, "Did read payload");
            Some(buf.into())
        } else {
            None
        };

        Ok(Self {
            id,
            request_id,
            payload,
        })
    }

    /// Serializes a [`Message`] struct into it's serialized format (see top level comment for format)
    pub fn serialize(self) -> Bytes {
        let payload = self.payload.clone();
        let payload_len = payload.clone().map_or(0, |payload| payload.len());
        let mut buf =
            BytesMut::with_capacity(self.request_id.len() + payload_len + 3 * size_of::<u32>());

        event!(Level::TRACE, "Will serialize id: {}", self.id);
        buf.put_u32(self.id);
        event!(
            Level::TRACE,
            "Will serialize request_id_len: {}",
            self.request_id.len()
        );
        buf.put_u32(self.request_id.len() as u32);
        event!(
            Level::TRACE,
            "Will serialize request_id: {}",
            self.request_id
        );
        buf.put(self.request_id.as_bytes());

        event!(Level::TRACE, "Will serialize payload_len: {}", payload_len);
        buf.put_u32(payload_len as u32);
        if let Some(payload) = payload {
            event!(Level::TRACE, "Will serialize payload: {:?}", payload);
            buf.put(payload);
        }

        assert_eq!(buf.capacity(), buf.len());

        buf.freeze()
    }
}

impl<M: IntoMessage> From<M> for Message {
    fn from(v: M) -> Self {
        Self {
            id: v.id(),
            request_id: v.request_id(),
            payload: v.payload(),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use tokio::io::AsyncRead;

    use crate::{
        error::{Error, InvalidRequest},
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
            Error::InvalidRequest(InvalidRequest::MaxMessageSizeExceeded { max, got }) => {
                assert_eq!(max, MAX_MESSAGE_SIZE);
                assert_eq!(got, MAX_MESSAGE_SIZE + 1);
            }
            _ => {
                panic!("Unexpected error: {}", err);
            }
        }
    }
}
