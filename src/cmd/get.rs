//! Get [`crate::cmd::Command`]
//!
//! This is a client facing command that returns [0..n] values.
//! When more than one value is returned associated with the same key, it means that a Conflict during [`crate::cmd::put::Put`] occurred.
//! To resolve the conflict, the Get response will include what is called a "Context".
//! A context is a bag of information that can only be deserialized by the server.
//! As a client, what you do to resolve a conflict is
//!  1. Do a Get and receive multiple values + a context as response
//!  2. Choose which value should be kept and send a new PUT passing the context received on GET as argument
//! The server then uses the Context to properly resolve the conflict.
use std::hash::Hash;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::persistency::storage::Value;
use crate::persistency::Db;
use crate::server::message::IntoMessage;
use crate::utils::serde_utf8_bytes;

use super::types::Context;

pub const GET_CMD: u32 = 2;

#[derive(Serialize, Deserialize)]
pub struct Get {
    #[serde(with = "serde_utf8_bytes")]
    key: Bytes,
    request_id: String,
}

impl Get {
    /// Constructs a new [`Get`] instance
    pub fn new(key: Bytes, request_id: String) -> Self {
        Self { key, request_id }
    }

    /// Executes the [`Get`] command using the specified [`Db`] instance
    ///
    pub async fn execute(self, db: Arc<Db>) -> Result<GetResponse> {
        let res = db.get(self.key.clone(), false).await?;
        // TODO: Include the proper crc of the value that has to be provided by storage
        let values = res.iter().map(|e| e.value.clone()).collect();
        let context = res.iter().fold(Context::default(), |mut acc, e| {
            acc.merge_version(&e.version);
            acc
        });

        Ok(GetResponse {
            values,
            context: context.serialize().into(),
        })
    }

    /// returns the cmd id for [`Get`]
    pub fn cmd_id() -> u32 {
        GET_CMD
    }
}

impl IntoMessage for Get {
    fn id(&self) -> u32 {
        Self::cmd_id()
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }

    fn request_id(&self) -> String {
        self.request_id.clone()
    }
}

/// The struct that represents a [`Get`] response payload
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Debug)]
pub struct GetResponse {
    /// A vector of [`Value`]s associated with the requested Key.
    /// If values.len() > 1 it means that a conflict during a previous Put was detected.
    /// To resolve the conflict, a user must issue a new PUT with the desired final value
    /// along the context argument returned by this API call.
    pub values: Vec<Value>,
    /// An opaque byte array used for object versioning/conflict resolution
    pub context: String,
}
