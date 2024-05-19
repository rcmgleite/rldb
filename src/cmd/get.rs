use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    error::{Error, Result},
    server::{
        message::{IntoMessage, Message},
        SyncStorageEngine,
    },
};

pub const GET_CMD: u32 = 2;

#[derive(Serialize, Deserialize)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: String) -> Self {
        Self { key }
    }

    pub async fn execute(self, storage_engine: SyncStorageEngine) -> GetResponse {
        let key_bytes = self.key.into();
        match storage_engine.get(&key_bytes).await {
            Ok(Some(value)) => GetResponse::Success {
                value: String::from_utf8(value.into()).unwrap(),
            },
            Ok(None) => GetResponse::Failure {
                message: "Not Found".into(),
            },
            Err(err) => GetResponse::Failure {
                message: err.to_string(),
            },
        }
    }

    pub fn try_from_request(request: Message) -> Result<Self> {
        if request.id != GET_CMD {
            return Err(Error::InvalidRequest {
                reason: format!(
                    "Unable to construct Get Command from Request. Expected id {} got {}",
                    GET_CMD, request.id
                ),
            });
        }

        if let Some(payload) = request.payload {
            let s: Self = serde_json::from_slice(&payload).map_err(|e| Error::InvalidRequest {
                reason: format!("Invalid json payload for Get request {}", e.to_string()),
            })?;
            Ok(s)
        } else {
            return Err(Error::InvalidRequest {
                reason: "Get message payload can't be None".to_string(),
            });
        }
    }
}

impl IntoMessage for Get {
    fn id(&self) -> u32 {
        GET_CMD
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}

#[derive(Serialize, Deserialize)]
pub enum GetResponse {
    Success { value: String },
    Failure { message: String },
}
