use anyhow::anyhow;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::server::{IntoRequest, Request, SyncStorageEngine};

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

    pub fn try_from_request(request: Request) -> anyhow::Result<Self> {
        if request.id != GET_CMD {
            return Err(anyhow!(
                "Unable to construct Get Command from Request. Expected id {} got {}",
                GET_CMD,
                request.id
            ));
        }

        if let Some(payload) = request.payload {
            let s: Self = serde_json::from_slice(&payload)?;
            Ok(s)
        } else {
            return Err(anyhow!("Get message payload can't be None"));
        }
    }
}

impl IntoRequest for Get {
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
