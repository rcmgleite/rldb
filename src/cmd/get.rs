use anyhow::anyhow;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    server::{IntoRequest, Message},
    storage_engine::StorageEngine,
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

    pub async fn execute<S: StorageEngine>(self, storage_engine: S) -> GetResponse {
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

    pub fn try_from_message(message: Message) -> anyhow::Result<Self> {
        if message.id != GET_CMD {
            return Err(anyhow!(
                "Unable to construct Get Command from Message. Expected id {} got {}",
                GET_CMD,
                message.id
            ));
        }

        if let Some(payload) = message.payload {
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

    fn payload(self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(&self).unwrap()))
    }
}

#[derive(Serialize, Deserialize)]
pub enum GetResponse {
    Success { value: String },
    Failure { message: String },
}
