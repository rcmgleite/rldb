use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::server::{message::IntoMessage, Db};

pub const GET_CMD: u32 = 2;

#[derive(Serialize, Deserialize)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: String) -> Self {
        Self { key }
    }

    pub async fn execute(self, db: Arc<Db>) -> GetResponse {
        let key_bytes = self.key.into();
        match db.storage_engine.get(&key_bytes).await {
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
