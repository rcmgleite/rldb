use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::server::message::IntoMessage;
use crate::server::Db;

pub const PUT_CMD: u32 = 3;

#[derive(Serialize, Deserialize)]
pub struct Put {
    key: String,
    value: String,
}

impl Put {
    pub fn new(key: String, value: String) -> Self {
        Self { key, value }
    }

    pub async fn execute(self, db: Arc<Db>) -> PutResponse {
        match db
            .storage_engine
            .put(self.key.into(), self.value.into())
            .await
        {
            Ok(()) => PutResponse::Success {
                message: "Ok".to_string(),
            },
            Err(err) => PutResponse::Failure {
                message: err.to_string(),
            },
        }
    }
}

impl IntoMessage for Put {
    fn id(&self) -> u32 {
        PUT_CMD
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}

#[derive(Serialize, Deserialize)]
pub enum PutResponse {
    Success { message: String },
    Failure { message: String },
}
