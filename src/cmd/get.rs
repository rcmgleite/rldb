use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::db::{Db, OwnsKeyResponse};
use crate::server::message::IntoMessage;

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
        if let OwnsKeyResponse::False { addr } = db.owns_key(self.key.as_bytes()) {
            return GetResponse::Failure {
                message: format!(
                    "Key owned by another node. Redirect request to node {:?}",
                    addr
                ),
            };
        }

        match db.get(self.key.as_bytes()).await {
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
}

#[derive(Serialize, Deserialize)]
pub enum GetResponse {
    Success { value: String },
    Failure { message: String },
}
