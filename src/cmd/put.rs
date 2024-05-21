use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::db::{Db, OwnsKeyResponse};
use crate::error::{Error, Result};
use crate::server::message::IntoMessage;

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

    pub async fn execute(self, db: Arc<Db>) -> Result<PutResponse> {
        if let OwnsKeyResponse::False { addr } = db.owns_key(self.key.as_bytes())? {
            return Err(Error::InvalidRequest {
                reason: format!(
                    "Key owned by another node. Redirect request to node {:?}",
                    addr
                ),
            });
        }

        db.put(self.key.into(), self.value.into()).await?;
        Ok(PutResponse {
            message: "Ok".to_string(),
        })
    }

    pub fn cmd_id() -> u32 {
        PUT_CMD
    }
}

impl IntoMessage for Put {
    fn id(&self) -> u32 {
        Self::cmd_id()
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}

#[derive(Serialize, Deserialize)]
pub struct PutResponse {
    message: String,
}
