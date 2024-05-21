use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::db::{Db, OwnsKeyResponse};
use crate::error::{Error, Result};
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

    pub async fn execute(self, db: Arc<Db>) -> Result<GetResponse> {
        if let OwnsKeyResponse::False { addr } = db.owns_key(self.key.as_bytes())? {
            return Err(Error::InvalidRequest {
                reason: format!(
                    "Key owned by another node. Redirect request to node {:?}",
                    addr
                ),
            });
        }

        let value = db.get(self.key.as_bytes()).await?;
        if let Some(value) = value {
            return Ok(GetResponse {
                value: String::from_utf8_lossy(&value).into(),
            });
        } else {
            return Err(Error::NotFound { key: self.key });
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
pub struct GetResponse {
    value: String,
}
