use anyhow::anyhow;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tracing::{event, Level};

use crate::server::{IntoRequest, Request, SyncStorageEngine};

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

    pub async fn execute(self, storage_engine: SyncStorageEngine) -> PutResponse {
        match storage_engine.put(self.key.into(), self.value.into()).await {
            Ok(()) => PutResponse::Success {
                message: "Ok".to_string(),
            },
            Err(err) => PutResponse::Failure {
                message: err.to_string(),
            },
        }
    }

    pub fn try_from_request(request: Request) -> anyhow::Result<Self> {
        event!(Level::DEBUG, "try_from_message: {:?}", request);
        if request.id != PUT_CMD {
            return Err(anyhow!(
                "Unable to construct Put Command from Message. Expected id {} got {}",
                PUT_CMD,
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

impl IntoRequest for Put {
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
