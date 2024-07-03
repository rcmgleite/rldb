//! Ping [`crate::cmd::Command`]
use serde::{Deserialize, Serialize};

use crate::{error::Result, server::message::IntoMessage};

pub const PING_CMD: u32 = 1;

#[derive(Serialize)]
pub struct Ping {
    request_id: String,
}

impl Ping {
    pub fn new(request_id: String) -> Self {
        Self { request_id }
    }

    pub async fn execute(self) -> Result<PingResponse> {
        Ok(PingResponse {
            message: "PONG".to_string(),
        })
    }
}

impl IntoMessage for Ping {
    fn id(&self) -> u32 {
        PING_CMD
    }

    fn request_id(&self) -> String {
        self.request_id.clone()
    }
}

/// [`Ping`] response payload
#[derive(Serialize, Deserialize)]
pub struct PingResponse {
    pub message: String,
}
