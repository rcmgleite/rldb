//! Ping [`crate::cmd::Command`]
use serde::{Deserialize, Serialize};

use crate::{error::Result, server::message::IntoMessage};

pub const PING_CMD: u32 = 1;

#[derive(Serialize)]
pub struct Ping;

impl Ping {
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
}

/// [`Ping`] response payload
#[derive(Serialize, Deserialize)]
pub struct PingResponse {
    message: String,
}
