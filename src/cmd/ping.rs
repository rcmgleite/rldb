use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::server::IntoRequest;

pub const PING_CMD: u32 = 1;

pub struct Ping;

impl Ping {
    pub async fn execute(self) -> PingResponse {
        PingResponse {
            message: "PONG".to_string(),
        }
    }
}

impl IntoRequest for Ping {
    fn cmd(&self) -> u32 {
        PING_CMD
    }

    fn into_request(self) -> Option<Bytes> {
        None
    }
}

#[derive(Serialize, Deserialize)]
pub struct PingResponse {
    message: String,
}
