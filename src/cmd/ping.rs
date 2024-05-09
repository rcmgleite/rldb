use serde::{Deserialize, Serialize};

use crate::server::IntoRequest;

pub const PING_CMD: u32 = 1;

#[derive(Serialize)]
pub struct Ping;

impl Ping {
    pub async fn execute(self) -> PingResponse {
        PingResponse {
            message: "PONG".to_string(),
        }
    }
}

impl IntoRequest for Ping {
    fn id(&self) -> u32 {
        PING_CMD
    }
}

#[derive(Serialize, Deserialize)]
pub struct PingResponse {
    message: String,
}
