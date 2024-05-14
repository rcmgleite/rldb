use anyhow::anyhow;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::server::{IntoRequest, Request};

const CMD_CLUSTER_REMOVE_NODE: u32 = 101;

#[derive(Serialize, Deserialize)]
pub struct RemoveNode {
    addr: String,
}

impl RemoveNode {
    pub fn new(addr: String) -> Self {
        Self { addr }
    }

    // TODO: Inject cluster state
    pub async fn execute(self) -> RemoveNodeResponse {
        todo!()
    }

    pub fn try_from_request(request: Request) -> anyhow::Result<Self> {
        if request.id != CMD_CLUSTER_REMOVE_NODE {
            return Err(anyhow!(
                "Unable to construct RemoveNode Command from Request. Expected id {} got {}",
                CMD_CLUSTER_REMOVE_NODE,
                request.id
            ));
        }

        if let Some(payload) = request.payload {
            let s: Self = serde_json::from_slice(&payload)?;
            Ok(s)
        } else {
            return Err(anyhow!("RemoveNode message payload can't be None"));
        }
    }
}

impl IntoRequest for RemoveNode {
    fn id(&self) -> u32 {
        CMD_CLUSTER_REMOVE_NODE
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}

#[derive(Serialize, Deserialize)]
pub enum RemoveNodeResponse {
    Success { message: String },
    Failure { message: String },
}
