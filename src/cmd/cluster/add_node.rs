use anyhow::anyhow;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::server::{IntoRequest, Request};

const CMD_CLUSTER_ADD_NODE: u32 = 100;

#[derive(Serialize, Deserialize)]
pub struct AddNode {
    addr: String,
}

impl AddNode {
    pub fn new(addr: String) -> Self {
        Self { addr }
    }

    // TODO: Inject cluster state
    pub async fn execute(self) -> AddNodeResponse {
        todo!()
    }

    pub fn try_from_request(request: Request) -> anyhow::Result<Self> {
        if request.id != CMD_CLUSTER_ADD_NODE {
            return Err(anyhow!(
                "Unable to construct AddNode Command from Request. Expected id {} got {}",
                CMD_CLUSTER_ADD_NODE,
                request.id
            ));
        }

        if let Some(payload) = request.payload {
            let s: Self = serde_json::from_slice(&payload)?;
            Ok(s)
        } else {
            return Err(anyhow!("AddNode message payload can't be None"));
        }
    }
}

impl IntoRequest for AddNode {
    fn id(&self) -> u32 {
        CMD_CLUSTER_ADD_NODE
    }

    fn payload(&self) -> Option<Bytes> {
        Some(Bytes::from(serde_json::to_string(self).unwrap()))
    }
}

#[derive(Serialize, Deserialize)]
pub enum AddNodeResponse {
    Success { message: String },
    Failure { message: String },
}
