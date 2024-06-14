//! Put [`crate::cmd::Command`]
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{event, Level};

use crate::client::db_client::DbClient;
use crate::client::Client;
use crate::error::{Error, Result};
use crate::persistency::quorum::{
    min_required_replicas::MinRequiredReplicas, Evaluation, OperationStatus, Quorum,
};
use crate::persistency::{Db, OwnsKeyResponse};
use crate::server::message::IntoMessage;
use crate::utils::serde_utf8_bytes;

pub const PUT_CMD: u32 = 3;

/// Struct that represents a deserialized Put payload
#[derive(Serialize, Deserialize)]
pub struct Put {
    #[serde(with = "serde_utf8_bytes")]
    key: Bytes,
    #[serde(with = "serde_utf8_bytes")]
    value: Bytes,
    replication: bool,
}

impl Put {
    /// Constructs a new [`Put`] [`crate::cmd::Command`]
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self {
            key,
            value,
            replication: false,
        }
    }

    /// constructs a new [`Put`] [`crate::cmd::Command`] for replication
    pub fn new_replication(key: Bytes, value: Bytes) -> Self {
        Self {
            key,
            value,
            replication: true,
        }
    }

    /// Executes a [`Put`] [`crate::cmd::Command`]
    pub async fn execute(self, db: Arc<Db>) -> Result<PutResponse> {
        if self.replication {
            event!(Level::DEBUG, "Executing a replication Put");
            // if this is a replication PUT, we don't have to deal with quorum
            db.put(self.key, self.value).await?;
        } else {
            event!(Level::DEBUG, "Executing a coordinator Put");
            // if this is not a replication PUT, we have to honor quorum before returning a success
            // if we have a quorum config, we have to make sure we wrote to enough replicas
            if let Some(quorum_config) = db.quorum_config() {
                let mut quorum =
                    MinRequiredReplicas::new(quorum_config.replicas, quorum_config.writes)?;
                let mut futures = FuturesUnordered::new();

                // the preference list will contain the node itself (otherwise there's a bug).
                // How can we assert that here? Maybe this should be guaranteed by the Db API instead...
                let preference_list = db.preference_list(&self.key)?;
                for node in preference_list {
                    futures.push(Self::do_put(
                        self.key.clone(),
                        self.value.clone(),
                        db.clone(),
                        node,
                    ))
                }

                // for now, let's wait til all futures either succeed or fail.
                // we don't strictly need that since we are quorum based..
                // doing it this way now for simplicity but this will have a latency impact
                //
                // For more info, see similar comment on [`crate::cmd::get::Get`]
                while let Some(res) = futures.next().await {
                    match res {
                        Ok(_) => {
                            let _ = quorum.update(OperationStatus::Success(()));
                        }
                        Err(err) => {
                            event!(Level::WARN, "Failed a PUT: {:?}", err);
                            let _ = quorum.update(OperationStatus::Failure(err));
                        }
                    }
                }

                let quorum_result = quorum.finish();

                match quorum_result.evaluation {
                    Evaluation::Reached => {}
                    Evaluation::NotReached | Evaluation::Unreachable => {
                        event!(
                            Level::WARN,
                            "quorum not reached: successes: {}, failures: {}",
                            quorum_result.successes.len(),
                            quorum_result.failures.len(),
                        );
                        return Err(Error::QuorumNotReached {
                            operation: "Put".to_string(),
                            reason: format!(
                                "Unable to execute a min of {} PUTs",
                                quorum_config.writes
                            ),
                        });
                    }
                }
            } else {
                db.put(self.key, self.value).await?;
            }
        }

        Ok(PutResponse {
            message: "Ok".to_string(),
        })
    }

    /// Wrapper function that allows the same inteface to put locally (using [`Db`]) or remotely (using [`DbClient`])
    async fn do_put(key: Bytes, value: Bytes, db: Arc<Db>, dst_addr: Bytes) -> Result<()> {
        if let OwnsKeyResponse::True = db.owns_key(&dst_addr)? {
            event!(Level::DEBUG, "Storing key : {:?} locally", key);
            db.put(key, value).await?;
        } else {
            event!(
                Level::DEBUG,
                "will store key : {:?} in node: {:?}",
                key,
                dst_addr
            );
            let mut client =
                DbClient::new(String::from_utf8(dst_addr.clone().into()).map_err(|e| {
                    event!(
                        Level::ERROR,
                        "Unable to parse addr as utf8 {}",
                        e.to_string()
                    );
                    Error::Internal(crate::error::Internal::Unknown {
                        reason: e.to_string(),
                    })
                })?);

            event!(Level::DEBUG, "connecting to node node: {:?}", dst_addr);
            client.connect().await?;

            // TODO: assert the response
            let _ = client.put(key.clone(), value, true).await?;

            event!(Level::DEBUG, "stored key : {:?} locally", key);
        }

        Ok(())
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

/// [`Put`] response payload in its deserialized form.
#[derive(Debug, Serialize, Deserialize)]
pub struct PutResponse {
    pub message: String,
}
