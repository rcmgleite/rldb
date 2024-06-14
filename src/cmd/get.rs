//! Get [`crate::cmd::Command`]
use std::hash::Hash;
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

pub const GET_CMD: u32 = 2;

#[derive(Serialize, Deserialize)]
pub struct Get {
    #[serde(with = "serde_utf8_bytes")]
    key: Bytes,
    replica: bool,
}

impl Get {
    /// Constructs a new [`Get`] instance
    pub fn new(key: Bytes) -> Self {
        Self {
            key,
            replica: false,
        }
    }

    pub fn new_replica(key: Bytes) -> Self {
        Self { key, replica: true }
    }

    /// Executes the [`Get`] command using the specified [`Db`] instance
    pub async fn execute(self, db: Arc<Db>) -> Result<GetResponse> {
        if self.replica {
            event!(Level::INFO, "executing a replica GET");
            let value = db.get(&self.key).await?;
            if let Some(value) = value {
                Ok(GetResponse { value })
            } else {
                Err(Error::NotFound { key: self.key })
            }
        } else {
            event!(Level::INFO, "executing a non-replica GET");
            if let Some(quorum_config) = db.quorum_config() {
                let mut quorum =
                    MinRequiredReplicas::new(quorum_config.replicas, quorum_config.reads)?;

                let mut futures = FuturesUnordered::new();
                let preference_list = db.preference_list(&self.key)?;
                event!(Level::INFO, "GET preference_list {:?}", preference_list);
                for node in preference_list {
                    futures.push(Self::do_get(self.key.clone(), db.clone(), node))
                }

                // TODO: we are waiting for all nodes on the preference list to return either error or success
                // this will cause latency issues and it's no necessary.. fix it later
                // Note: The [`crate::cluster::quorum::Quorum`] API already handles early evaluation
                // in case quorum is not reachable. The change to needed here is fairly small in this regard.
                // What's missing is deciding on:
                //  1. what do we do with inflight requests in case of an early success?
                //  2. (only for the PUT case) what do we do with successful PUT requests? rollback? what does that look like?
                while let Some(res) = futures.next().await {
                    match res {
                        Ok(res) => {
                            let _ = quorum.update(OperationStatus::Success(res));
                        }
                        Err(err) => {
                            event!(
                                Level::WARN,
                                "Got a failed GET from a remote host: {:?}",
                                err
                            );

                            let _ = quorum.update(OperationStatus::Failure(err));
                        }
                    }
                }

                event!(Level::INFO, "quorum: {:?}", quorum);

                let quorum_result = quorum.finish();
                match quorum_result.evaluation {
                    Evaluation::Reached => Ok(quorum_result.successes[0].clone()),
                    Evaluation::NotReached | Evaluation::Unreachable => {
                        if quorum_result.failures.iter().all(|err| {
                            let is_not_found = err.is_not_found();
                            println!("DEBUG: {} {}", err, is_not_found);
                            is_not_found
                        }) {
                            return Err(Error::NotFound { key: self.key });
                        }

                        Err(Error::QuorumNotReached {
                            operation: "Get".to_string(),
                            reason: format!(
                                "Unable to execute {} successful GETs",
                                quorum_config.reads
                            ),
                        })
                    }
                }
            } else {
                let value = db.get(&self.key).await?;
                if let Some(value) = value {
                    Ok(GetResponse { value })
                } else {
                    Err(Error::NotFound { key: self.key })
                }
            }
        }
    }

    async fn do_get(key: Bytes, db: Arc<Db>, src_addr: Bytes) -> Result<GetResponse> {
        if let OwnsKeyResponse::True = db.owns_key(&src_addr)? {
            event!(
                Level::INFO,
                "node is part of preference_list {:?}",
                src_addr
            );
            let res = db.get(&key).await?;
            if let Some(res) = res {
                Ok(GetResponse { value: res })
            } else {
                Err(Error::NotFound { key })
            }
        } else {
            event!(
                Level::INFO,
                "node is NOT part of preference_list {:?} - will have to do a remote call",
                src_addr
            );
            let mut client =
                DbClient::new(String::from_utf8(src_addr.clone().into()).map_err(|e| {
                    event!(
                        Level::ERROR,
                        "Unable to parse addr as utf8 {}",
                        e.to_string()
                    );
                    Error::Internal(crate::error::Internal::Unknown {
                        reason: e.to_string(),
                    })
                })?);

            event!(Level::INFO, "connecting to node node: {:?}", src_addr);
            client.connect().await?;

            Ok(client.get(key.clone(), true).await?)
        }
    }

    /// returns the cmd id for [`Get`]
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

/// The struct that represents a [`Get`] response payload
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct GetResponse {
    #[serde(with = "serde_utf8_bytes")]
    pub value: Bytes,
}
