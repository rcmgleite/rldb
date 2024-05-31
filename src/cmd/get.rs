//! Get [`crate::cmd::Command`]
use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{event, Level};

use crate::client::db_client::DbClient;
use crate::client::Client;
use crate::db::{Db, OwnsKeyResponse};
use crate::error::{Error, Result};
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
                return Ok(GetResponse { value });
            } else {
                return Err(Error::NotFound { key: self.key });
            }
        } else {
            event!(Level::INFO, "executing a non-replica GET");
            if let Some(quorum_config) = db.quorum_config() {
                let mut futures = FuturesUnordered::new();
                let preference_list = db.preference_list(&self.key)?;
                event!(Level::INFO, "GET preference_list {:?}", preference_list);
                for node in preference_list {
                    futures.push(Self::do_get(self.key.clone(), db.clone(), node))
                }

                // TODO: we are waiting for all nodes on the preference list to return either error or success
                // this will cause latency issues and it's no necessary.. fix it later
                let mut results = Vec::new();
                while let Some(res) = futures.next().await {
                    if let Ok(res) = res {
                        results.push(res);
                    } else {
                        event!(
                            Level::WARN,
                            "Got a failed GET from a remote host: {:?}",
                            res
                        );
                    }
                }

                event!(Level::INFO, "raw results: {:?}", results);
                // TODO: very cumbersome logic... will have to make this better later
                let mut successes = 0;
                if results.len() >= quorum_config.reads {
                    let mut result_freq: HashMap<Bytes, usize> = HashMap::new();
                    for result in results {
                        *result_freq.entry(result).or_default() += 1;
                    }

                    event!(Level::WARN, "result_freq: {:?}", result_freq);

                    for (res, freq) in result_freq {
                        if freq >= quorum_config.reads {
                            return Ok(GetResponse { value: res });
                        }

                        successes = freq;
                    }
                }

                return Err(Error::QuorumNotReached {
                    required: quorum_config.reads,
                    got: successes,
                });
            } else {
                let value = db.get(&self.key).await?;
                if let Some(value) = value {
                    return Ok(GetResponse { value });
                } else {
                    return Err(Error::NotFound { key: self.key });
                }
            }
        }
    }

    async fn do_get(key: Bytes, db: Arc<Db>, src_addr: Bytes) -> Result<Bytes> {
        if let OwnsKeyResponse::True = db.owns_key(&src_addr)? {
            event!(
                Level::INFO,
                "node is part of preference_list {:?}",
                src_addr
            );
            let res = db.get(&key).await?;
            if let Some(res) = res {
                return Ok(res);
            } else {
                return Err(Error::NotFound { key });
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
                    Error::Internal(crate::error::Internal::Unknown)
                })?);

            event!(Level::INFO, "connecting to node node: {:?}", src_addr);
            client.connect().await.map_err(|e| {
                event!(
                    Level::ERROR,
                    "Unable to connect to node while executing GET {}",
                    e.to_string()
                );
                Error::Internal(crate::error::Internal::Unknown)
            })?;

            let resp = client.get(key.clone(), true).await.map_err(|e| {
                event!(Level::ERROR, "failed to GET {}", e.to_string());
                Error::Internal(crate::error::Internal::Unknown)
            })?;

            Ok(resp.value)
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
#[derive(Debug, Serialize, Deserialize)]
pub struct GetResponse {
    #[serde(with = "serde_utf8_bytes")]
    pub value: Bytes,
}
