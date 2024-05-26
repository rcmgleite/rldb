use std::fmt::Display;

use serde::Serialize;

use crate::client::error::Error as ClientError;

#[derive(Debug, Serialize)]
pub enum Error {
    ClusterHasOnlySelf,
    Internal { reason: String },
    Logic { reason: String },
    Client(ClientError),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

impl From<ClientError> for Error {
    fn from(value: ClientError) -> Self {
        Self::Client(value)
    }
}
