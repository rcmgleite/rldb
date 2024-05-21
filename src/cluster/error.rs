use std::fmt::Display;

use serde::Serialize;

#[derive(Debug, Serialize)]
pub enum Error {
    Internal { reason: String },
    Logic { reason: String },
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
