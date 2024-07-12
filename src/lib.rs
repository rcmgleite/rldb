//! RLDB (Rusty Learning Dynamo Database) is an educational project that provides a Rust implementation of
//! the [Amazon dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf).
//! This project aims to help developers and students understand the principles behind distributed key value data stores.
pub mod client;
pub mod cluster;
pub mod cmd;
pub mod error;
pub mod persistency;
pub mod server;
pub mod telemetry;
pub mod test_utils;
pub mod utils;

#[cfg(test)]
extern crate quickcheck;

#[cfg(test)]
extern crate quickcheck_async;

#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;
