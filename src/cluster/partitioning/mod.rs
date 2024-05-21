use crate::cluster::error::Result;
use bytes::Bytes;
use std::fmt::Debug;

pub mod consistent_hashing;

pub trait PartitioningScheme: Debug {
    fn add_node(&mut self, key: Bytes) -> Result<()>;

    fn remove_node(&mut self, key: &[u8]) -> Result<()>;

    fn key_owner(&self, key: &[u8]) -> Result<Bytes>;
}
