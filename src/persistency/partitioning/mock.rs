//! The mock module contains a mock [`PartitioningScheme`] used for tests
use crate::error::{Error, Result};
use bytes::Bytes;
use std::collections::HashSet;

use super::PartitioningScheme;

/// For the purpose of these tests, we don't care too much about PartitioningScheme errors..
/// Let's just keep track of mutations on the nodes that are part of the scheme
/// and that's it.
#[derive(Default)]
pub struct MockPartitioningScheme {
    nodes: HashSet<Bytes>,
}

impl PartitioningScheme for MockPartitioningScheme {
    fn add_node(&mut self, key: Bytes) -> Result<()> {
        self.nodes.insert(key);
        Ok(())
    }

    fn remove_node(&mut self, key: &[u8]) -> Result<()> {
        self.nodes.remove(&Bytes::copy_from_slice(key));
        Ok(())
    }

    #[allow(clippy::never_loop)]
    fn key_owner(&self, _key: &[u8]) -> Result<Bytes> {
        for key in self.nodes.iter() {
            return Ok(key.clone());
        }
        Err(Error::Generic {
            reason: "mock".to_string(),
        })
    }

    fn preference_list(&self, _key: &[u8], _list_size: usize) -> Result<Vec<Bytes>> {
        todo!()
    }
}
