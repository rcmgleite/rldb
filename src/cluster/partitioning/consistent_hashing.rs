//! Consistent-hashing is the default [`PartitioningScheme`] for rldb
use crate::cluster::error::{Error, Result};
use bytes::Bytes;
use murmur3::murmur3_x86_128;
use std::{collections::HashSet, io::Cursor};

use super::PartitioningScheme;

/// Let's force the usage of Hash functions that return u128 for now..
type HashFunctionReturnType = u128;

/// ConsistentHashing is one of the partitioning schemes implemented for rldb.
/// The goal of consistent hashing is to enable a distributed system to decide
/// which storage node should own a specific key. It does it by creating a
/// fixed hash space - in this case from [0, 2^128)
/// and computing the hash of both the storage nodes and the keys being stored.
/// The node that owns the key is the first node whose hash is higher than the hash of the key.
/// Note that this hash space should be viewed as a circular buffer (or hash ring). Let's
/// try to understand the hash ring statement through an example:
///
/// In this example we have a hash space that goes from 0 to 10 (ie: the hash function returns a number between 0 and 10).
/// Nodes:     ['A', 'B', 'C']
/// Nodes_hash:[ 2 ,  5 ,  8 ]
///
/// key to store: 'foo', hash('foo') = 4 -> owned by node B (hash 5)
/// key to store: 'bar', hash('bar) = 7 -> owned by node C (hash 8)
/// key to store: 'zoo', hash('zoo') = 9 -> owned by node A (hash 2)
///   - this last one shows the circular nature of the Nodes_hash, which is way
///    we sometimes refer this array as a 'hash ring'
///
/// **The important property of consistent hashing is that if a node is added/removed, not all
/// nodes need to re-sync their data.**
///
/// Implementation notes:
///  1. Passing a function pointer might not be ideal since we already have traits for Hash/Hasher..
///    - one of the biggest downsides is that a single function pointer requires the entire
///     byte slice that we want to hash to be passed at once (and therefore loaded into memory).
///     Usually Hasher APIs include an `update` and a `finalize` in order to be able to handle streaming data.
///     Since we are going to hash fairly small byte slices here (usually an ip/port) pair, this is not a problem.
#[derive(Clone, Debug)]
pub struct ConsistentHashing {
    nodes: Vec<Bytes>,
    hashes: Vec<HashFunctionReturnType>,
    hash_fn: fn(&[u8]) -> HashFunctionReturnType,
}

impl Default for ConsistentHashing {
    fn default() -> Self {
        Self {
            nodes: Default::default(),
            hashes: Default::default(),
            hash_fn: murmur3_hash,
        }
    }
}

impl ConsistentHashing {
    pub fn new_with_hash_fn(hash_fn: fn(&[u8]) -> HashFunctionReturnType) -> Self {
        Self {
            hashes: Vec::new(),
            nodes: Vec::new(),
            hash_fn,
        }
    }
}

impl PartitioningScheme for ConsistentHashing {
    fn add_node(&mut self, node: Bytes) -> Result<()> {
        let node_hash = (self.hash_fn)(&node);
        match self.hashes.binary_search(&node_hash) {
            Ok(_) => Err(Error::Internal { reason: "ConsistentHashing found a collision on its hash algorithm. This is currently an un-recoverable issue...".to_string() }),
            Err(index) => {
                self.hashes.insert(index, node_hash);
                self.nodes.insert(index, node);
                Ok(())
            }
        }
    }

    fn remove_node(&mut self, node: &[u8]) -> Result<()> {
        let node_hash = (self.hash_fn)(node);
        if let Ok(index) = self.hashes.binary_search(&node_hash) {
            self.hashes.remove(index);
            self.nodes.remove(index);
        }

        Ok(())
    }

    fn key_owner(&self, key: &[u8]) -> Result<Bytes> {
        let index = self.key_owner_index(key)?;
        Ok(self.nodes[index].clone())
    }

    fn preference_list(&self, key: &[u8], list_size: usize) -> Result<Vec<Bytes>> {
        let owner_index = self.key_owner_index(key)?;
        let mut res = Vec::with_capacity(list_size);

        let mut visited_indexes = HashSet::new();
        for i in 0..list_size {
            let index = (owner_index + i) % self.nodes.len();
            if visited_indexes.contains(&index) {
                continue;
            } else {
                visited_indexes.insert(index);
            }

            res.push(self.nodes[index].clone());
        }

        Ok(res)
    }
}

impl ConsistentHashing {
    fn key_owner_index(&self, key: &[u8]) -> Result<usize> {
        if self.nodes.is_empty() {
            return Err(Error::Logic {
                reason: "Can't ask for owner if no nodes are present".to_string(),
            });
        }

        let key_hash = (self.hash_fn)(key);
        Ok(self.hashes.partition_point(|elem| *elem < key_hash) % self.nodes.len())
    }
}

/// Internal functions used by the ConsistentHashing implementation
/// TODO: inject the hash function instead of hardcoding one...
/// TODO: murmur might not be a good fit since it's not a cryptographic hash impl (will have collisions more frequently)
pub fn murmur3_hash(key: &[u8]) -> HashFunctionReturnType {
    murmur3_x86_128(&mut Cursor::new(key), 0).unwrap()
}

#[cfg(test)]
mod tests {
    use super::ConsistentHashing;
    use crate::cluster::partitioning::{
        consistent_hashing::{murmur3_hash, HashFunctionReturnType},
        PartitioningScheme,
    };
    use bytes::Bytes;
    use quickcheck::{quickcheck, Arbitrary};
    use rand::{distributions::Alphanumeric, Rng};
    use std::{collections::HashMap, ops::Range};

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct TestNode {
        addr: Bytes,
    }

    #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
    struct AddNodeTestInput {
        nodes: Vec<TestNode>,
    }

    fn generate_random_ascii_string(range_size: Range<usize>) -> String {
        let string_size = rand::thread_rng().gen_range(range_size);
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(string_size)
            .map(char::from)
            .collect()
    }

    fn generate_random_nodes(range: Range<usize>) -> Vec<TestNode> {
        let n_nodes = rand::thread_rng().gen_range(range);
        let mut nodes = Vec::with_capacity(n_nodes);
        for _ in 0..n_nodes {
            nodes.push(TestNode {
                addr: Bytes::from(generate_random_ascii_string(10..20)),
            })
        }
        nodes.sort();
        nodes.dedup_by(|a, b| a.addr.eq_ignore_ascii_case(&b.addr));
        nodes
    }

    fn generate_random_keys(range: Range<usize>) -> Vec<String> {
        let n_keys = rand::thread_rng().gen_range(range);
        let mut keys = Vec::with_capacity(n_keys);
        for _ in 0..n_keys {
            keys.push(generate_random_ascii_string(1..20));
        }

        keys
    }

    impl Arbitrary for TestNode {
        fn arbitrary(_: &mut quickcheck::Gen) -> Self {
            Self {
                addr: Bytes::from(generate_random_ascii_string(1..20)),
            }
        }
    }

    impl Arbitrary for AddNodeTestInput {
        fn arbitrary(_: &mut quickcheck::Gen) -> Self {
            Self {
                nodes: generate_random_nodes(1..50),
            }
        }
    }

    /// This test assets on the following invariants
    /// 1. All Nodes provided are properly added when `add_node` is called
    /// 2. the internal vector [`keys`] contain all keys from the provided nodes
    ///   and it is sorted by the key_hash
    /// 3. The internal vector [`nodes`] is synchronized with the [`keys`] vector
    #[quickcheck]
    fn test_add_nodes_randomized(test_input: AddNodeTestInput) {
        let mut ring = ConsistentHashing::default();

        let node_hash_mapping: HashMap<HashFunctionReturnType, Bytes> = test_input
            .nodes
            .iter()
            .map(|elem| (murmur3_hash(&elem.addr), elem.addr.clone()))
            .collect();

        for node in test_input.nodes.iter() {
            ring.add_node(node.addr.clone()).unwrap();
        }

        // now compute the hashes manually, construct a vector and sort it.
        // This has to be the same as ring.keys
        let mut nodes_hashes: Vec<HashFunctionReturnType> = test_input
            .nodes
            .iter()
            .map(|e| murmur3_hash(&e.addr))
            .collect();
        nodes_hashes.sort();

        // ring.keys contains all keys and it's sorted
        assert_eq!(ring.hashes.len(), test_input.nodes.len());
        assert_eq!(ring.hashes, nodes_hashes);
        assert_eq!(ring.nodes.len(), test_input.nodes.len());

        // now make sure that the ring.keys and ring.nodes are synchronized
        // AND that they contain all nodes provided by test_input
        for i in 0..test_input.nodes.len() {
            assert_eq!(node_hash_mapping[&ring.hashes[i]], ring.nodes[i]);
        }
    }

    #[derive(Debug, Clone)]
    struct KeyOwnerTestInput {
        nodes: Vec<TestNode>,
        keys: Vec<String>,
    }

    impl Arbitrary for KeyOwnerTestInput {
        fn arbitrary(_: &mut quickcheck::Gen) -> Self {
            Self {
                nodes: generate_random_nodes(1..20),
                keys: generate_random_keys(50..100),
            }
        }
    }

    // Randomized inputs - here we are simply trying to see if there are
    // edge cases that might crash the application or return unexpected errors
    #[quickcheck]
    fn test_key_owner_randomized(test_input: KeyOwnerTestInput) {
        let mut ring = ConsistentHashing::default();

        for node in test_input.nodes.iter() {
            ring.add_node(node.addr.clone()).unwrap();
        }

        for key in test_input.keys.iter() {
            ring.key_owner(key.as_bytes()).unwrap();
        }
    }

    fn test_hash_fn(key: &[u8]) -> u128 {
        // this table precisely maps known keys to known hashes.
        // we will build test cases to cover all cases based on these known keys.
        let table: HashMap<Bytes, u128> = vec![
            (Bytes::from_static(b"Node A"), 10u128),
            (Bytes::from_static(b"Node B"), 20u128),
            (Bytes::from_static(b"Node C"), 30u128),
            (Bytes::from_static(b"Node D"), 40u128),
            (Bytes::from_static(b"key 1"), 1u128),
            (Bytes::from_static(b"key 2"), 5u128),
            (Bytes::from_static(b"key 3"), 10u128),
            (Bytes::from_static(b"key 4"), 11u128),
            (Bytes::from_static(b"key 5"), 19u128),
            (Bytes::from_static(b"key 6"), 20u128),
            (Bytes::from_static(b"key 7"), 21u128),
            (Bytes::from_static(b"key 8"), 28u128),
            (Bytes::from_static(b"key 9"), 30u128),
            (Bytes::from_static(b"key 10"), 31u128),
            (Bytes::from_static(b"key 11"), 39u128),
            (Bytes::from_static(b"key 12"), 40u128),
            (Bytes::from_static(b"key 13"), 41u128),
        ]
        .into_iter()
        .collect();

        table[&Bytes::copy_from_slice(key)]
    }

    fn test_nodes() -> Vec<TestNode> {
        vec![
            TestNode {
                addr: Bytes::from_static(b"Node A"),
            },
            TestNode {
                addr: Bytes::from_static(b"Node B"),
            },
            TestNode {
                addr: Bytes::from_static(b"Node C"),
            },
            TestNode {
                addr: Bytes::from_static(b"Node D"),
            },
        ]
    }

    fn test_keys() -> Vec<Bytes> {
        vec![
            Bytes::from_static(b"key 1"),
            Bytes::from_static(b"key 2"),
            Bytes::from_static(b"key 3"),
            Bytes::from_static(b"key 4"),
            Bytes::from_static(b"key 5"),
            Bytes::from_static(b"key 6"),
            Bytes::from_static(b"key 7"),
            Bytes::from_static(b"key 8"),
            Bytes::from_static(b"key 9"),
            Bytes::from_static(b"key 10"),
            Bytes::from_static(b"key 11"),
            Bytes::from_static(b"key 12"),
            Bytes::from_static(b"key 13"),
        ]
    }
    struct TableTest {
        key: Bytes,
        owner: TestNode,
    }

    #[test]
    fn test_key_owner_table() {
        let nodes = test_nodes();
        let keys = test_keys();

        let test_cases: Vec<TableTest> = vec![
            TableTest {
                key: keys[0].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[1].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[2].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[3].clone(),
                owner: nodes[1].clone(),
            },
            TableTest {
                key: keys[4].clone(),
                owner: nodes[1].clone(),
            },
            TableTest {
                key: keys[5].clone(),
                owner: nodes[1].clone(),
            },
            TableTest {
                key: keys[6].clone(),
                owner: nodes[2].clone(),
            },
            TableTest {
                key: keys[7].clone(),
                owner: nodes[2].clone(),
            },
            TableTest {
                key: keys[8].clone(),
                owner: nodes[2].clone(),
            },
            TableTest {
                key: keys[9].clone(),
                owner: nodes[3].clone(),
            },
            TableTest {
                key: keys[10].clone(),
                owner: nodes[3].clone(),
            },
            TableTest {
                key: keys[11].clone(),
                owner: nodes[3].clone(),
            },
            TableTest {
                key: keys[12].clone(), //this is where we go around the circular buffer back to node A
                owner: nodes[0].clone(),
            },
        ];

        let mut ring = ConsistentHashing::new_with_hash_fn(test_hash_fn);
        for node in nodes.iter() {
            ring.add_node(node.addr.clone()).unwrap();
        }

        for test_case in test_cases {
            assert_eq!(
                test_case.owner.addr,
                ring.key_owner(&test_case.key).unwrap()
            );
        }
    }

    #[test]
    fn test_single_node() {
        let mut ring = ConsistentHashing::new_with_hash_fn(test_hash_fn);
        let nodes = test_nodes();
        ring.add_node(nodes[0].addr.clone()).unwrap();

        // All keys must belong to this single node
        let keys = test_keys();
        for key in keys {
            assert_eq!(nodes[0].addr, ring.key_owner(&key).unwrap());
        }
    }

    #[test]
    fn test_add_node() {
        let mut ring = ConsistentHashing::new_with_hash_fn(test_hash_fn);
        let nodes = test_nodes();
        ring.add_node(nodes[0].addr.clone()).unwrap();

        // All keys must belong to this single node
        let keys = test_keys();
        for key in keys.iter() {
            assert_eq!(nodes[0].addr.clone(), ring.key_owner(&key).unwrap());
        }

        // By adding a node, we change the ownership.
        // let's make sure this change works as expected
        ring.add_node(nodes[1].addr.clone()).unwrap();
        let test_cases: Vec<TableTest> = vec![
            TableTest {
                key: keys[0].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[1].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[2].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[3].clone(),
                owner: nodes[1].clone(),
            },
            TableTest {
                key: keys[4].clone(),
                owner: nodes[1].clone(),
            },
            TableTest {
                key: keys[5].clone(),
                owner: nodes[1].clone(),
            },
            TableTest {
                key: keys[6].clone(), //this is where we go around the circular buffer back to node A
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[7].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[8].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[9].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[10].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[11].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[12].clone(),
                owner: nodes[0].clone(),
            },
        ];

        for test_case in test_cases {
            assert_eq!(
                test_case.owner.addr,
                ring.key_owner(&test_case.key).unwrap()
            );
        }
    }

    #[test]
    fn test_remove_node() {
        let mut ring = ConsistentHashing::new_with_hash_fn(test_hash_fn);
        let nodes = test_nodes();

        // start with nodes A and B
        ring.add_node(nodes[0].addr.clone()).unwrap();
        ring.add_node(nodes[1].addr.clone()).unwrap();

        let keys = test_keys();
        let test_cases: Vec<TableTest> = vec![
            TableTest {
                key: keys[0].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[1].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[2].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[3].clone(),
                owner: nodes[1].clone(),
            },
            TableTest {
                key: keys[4].clone(),
                owner: nodes[1].clone(),
            },
            TableTest {
                key: keys[5].clone(),
                owner: nodes[1].clone(),
            },
            TableTest {
                key: keys[6].clone(), //this is where we go around the circular buffer back to node A
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[7].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[8].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[9].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[10].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[11].clone(),
                owner: nodes[0].clone(),
            },
            TableTest {
                key: keys[12].clone(),
                owner: nodes[0].clone(),
            },
        ];

        for test_case in test_cases {
            assert_eq!(
                test_case.owner.addr,
                ring.key_owner(&test_case.key).unwrap()
            );
        }

        // now let's remove node A and make sure all keys belong to node B
        ring.remove_node(&nodes[0].addr).unwrap();
        for key in keys.iter() {
            assert_eq!(nodes[1].addr, ring.key_owner(&key).unwrap());
        }
    }

    #[test]
    fn test_key_owner_without_nodes() {
        let ring = ConsistentHashing::default();
        let random_key = String::from("foo");
        assert!(ring.key_owner(random_key.as_bytes()).is_err());
    }
}
