use anyhow::anyhow;
use murmur3::murmur3_x86_128;
use std::io::Cursor;

/// Let's force the usage of Hash functions that return u128 for now..
type HashFunctionReturnType = u128;

/// Currently a Node is identified by a single addrs.
///  1. This struct likely do not belong in this module
///  2. when we have vnodes this structure might include more stuff
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Node {
    addr: String,
}

/// ConsistentHashing is one of the partitioning schemes implemented for rldb.
/// The goal of consistent hashing is to enable a distributed system to decide
/// which storage node should own a specific key. It does it by creating a
/// fixed hash space - in this case from [0, 2^128)
/// and computing the hash of both the storage nodes and the keys being stored.
/// The node that owns the key is the first node whose hash is higher than the hash of the key.
/// Note that this hash space should be viewed as a circular buffer (or hash ring). Let's
/// try to understand the hash ring statement through an example:
///
/// In this example, hash space that goes from [0,10] (ie: the hash function returns a number between 0 and 10).
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
#[derive(Debug)]
pub struct ConsistentHashing {
    nodes: Vec<Node>,
    keys: Vec<HashFunctionReturnType>,
    hash_fn: fn(&[u8]) -> HashFunctionReturnType,
}

impl ConsistentHashing {
    pub fn new_with_hash_fn(hash_fn: fn(&[u8]) -> HashFunctionReturnType) -> Self {
        Self {
            keys: Vec::new(),
            nodes: Vec::new(),
            hash_fn,
        }
    }

    /// Adds a new node to the hash ring
    pub fn add_node(&mut self, node: Node) -> anyhow::Result<HashFunctionReturnType> {
        let node_hash = (self.hash_fn)(node.addr.as_bytes());
        match self.keys.binary_search(&node_hash) {
            Ok(_) =>return Err(anyhow!("ConsistentHashing found a collision on its hash algorithm. This is currently an un-recoverable issue...")),
            Err(index) => {
                self.keys.insert(index, node_hash);
                self.nodes.insert(index, node);
                return Ok(index as HashFunctionReturnType)
            }
        }
    }

    /// Removes an existing node from the hash ring
    pub fn remove_node(&mut self, node: Node) {
        let node_hash = (self.hash_fn)(node.addr.as_bytes());
        if let Ok(index) = self.keys.binary_search(&node_hash) {
            self.keys.remove(index);
            self.nodes.remove(index);
        }
    }

    /// Finds the owner [`Node`] of a given key
    /// Returns an error if the current ConsistentHash instance doesn't have at least one [`Node`]
    pub fn key_owner(&self, key: &[u8]) -> anyhow::Result<Node> {
        if self.nodes.is_empty() {
            return Err(anyhow!("Can't ask for owner if no nodes are present"));
        }

        let key_hash = (self.hash_fn)(key);
        let index = self.keys.partition_point(|elem| *elem < key_hash) % self.nodes.len();

        Ok(self.nodes[index].clone())
    }
}

impl Default for ConsistentHashing {
    fn default() -> Self {
        Self {
            nodes: Default::default(),
            keys: Default::default(),
            hash_fn: murmur3_hash,
        }
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
    use crate::cluster::partitioning::{murmur3_hash, HashFunctionReturnType, Node};
    use quickcheck::{quickcheck, Arbitrary};
    use rand::{distributions::Alphanumeric, Rng};
    use std::{collections::HashMap, ops::Range};

    #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
    struct AddNodeTestInput {
        nodes: Vec<Node>,
    }

    fn generate_random_ascii_string(range_size: Range<usize>) -> String {
        let string_size = rand::thread_rng().gen_range(range_size);
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(string_size)
            .map(char::from)
            .collect()
    }

    fn generate_random_nodes(range: Range<usize>) -> Vec<Node> {
        let n_nodes = rand::thread_rng().gen_range(range);
        let mut nodes = Vec::with_capacity(n_nodes);
        for _ in 0..n_nodes {
            nodes.push(Node {
                addr: generate_random_ascii_string(10..20),
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

    impl Arbitrary for Node {
        fn arbitrary(_: &mut quickcheck::Gen) -> Self {
            Self {
                addr: generate_random_ascii_string(1..20),
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

        let node_hash_mapping: HashMap<HashFunctionReturnType, String> = test_input
            .nodes
            .iter()
            .map(|elem| (murmur3_hash(elem.addr.as_bytes()), elem.addr.clone()))
            .collect();

        for node in test_input.nodes.iter() {
            ring.add_node(Node {
                addr: node.addr.clone(),
            })
            .unwrap();
        }

        // now compute the hashes manually, construct a vector and sort it.
        // This has to be the same as ring.keys
        let mut nodes_key: Vec<HashFunctionReturnType> = test_input
            .nodes
            .iter()
            .map(|e| murmur3_hash(e.addr.as_bytes()))
            .collect();
        nodes_key.sort();

        // ring.keys contains all keys and it's sorted
        assert_eq!(ring.keys.len(), test_input.nodes.len());
        assert_eq!(ring.keys, nodes_key);
        assert_eq!(ring.nodes.len(), test_input.nodes.len());

        // now make sure that the ring.keys and ring.nodes are synchronized
        for i in 0..ring.keys.len() {
            assert_eq!(node_hash_mapping[&ring.keys[i]], ring.nodes[i].addr);
        }
    }

    #[derive(Debug, Clone)]
    struct KeyOwnerTestInput {
        nodes: Vec<Node>,
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

    // TODO: Assert the key_owner return..
    // currently we just make sure that no crashes happen (eg: overflow)
    #[quickcheck]
    fn test_key_owner_randomized(test_input: KeyOwnerTestInput) {
        let mut ring = ConsistentHashing::default();

        for node in test_input.nodes.iter() {
            ring.add_node(node.clone()).unwrap();
        }

        for key in test_input.keys.iter() {
            ring.key_owner(key.as_bytes()).unwrap();
        }
    }

    fn test_hash_fn(key: &[u8]) -> u128 {
        // this table precisely maps known keys to known hashes.
        // we will build test cases to cover all cases based on these known keys.
        let table: HashMap<String, u128> = vec![
            ("Node A".to_string(), 10u128),
            ("Node B".to_string(), 20u128),
            ("Node C".to_string(), 30u128),
            ("Node D".to_string(), 40u128),
            ("key 1".to_string(), 1u128),
            ("key 2".to_string(), 5u128),
            ("key 3".to_string(), 10u128),
            ("key 4".to_string(), 11u128),
            ("key 5".to_string(), 19u128),
            ("key 6".to_string(), 20u128),
            ("key 7".to_string(), 21u128),
            ("key 8".to_string(), 28u128),
            ("key 9".to_string(), 30u128),
            ("key 10".to_string(), 31u128),
            ("key 11".to_string(), 39u128),
            ("key 12".to_string(), 40u128),
            ("key 13".to_string(), 41u128),
        ]
        .into_iter()
        .collect();

        table[&String::from_utf8(Vec::from(key)).unwrap()]
    }

    fn test_nodes() -> Vec<Node> {
        vec![
            Node {
                addr: "Node A".to_string(),
            },
            Node {
                addr: "Node B".to_string(),
            },
            Node {
                addr: "Node C".to_string(),
            },
            Node {
                addr: "Node D".to_string(),
            },
        ]
    }

    fn test_keys() -> Vec<String> {
        vec![
            "key 1".to_string(),
            "key 2".to_string(),
            "key 3".to_string(),
            "key 4".to_string(),
            "key 5".to_string(),
            "key 6".to_string(),
            "key 7".to_string(),
            "key 8".to_string(),
            "key 9".to_string(),
            "key 10".to_string(),
            "key 11".to_string(),
            "key 12".to_string(),
            "key 13".to_string(),
        ]
    }
    struct TableTest {
        key: String,
        owner: Node,
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
            ring.add_node(node.clone()).unwrap();
        }

        for test_case in test_cases {
            assert_eq!(
                test_case.owner,
                ring.key_owner(test_case.key.as_bytes()).unwrap()
            );
        }
    }

    #[test]
    fn test_single_node() {
        let mut ring = ConsistentHashing::new_with_hash_fn(test_hash_fn);
        let nodes = test_nodes();
        ring.add_node(nodes[0].clone()).unwrap();

        // All keys must belong to this single node
        let keys = test_keys();
        for key in keys {
            assert_eq!(nodes[0].clone(), ring.key_owner(key.as_bytes()).unwrap());
        }
    }

    #[test]
    fn test_add_node() {
        let mut ring = ConsistentHashing::new_with_hash_fn(test_hash_fn);
        let nodes = test_nodes();
        ring.add_node(nodes[0].clone()).unwrap();

        // All keys must belong to this single node
        let keys = test_keys();
        for key in keys.iter() {
            assert_eq!(nodes[0].clone(), ring.key_owner(key.as_bytes()).unwrap());
        }

        // By adding a node, we change the ownership.
        // let's make sure this change works as expected
        ring.add_node(nodes[1].clone()).unwrap();
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
                test_case.owner,
                ring.key_owner(test_case.key.as_bytes()).unwrap()
            );
        }
    }

    #[test]
    fn test_remove_node() {
        let mut ring = ConsistentHashing::new_with_hash_fn(test_hash_fn);
        let nodes = test_nodes();

        // start with nodes A and B
        ring.add_node(nodes[0].clone()).unwrap();
        ring.add_node(nodes[1].clone()).unwrap();

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
                test_case.owner,
                ring.key_owner(test_case.key.as_bytes()).unwrap()
            );
        }

        // now let's remove node A and make sure all keys belong to node B
        ring.remove_node(nodes[0].clone());
        for key in keys.iter() {
            assert_eq!(nodes[1].clone(), ring.key_owner(key.as_bytes()).unwrap());
        }
    }
}
