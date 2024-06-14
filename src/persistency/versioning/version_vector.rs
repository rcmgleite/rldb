//! Naive Version vector implementation
//!
//! Still not sure if these APIs are exactly what a version vector should provide..
//! This is what I understood from the papares on VersionVector implementations
use std::{collections::HashMap, mem::size_of};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

/// Type alias for a processId
///
/// A ProcessId is an identifier of the owner of a mutation that needs to be tracked by a [`VersionVector`].
/// In rldb this will be a rldb node that received a PUT or DELETE request.
pub type ProcessId = u128;
/// Type alias for Version
///
/// Currently using u128 but that's absolutely too much.. might be able to find better representations for this later..
pub type Version = u128;

/// Enum used to determine the causality between [`VersionVector`] instances
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VersionVectorOrd {
    Equals,
    HappenedBefore,
    HappenedAfter,
    HappenedConcurrently,
}

/// The [`VersionVector`] definition
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionVector {
    id: ProcessId,
    versions: HashMap<ProcessId, Version>,
}

impl VersionVector {
    /// Consturcts a new instance of a [`VersionVector`]
    pub fn new(self_pid: ProcessId) -> Self {
        Self {
            id: self_pid,
            versions: Default::default(),
        }
    }

    /// Increments its own version.
    ///
    /// This should be used every time a mutation is applied on a given processid
    pub fn increment(&mut self) {
        let entry = self.versions.entry(self.id).or_insert(0);
        *entry += 1;
    }

    fn for_every_process_id_and_version<F>(lhs: &VersionVector, rhs: &VersionVector, mut f: F)
    where
        F: FnMut(&ProcessId, &Version, &Version),
    {
        let mut process_ids: Vec<&u128> = lhs.versions.keys().chain(rhs.versions.keys()).collect();
        process_ids.sort();
        process_ids.dedup();
        let process_ids = process_ids.into_iter();

        for pid in process_ids {
            let lhs_version = lhs.versions.get(pid).unwrap_or(&0);
            let rhs_version = rhs.versions.get(pid).unwrap_or(&0);

            f(pid, lhs_version, rhs_version)
        }
    }

    /// Compares 2 instances of [`VersionVector`]s and determines it's causality
    pub fn causality(&self, rhs: &VersionVector) -> VersionVectorOrd {
        let mut happened_before = false;
        let mut happened_after = false;
        Self::for_every_process_id_and_version(self, rhs, |_, lhs_version, rhs_version| {
            if *lhs_version > *rhs_version {
                happened_after = true;
            }

            if *lhs_version < *rhs_version {
                happened_before = true;
            }
        });

        if happened_before && happened_after {
            VersionVectorOrd::HappenedConcurrently
        } else if happened_before {
            VersionVectorOrd::HappenedBefore
        } else if happened_after {
            VersionVectorOrd::HappenedAfter
        } else {
            VersionVectorOrd::Equals
        }
    }

    /// Merges 2 [`VersionVector`] instances into self by picking the max version for each pid.
    pub fn merge(&mut self, rhs: &VersionVector) {
        let mut ret = VersionVector::new(self.id);
        Self::for_every_process_id_and_version(self, rhs, |pid, lhs_version, rhs_version| {
            ret.versions
                .entry(*pid)
                .or_insert(std::cmp::max(*lhs_version, *rhs_version));
        });

        *self = ret;
    }

    /// Serializes the self into [`bytes::Bytes`].
    ///
    /// Format: |  u32  | u128 |   u128  | u128 |   u128  | ...
    ///         |n_items|  pid | version |  pid | version | ...
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32(self.versions.len() as u32);
        for (k, v) in self.versions.iter() {
            buf.put_u128(*k);
            buf.put_u128(*v);
        }

        buf.freeze()
    }

    /// Deserializes [bytes::Bytes] into [`VersionVector`]
    pub fn deserialize(self_id: ProcessId, mut serialized_vv: Bytes) -> Result<Self> {
        let min_size = size_of::<u32>();
        if serialized_vv.len() < min_size {
            return Err(Error::Internal(crate::error::Internal::Unknown { reason: format!(
                    "buffer provided to deserialize into VersionVector is too small. Expected size of at least {}, got {}",  min_size, serialized_vv.len())}));
        }

        let n_items = serialized_vv.get_u32() as usize;
        let expected_size = n_items * 2 * size_of::<u128>();
        if serialized_vv.len() != expected_size {
            return Err(Error::Internal(crate::error::Internal::Unknown { reason: format!(
                    "buffer provided to deserialize into VersionVector has the wrong size. Expected {}, got {}",  expected_size, serialized_vv.len())}));
        }

        let mut versions = HashMap::with_capacity(n_items);
        for _ in 0..n_items {
            let pid = serialized_vv.get_u128();
            let version = serialized_vv.get_u128();
            versions.insert(pid, version);
        }

        Ok(Self {
            id: self_id,
            versions,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use bytes::{BufMut, Bytes, BytesMut};

    use crate::{error::Internal, persistency::versioning::version_vector::VersionVectorOrd};

    use super::{ProcessId, Version, VersionVector};

    #[test]
    fn test_version_vector_increment() {
        let mut vv = VersionVector::new(0);
        assert!(vv.versions.get(&0).is_none());
        vv.increment();
        assert_eq!(*vv.versions.get(&0).unwrap(), 1);
        vv.increment();
        assert_eq!(*vv.versions.get(&0).unwrap(), 2);
    }

    #[derive(Debug)]
    struct CausalityTableTestEntry {
        lhs_versions: Vec<(ProcessId, Version)>,
        rhs_versions: Vec<(ProcessId, Version)>,
        expected_order: VersionVectorOrd,
    }

    // generates multiple cases that we know the answer to for the cmp function
    fn generate_cmd_table_test() -> Vec<CausalityTableTestEntry> {
        vec![
            CausalityTableTestEntry {
                lhs_versions: vec![],
                rhs_versions: vec![],
                expected_order: VersionVectorOrd::Equals,
            },
            CausalityTableTestEntry {
                lhs_versions: vec![(0, 1)],
                rhs_versions: vec![],
                expected_order: VersionVectorOrd::HappenedAfter,
            },
            CausalityTableTestEntry {
                lhs_versions: vec![],
                rhs_versions: vec![(0, 1)],
                expected_order: VersionVectorOrd::HappenedBefore,
            },
            CausalityTableTestEntry {
                lhs_versions: vec![(0, 1)],
                rhs_versions: vec![(1, 1)],
                expected_order: VersionVectorOrd::HappenedConcurrently,
            },
            CausalityTableTestEntry {
                lhs_versions: vec![(0, 0), (1, 1), (2, 1), (3, 1), (4, 1)],
                rhs_versions: vec![(1, 1), (2, 1), (3, 1), (4, 1)],
                expected_order: VersionVectorOrd::Equals,
            },
            CausalityTableTestEntry {
                lhs_versions: vec![(0, 1)],
                rhs_versions: vec![(0, 1), (1, 1)],
                expected_order: VersionVectorOrd::HappenedBefore,
            },
            CausalityTableTestEntry {
                lhs_versions: vec![(0, 0), (1, 1), (3, 1), (4, 1)],
                rhs_versions: vec![(1, 1), (2, 1), (3, 1), (4, 1)],
                expected_order: VersionVectorOrd::HappenedBefore,
            },
            CausalityTableTestEntry {
                lhs_versions: vec![(1, 4), (2, 5), (3, 2), (4, 5)],
                rhs_versions: vec![(1, 4), (2, 5), (3, 2), (4, 4)],
                expected_order: VersionVectorOrd::HappenedAfter,
            },
            CausalityTableTestEntry {
                lhs_versions: vec![(1, 4), (2, 5), (3, 2), (4, 5)],
                rhs_versions: vec![(1, 4), (2, 5), (3, 3), (4, 4)],
                expected_order: VersionVectorOrd::HappenedConcurrently,
            },
        ]
    }

    #[test]
    fn test_version_vector_causality_table_test() {
        let tests = generate_cmd_table_test();
        for test in tests {
            let mut lhs = VersionVector::new(0);
            for version in test.lhs_versions.iter() {
                *lhs.versions.entry(version.0).or_insert(0) = version.1;
            }

            let mut rhs = VersionVector::new(1);
            for version in test.rhs_versions.iter() {
                *rhs.versions.entry(version.0).or_insert(0) = version.1;
            }

            if lhs.causality(&rhs) != test.expected_order {
                panic!(
                    "Failed for test {:?}\nExpected: {:?}, got: {:?}",
                    test,
                    test.expected_order,
                    lhs.causality(&rhs)
                );
            }
        }
    }

    #[test]
    fn test_merge_simple() {
        let mut vv1 = VersionVector::new(0);
        vv1.increment();
        let mut vv2 = VersionVector::new(1);
        vv2.increment();

        vv1.merge(&vv2);
        assert_eq!(vv1.id, 0);
        assert_eq!(vv1.versions, vec![(0, 1), (1, 1)].into_iter().collect());
    }

    #[test]
    fn test_merge_complex() {
        let mut vv1 = VersionVector::new(0);
        vv1.versions = vec![(0, 10), (1, 20), (4, 2), (5, 1), (10, 100)]
            .into_iter()
            .collect();
        let mut vv2 = VersionVector::new(1);
        vv2.versions = vec![(0, 15), (1, 15), (5, 1), (10, 101), (12, 12)]
            .into_iter()
            .collect();

        vv1.merge(&vv2);
        assert_eq!(vv1.id, 0);
        assert_eq!(
            vv1.versions,
            vec![(0, 15), (1, 20), (4, 2), (5, 1), (10, 101), (12, 12)]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut vv = VersionVector::new(0);
        vv.versions = vec![(0, 10), (1, 20), (4, 2), (5, 1), (10, 100)]
            .into_iter()
            .collect();

        let serialized = vv.serialize();
        assert_eq!(
            serialized.len(),
            size_of::<u32>() + (vv.versions.len() * 2 * size_of::<u128>())
        );

        let deserialized = VersionVector::deserialize(vv.id, serialized).unwrap();
        assert_eq!(vv, deserialized);
    }

    #[test]
    fn test_fail_to_deserialize_buffer_too_small() {
        let err = VersionVector::deserialize(0, Bytes::from("a"))
            .err()
            .unwrap();

        match err {
            crate::error::Error::Internal(Internal::Unknown { .. }) => {}
            _ => {
                panic!("Unexpected err: {}", err)
            }
        }
    }

    #[test]
    fn test_fail_to_deserialize_buffer_size_mismatch() {
        let mut buf = BytesMut::new();
        buf.put_u32(10);
        let err = VersionVector::deserialize(0, buf.freeze()).err().unwrap();

        match err {
            crate::error::Error::Internal(Internal::Unknown { .. }) => {}
            _ => {
                panic!("Unexpected err: {}", err)
            }
        }
    }
}
