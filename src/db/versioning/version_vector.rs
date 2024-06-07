//! Naive Version vector implementation
//!
//! Still not sure if these APIs are exactly what a version vector should provide..
//! This is what I understood from the papares on VersionVector implementations
use std::collections::HashMap;

/// Type alias for a processId
///
/// A ProcessId is an identifier of the owner of a mutation that needs to be tracked by a [`VersionVector`].
/// In rldb this will be a rldb node that received a PUT or DELETE request.
type ProcessId = u128;
/// Type alias for Version
///
/// Currently using u128 but that's absolutely too much.. might be able to find better representations for this later..
type Version = u128;

/// Enum used to determine the causality between [`VersionVector`] instances
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VersionVectorOrd {
    Equals,
    HappenedBefore,
    HappenedAfter,
    HappenedConcurrently,
}

/// The [`VersionVector`] definition
#[derive(Clone, Debug)]
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
        let mut process_ids = process_ids.into_iter();

        while let Some(pid) = process_ids.next() {
            let lhs_version = lhs.versions.get(pid).unwrap_or(&0);
            let rhs_version = rhs.versions.get(pid).unwrap_or(&0);

            f(pid, lhs_version, rhs_version)
        }
    }

    /// Compares 2 instances of [`VersionVector`]s and determines it's causality
    pub fn cmp(&self, rhs: &VersionVector) -> VersionVectorOrd {
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
}

#[cfg(test)]
mod tests {
    use crate::db::versioning::version_vector::VersionVectorOrd;

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
    struct CmdTableTestEntry {
        lhs_versions: Vec<(ProcessId, Version)>,
        rhs_versions: Vec<(ProcessId, Version)>,
        order: VersionVectorOrd,
    }

    // generates multiple cases that we know the answer to for the cmp function
    fn generate_cmd_table_test() -> Vec<CmdTableTestEntry> {
        vec![
            CmdTableTestEntry {
                lhs_versions: vec![],
                rhs_versions: vec![],
                order: VersionVectorOrd::Equals,
            },
            CmdTableTestEntry {
                lhs_versions: vec![(0, 1)],
                rhs_versions: vec![],
                order: VersionVectorOrd::HappenedAfter,
            },
            CmdTableTestEntry {
                lhs_versions: vec![],
                rhs_versions: vec![(0, 1)],
                order: VersionVectorOrd::HappenedBefore,
            },
            CmdTableTestEntry {
                lhs_versions: vec![(0, 1)],
                rhs_versions: vec![(1, 1)],
                order: VersionVectorOrd::HappenedConcurrently,
            },
            CmdTableTestEntry {
                lhs_versions: vec![(0, 0), (1, 1), (2, 1), (3, 1), (4, 1)],
                rhs_versions: vec![(1, 1), (2, 1), (3, 1), (4, 1)],
                order: VersionVectorOrd::Equals,
            },
            CmdTableTestEntry {
                lhs_versions: vec![(0, 1)],
                rhs_versions: vec![(0, 1), (1, 1)],
                order: VersionVectorOrd::HappenedBefore,
            },
            CmdTableTestEntry {
                lhs_versions: vec![(0, 0), (1, 1), (3, 1), (4, 1)],
                rhs_versions: vec![(1, 1), (2, 1), (3, 1), (4, 1)],
                order: VersionVectorOrd::HappenedBefore,
            },
            CmdTableTestEntry {
                lhs_versions: vec![(1, 4), (2, 5), (3, 2), (4, 5)],
                rhs_versions: vec![(1, 4), (2, 5), (3, 2), (4, 4)],
                order: VersionVectorOrd::HappenedAfter,
            },
            CmdTableTestEntry {
                lhs_versions: vec![(1, 4), (2, 5), (3, 2), (4, 5)],
                rhs_versions: vec![(1, 4), (2, 5), (3, 3), (4, 4)],
                order: VersionVectorOrd::HappenedConcurrently,
            },
        ]
    }

    #[test]
    fn test_version_vector_cmd_table_test() {
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

            if lhs.cmp(&rhs) != test.order {
                panic!(
                    "Failed for test {:?}\nExpected: {:?}, got: {:?}",
                    test,
                    test.order,
                    lhs.cmp(&rhs)
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
}