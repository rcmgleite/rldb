//! Naive Version vector implementation
//!
//! Still not sure if these APIs are exactly what a version vector should provide..
//! This is what I understood from the papares on VersionVector implementations
use std::collections::BTreeMap;

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
    HappenedBefore,
    HappenedAfter,
    HappenedConcurrently,
}

/// The [`VersionVector`] definition
#[derive(Clone, Debug)]
pub struct VersionVector {
    id: ProcessId,
    versions: BTreeMap<ProcessId, Version>,
}

impl VersionVector {
    /// Consturcts a new instance of a [`VersionVector`]
    pub fn new(self_pid: ProcessId) -> Self {
        Self {
            id: self_pid,
            versions: Default::default(),
        }
    }

    /// Bumps the version of self.
    ///
    /// This should be used every time a mutation is applied on a given processid
    pub fn bump_version(&mut self) {
        let entry = self.versions.entry(self.id).or_insert(0);
        *entry += 1;
    }

    /// Compares 2 instances of [`VersionVector`]s and determines it's causality
    pub fn cmp(&self, rhs: &VersionVector) -> VersionVectorOrd {
        let mut happened_before = false;
        let mut happened_after = false;
        let mut happened_concurrently = false;
        let mut lhs_iter = self.versions.iter();
        let mut rhs_iter = rhs.versions.iter();

        loop {
            match (lhs_iter.next(), rhs_iter.next()) {
                // Both vectors are done.. nothing to do
                (None, None) => {
                    break;
                }
                // only rhs has a version - this means self happened before lhs
                (None, Some(_)) => {
                    happened_before = true;
                    break;
                }
                // only self has a version - this means self happened after rhs
                (Some(_), None) => {
                    happened_after = true;
                    break;
                }
                // here's the case where both elements have a version. Simply compare the versions to find which one happened before
                (Some((lhs_pid, lhs_version)), Some((rhs_pid, rhs_version))) => {
                    if *lhs_pid != *rhs_pid {
                        happened_concurrently = true;
                        break;
                    }

                    if lhs_version > rhs_version {
                        happened_after = true;
                    }

                    if lhs_version < rhs_version {
                        happened_before = true;
                    }
                }
            }
        }

        if happened_concurrently || (happened_after && happened_before) {
            VersionVectorOrd::HappenedConcurrently
        } else if happened_after {
            VersionVectorOrd::HappenedAfter
        } else {
            VersionVectorOrd::HappenedBefore
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::db::versioning::version_vector::VersionVectorOrd;

    use super::VersionVector;

    #[test]
    fn test_version_vector_concurrent() {
        let mut vv1 = VersionVector::new(0);
        vv1.bump_version();

        let mut vv2 = VersionVector::new(1);
        vv2.bump_version();

        assert_eq!(vv1.cmp(&vv2), VersionVectorOrd::HappenedConcurrently);
    }

    #[test]
    fn test_version_vector_happened_before() {
        let mut vv1 = VersionVector::new(0);
        let mut vv2 = VersionVector::new(1);

        *vv1.versions.entry(0).or_insert(0) = 1;
        *vv2.versions.entry(0).or_insert(0) = 1;
        *vv2.versions.entry(1).or_insert(0) = 1;

        assert_eq!(vv1.cmp(&vv2), VersionVectorOrd::HappenedBefore);
    }

    #[test]
    fn test_version_vector_happened_after() {
        let mut vv1 = VersionVector::new(0);
        let vv2 = VersionVector::new(1);

        *vv1.versions.entry(1).or_insert(0) = 1;

        assert_eq!(vv1.cmp(&vv2), VersionVectorOrd::HappenedAfter);
    }
}
