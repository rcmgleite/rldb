//! MinRequiredReplicas is a simple quorum algorithm that returns [`Evaluation::Reached`] enough calls to
//! [`Quorum::update`] are issued with [`OperationStatus::Success`] as argument.
//!
//! If the quorum was not reached, but still can, calls to update will return [`Evaluation::NotReached`]
//! Finally, if quorum was reached, calls to update will return [`Evaluation::Reached`]
//!
//! Note that the number of calls to [`Quorum::update`] is bound by the [`MinRequiredReplicas::new`] n_replicas argument.
//! If you call update more than n_replicas times an error will be returned.
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

use super::{Evaluation, OperationStatus, Quorum, QuorumResult};
use crate::error::{Error, Result};

/// Definition of a MinRequiredReplicas [`Quorum`] type
#[derive(Debug)]
pub struct MinRequiredReplicas<T, E> {
    /// number of replicas that need to succeed in order for the quorum to me met
    required_successes: usize,
    /// hashmap containing the frequencies which each T value happened
    successes: HashMap<T, usize>,
    met_quorum: HashSet<T>,
    /// tracks all failures received
    failures: Vec<E>,
    /// Current state of this quorum instance
    current_state: Evaluation<T>,
}

impl<T, E> MinRequiredReplicas<T, E> {
    /// Constructs a new instance of [`MinRequiredReplicas`]
    ///
    /// # Error
    pub fn new(required_successes: usize) -> Result<Self> {
        if required_successes < 1 {
            return Err(Error::Logic {
                reason: format!(
                    "required_success has to be greated than 1. got{}",
                    required_successes
                ),
            });
        }

        Ok(Self {
            required_successes,
            successes: Default::default(),
            met_quorum: Default::default(),
            failures: Default::default(),
            current_state: Evaluation::NotReached,
        })
    }
}

/// Note: T is now required to be clone. We expect this clone operation to be cheap (eg: calling clone on [`bytes::Bytes`]),
/// otherwise this operation can get very expensive
impl<T: Hash + Eq + Clone + std::fmt::Debug, E: std::error::Error> Quorum<T, E>
    for MinRequiredReplicas<T, E>
{
    fn update(&mut self, operation_status: OperationStatus<T, E>) -> Result<Evaluation<T>> {
        match operation_status {
            OperationStatus::Success(item) => {
                let entry = self.successes.entry(item.clone()).or_insert(0);
                *entry += 1;

                if *entry >= self.required_successes {
                    self.met_quorum.insert(item);
                }
            }
            OperationStatus::Failure(err) => {
                self.failures.push(err);
            }
        }

        self.current_state = if !self.met_quorum.is_empty() {
            // unwrap is safe because of the restriction during construction of this instance where
            // required_successes has to be higher than 0
            Evaluation::Reached(self.met_quorum.iter().cloned().collect())
        } else {
            Evaluation::NotReached
        };

        Ok(self.current_state.clone())
    }

    fn finish(self) -> QuorumResult<T, E> {
        QuorumResult {
            evaluation: self.current_state,
            failures: self.failures,
            partial_successes: self.successes,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::{Error, Internal},
        persistency::quorum::{Evaluation, OperationStatus, Quorum},
    };

    use super::MinRequiredReplicas;

    #[test]
    fn test_quorum_reached() {
        let mut q = MinRequiredReplicas::new(2).unwrap();
        assert_eq!(
            q.update(OperationStatus::Success(())).unwrap(),
            Evaluation::NotReached
        );
        assert_eq!(
            q.update(OperationStatus::Success(())).unwrap(),
            Evaluation::Reached(vec![()])
        );

        // even after the quorum is reached, there's nothing that prevents a client from calling update again.
        assert_eq!(
            q.update(OperationStatus::Failure(Error::Internal(
                Internal::Unknown {
                    reason: "fake".to_string()
                }
            )))
            .unwrap(),
            Evaluation::Reached(vec![()])
        );

        let quorum_result = q.finish();
        assert_eq!(quorum_result.evaluation, Evaluation::Reached(vec![()]));
        assert_eq!(quorum_result.failures.len(), 1);
    }

    #[test]
    fn test_quorum_not_reached() {
        let mut q: MinRequiredReplicas<(), Error> = MinRequiredReplicas::new(2).unwrap();
        assert_eq!(
            q.update(OperationStatus::Failure(Error::Internal(
                Internal::Unknown {
                    reason: "fake".to_string()
                }
            )))
            .unwrap(),
            Evaluation::NotReached
        );

        assert_eq!(
            q.update(OperationStatus::Failure(Error::Internal(
                Internal::Unknown {
                    reason: "fake".to_string()
                }
            )))
            .unwrap(),
            Evaluation::NotReached
        );

        assert_eq!(
            q.update(OperationStatus::Failure(Error::Internal(
                Internal::Unknown {
                    reason: "fake".to_string()
                }
            )))
            .unwrap(),
            Evaluation::NotReached
        );

        let quorum_result = q.finish();
        assert_eq!(quorum_result.evaluation, Evaluation::NotReached);
        assert_eq!(quorum_result.failures.len(), 3);
    }

    #[test]
    fn test_failed_to_construct() {
        let err = MinRequiredReplicas::<(), Error>::new(0).err().unwrap();
        match err {
            crate::error::Error::Logic { .. } => { /* noop */ }
            _ => {
                panic!("Unexpected err {}", err);
            }
        }
    }
}
