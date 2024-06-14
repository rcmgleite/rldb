//! MinRequiredReplicas is a simple quorum algorithm that returns [`Evaluation::Reached`] enough calls to
//! [`Quorum::update`] are issued with [`OperationStatus::Success`] as argument.
//!
//! If the quorum is unreachable, calls to update will return [`Evaluation::Unreachable`].
//! If the quorum was not reached, but still can, calls to update will return [`Evaluation::NotReached`]
//! Finally, if quorum was reached, calls to update will return [`Evaluation::Reached`]
//!
//! Note that the number of calls to [`Quorum::update`] is bound by the [`MinRequiredReplicas::new`] n_replicas argument.
//! If you call update more than n_replicas times an error will be returned.
use super::{Evaluation, OperationStatus, Quorum, QuorumResult};
use crate::error::{Error, Result};
use std::hash::Hash;

/// Definition of a MinRequiredReplicas [`Quorum`] type
#[derive(Debug)]
pub struct MinRequiredReplicas<T, E> {
    /// Number of replicas that were not yet evaluated. This is initially set to the total number of replicas
    /// that exist
    remaining_replicas: usize,
    /// number of replicas that need to succeed in order for the quorum to me met
    required_successes: usize,
    /// hashmap containing the frequencies which each T value happened
    successes: Vec<T>,
    /// tracks all failures received
    failures: Vec<E>,
    /// Current state of this quorum instance
    current_state: Evaluation,
}

impl<T, E> MinRequiredReplicas<T, E> {
    /// Constructs a new instance of [`MinRequiredReplicas`]
    ///
    /// # Error
    /// This function returns an error if the provided `n_replicas` arugment is less than `required_successes`
    pub fn new(n_replicas: usize, required_successes: usize) -> Result<Self> {
        if n_replicas < required_successes {
            return Err(Error::Logic {
                reason: format!(
                    "n_replicas ({}) need to be higher than required_successes({}).",
                    n_replicas, required_successes
                ),
            });
        }

        Ok(Self {
            remaining_replicas: n_replicas,
            required_successes,
            successes: Default::default(),
            failures: Default::default(),
            current_state: Evaluation::NotReached,
        })
    }
}

impl<T: Eq + Hash, E: std::error::Error> Quorum<T, E> for MinRequiredReplicas<T, E> {
    fn update(&mut self, operation_status: OperationStatus<T, E>) -> Result<Evaluation> {
        if self.remaining_replicas == 0 {
            return Err(Error::Logic { reason: "Calling `update` on MinRequiredReplicas Quorum more times than the total number of replicas. This is a bug".to_string() });
        }

        match operation_status {
            OperationStatus::Success(item) => {
                self.successes.push(item);
            }
            OperationStatus::Failure(err) => {
                self.failures.push(err);
            }
        }

        self.remaining_replicas -= 1;

        // no point trying to evaluate again given the quorum is already unreachable
        if self.current_state == Evaluation::Unreachable {
            return Ok(self.current_state);
        }

        self.current_state = if self.successes.len() >= self.required_successes {
            Evaluation::Reached
        } else if self.remaining_replicas + self.successes.len() < self.required_successes {
            Evaluation::Unreachable
        } else {
            Evaluation::NotReached
        };

        Ok(self.current_state)
    }

    fn finish(self) -> QuorumResult<T, E> {
        QuorumResult {
            evaluation: self.current_state,
            successes: self.successes,
            failures: self.failures,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Error,
        persistency::quorum::{Evaluation, OperationStatus, Quorum},
    };

    use super::MinRequiredReplicas;

    #[test]
    fn test_quorum_reached() {
        let mut q = MinRequiredReplicas::new(3, 2).unwrap();
        assert_eq!(
            q.update(OperationStatus::Success(())).unwrap(),
            Evaluation::NotReached
        );
        assert_eq!(
            q.update(OperationStatus::Success(())).unwrap(),
            Evaluation::Reached
        );

        // even after the quorum is reached, there's nothing that prevents a client from calling update again.
        assert_eq!(
            q.update(OperationStatus::Failure(Error::Generic {
                reason: "fake".to_string(),
            }))
            .unwrap(),
            Evaluation::Reached
        );

        let quorum_result = q.finish();
        assert_eq!(quorum_result.evaluation, Evaluation::Reached);
        assert_eq!(quorum_result.successes.len(), 2);
        assert_eq!(quorum_result.failures.len(), 1);
    }

    #[test]
    fn test_quorum_not_reached() {
        let mut q: MinRequiredReplicas<(), Error> = MinRequiredReplicas::new(3, 2).unwrap();
        assert_eq!(
            q.update(OperationStatus::Failure(Error::Generic {
                reason: "fake".to_string(),
            }))
            .unwrap(),
            Evaluation::NotReached
        );

        assert_eq!(
            q.update(OperationStatus::Failure(Error::Generic {
                reason: "fake".to_string(),
            }))
            .unwrap(),
            Evaluation::Unreachable
        );

        assert_eq!(
            q.update(OperationStatus::Failure(Error::Generic {
                reason: "fake".to_string(),
            }))
            .unwrap(),
            Evaluation::Unreachable
        );

        let quorum_result = q.finish();
        assert_eq!(quorum_result.evaluation, Evaluation::Unreachable);
        assert!(quorum_result.successes.is_empty());
        assert_eq!(quorum_result.failures.len(), 3);
    }

    #[test]
    fn test_failed_to_construct() {
        let err = MinRequiredReplicas::<(), Error>::new(2, 3).err().unwrap();
        match err {
            crate::error::Error::Logic { .. } => { /* noop */ }
            _ => {
                panic!("Unexpected err {}", err);
            }
        }
    }

    #[test]
    fn test_call_update_more_times_than_allowed() {
        let mut q: MinRequiredReplicas<(), Error> = MinRequiredReplicas::new(3, 2).unwrap();
        assert_eq!(
            q.update(OperationStatus::Success(())).unwrap(),
            Evaluation::NotReached
        );
        assert_eq!(
            q.update(OperationStatus::Success(())).unwrap(),
            Evaluation::Reached
        );
        assert_eq!(
            q.update(OperationStatus::Success(())).unwrap(),
            Evaluation::Reached
        );

        let err = q.update(OperationStatus::Success(())).err().unwrap();
        match err {
            crate::error::Error::Logic { .. } => { /* noop */ }
            _ => {
                panic!("Unexpected err {}", err);
            }
        }
    }
}
