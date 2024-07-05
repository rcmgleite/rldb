//! MinRequiredReplicas is a simple quorum algorithm that returns [`Evaluation::Reached`] enough calls to
//! [`Quorum::update`] are issued with [`OperationStatus::Success`] as argument.
//!
//! If the quorum is unreachable, calls to update will return [`Evaluation::Unreachable`].
//! If the quorum was not reached, but still can, calls to update will return [`Evaluation::NotReached`]
//! Finally, if quorum was reached, calls to update will return [`Evaluation::Reached`]
//!
//! Note that the number of calls to [`Quorum::update`] is bound by the [`MinRequiredReplicas::new`] n_replicas argument.
//! If you call update more than n_replicas times an error will be returned.
use std::{collections::HashMap, hash::Hash};

use tracing::{event, Level};

use super::{Evaluation, OperationStatus, Quorum, QuorumResult};
use crate::error::{Error, Result};

/// Definition of a MinRequiredReplicas [`Quorum`] type
#[derive(Debug)]
pub struct MinRequiredReplicas<T, E> {
    total_replicas: usize,
    /// Number of replicas that were not yet evaluated. This is initially set to the total number of replicas
    /// that exist
    remaining_replicas: usize,
    /// number of replicas that need to succeed in order for the quorum to me met
    required_successes: usize,
    /// hashmap containing the frequencies which each T value happened
    successes: HashMap<T, usize>,
    /// which T has the most successes
    highest_success_count_per_value: Option<(T, usize)>,
    /// tracks all failures received
    failures: Vec<E>,
    /// Current state of this quorum instance
    current_state: Evaluation<T>,
}

impl<T, E> MinRequiredReplicas<T, E> {
    /// Constructs a new instance of [`MinRequiredReplicas`]
    ///
    /// # Error
    /// This function returns an error if the provided `n_replicas` arugment is less than `required_successes`
    pub fn new(n_replicas: usize, required_successes: usize) -> Result<Self> {
        if required_successes < 1 {
            return Err(Error::Logic {
                reason: format!(
                    "required_success has to be greated than 1. got{}",
                    required_successes
                ),
            });
        }

        if n_replicas < required_successes {
            return Err(Error::Logic {
                reason: format!(
                    "n_replicas ({}) need to be higher than required_successes({}).",
                    n_replicas, required_successes
                ),
            });
        }

        Ok(Self {
            total_replicas: n_replicas,
            remaining_replicas: n_replicas,
            required_successes,
            highest_success_count_per_value: None,
            successes: Default::default(),
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
        if self.remaining_replicas == 0 {
            return Err(Error::Logic { reason: "Calling `update` on MinRequiredReplicas Quorum more times than the total number of replicas. This is a bug".to_string() });
        }

        match operation_status {
            OperationStatus::Success(item) => {
                event!(Level::WARN, "DEBUG: {:?}", item);
                {
                    let entry = self.successes.entry(item.clone()).or_insert(0);
                    *entry += 1;
                }

                for s in self.successes.iter() {
                    match &self.highest_success_count_per_value {
                        Some(highest_success_count_per_value) => {
                            self.highest_success_count_per_value = Some((
                                highest_success_count_per_value.0.clone(),
                                std::cmp::max(highest_success_count_per_value.1, *s.1),
                            ));
                        }
                        None => {
                            self.highest_success_count_per_value = Some((s.0.clone(), *s.1));
                        }
                    }
                }
            }
            OperationStatus::Failure(err) => {
                self.failures.push(err);
            }
        }

        self.remaining_replicas -= 1;

        // no point trying to evaluate again given the quorum is already unreachable
        if self.current_state == Evaluation::Unreachable {
            return Ok(self.current_state.clone());
        }

        let highest_success_count_per_value = self
            .highest_success_count_per_value
            .as_ref()
            .map(|e| e.1)
            .unwrap_or_default();
        let highest_success_value = self.highest_success_count_per_value.clone().map(|e| e.0);

        self.current_state = if highest_success_count_per_value >= self.required_successes {
            // unwrap is safe because of the restriction during construction of this instance where
            // required_successes has to be higher than 0
            Evaluation::Reached(highest_success_value.unwrap())
        } else if self.remaining_replicas + highest_success_count_per_value
            < self.required_successes
        {
            Evaluation::Unreachable
        } else {
            Evaluation::NotReached
        };

        Ok(self.current_state.clone())
    }

    fn finish(self) -> QuorumResult<T, E> {
        QuorumResult {
            evaluation: self.current_state,
            failures: self.failures,
            total: self.total_replicas,
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
        let mut q = MinRequiredReplicas::new(3, 2).unwrap();
        assert_eq!(
            q.update(OperationStatus::Success(())).unwrap(),
            Evaluation::NotReached
        );
        assert_eq!(
            q.update(OperationStatus::Success(())).unwrap(),
            Evaluation::Reached(())
        );

        // even after the quorum is reached, there's nothing that prevents a client from calling update again.
        assert_eq!(
            q.update(OperationStatus::Failure(Error::Internal(
                Internal::Unknown {
                    reason: "fake".to_string()
                }
            )))
            .unwrap(),
            Evaluation::Reached(())
        );

        let quorum_result = q.finish();
        assert_eq!(quorum_result.evaluation, Evaluation::Reached(()));
        assert_eq!(quorum_result.failures.len(), 1);
    }

    #[test]
    fn test_quorum_not_reached() {
        let mut q: MinRequiredReplicas<(), Error> = MinRequiredReplicas::new(3, 2).unwrap();
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
            Evaluation::Unreachable
        );

        assert_eq!(
            q.update(OperationStatus::Failure(Error::Internal(
                Internal::Unknown {
                    reason: "fake".to_string()
                }
            )))
            .unwrap(),
            Evaluation::Unreachable
        );

        let quorum_result = q.finish();
        assert_eq!(quorum_result.evaluation, Evaluation::Unreachable);
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
            Evaluation::Reached(())
        );
        assert_eq!(
            q.update(OperationStatus::Success(())).unwrap(),
            Evaluation::Reached(())
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
