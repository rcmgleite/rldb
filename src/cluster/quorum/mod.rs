//! Module that contain quorum algorithm implementations

pub mod min_required_replicas;
use crate::cluster::error::Result;

/// The result of a [`Quorum::finish`] call.
///
/// If quorum was reached, returns the [`Evaluation::Reached`] variant containing the original type T and how many times this value was found
/// If quorum was NOT reached, returns the [`Evaluation::NotReached`] variant containing all errors encountered
///
/// Note 1: This trait allows for multiple results to meet quorum. For that reason, the [`Evaluation::Reached`] variant contains
///  an array of Vec<T, usize> where usize is how many times the given value was received
/// Note 2: The API could be a bit nicer if we always returned the failures (might be useful for logging?)
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Evaluation {
    /// Quorum was reached
    Reached,
    /// Quorum was not reached yet but may reached be in the future
    NotReached,
    /// Given the current state, it's impossible to reach quorum no matter how many more operations succeed
    Unreachable,
}

#[derive(Debug)]
pub struct QuorumResult<T, E> {
    pub evaluation: Evaluation,
    pub successes: Vec<T>,
    pub failures: Vec<E>,
}

/// Argument passed to the [`Quorum::update`] function to mark an operation as either a Success or a Failure
pub enum OperationStatus<T, E> {
    Success(T),
    Failure(E),
}

/// Trait that defines the Quorum interface.
pub trait Quorum<T, E: std::error::Error> {
    /// Updates the Quorum internal state with either a success or a failure
    fn update(&mut self, operation_status: OperationStatus<T, E>) -> Result<Evaluation>;

    /// Returns Ok if the quorum was met or an Error otherwise
    fn finish(self) -> QuorumResult<T, E>;
}
