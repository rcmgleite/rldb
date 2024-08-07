//! Module that contain quorum algorithm implementations

pub mod min_required_replicas;
use std::collections::HashMap;

use crate::error::Result;

/// The result of a [`Quorum::finish`] call.
///
/// If quorum was reached, returns the [`Evaluation::Reached`] variant containing the original type T and how many times this value was found
/// If quorum was NOT reached, returns the [`Evaluation::NotReached`] variant containing all errors encountered
///
/// Note 1: This trait allows for multiple results to meet quorum. For that reason, the [`Evaluation::Reached`] variant contains
///  an array of Vec<T, usize> where usize is how many times the given value was received
/// Note 2: The API could be a bit nicer if we always returned the failures (might be useful for logging?)
#[derive(Clone, Debug, PartialEq)]
pub enum Evaluation<T> {
    /// Quorum was reached
    /// Note that this returns the value which met quorum. This means that a user of this interface
    /// is able to stop the quorum checking process and proceed without calling [`Quorum::finish`] if they want to.
    /// This is especially useful when the workflow already met quorum but other operations are still inflight
    Reached(Vec<T>),
    /// Quorum was not reached yet but may reached be in the future
    NotReached,
}

/// The result of a [`Quorum::finish`] call
#[derive(Debug)]
pub struct QuorumResult<T, E> {
    /// The quorum evaluation - see [`Evaluation`]
    pub evaluation: Evaluation<T>,
    /// failed items
    pub failures: Vec<E>,
    pub partial_successes: HashMap<T, usize>,
}

/// Argument passed to the [`Quorum::update`] function to mark an operation as either a Success or a Failure
pub enum OperationStatus<T, E> {
    Success(T),
    Failure(E),
}

/// Trait that defines the Quorum interface.
pub trait Quorum<T, E: std::error::Error> {
    /// Updates the Quorum internal state with either a success or a failure
    fn update(&mut self, operation_status: OperationStatus<T, E>) -> Result<Evaluation<T>>;

    /// Returns Ok if the quorum was met or an Error otherwise
    fn finish(self) -> QuorumResult<T, E>;
}
