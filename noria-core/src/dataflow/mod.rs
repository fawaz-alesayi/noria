//! # Dataflow Execution Engine
//!
//! This module implements the core dataflow execution model from the Noria paper,
//! providing incremental view maintenance through differential updates.
//!
//! ## Differential Dataflow Model
//!
//! Unlike traditional caches that invalidate on write, Noria propagates *changes*
//! (deltas) through a graph of operators. Each change is represented as either:
//!
//! - **Positive record** (`Record::Positive`): An insertion or the "new" side of an update
//! - **Negative record** (`Record::Negative`): A deletion or the "old" side of an update
//!
//! For example, updating a row from `{id: 1, name: "Alice"}` to `{id: 1, name: "Bob"}`
//! produces two records:
//!
//! ```text
//! - {id: 1, name: "Alice"}   // Retract old value
//! + {id: 1, name: "Bob"}     // Insert new value
//! ```
//!
//! This delta representation enables **incremental computation**: aggregate operators
//! can update their state (e.g., increment/decrement a count) rather than recomputing
//! from scratch.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
//! │ Base Table  │────▶│  Operator   │────▶│    View     │
//! │   (node)    │     │   (node)    │     │   (node)    │
//! └─────────────┘     └─────────────┘     └─────────────┘
//!       │                   │                   │
//!       │ Records           │ Records           │ State
//!       │ (deltas)          │ (transformed)     │ (materialized)
//!       ▼                   ▼                   ▼
//! ```
//!
//! The [`LocalExecutor`] manages a directed acyclic graph (DAG) of nodes:
//! - **Base tables**: Entry points for CDC events from the database
//! - **Operators**: Transform records (filter, project, join, aggregate)
//! - **Views**: Materialized state that can be queried via O(1) lookups
//!
//! ## Key Types
//!
//! - [`Record`]: A single positive or negative data row
//! - [`Records`]: A collection of deltas to propagate through the graph
//! - [`LocalExecutor`]: The dataflow graph engine
//! - [`ViewHandle`]: Reference to a materialized view for lookups
//!
//! ## Paper Reference
//!
//! See Section 3 of the Noria paper for the full dataflow semantics:
//! <https://pdos.csail.mit.edu/papers/noria:osdi18.pdf>
//!
//! [`LocalExecutor`]: executor::LocalExecutor
//! [`ViewHandle`]: executor::ViewHandle

pub mod executor;
pub mod ops;
pub mod state;

pub use executor::{LocalExecutor, ViewHandle, ExecutorStats, NodeIndex};
pub use ops::{
    AggregateFunc, AggregateOp, FilterCondition, FilterOp, IdentityOp,
    JoinOp, JoinType, Operator, OperatorType, ProcessingResult, ProjectOp,
};
pub use state::{
    LookupResult, MemoryState, State, StateKey, StateSnapshot, Row,
    IntegerArrayState, DynamicState, is_integer_key, create_optimal_state,
};

use noria::DataType;

/// A single data record in the differential dataflow model.
///
/// Records are the fundamental unit of data propagation in Noria. Each record
/// represents either an insertion (`Positive`) or a retraction (`Negative`).
///
/// ## Differential Semantics
///
/// - **INSERT**: Emits one `Positive` record with the new row
/// - **DELETE**: Emits one `Negative` record with the old row
/// - **UPDATE**: Emits a `Negative` (old) followed by a `Positive` (new)
///
/// This representation allows operators to process changes incrementally.
/// For example, a COUNT aggregate can simply increment/decrement rather
/// than recomputing the full count.
///
/// ## Example
///
/// ```ignore
/// // An insert becomes a positive record
/// let insert = Record::Positive(vec![DataType::Int(1), DataType::from("Alice")]);
///
/// // A delete becomes a negative record
/// let delete = Record::Negative(vec![DataType::Int(1), DataType::from("Alice")]);
/// ```
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Record {
    /// An insertion - this row should be added to downstream state
    Positive(Vec<DataType>),
    /// A retraction - this row should be removed from downstream state
    Negative(Vec<DataType>),
}

impl Record {
    pub fn row(&self) -> &[DataType] {
        match self {
            Record::Positive(v) | Record::Negative(v) => v,
        }
    }

    pub fn is_positive(&self) -> bool {
        matches!(self, Record::Positive(_))
    }

    pub fn into_row(self) -> Vec<DataType> {
        match self {
            Record::Positive(v) | Record::Negative(v) => v,
        }
    }
}

impl From<Vec<DataType>> for Record {
    fn from(v: Vec<DataType>) -> Self {
        Record::Positive(v)
    }
}

/// A batch of records (deltas) to propagate through the dataflow graph.
///
/// `Records` is the primary container for passing changes between operators.
/// Batching multiple records together enables important optimizations:
///
/// ## Batch Processing Benefits
///
/// 1. **Reduced lock contention**: One lock acquisition per batch, not per record
/// 2. **Aggregate optimization**: Multiple changes to the same group key can be
///    combined before emitting (e.g., +5, -3 → net +2)
/// 3. **Reduced retraction overhead**: Intermediate states don't need to be
///    materialized between records in the same batch
///
/// This matches the "async batch processing" optimization from Section 4.3 of
/// the Noria paper. See `OPTIMIZATIONS.md` for benchmarks.
///
/// ## Example
///
/// ```ignore
/// // Create from raw row data (all become positive records)
/// let records: Records = vec![
///     vec![DataType::Int(1), DataType::from("Alice")],
///     vec![DataType::Int(2), DataType::from("Bob")],
/// ].into();
///
/// // Or build incrementally
/// let mut records = Records::new();
/// records.push(Record::Positive(vec![DataType::Int(1)]));
/// records.push(Record::Negative(vec![DataType::Int(2)]));
/// ```
#[derive(Clone, Default, Debug)]
pub struct Records(Vec<Record>);

impl Records {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn push(&mut self, r: Record) {
        self.0.push(r);
    }
}

impl From<Vec<Record>> for Records {
    fn from(v: Vec<Record>) -> Self {
        Self(v)
    }
}

impl From<Vec<Vec<DataType>>> for Records {
    fn from(v: Vec<Vec<DataType>>) -> Self {
        Self(v.into_iter().map(Record::Positive).collect())
    }
}

impl IntoIterator for Records {
    type Item = Record;
    type IntoIter = std::vec::IntoIter<Record>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Records {
    type Item = &'a Record;
    type IntoIter = std::slice::Iter<'a, Record>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_creation() {
        let row = vec![DataType::Int(1), DataType::Int(2)];
        let record: Record = row.clone().into();

        assert!(record.is_positive());
        assert_eq!(record.row(), &row[..]);
    }

    #[test]
    fn test_records_collection() {
        let mut records = Records::new();
        assert!(records.is_empty());

        records.push(Record::Positive(vec![DataType::Int(1)]));
        records.push(Record::Negative(vec![DataType::Int(2)]));

        assert_eq!(records.len(), 2);

        let collected: Vec<_> = records.into_iter().collect();
        assert_eq!(collected.len(), 2);
        assert!(collected[0].is_positive());
        assert!(!collected[1].is_positive());
    }
}
