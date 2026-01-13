//! Database-agnostic dataflow execution engine.
//!
//! This module provides a single-threaded dataflow execution engine
//! that can be used with any database backend through the adapter traits.

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

/// A record is a single positive or negative data record.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Record {
    Positive(Vec<DataType>),
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

/// A collection of records (deltas).
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
