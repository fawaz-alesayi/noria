//! Local dataflow execution engine for noria-sqlite.
//!
//! This module provides a simplified, single-threaded dataflow execution engine
//! that reuses Noria's operator concepts but runs entirely locally with SQLite
//! as the base table storage.
//!
//! This module re-exports core dataflow types from `noria-core` to ensure type
//! compatibility and enable optimized state implementations like IntegerArrayState.

mod adapter;
mod core_adapter;
mod engine;
mod executor;
mod ops;
mod session;
mod sql;

// Re-export core dataflow types from noria-core for type compatibility
// This enables using optimized state implementations like IntegerArrayState
pub use noria_core::dataflow::{Record, Records};
pub use noria_core::dataflow::{
    LookupResult, MemoryState, State, StateKey, StateSnapshot,
    IntegerArrayState, DynamicState, is_integer_key, create_optimal_state,
};

pub use adapter::SqliteAdapter;
pub use core_adapter::{SqliteCdcSource, SqliteDatabaseAdapter};
pub use engine::{EngineStats, NoriaEngine, NoriaView};
pub use executor::{LocalExecutor, ViewHandle};
pub use ops::{AggregateFunc, AggregateOp, FilterCondition, FilterOp, Operator, OperatorType, ProjectOp};
pub use session::{CdcEvent, SessionTracker};
pub use sql::{SqlConverter, SqlError, SqlResult, TableSchema};

#[cfg(test)]
mod tests {
    use super::*;
    use noria::DataType;

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
