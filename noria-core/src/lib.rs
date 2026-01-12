//! Database-agnostic dataflow engine for caching.
//!
//! noria-core provides the core dataflow execution engine that can be used
//! with any database backend through the adapter traits.
//!
//! # Architecture
//!
//! - `dataflow`: Core dataflow types (Record, Records, LocalExecutor, operators)
//! - `sql`: SQL parser and query-to-dataflow conversion (using sqlparser-rs)
//! - `view_cache`: Lock-free view caching with evmap
//! - `adapter`: Traits for database-specific implementations
//!
//! # Usage
//!
//! ```ignore
//! use noria_core::prelude::*;
//!
//! // Create an executor
//! let mut executor = LocalExecutor::new();
//!
//! // Add a base table
//! let users = executor.add_base_table("users", vec!["id".into(), "name".into()]);
//! let view = executor.materialize(users, vec![0]);
//!
//! // Apply writes
//! let records: Records = vec![
//!     vec![DataType::Int(1), DataType::from("Alice")],
//! ].into();
//! executor.apply_write("users", records);
//!
//! // Lookup
//! let result = executor.lookup(&view, &[DataType::Int(1)]);
//! ```

pub mod adapter;
pub mod dataflow;
pub mod metrics;
pub mod sql;
pub mod view_cache;

/// Re-export commonly used types.
pub mod prelude {
    pub use crate::adapter::{CdcEvent, CdcSource, DatabaseAdapter, NullCdcSource, TableSchema};
    pub use crate::dataflow::{
        executor::{ExecutorStats, LocalExecutor, NodeIndex, ViewHandle},
        ops::{
            AggregateFunc, AggregateOp, FilterCondition, FilterOp, IdentityOp, JoinOp, JoinType,
            Operator, OperatorType, ProcessingResult, ProjectOp,
        },
        state::{LookupResult, MemoryState, State, StateKey, StateSnapshot},
        Record, Records,
    };
    pub use crate::sql::{SqlConverter, SqlDialect, SqlError, SqlResult};
    pub use crate::view_cache::{CachedRow, ViewCacheStats, ViewHandle as CacheViewHandle};
    pub use noria::DataType;
}

// Re-export at crate root for convenience
pub use adapter::{CdcEvent, CdcSource, DatabaseAdapter, NullCdcSource, TableSchema};
pub use dataflow::{
    executor::{ExecutorStats, LocalExecutor, NodeIndex, ViewHandle},
    ops::{
        AggregateFunc, AggregateOp, FilterCondition, FilterOp, IdentityOp, JoinOp, JoinType,
        Operator, OperatorType, ProcessingResult, ProjectOp,
    },
    state::{LookupResult, MemoryState, State, StateKey, StateSnapshot},
    Record, Records,
};
pub use sql::{SqlConverter, SqlDialect, SqlError, SqlResult};
pub use view_cache::{CachedRow, ViewCacheStats, ViewHandle as CacheViewHandle};

// Re-export DataType from noria
pub use noria::DataType;

// Re-export metrics
pub use metrics::{Metrics, MetricsSnapshot};
