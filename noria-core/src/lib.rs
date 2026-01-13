//! # Noria Core: Database-Agnostic Dataflow Engine
//!
//! This crate provides the core dataflow execution engine from the Noria system,
//! adapted for use as an in-process caching layer for any database backend.
//!
//! ## Background: The Noria Paper
//!
//! Noria is a partially-stateful dataflow system designed for high-performance
//! web applications. Unlike traditional caches that simply store and invalidate,
//! Noria maintains **materialized views** that are incrementally updated as the
//! underlying data changes.
//!
//! Key concepts from the paper:
//!
//! - **Partially-Stateful Dataflow**: Operators maintain only the state they need,
//!   and missing state can be reconstructed on-demand via "upqueries"
//! - **Differential Updates**: Changes propagate as positive (insert) and negative
//!   (retraction) records, enabling incremental view maintenance
//! - **Push-Based Propagation**: Writes push deltas through the dataflow graph,
//!   so reads are O(1) cache lookups rather than recomputation
//!
//! For the full details, see the original paper:
//! <https://pdos.csail.mit.edu/papers/noria:osdi18.pdf>
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     LocalExecutor                           │
//! │  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐ │
//! │  │ Base Tables │───▶│  Operators  │───▶│ Materialized    │ │
//! │  │  (sources)  │    │ (σ,π,⋈,γ)  │    │ Views (sinks)   │ │
//! │  └─────────────┘    └─────────────┘    └─────────────────┘ │
//! │         ▲                                      │           │
//! │         │ CDC events                 O(1) lookup           │
//! │         │                                      ▼           │
//! └─────────┴──────────────────────────────────────────────────┘
//!           │                                      │
//!     Database Adapter                      Application
//! ```
//!
//! ## Modules
//!
//! - [`dataflow`]: Core dataflow types ([`Record`], [`Records`], [`LocalExecutor`])
//!   and operators ([`FilterOp`], [`ProjectOp`], [`JoinOp`], [`AggregateOp`])
//! - [`sql`]: SQL parser and query-to-dataflow conversion using `sqlparser-rs`
//! - [`view_cache`]: Lock-free view caching with `evmap`
//! - [`adapter`]: Traits for database-specific implementations ([`DatabaseAdapter`],
//!   [`CdcSource`])
//!
//! ## Performance Optimizations
//!
//! This implementation includes several optimizations beyond the original Noria:
//!
//! - **Arc-wrapped rows**: Lookups return `Arc<Vec<DataType>>` for O(1) cloning
//! - **IntegerArrayState**: O(1) direct indexing for integer primary keys
//! - **DynamicState**: Auto-detects key type and selects optimal storage
//! - **`needs_state()` trait**: Skips expensive snapshots for stateful operators
//!
//! See `OPTIMIZATIONS.md` for benchmarks and profiling data.
//!
//! ## Example
//!
//! ```ignore
//! use noria_core::prelude::*;
//!
//! // Create an executor (the dataflow graph engine)
//! let mut executor = LocalExecutor::new();
//!
//! // Add a base table (represents a database table)
//! let users = executor.add_base_table("users", vec!["id".into(), "name".into()]);
//!
//! // Materialize a view keyed on column 0 (id)
//! let view = executor.materialize(users, vec![0]);
//!
//! // Apply writes (CDC events from the database)
//! let records: Records = vec![
//!     vec![DataType::Int(1), DataType::from("Alice")],
//! ].into();
//! executor.apply_write("users", records);
//!
//! // Lookup is O(1) - no recomputation needed
//! let result = executor.lookup(&view, &[DataType::Int(1)]);
//! ```
//!
//! [`Record`]: dataflow::Record
//! [`Records`]: dataflow::Records
//! [`LocalExecutor`]: dataflow::LocalExecutor
//! [`FilterOp`]: dataflow::ops::FilterOp
//! [`ProjectOp`]: dataflow::ops::ProjectOp
//! [`JoinOp`]: dataflow::ops::JoinOp
//! [`AggregateOp`]: dataflow::ops::AggregateOp
//! [`DatabaseAdapter`]: adapter::DatabaseAdapter
//! [`CdcSource`]: adapter::CdcSource

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
