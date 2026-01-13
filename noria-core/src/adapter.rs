//! # Database Adapter Traits
//!
//! This module defines the traits for integrating database backends with
//! the noria-core dataflow engine. The design enables Noria to work with
//! any database that can provide:
//!
//! 1. **Schema information** (for SQL-to-dataflow conversion)
//! 2. **Upquery capability** (for filling partial state holes)
//! 3. **CDC events** (for propagating changes)
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Application Layer                        │
//! └─────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    noria-core Engine                        │
//! │  ┌──────────────────┐  ┌─────────────────────────────────┐ │
//! │  │  LocalExecutor   │  │  SqlConverter                    │ │
//! │  │  (dataflow DAG)  │  │  (SQL → operators)               │ │
//! │  └────────┬─────────┘  └─────────────────────────────────┘ │
//! │           │                                                 │
//! │           │ DatabaseAdapter trait                           │
//! │           │ CdcSource trait                                 │
//! │           ▼                                                 │
//! │  ┌──────────────────────────────────────────────────────┐  │
//! │  │              Adapter Implementation                   │  │
//! │  │  (SqliteAdapter, PostgresAdapter, MySqlAdapter, ...) │  │
//! │  └──────────────────────────────────────────────────────┘  │
//! └─────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Database Layer                           │
//! │  SQLite / PostgreSQL / MySQL / ...                          │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Supported Backends
//!
//! | Database   | CDC Mechanism       | Status |
//! |------------|---------------------|--------|
//! | SQLite     | Preupdate hook      | **Implemented** |
//! | PostgreSQL | Logical replication | Planned |
//! | MySQL      | Binlog              | Planned |
//!
//! ## Key Traits
//!
//! - [`DatabaseAdapter`]: Schema discovery and upqueries
//! - [`CdcSource`]: Change Data Capture event stream
//! - [`TableSchema`]: Table metadata
//! - [`CdcEvent`]: Individual change events (insert/update/delete)
//!
//! ## Thread Safety Note
//!
//! The [`DatabaseAdapter`] trait does **not** require `Send + Sync` because
//! some database connections (e.g., SQLite) are not thread-safe. Adapters
//! should be accessed from a single thread, with external synchronization
//! if needed.
//!
//! ## Paper Reference
//!
//! See Section 5 "Implementation" of the Noria paper for CDC integration:
//! <https://pdos.csail.mit.edu/papers/noria:osdi18.pdf>

use noria::DataType;

/// Schema information for a database table.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Table name
    pub name: String,
    /// Column names in order
    pub columns: Vec<String>,
    /// Primary key column indices (if any)
    pub primary_key: Vec<usize>,
}

impl TableSchema {
    /// Create a new table schema.
    pub fn new(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            columns,
            primary_key: vec![],
        }
    }

    /// Set the primary key columns.
    pub fn with_primary_key(mut self, columns: Vec<usize>) -> Self {
        self.primary_key = columns;
        self
    }

    /// Get the column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c == name)
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// Change Data Capture event representing a database modification.
#[derive(Debug, Clone)]
pub enum CdcEvent {
    /// A row was inserted into a table.
    Insert {
        table: String,
        row: Vec<DataType>,
    },
    /// A row was deleted from a table.
    Delete {
        table: String,
        row: Vec<DataType>,
    },
    /// A row was updated in a table.
    Update {
        table: String,
        old: Vec<DataType>,
        new: Vec<DataType>,
    },
}

impl CdcEvent {
    /// Get the table name for this event.
    pub fn table(&self) -> &str {
        match self {
            CdcEvent::Insert { table, .. } => table,
            CdcEvent::Delete { table, .. } => table,
            CdcEvent::Update { table, .. } => table,
        }
    }
}

/// Trait for database adapters.
///
/// Implementations provide database-specific functionality for:
/// - Schema discovery
/// - Upqueries (fetching data on cache miss)
///
/// Note: This trait does not require `Send + Sync` because some database
/// connections (e.g., SQLite) are not thread-safe. Thread safety should
/// be handled at the application level (e.g., using a mutex).
pub trait DatabaseAdapter {
    /// Error type for database operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Get the schema for a table.
    ///
    /// Returns `None` if the table doesn't exist.
    fn table_schema(&self, table: &str) -> Result<Option<TableSchema>, Self::Error>;

    /// Execute an upquery to fetch data from the database.
    ///
    /// This is called on cache miss to populate the cache.
    /// The SQL should be a SELECT query, and params are bound positionally.
    fn upquery(
        &self,
        sql: &str,
        params: &[DataType],
    ) -> Result<Vec<Vec<DataType>>, Self::Error>;

    /// Get all table names in the database.
    fn list_tables(&self) -> Result<Vec<String>, Self::Error>;
}

/// Trait for Change Data Capture sources.
///
/// Implementations provide database-specific CDC mechanisms.
/// This can be:
/// - Synchronous (SQLite session extension)
/// - Asynchronous (Postgres logical replication, MySQL binlog)
///
/// Note: This trait does not require `Send` because CDC implementations
/// may hold non-thread-safe database connections (e.g., SQLite).
/// For async/multi-threaded CDC (e.g., Postgres), implementations
/// should ensure thread safety internally.
pub trait CdcSource {
    /// Poll for new CDC events.
    ///
    /// Returns a vector of events captured since the last poll.
    /// Returns an empty vector if no events are available.
    fn poll(&mut self) -> Vec<CdcEvent>;

    /// Enable CDC tracking for a table.
    ///
    /// Some CDC mechanisms require explicit registration of tables to track.
    fn track_table(&mut self, table: &str);

    /// Check if CDC is currently active.
    fn is_active(&self) -> bool;

    /// Reset CDC state (clear pending events).
    fn reset(&mut self);
}

/// A no-op CDC source for testing or when CDC is disabled.
pub struct NullCdcSource;

impl CdcSource for NullCdcSource {
    fn poll(&mut self) -> Vec<CdcEvent> {
        Vec::new()
    }

    fn track_table(&mut self, _table: &str) {}

    fn is_active(&self) -> bool {
        false
    }

    fn reset(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_schema() {
        let schema = TableSchema::new(
            "users",
            vec!["id".to_string(), "name".to_string(), "email".to_string()],
        )
        .with_primary_key(vec![0]);

        assert_eq!(schema.name, "users");
        assert_eq!(schema.column_count(), 3);
        assert_eq!(schema.column_index("name"), Some(1));
        assert_eq!(schema.column_index("nonexistent"), None);
        assert_eq!(schema.primary_key, vec![0]);
    }

    #[test]
    fn test_cdc_event() {
        let insert = CdcEvent::Insert {
            table: "users".to_string(),
            row: vec![DataType::Int(1), DataType::from("Alice")],
        };
        assert_eq!(insert.table(), "users");

        let delete = CdcEvent::Delete {
            table: "posts".to_string(),
            row: vec![DataType::Int(42)],
        };
        assert_eq!(delete.table(), "posts");

        let update = CdcEvent::Update {
            table: "comments".to_string(),
            old: vec![DataType::Int(1), DataType::from("old")],
            new: vec![DataType::Int(1), DataType::from("new")],
        };
        assert_eq!(update.table(), "comments");
    }

    #[test]
    fn test_null_cdc_source() {
        let mut source = NullCdcSource;
        assert!(!source.is_active());
        assert!(source.poll().is_empty());
        source.track_table("users"); // no-op
        source.reset(); // no-op
    }
}
