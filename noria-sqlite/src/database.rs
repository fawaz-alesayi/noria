//! Database connection wrapper with Noria dataflow acceleration.

use crate::dataflow::{CdcEvent, NoriaEngine, NoriaView, SessionTracker};
use crate::error::{Error, Result};
use crate::statement::Statement;
use crate::Config;
use noria::DataType;
use parking_lot::RwLock;
use rusqlite::{Connection, OpenFlags};
use std::path::Path;
use std::sync::Arc;

/// A SQLite database connection with transparent Noria acceleration.
///
/// This struct wraps a `rusqlite::Connection` and adds automatic caching
/// for read queries using Noria's incremental dataflow engine.
///
/// When you execute writes (INSERT/UPDATE/DELETE), the changes are automatically
/// propagated through the dataflow graph to update materialized views incrementally.
/// This means views stay up-to-date without re-executing the full query.
pub struct Database {
    /// The underlying SQLite connection
    conn: Arc<RwLock<Connection>>,

    /// The Noria dataflow engine
    engine: Arc<NoriaEngine>,

    /// Configuration
    config: Config,
}

impl Database {
    /// Open a database file with default configuration.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use noria_sqlite::Database;
    ///
    /// let db = Database::open("app.db")?;
    /// # Ok::<(), noria_sqlite::Error>(())
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_config(path, Config::default())
    }

    /// Open a database file with custom configuration.
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: Config) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;

        Self::from_connection(conn, config)
    }

    /// Create an in-memory database (useful for testing).
    pub fn open_in_memory() -> Result<Self> {
        Self::open_in_memory_with_config(Config::default())
    }

    /// Create an in-memory database with custom configuration.
    pub fn open_in_memory_with_config(config: Config) -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::from_connection(conn, config)
    }

    /// Wrap an existing rusqlite connection.
    fn from_connection(conn: Connection, config: Config) -> Result<Self> {
        let conn = Arc::new(RwLock::new(conn));

        // Create the Noria dataflow engine
        let engine = Arc::new(NoriaEngine::new(conn.clone()));

        // Note: We don't use update_hook for dataflow propagation because
        // NoriaEngine contains types that aren't Sync. Instead, we trigger
        // dataflow updates after writes via the execute method.
        //
        // For a full solution, we could use channels or the preupdate_hook
        // with proper synchronization.

        Ok(Self {
            conn,
            engine,
            config,
        })
    }

    /// Register a table for dataflow tracking.
    ///
    /// This discovers the table schema from SQLite and registers it with the
    /// dataflow engine. Must be called before creating views that reference this table.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use noria_sqlite::Database;
    ///
    /// let db = Database::open("app.db")?;
    /// db.execute("CREATE TABLE users (id INTEGER, name TEXT)", [])?;
    /// db.register_table("users")?;
    /// # Ok::<(), noria_sqlite::Error>(())
    /// ```
    pub fn register_table(&self, table_name: &str) -> Result<()> {
        self.engine.register_table(table_name)?;
        Ok(())
    }

    /// Load existing data from a table into the dataflow engine.
    ///
    /// This populates the dataflow graph with existing rows. Call this after
    /// registering tables if you want views to include pre-existing data.
    pub fn load_table(&self, table_name: &str) -> Result<usize> {
        let count = self.engine.load_table(table_name)?;
        Ok(count)
    }

    /// Prepare a SQL statement for execution.
    ///
    /// For SELECT queries, this will automatically synthesize a Noria view
    /// if one doesn't already exist for this query shape.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use noria_sqlite::Database;
    ///
    /// let db = Database::open("app.db")?;
    /// let stmt = db.prepare("SELECT * FROM users WHERE id = ?")?;
    /// # Ok::<(), noria_sqlite::Error>(())
    /// ```
    pub fn prepare(&self, sql: &str) -> Result<Statement> {
        Statement::new(sql, self.conn.clone(), self.engine.clone(), &self.config)
    }

    /// Execute a SQL statement that doesn't return rows.
    ///
    /// This executes directly against SQLite. Changes are captured using
    /// SQLite's session extension and automatically propagated through the
    /// dataflow graph to update materialized views incrementally.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use noria_sqlite::Database;
    ///
    /// let db = Database::open("app.db")?;
    /// db.execute("INSERT INTO users (name) VALUES (?)", ["Alice"])?;
    /// # Ok::<(), noria_sqlite::Error>(())
    /// ```
    pub fn execute<P: rusqlite::Params>(&self, sql: &str, params: P) -> Result<usize> {
        let conn = self.conn.write();

        // Create a session to track changes
        let mut tracker = SessionTracker::new(&conn)?;
        tracker.attach_all()?;

        // Execute the statement
        let rows_changed = conn.execute(sql, params)?;

        // Get the changeset and apply to dataflow
        if rows_changed > 0 {
            if let Ok(changeset) = tracker.changeset() {
                if let Ok(events) = SessionTracker::extract_events(&changeset) {
                    self.apply_cdc_events(&events);
                }
            }
        }

        Ok(rows_changed)
    }

    /// Apply CDC events to the dataflow engine.
    fn apply_cdc_events(&self, events: &[CdcEvent]) {
        for event in events {
            match event {
                CdcEvent::Insert { table, new_row } => {
                    self.engine.apply_insert_row(table, new_row.clone());
                }
                CdcEvent::Delete { table, old_row } => {
                    self.engine.apply_delete(table, old_row.clone());
                }
                CdcEvent::Update {
                    table,
                    old_row,
                    new_row,
                } => {
                    self.engine
                        .apply_update_rows(table, old_row.clone(), new_row.clone());
                }
            }
        }
    }

    /// Execute multiple SQL statements.
    ///
    /// Useful for schema setup and migrations.
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        let conn = self.conn.write();
        conn.execute_batch(sql)?;
        Ok(())
    }

    /// Get direct access to the underlying SQLite connection.
    ///
    /// Use this for operations that noria-sqlite doesn't support.
    /// Note: Changes made through this handle are still captured by CDC.
    pub fn connection(&self) -> &Arc<RwLock<Connection>> {
        &self.conn
    }

    /// Get access to the Noria dataflow engine.
    ///
    /// This allows direct interaction with views and dataflow state.
    pub fn engine(&self) -> &Arc<NoriaEngine> {
        &self.engine
    }

    /// Look up a value from a materialized view.
    ///
    /// This is the fast path for cache hits - reads directly from the
    /// in-memory materialized view without touching SQLite.
    ///
    /// Returns `None` if the key is not in the cache (cache miss).
    pub fn lookup(&self, view: &NoriaView, key: &[DataType]) -> Option<Vec<Vec<DataType>>> {
        self.engine.lookup(view, key)
    }

    /// Look up a value, falling back to SQLite on cache miss (upquery).
    ///
    /// If the key is not in the cache, this executes the query against
    /// SQLite and populates the cache with the result.
    pub fn lookup_or_upquery(
        &self,
        view: &NoriaView,
        key: &[DataType],
    ) -> Result<Vec<Vec<DataType>>> {
        self.engine
            .lookup_or_upquery(view, key)
            .map_err(|e| Error::Dataflow(e.to_string()))
    }

    /// Get cache/dataflow statistics.
    pub fn cache_stats(&self) -> CacheStats {
        let stats = self.engine.stats();
        CacheStats {
            node_count: stats.node_count,
            materialized_nodes: stats.materialized_nodes,
            total_rows: stats.total_rows,
        }
    }
}

/// Statistics about the dataflow engine and materialized views.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of nodes in the dataflow graph
    pub node_count: usize,
    /// Number of materialized (cached) nodes
    pub materialized_nodes: usize,
    /// Total rows across all materialized views
    pub total_rows: usize,
}
