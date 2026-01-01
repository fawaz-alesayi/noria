//! Database connection wrapper

use crate::error::Result;
use crate::statement::Statement;
use crate::view_cache::ViewCache;
use crate::worker::Worker;
use crate::Config;
use parking_lot::RwLock;
use rusqlite::{Connection, OpenFlags};
use std::path::Path;
use std::sync::Arc;

/// A SQLite database connection with transparent Noria acceleration.
///
/// This struct wraps a `rusqlite::Connection` and adds automatic caching
/// for read queries using Noria's dataflow engine.
pub struct Database {
    /// The underlying SQLite connection
    conn: Arc<RwLock<Connection>>,

    /// The view cache (evmap-backed)
    view_cache: Arc<ViewCache>,

    /// Background worker for dataflow processing
    worker: Arc<Worker>,

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
        let view_cache = Arc::new(ViewCache::new(config.max_cache_memory));
        let worker = Arc::new(Worker::new(conn.clone(), view_cache.clone())?);

        Ok(Self {
            conn,
            view_cache,
            worker,
            config,
        })
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
        Statement::new(
            sql,
            self.conn.clone(),
            self.view_cache.clone(),
            self.worker.clone(),
            &self.config,
        )
    }

    /// Execute a SQL statement that doesn't return rows.
    ///
    /// This bypasses the cache and executes directly against SQLite.
    /// Changes are captured by the CDC observer and propagated to views.
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
        let rows_changed = conn.execute(sql, params)?;
        // CDC will capture the change automatically via session extension
        Ok(rows_changed)
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

    /// Force a cache flush (useful for testing).
    pub fn flush_cache(&self) {
        self.view_cache.flush();
    }

    /// Get cache statistics.
    pub fn cache_stats(&self) -> CacheStats {
        self.view_cache.stats()
    }
}

/// Statistics about the view cache
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
    /// Number of views currently cached
    pub view_count: usize,
    /// Approximate memory usage in bytes
    pub memory_bytes: usize,
}
