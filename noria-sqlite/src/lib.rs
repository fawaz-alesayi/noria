//! # noria-sqlite
//!
//! A transparent caching layer for SQLite using Noria's dataflow engine.
//!
//! This crate provides a drop-in replacement for `rusqlite` that automatically
//! accelerates read queries by maintaining incrementally-updated materialized views.
//!
//! ## Quick Start
//!
//! ```no_run
//! use noria_sqlite::Database;
//!
//! let db = Database::open("app.db")?;
//!
//! // Writes go directly to SQLite
//! db.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("Alice", 30))?;
//!
//! // Reads are automatically accelerated
//! let stmt = db.prepare("SELECT name, age FROM users WHERE id = ?")?;
//! let (name, age): (String, i32) = stmt.query_row([42], |row| {
//!     Ok((row.get(0)?, row.get(1)?))
//! })?;
//! # Ok::<(), noria_sqlite::Error>(())
//! ```
//!
//! ## How It Works
//!
//! 1. **Write Path**: All writes go directly to SQLite. The Session Extension
//!    captures changes and feeds them into the Noria dataflow graph.
//!
//! 2. **Read Path**: When you call `prepare()`, noria-sqlite automatically
//!    synthesizes a materialized view for that query. Subsequent reads hit
//!    the in-memory cache (O(1) lookup) instead of querying SQLite.
//!
//! 3. **Cache Misses**: If a key isn't in the cache, an "upquery" fetches it
//!    from SQLite, populates the cache, and returns the result.

mod database;
mod error;
mod statement;
mod view_cache;
mod worker;

pub use database::Database;
pub use error::{Error, Result};
pub use statement::Statement;

/// Configuration options for the database connection
#[derive(Debug, Clone)]
pub struct Config {
    /// Maximum memory (in bytes) for cached views. Default: 100MB
    pub max_cache_memory: usize,

    /// How long to wait for view synthesis before falling back to SQLite (ms)
    pub view_synthesis_timeout_ms: u64,

    /// Enable consistency guard (bypass cache for recently-written keys)
    pub enable_consistency_guard: bool,

    /// Consistency window in milliseconds
    pub consistency_window_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_cache_memory: 100 * 1024 * 1024, // 100MB
            view_synthesis_timeout_ms: 5000,      // 5 seconds
            enable_consistency_guard: true,
            consistency_window_ms: 50,            // 50ms
        }
    }
}
