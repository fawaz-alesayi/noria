//! Error types for noria-sqlite

use thiserror::Error;

/// Result type for noria-sqlite operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for noria-sqlite operations
#[derive(Error, Debug)]
pub enum Error {
    /// SQLite error from the underlying rusqlite connection
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// Error during view synthesis
    #[error("View synthesis failed: {0}")]
    ViewSynthesis(String),

    /// Error in the dataflow worker
    #[error("Dataflow worker error: {0}")]
    Worker(String),

    /// Query is not supported for caching
    #[error("Query not cacheable: {0}")]
    NotCacheable(String),

    /// Timeout waiting for view synthesis
    #[error("View synthesis timeout after {0}ms")]
    Timeout(u64),

    /// Channel communication error
    #[error("Internal channel error: {0}")]
    Channel(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),
}
