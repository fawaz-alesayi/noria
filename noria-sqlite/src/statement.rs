//! Prepared statement wrapper with automatic view acceleration

use crate::error::{Error, Result};
use crate::view_cache::{CachedRow, ViewCache, ViewHandle};
use crate::worker::Worker;
use crate::Config;
use parking_lot::RwLock;
use rusqlite::Connection;
use std::sync::Arc;

/// A prepared SQL statement that may be accelerated by a Noria view.
pub struct Statement {
    /// The original SQL text
    sql: String,

    /// Normalized SQL (for cache key)
    #[allow(dead_code)]
    normalized_sql: String,

    /// View handle if this query is cached
    view_handle: Option<ViewHandle>,

    /// Fallback to SQLite if view isn't available
    conn: Arc<RwLock<Connection>>,

    /// Reference to view cache
    view_cache: Arc<ViewCache>,

    /// Reference to worker for upqueries
    worker: Arc<Worker>,

    /// Configuration
    config: Config,
}

impl Statement {
    /// Create a new statement, potentially synthesizing a view.
    pub(crate) fn new(
        sql: &str,
        conn: Arc<RwLock<Connection>>,
        view_cache: Arc<ViewCache>,
        worker: Arc<Worker>,
        config: &Config,
    ) -> Result<Self> {
        let normalized_sql = normalize_sql(sql);

        // Check if this query is cacheable (SELECT with parameters)
        let view_handle = if is_cacheable_query(sql) {
            // Try to get or create a view for this query
            match view_cache.get_or_create_view(&normalized_sql, &worker, config) {
                Ok(handle) => Some(handle),
                Err(e) => {
                    tracing::warn!("Failed to create view for query, falling back to SQLite: {}", e);
                    None
                }
            }
        } else {
            None
        };

        Ok(Self {
            sql: sql.to_string(),
            normalized_sql,
            view_handle,
            conn,
            view_cache,
            worker,
            config: config.clone(),
        })
    }

    /// Query for a single row.
    ///
    /// If a view exists for this query, attempts to read from the cache first.
    /// Falls back to SQLite on cache miss or if no view exists.
    pub fn query_row<T, P, F>(&self, params: P, f: F) -> Result<T>
    where
        P: IntoIterator,
        P::Item: rusqlite::ToSql,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let params: Vec<_> = params.into_iter().collect();

        // Try cache first if we have a view
        if let Some(ref view_handle) = self.view_handle {
            if let Some(row) = self.try_cache_lookup(view_handle, &params) {
                self.view_cache.record_hit();
                return row_to_result(row, f);
            }
            self.view_cache.record_miss();
        }

        // Fall back to SQLite
        self.sqlite_query_row(&params, f)
    }

    /// Query for multiple rows.
    pub fn query_map<T, P, F>(&self, params: P, f: F) -> Result<Vec<T>>
    where
        P: IntoIterator,
        P::Item: rusqlite::ToSql,
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let params: Vec<_> = params.into_iter().collect();

        // Try cache first if we have a view
        if let Some(ref view_handle) = self.view_handle {
            if let Some(rows) = self.try_cache_lookup_multi(view_handle, &params) {
                self.view_cache.record_hit();
                return rows_to_results(rows, f);
            }
            self.view_cache.record_miss();
        }

        // Fall back to SQLite
        self.sqlite_query_map(&params, f)
    }

    /// Attempt to read from the cache (single row).
    fn try_cache_lookup<T: rusqlite::ToSql>(
        &self,
        view_handle: &ViewHandle,
        params: &[T],
    ) -> Option<CachedRow> {
        // Check consistency guard - if we recently wrote to related tables,
        // bypass the cache to ensure read-your-writes
        if self.config.enable_consistency_guard
            && self
                .worker
                .is_recently_modified(&view_handle.tables, self.config.consistency_window_ms)
        {
            return None;
        }

        // Build the lookup key from parameters
        let key = params_to_key(params);

        // Look up in evmap - returns first row if found
        view_handle.lookup_one(&key)
    }

    /// Attempt multi-row cache lookup.
    fn try_cache_lookup_multi<T: rusqlite::ToSql>(
        &self,
        view_handle: &ViewHandle,
        params: &[T],
    ) -> Option<Vec<CachedRow>> {
        if self.config.enable_consistency_guard
            && self
                .worker
                .is_recently_modified(&view_handle.tables, self.config.consistency_window_ms)
        {
            return None;
        }

        let key = params_to_key(params);
        view_handle.lookup(&key)
    }

    /// Execute query directly against SQLite.
    fn sqlite_query_row<T, S, F>(&self, params: &[S], f: F) -> Result<T>
    where
        S: rusqlite::ToSql,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let conn = self.conn.read();
        let mut stmt = conn.prepare_cached(&self.sql)?;
        let result = stmt.query_row(rusqlite::params_from_iter(params), f)?;
        Ok(result)
    }

    /// Execute query directly against SQLite, returning multiple rows.
    fn sqlite_query_map<T, S, F>(&self, params: &[S], mut f: F) -> Result<Vec<T>>
    where
        S: rusqlite::ToSql,
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let conn = self.conn.read();
        let mut stmt = conn.prepare_cached(&self.sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), &mut f)?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Check if this statement is accelerated by a view.
    pub fn is_cached(&self) -> bool {
        self.view_handle.is_some()
    }

    /// Get the original SQL text.
    pub fn sql(&self) -> &str {
        &self.sql
    }
}

/// Normalize SQL for cache key generation.
///
/// This strips whitespace variations and normalizes case.
fn normalize_sql(sql: &str) -> String {
    // Simple normalization - in production this would be more sophisticated
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_uppercase()
}

/// Check if a query is suitable for caching.
///
/// Currently only SELECT queries with parameter placeholders are cached.
fn is_cacheable_query(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();

    // Only cache SELECT queries
    if !upper.starts_with("SELECT") {
        return false;
    }

    // Must have at least one parameter placeholder
    if !sql.contains('?') && !sql.contains('$') {
        return false;
    }

    // Exclude certain patterns that don't cache well
    if upper.contains("RANDOM()") || upper.contains("NOW()") || upper.contains("CURRENT_") {
        return false;
    }

    true
}

/// Convert parameters to a cache lookup key.
fn params_to_key<T: rusqlite::ToSql>(params: &[T]) -> Vec<noria::DataType> {
    params
        .iter()
        .map(|p| {
            // Convert rusqlite value to Noria DataType
            // This is a simplified version - full implementation would handle all types
            use rusqlite::types::ToSqlOutput;
            match p.to_sql() {
                Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Integer(i))) => {
                    noria::DataType::from(i)
                }
                Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Text(s))) => {
                    noria::DataType::from(std::str::from_utf8(s).unwrap_or(""))
                }
                Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Real(f))) => {
                    noria::DataType::from(f)
                }
                Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Null)) => noria::DataType::None,
                _ => noria::DataType::None,
            }
        })
        .collect()
}

/// Convert a cached row to a result using the provided closure.
fn row_to_result<T, F>(_row: CachedRow, _f: F) -> Result<T>
where
    F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
{
    // TODO: Implement proper conversion from CachedRow to rusqlite::Row
    // This requires creating a synthetic Row that the closure can read from
    Err(Error::NotCacheable(
        "Cache row conversion not yet implemented".to_string(),
    ))
}

/// Convert cached rows to results.
fn rows_to_results<T, F>(_rows: Vec<CachedRow>, _f: F) -> Result<Vec<T>>
where
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
{
    Err(Error::NotCacheable(
        "Cache row conversion not yet implemented".to_string(),
    ))
}
