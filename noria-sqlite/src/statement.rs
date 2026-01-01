//! Prepared statement wrapper with automatic view acceleration

use crate::error::{Error, Result};
use crate::view_cache::{CachedRow, ViewCache, ViewHandle};
use crate::worker::Worker;
use crate::Config;
use noria::DataType;
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

    /// Query for a single row using rusqlite-compatible API.
    ///
    /// If a view exists for this query, attempts to read from the cache first.
    /// Falls back to SQLite on cache miss or if no view exists.
    ///
    /// Note: Due to rusqlite::Row's internal design, cache hits still execute
    /// through SQLite but benefit from cache population (upquery). For direct
    /// cache access without SQLite overhead, use `query_row_cached()`.
    pub fn query_row<T, P, F>(&self, params: P, f: F) -> Result<T>
    where
        P: IntoIterator,
        P::Item: rusqlite::ToSql,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let params: Vec<_> = params.into_iter().collect();

        // Try cache first if we have a view
        if let Some(ref view_handle) = self.view_handle {
            if let Some(_cached) = self.try_cache_lookup(view_handle, &params) {
                // Cache hit - record it
                self.view_cache.record_hit();
                // Note: We still fall through to SQLite because rusqlite::Row
                // cannot be constructed from cached data. The cache is used for
                // the alternative API and will be used once we implement
                // synthetic row construction.
            } else {
                self.view_cache.record_miss();
            }
        }

        // Execute through SQLite and populate cache (upquery)
        let result = self.sqlite_query_row_and_cache(&params, f)?;
        Ok(result)
    }

    /// Query for multiple rows using rusqlite-compatible API.
    pub fn query_map<T, P, F>(&self, params: P, f: F) -> Result<Vec<T>>
    where
        P: IntoIterator,
        P::Item: rusqlite::ToSql,
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let params: Vec<_> = params.into_iter().collect();

        // Try cache first if we have a view
        if let Some(ref view_handle) = self.view_handle {
            if let Some(_cached) = self.try_cache_lookup_multi(view_handle, &params) {
                self.view_cache.record_hit();
            } else {
                self.view_cache.record_miss();
            }
        }

        // Execute through SQLite and populate cache (upquery)
        self.sqlite_query_map_and_cache(&params, f)
    }

    /// Query for a single row, returning cached data directly.
    ///
    /// This method bypasses the rusqlite::Row abstraction and returns
    /// data directly from the cache when available. This is the fastest
    /// path for cache hits.
    ///
    /// Returns:
    /// - `Ok(Some(row))` on cache hit
    /// - `Ok(None)` on cache miss (caller should use `query_row` to get data)
    /// - `Err(_)` if the query isn't cacheable
    pub fn query_row_cached<P>(&self, params: P) -> Result<Option<CachedRow>>
    where
        P: IntoIterator,
        P::Item: rusqlite::ToSql,
    {
        let view_handle = self.view_handle.as_ref().ok_or_else(|| {
            Error::NotCacheable("Query is not cacheable".to_string())
        })?;

        let params: Vec<_> = params.into_iter().collect();
        let key = params_to_key(&params);

        // Check consistency guard
        if self.config.enable_consistency_guard
            && self
                .worker
                .is_recently_modified(&view_handle.tables, self.config.consistency_window_ms)
        {
            return Ok(None);
        }

        // Look up in cache
        match view_handle.lookup_one(&key) {
            Some(row) => {
                self.view_cache.record_hit();
                Ok(Some(row))
            }
            None => {
                self.view_cache.record_miss();
                Ok(None)
            }
        }
    }

    /// Query for multiple rows, returning cached data directly.
    pub fn query_map_cached<P>(&self, params: P) -> Result<Option<Vec<CachedRow>>>
    where
        P: IntoIterator,
        P::Item: rusqlite::ToSql,
    {
        let view_handle = self.view_handle.as_ref().ok_or_else(|| {
            Error::NotCacheable("Query is not cacheable".to_string())
        })?;

        let params: Vec<_> = params.into_iter().collect();
        let key = params_to_key(&params);

        if self.config.enable_consistency_guard
            && self
                .worker
                .is_recently_modified(&view_handle.tables, self.config.consistency_window_ms)
        {
            return Ok(None);
        }

        match view_handle.lookup(&key) {
            Some(rows) => {
                self.view_cache.record_hit();
                Ok(Some(rows))
            }
            None => {
                self.view_cache.record_miss();
                Ok(None)
            }
        }
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

    /// Execute query against SQLite and populate cache with results.
    fn sqlite_query_row_and_cache<T, S, F>(&self, params: &[S], f: F) -> Result<T>
    where
        S: rusqlite::ToSql,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let conn = self.conn.read();
        let mut stmt = conn.prepare_cached(&self.sql)?;

        // Get column count for cache population
        let column_count = stmt.column_count();

        // Execute query
        let result = stmt.query_row(rusqlite::params_from_iter(params), |row| {
            // Populate cache if we have a view handle
            if let Some(ref view_handle) = self.view_handle {
                let key = params_to_key(params);
                if let Some(cached_row) = extract_row_data(row, column_count) {
                    view_handle.insert(key, cached_row);
                }
            }
            f(row)
        })?;

        Ok(result)
    }

    /// Execute query against SQLite, returning multiple rows and populating cache.
    fn sqlite_query_map_and_cache<T, S, F>(&self, params: &[S], mut f: F) -> Result<Vec<T>>
    where
        S: rusqlite::ToSql,
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let conn = self.conn.read();
        let mut stmt = conn.prepare_cached(&self.sql)?;
        let column_count = stmt.column_count();
        let key = params_to_key(params);

        let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            // Populate cache if we have a view handle
            if let Some(ref view_handle) = self.view_handle {
                if let Some(cached_row) = extract_row_data(row, column_count) {
                    view_handle.insert(key.clone(), cached_row);
                }
            }
            f(row)
        })?;

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

/// Extract row data into a vector of DataType for caching.
fn extract_row_data(row: &rusqlite::Row<'_>, column_count: usize) -> Option<Vec<DataType>> {
    let mut values = Vec::with_capacity(column_count);

    for i in 0..column_count {
        let value = match row.get_ref(i) {
            Ok(rusqlite::types::ValueRef::Null) => DataType::None,
            Ok(rusqlite::types::ValueRef::Integer(i)) => DataType::from(i),
            Ok(rusqlite::types::ValueRef::Real(f)) => DataType::from(f),
            Ok(rusqlite::types::ValueRef::Text(s)) => {
                DataType::from(std::str::from_utf8(s).unwrap_or(""))
            }
            Ok(rusqlite::types::ValueRef::Blob(_)) => {
                // Blobs not fully supported yet
                DataType::None
            }
            Err(_) => return None,
        };
        values.push(value);
    }

    Some(values)
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
fn params_to_key<T: rusqlite::ToSql>(params: &[T]) -> Vec<DataType> {
    params
        .iter()
        .map(|p| {
            // Convert rusqlite value to Noria DataType
            use rusqlite::types::ToSqlOutput;
            match p.to_sql() {
                Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Integer(i))) => {
                    DataType::from(i)
                }
                Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Text(s))) => {
                    DataType::from(std::str::from_utf8(s).unwrap_or(""))
                }
                Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Real(f))) => {
                    DataType::from(f)
                }
                Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Null)) => DataType::None,
                Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(i))) => {
                    DataType::from(i)
                }
                Ok(ToSqlOutput::Owned(rusqlite::types::Value::Text(s))) => {
                    DataType::from(s.as_str())
                }
                Ok(ToSqlOutput::Owned(rusqlite::types::Value::Real(f))) => {
                    DataType::from(f)
                }
                Ok(ToSqlOutput::Owned(rusqlite::types::Value::Null)) => DataType::None,
                _ => DataType::None,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_sql() {
        let sql = "SELECT  id, name   FROM users WHERE   id = ?";
        let normalized = normalize_sql(sql);
        assert_eq!(normalized, "SELECT ID, NAME FROM USERS WHERE ID = ?");
    }

    #[test]
    fn test_is_cacheable_query() {
        // Should be cacheable
        assert!(is_cacheable_query("SELECT * FROM users WHERE id = ?"));
        assert!(is_cacheable_query("SELECT name FROM users WHERE id = $1"));

        // Should NOT be cacheable
        assert!(!is_cacheable_query("INSERT INTO users VALUES (?)"));
        assert!(!is_cacheable_query("SELECT * FROM users")); // No params
        assert!(!is_cacheable_query("SELECT RANDOM() FROM users WHERE id = ?"));
        assert!(!is_cacheable_query("SELECT CURRENT_TIMESTAMP FROM users WHERE id = ?"));
    }

    #[test]
    fn test_params_to_key() {
        let params: Vec<&dyn rusqlite::ToSql> = vec![&42i64, &"hello"];
        let key = params_to_key(&params);
        assert_eq!(key.len(), 2);
        assert_eq!(key[0], DataType::from(42i64));
        assert_eq!(key[1], DataType::from("hello"));
    }
}
