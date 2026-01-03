//! Prepared statement wrapper with automatic view acceleration

use crate::dataflow::{NoriaEngine, NoriaView};
use crate::error::{Error, Result};
use crate::Config;
use noria::DataType;
use parking_lot::RwLock;
use rusqlite::Connection;
use std::sync::Arc;

/// A prepared SQL statement that may be accelerated by a Noria view.
pub struct Statement {
    /// The original SQL text
    sql: String,

    /// Noria view handle if this query is materialized
    view: Option<NoriaView>,

    /// Reference to the Noria dataflow engine
    engine: Arc<NoriaEngine>,

    /// Fallback to SQLite if view isn't available
    conn: Arc<RwLock<Connection>>,

    /// Configuration
    #[allow(dead_code)]
    config: Config,
}

impl Statement {
    /// Create a new statement, potentially synthesizing a view.
    pub(crate) fn new(
        sql: &str,
        conn: Arc<RwLock<Connection>>,
        engine: Arc<NoriaEngine>,
        config: &Config,
    ) -> Result<Self> {
        // Check if this query is cacheable (SELECT with parameters)
        // Skip view creation if acceleration is disabled (passthrough mode)
        let view = if config.acceleration_disabled {
            None
        } else if is_cacheable_query(sql) {
            // Try to create a view for this query
            match engine.create_view(sql) {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::warn!(
                        "Failed to create view for query, falling back to SQLite: {}",
                        e
                    );
                    None
                }
            }
        } else {
            None
        };

        Ok(Self {
            sql: sql.to_string(),
            view,
            engine,
            conn,
            config: config.clone(),
        })
    }

    /// Query for a single row using rusqlite-compatible API.
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
        if let Some(ref view) = self.view {
            let key = params_to_key(&params);
            if let Some(rows) = self.engine.lookup(view, &key) {
                if !rows.is_empty() {
                    // Cache hit - but we still need to return a rusqlite::Row
                    // For now, fall through to SQLite
                    // TODO: Implement synthetic row construction
                }
            }
        }

        // Execute through SQLite
        let conn = self.conn.read();
        let mut stmt = conn.prepare_cached(&self.sql)?;
        let result = stmt.query_row(rusqlite::params_from_iter(&params), f)?;
        Ok(result)
    }

    /// Query for multiple rows using rusqlite-compatible API.
    pub fn query_map<T, P, F>(&self, params: P, mut f: F) -> Result<Vec<T>>
    where
        P: IntoIterator,
        P::Item: rusqlite::ToSql,
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let params: Vec<_> = params.into_iter().collect();

        // Execute through SQLite
        let conn = self.conn.read();
        let mut stmt = conn.prepare_cached(&self.sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(&params), |row| f(row))?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Query for a single row, returning cached data directly.
    ///
    /// This method bypasses the rusqlite::Row abstraction and returns
    /// data directly from the Noria materialized view when available.
    /// This is the fastest path for cache hits.
    ///
    /// Returns:
    /// - `Ok(Some(row))` on cache hit
    /// - `Ok(None)` on cache miss (caller should use `query_row` to get data)
    /// - `Err(_)` if the query isn't cacheable
    pub fn query_row_cached<P>(&self, params: P) -> Result<Option<Vec<DataType>>>
    where
        P: IntoIterator,
        P::Item: rusqlite::ToSql,
    {
        let view = self.view.as_ref().ok_or_else(|| {
            Error::NotCacheable("Query is not cacheable".to_string())
        })?;

        let params: Vec<_> = params.into_iter().collect();
        let key = params_to_key(&params);

        // Look up in materialized view
        match self.engine.lookup(view, &key) {
            Some(rows) if !rows.is_empty() => Ok(Some(rows[0].clone())),
            _ => Ok(None),
        }
    }

    /// Query for multiple rows, returning cached data directly.
    pub fn query_map_cached<P>(&self, params: P) -> Result<Option<Vec<Vec<DataType>>>>
    where
        P: IntoIterator,
        P::Item: rusqlite::ToSql,
    {
        let view = self.view.as_ref().ok_or_else(|| {
            Error::NotCacheable("Query is not cacheable".to_string())
        })?;

        let params: Vec<_> = params.into_iter().collect();
        let key = params_to_key(&params);

        // Look up in materialized view
        self.engine.lookup(view, &key).map_or(Ok(None), |rows| Ok(Some(rows)))
    }

    /// Query with upquery support - falls back to SQLite on cache miss.
    ///
    /// This is the recommended way to query when you want both:
    /// - Fast cache hits from the materialized view
    /// - Automatic population from SQLite on cache miss
    pub fn query_row_cached_or_upquery<P>(&self, params: P) -> Result<Option<Vec<DataType>>>
    where
        P: IntoIterator,
        P::Item: rusqlite::ToSql,
    {
        let view = self.view.as_ref().ok_or_else(|| {
            Error::NotCacheable("Query is not cacheable".to_string())
        })?;

        let params: Vec<_> = params.into_iter().collect();
        let key = params_to_key(&params);

        // Try lookup, upquery on miss
        match self.engine.lookup_or_upquery(view, &key) {
            Ok(rows) if !rows.is_empty() => Ok(Some(rows[0].clone())),
            Ok(_) => Ok(None),
            Err(e) => Err(Error::Dataflow(e.to_string())),
        }
    }

    /// Check if this statement is accelerated by a view.
    pub fn is_cached(&self) -> bool {
        self.view.is_some()
    }

    /// Get the original SQL text.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Get the underlying view if one exists.
    pub fn view(&self) -> Option<&NoriaView> {
        self.view.as_ref()
    }
}

/// Check if a query is suitable for caching.
///
/// Currently only SELECT queries with parameter placeholders are cached.
pub fn is_cacheable_query(sql: &str) -> bool {
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

/// Normalize SQL for cache key generation.
///
/// This strips whitespace variations and normalizes case.
pub fn normalize_sql(sql: &str) -> String {
    // Simple normalization - in production this would be more sophisticated
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_uppercase()
}

/// Convert parameters to a cache lookup key.
pub fn params_to_key<T: rusqlite::ToSql>(params: &[T]) -> Vec<DataType> {
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
                Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Real(f))) => DataType::from(f),
                Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Null)) => DataType::None,
                Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(i))) => DataType::from(i),
                Ok(ToSqlOutput::Owned(rusqlite::types::Value::Text(s))) => {
                    DataType::from(s.as_str())
                }
                Ok(ToSqlOutput::Owned(rusqlite::types::Value::Real(f))) => DataType::from(f),
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
        assert!(!is_cacheable_query(
            "SELECT CURRENT_TIMESTAMP FROM users WHERE id = ?"
        ));
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
