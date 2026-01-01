//! Node.js bindings for noria-sqlite.
//!
//! This module provides a JavaScript API similar to better-sqlite3,
//! with transparent incremental view maintenance powered by Noria.

use napi::bindgen_prelude::*;
use napi_derive::napi;
use noria_sqlite::{Database as NoriaDatabase, Statement as NoriaStatement};
use std::sync::Arc;

/// A SQLite database connection with transparent Noria acceleration.
///
/// @example
/// ```js
/// const Database = require('noria-sqlite');
///
/// const db = new Database(':memory:');
/// db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
/// db.exec("INSERT INTO users VALUES (1, 'Alice')");
///
/// const stmt = db.prepare('SELECT * FROM users WHERE id = ?');
/// const row = stmt.get(1);
/// console.log(row); // { id: 1, name: 'Alice' }
/// ```
#[napi]
pub struct Database {
    inner: Arc<NoriaDatabase>,
}

#[napi]
impl Database {
    /// Create a new database connection.
    ///
    /// @param filename - The path to the database file, or ':memory:' for an in-memory database.
    #[napi(constructor)]
    pub fn new(filename: String) -> Result<Self> {
        let db = if filename == ":memory:" {
            NoriaDatabase::open_in_memory()
        } else {
            NoriaDatabase::open(&filename)
        }
        .map_err(|e| Error::from_reason(format!("Failed to open database: {}", e)))?;

        Ok(Self {
            inner: Arc::new(db),
        })
    }

    /// Execute one or more SQL statements.
    ///
    /// This is for DDL and data modification statements that don't return rows.
    ///
    /// @param sql - The SQL to execute.
    /// @returns The database instance for chaining.
    #[napi]
    pub fn exec(&self, sql: String) -> Result<&Self> {
        self.inner
            .execute_batch(&sql)
            .map_err(|e| Error::from_reason(format!("SQL error: {}", e)))?;
        Ok(self)
    }

    /// Prepare a SQL statement for execution.
    ///
    /// For SELECT queries with parameters, this automatically creates
    /// a Noria materialized view for O(1) lookups on subsequent calls.
    ///
    /// @param sql - The SQL statement with optional ? placeholders.
    /// @returns A prepared statement.
    #[napi]
    pub fn prepare(&self, sql: String) -> Result<Statement> {
        let stmt = self
            .inner
            .prepare(&sql)
            .map_err(|e| Error::from_reason(format!("Failed to prepare statement: {}", e)))?;

        Ok(Statement {
            inner: Arc::new(stmt),
            db: self.inner.clone(),
            sql,
        })
    }

    /// Get statistics about the dataflow engine.
    #[napi]
    pub fn stats(&self) -> CacheStats {
        let stats = self.inner.cache_stats();
        CacheStats {
            node_count: stats.node_count as i64,
            materialized_nodes: stats.materialized_nodes as i64,
            total_rows: stats.total_rows as i64,
        }
    }

    /// Close the database connection.
    ///
    /// Note: In Rust, the connection is automatically closed when dropped.
    #[napi]
    pub fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// A prepared SQL statement.
#[napi]
pub struct Statement {
    inner: Arc<NoriaStatement>,
    db: Arc<NoriaDatabase>,
    sql: String,
}

#[napi]
impl Statement {
    /// Execute the statement and return the first row.
    ///
    /// For integer params, pass them directly. For strings, pass strings.
    ///
    /// @param params - Bind parameters as an array of numbers or strings.
    /// @returns The first row as an object, or undefined if no rows.
    #[napi]
    pub fn get(&self, params: Option<Vec<serde_json::Value>>) -> Result<Option<serde_json::Value>> {
        let params = params.unwrap_or_default();

        // Convert JSON values to rusqlite params
        let param_values: Vec<Box<dyn rusqlite::ToSql>> = params
            .iter()
            .map(|v| -> Box<dyn rusqlite::ToSql> {
                match v {
                    serde_json::Value::Null => Box::new(rusqlite::types::Null),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Box::new(i)
                        } else if let Some(f) = n.as_f64() {
                            Box::new(f)
                        } else {
                            Box::new(rusqlite::types::Null)
                        }
                    }
                    serde_json::Value::String(s) => Box::new(s.clone()),
                    serde_json::Value::Bool(b) => Box::new(*b as i64),
                    _ => Box::new(rusqlite::types::Null),
                }
            })
            .collect();

        let param_refs: Vec<&dyn rusqlite::ToSql> =
            param_values.iter().map(|b| b.as_ref()).collect();

        // Execute query
        let result = self.inner.query_row(&param_refs, |row| {
            let mut obj = serde_json::Map::new();
            for i in 0..row.as_ref().column_count() {
                let default_name = format!("col{}", i);
                let name = row.as_ref().column_name(i).unwrap_or(&default_name);
                let value = row_value_to_json(row, i)?;
                obj.insert(name.to_string(), value);
            }
            Ok(serde_json::Value::Object(obj))
        });

        match result {
            Ok(val) => Ok(Some(val)),
            Err(noria_sqlite::Error::Sqlite(rusqlite::Error::QueryReturnedNoRows)) => Ok(None),
            Err(e) => Err(Error::from_reason(format!("Query error: {}", e))),
        }
    }

    /// Execute the statement and return all rows.
    ///
    /// @param params - Bind parameters as an array of numbers or strings.
    /// @returns An array of row objects.
    #[napi]
    pub fn all(&self, params: Option<Vec<serde_json::Value>>) -> Result<Vec<serde_json::Value>> {
        let params = params.unwrap_or_default();

        // Convert JSON values to rusqlite params
        let param_values: Vec<Box<dyn rusqlite::ToSql>> = params
            .iter()
            .map(|v| -> Box<dyn rusqlite::ToSql> {
                match v {
                    serde_json::Value::Null => Box::new(rusqlite::types::Null),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Box::new(i)
                        } else if let Some(f) = n.as_f64() {
                            Box::new(f)
                        } else {
                            Box::new(rusqlite::types::Null)
                        }
                    }
                    serde_json::Value::String(s) => Box::new(s.clone()),
                    serde_json::Value::Bool(b) => Box::new(*b as i64),
                    _ => Box::new(rusqlite::types::Null),
                }
            })
            .collect();

        let param_refs: Vec<&dyn rusqlite::ToSql> =
            param_values.iter().map(|b| b.as_ref()).collect();

        let results = self
            .inner
            .query_map(&param_refs, |row| {
                let mut obj = serde_json::Map::new();
                for i in 0..row.as_ref().column_count() {
                    let default_name = format!("col{}", i);
                    let name = row.as_ref().column_name(i).unwrap_or(&default_name);
                    let value = row_value_to_json(row, i)?;
                    obj.insert(name.to_string(), value);
                }
                Ok(serde_json::Value::Object(obj))
            })
            .map_err(|e| Error::from_reason(format!("Query error: {}", e)))?;

        Ok(results)
    }

    /// Execute the statement without returning rows (for INSERT/UPDATE/DELETE).
    ///
    /// @param params - Bind parameters as an array of numbers or strings.
    /// @returns Info about the execution.
    #[napi]
    pub fn run(&self, params: Option<Vec<serde_json::Value>>) -> Result<RunResult> {
        let params = params.unwrap_or_default();

        // Convert JSON values to rusqlite params
        let param_values: Vec<Box<dyn rusqlite::ToSql>> = params
            .iter()
            .map(|v| -> Box<dyn rusqlite::ToSql> {
                match v {
                    serde_json::Value::Null => Box::new(rusqlite::types::Null),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Box::new(i)
                        } else if let Some(f) = n.as_f64() {
                            Box::new(f)
                        } else {
                            Box::new(rusqlite::types::Null)
                        }
                    }
                    serde_json::Value::String(s) => Box::new(s.clone()),
                    serde_json::Value::Bool(b) => Box::new(*b as i64),
                    _ => Box::new(rusqlite::types::Null),
                }
            })
            .collect();

        let param_refs: Vec<&dyn rusqlite::ToSql> =
            param_values.iter().map(|b| b.as_ref()).collect();

        let changes = self
            .db
            .execute(&self.sql, rusqlite::params_from_iter(&param_refs))
            .map_err(|e| Error::from_reason(format!("Execute error: {}", e)))?;

        Ok(RunResult {
            changes: changes as i64,
            last_insert_rowid: 0,
        })
    }

    /// Check if this statement is cached (has a materialized view).
    #[napi(getter)]
    pub fn cached(&self) -> bool {
        self.inner.is_cached()
    }
}

/// Statistics about the dataflow cache.
#[napi(object)]
pub struct CacheStats {
    pub node_count: i64,
    pub materialized_nodes: i64,
    pub total_rows: i64,
}

/// Result of running a statement.
#[napi(object)]
pub struct RunResult {
    pub changes: i64,
    pub last_insert_rowid: i64,
}

/// Convert a rusqlite row value to JSON.
fn row_value_to_json(row: &rusqlite::Row, idx: usize) -> rusqlite::Result<serde_json::Value> {
    use rusqlite::types::ValueRef;

    Ok(match row.get_ref(idx)? {
        ValueRef::Null => serde_json::Value::Null,
        ValueRef::Integer(i) => serde_json::Value::Number(i.into()),
        ValueRef::Real(f) => serde_json::json!(f),
        ValueRef::Text(s) => {
            serde_json::Value::String(std::str::from_utf8(s).unwrap_or("").to_string())
        }
        ValueRef::Blob(b) => serde_json::json!({ "__blob": b.len() }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_creation() {
        let db = Database::new(":memory:".to_string()).unwrap();
        db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY)".to_string())
            .unwrap();
    }
}
