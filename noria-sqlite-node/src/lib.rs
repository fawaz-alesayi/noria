//! Node.js bindings for noria-sqlite - better-sqlite3 compatible API.
//!
//! This module provides a JavaScript API that is fully compatible with better-sqlite3,
//! with transparent incremental view maintenance powered by Noria.

use napi::bindgen_prelude::*;
use napi_derive::napi;
use noria_sqlite::{Database as NoriaDatabase, Statement as NoriaStatement};
use parking_lot::Mutex;
use std::sync::Arc;

/// SqliteError for compatibility with better-sqlite3
#[napi]
pub struct SqliteError {
    pub message: String,
    pub code: String,
}

/// A SQLite database connection with transparent Noria acceleration.
/// API compatible with better-sqlite3.
#[napi(js_name = "Database")]
pub struct Database {
    inner: Arc<NoriaDatabase>,
    filename: String,
    is_memory: bool,
    is_readonly: bool,
    is_open: bool,
}

#[napi]
impl Database {
    /// Create a new database connection.
    /// @param filename - Path to database file, or ':memory:' for in-memory database
    /// @param options - Optional configuration object
    #[napi(constructor)]
    pub fn new(filename: Option<String>, options: Option<DatabaseOptions>) -> Result<Self> {
        let filename = filename.unwrap_or_default();
        let filename = filename.trim().to_string();
        let options = options.unwrap_or_default();

        // Validate options
        if options.readonly.unwrap_or(false) && (filename.is_empty() || filename == ":memory:") {
            return Err(Error::new(
                Status::InvalidArg,
                "In-memory/temporary databases cannot be readonly",
            ));
        }

        let is_memory = filename.is_empty() || filename == ":memory:";
        let is_readonly = options.readonly.unwrap_or(false);

        let db = if is_memory {
            NoriaDatabase::open_in_memory()
        } else {
            NoriaDatabase::open(&filename)
        }
        .map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("SQLITE_CANTOPEN: {}", e),
            )
        })?;

        Ok(Self {
            inner: Arc::new(db),
            filename: if filename.is_empty() {
                String::new()
            } else {
                filename
            },
            is_memory,
            is_readonly,
            is_open: true,
        })
    }

    /// The filename passed to the constructor.
    #[napi(getter)]
    pub fn name(&self) -> String {
        self.filename.clone()
    }

    /// Whether the database is open.
    #[napi(getter)]
    pub fn open(&self) -> bool {
        self.is_open
    }

    /// Whether the database is in-memory.
    #[napi(getter)]
    pub fn memory(&self) -> bool {
        self.is_memory
    }

    /// Whether the database is read-only.
    #[napi(getter)]
    pub fn readonly(&self) -> bool {
        self.is_readonly
    }

    /// Whether the database is currently in a transaction.
    #[napi(getter, js_name = "inTransaction")]
    pub fn in_transaction(&self) -> bool {
        // For now, always return false. Full implementation would track transaction state.
        false
    }

    /// Prepare a SQL statement.
    #[napi]
    pub fn prepare(&self, sql: String) -> Result<Statement> {
        if !self.is_open {
            return Err(Error::new(
                Status::GenericFailure,
                "The database connection is not open",
            ));
        }

        let stmt = self
            .inner
            .prepare(&sql)
            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;

        // Determine if this is a reader (SELECT/PRAGMA) statement
        let sql_upper = sql.trim().to_uppercase();
        let is_reader = sql_upper.starts_with("SELECT")
            || sql_upper.starts_with("WITH")
            || sql_upper.starts_with("PRAGMA")
            || sql_upper.contains(" RETURNING ");

        Ok(Statement {
            inner: Arc::new(Mutex::new(stmt)),
            db: self.inner.clone(),
            sql,
            is_reader,
            pluck_mode: false,
            expand_mode: false,
            raw_mode: false,
        })
    }

    /// Execute one or more SQL statements.
    /// Note: Returns void in Rust; JavaScript wrapper handles chaining.
    #[napi(js_name = "_exec")]
    pub fn exec(&self, sql: String) -> Result<()> {
        if !self.is_open {
            return Err(Error::new(
                Status::GenericFailure,
                "The database connection is not open",
            ));
        }

        self.inner
            .execute_batch(&sql)
            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;

        Ok(())
    }

    /// Close the database connection.
    #[napi]
    pub fn close(&mut self) -> Result<()> {
        self.is_open = false;
        Ok(())
    }
}

/// Database constructor options
#[napi(object)]
#[derive(Default)]
pub struct DatabaseOptions {
    pub readonly: Option<bool>,
    pub file_must_exist: Option<bool>,
    pub timeout: Option<i32>,
}

/// A prepared SQL statement.
#[napi]
pub struct Statement {
    inner: Arc<Mutex<NoriaStatement>>,
    db: Arc<NoriaDatabase>,
    sql: String,
    is_reader: bool,
    pluck_mode: bool,
    expand_mode: bool,
    raw_mode: bool,
}

#[napi]
impl Statement {
    /// Whether this statement returns data (is a SELECT or has RETURNING).
    #[napi(getter)]
    pub fn reader(&self) -> bool {
        self.is_reader
    }

    /// The source SQL string.
    #[napi(getter)]
    pub fn source(&self) -> String {
        self.sql.clone()
    }

    /// Execute the statement and return the first row.
    /// Throws if this is not a reader statement.
    #[napi(ts_args_type = "...params: any[]")]
    pub fn get(&self, params: Vec<serde_json::Value>) -> Result<Option<serde_json::Value>> {
        if !self.is_reader {
            return Err(Error::new(
                Status::InvalidArg,
                "This statement does not return data. Use run() instead",
            ));
        }

        let flat_params = flatten_params(&params)?;
        let param_values = convert_params(&flat_params);
        let param_refs: Vec<&dyn rusqlite::ToSql> =
            param_values.iter().map(|b| b.as_ref()).collect();

        let stmt = self.inner.lock();
        let result = stmt.query_row(&param_refs, |row| {
            if self.pluck_mode {
                // Return just the first column value
                row_value_to_json(row, 0)
            } else if self.raw_mode {
                // Return as array
                let mut arr = Vec::new();
                for i in 0..row.as_ref().column_count() {
                    arr.push(row_value_to_json(row, i)?);
                }
                Ok(serde_json::Value::Array(arr))
            } else {
                // Return as object
                let mut obj = serde_json::Map::new();
                for i in 0..row.as_ref().column_count() {
                    let default_name = format!("col{}", i);
                    let name = row.as_ref().column_name(i).unwrap_or(&default_name);
                    let value = row_value_to_json(row, i)?;
                    obj.insert(name.to_string(), value);
                }
                Ok(serde_json::Value::Object(obj))
            }
        });

        match result {
            Ok(val) => Ok(Some(val)),
            Err(noria_sqlite::Error::Sqlite(rusqlite::Error::QueryReturnedNoRows)) => Ok(None),
            Err(e) => Err(Error::new(
                Status::GenericFailure,
                format!("SQLITE_ERROR: {}", e),
            )),
        }
    }

    /// Execute the statement and return all rows.
    #[napi(ts_args_type = "...params: any[]")]
    pub fn all(&self, params: Vec<serde_json::Value>) -> Result<Vec<serde_json::Value>> {
        let flat_params = flatten_params(&params)?;
        let param_values = convert_params(&flat_params);
        let param_refs: Vec<&dyn rusqlite::ToSql> =
            param_values.iter().map(|b| b.as_ref()).collect();

        let stmt = self.inner.lock();
        let results = stmt
            .query_map(&param_refs, |row| {
                if self.pluck_mode {
                    row_value_to_json(row, 0)
                } else if self.raw_mode {
                    let mut arr = Vec::new();
                    for i in 0..row.as_ref().column_count() {
                        arr.push(row_value_to_json(row, i)?);
                    }
                    Ok(serde_json::Value::Array(arr))
                } else {
                    let mut obj = serde_json::Map::new();
                    for i in 0..row.as_ref().column_count() {
                        let default_name = format!("col{}", i);
                        let name = row.as_ref().column_name(i).unwrap_or(&default_name);
                        let value = row_value_to_json(row, i)?;
                        obj.insert(name.to_string(), value);
                    }
                    Ok(serde_json::Value::Object(obj))
                }
            })
            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;

        Ok(results)
    }

    /// Execute the statement and return info about the execution.
    #[napi(ts_args_type = "...params: any[]")]
    pub fn run(&self, params: Vec<serde_json::Value>) -> Result<RunResult> {
        let flat_params = flatten_params(&params)?;
        let param_values = convert_params(&flat_params);
        let param_refs: Vec<&dyn rusqlite::ToSql> =
            param_values.iter().map(|b| b.as_ref()).collect();

        let changes = self
            .db
            .execute(&self.sql, rusqlite::params_from_iter(&param_refs))
            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;

        // Get last_insert_rowid from the connection
        let last_rowid = {
            let conn = self.db.connection().read();
            conn.last_insert_rowid()
        };

        Ok(RunResult {
            changes: changes as i64,
            last_insert_rowid: last_rowid,
        })
    }

    /// Enable pluck mode - return only the first column value.
    #[napi]
    pub fn pluck(&mut self, enabled: Option<bool>) -> &Self {
        self.pluck_mode = enabled.unwrap_or(true);
        if self.pluck_mode {
            self.expand_mode = false;
            self.raw_mode = false;
        }
        self
    }

    /// Enable expand mode - group columns by table.
    #[napi]
    pub fn expand(&mut self, enabled: Option<bool>) -> &Self {
        self.expand_mode = enabled.unwrap_or(true);
        if self.expand_mode {
            self.pluck_mode = false;
            self.raw_mode = false;
        }
        self
    }

    /// Enable raw mode - return rows as arrays.
    #[napi]
    pub fn raw(&mut self, enabled: Option<bool>) -> &Self {
        self.raw_mode = enabled.unwrap_or(true);
        if self.raw_mode {
            self.pluck_mode = false;
            self.expand_mode = false;
        }
        self
    }

    /// Bind parameters for reuse.
    #[napi(ts_args_type = "...params: any[]")]
    pub fn bind(&self, _params: Vec<serde_json::Value>) -> Result<&Self> {
        // For now, just return self - actual binding happens at execution time
        Ok(self)
    }
}

/// Result of running a statement.
#[napi(object)]
pub struct RunResult {
    pub changes: i64,
    #[napi(js_name = "lastInsertRowid")]
    pub last_insert_rowid: i64,
}

/// Flatten nested arrays in params (better-sqlite3 accepts arrays mixed with values)
fn flatten_params(params: &[serde_json::Value]) -> Result<Vec<serde_json::Value>> {
    let mut result = Vec::new();
    for param in params {
        match param {
            serde_json::Value::Array(arr) => {
                for item in arr {
                    result.push(item.clone());
                }
            }
            _ => result.push(param.clone()),
        }
    }
    Ok(result)
}

/// Convert JSON values to rusqlite params.
fn convert_params(params: &[serde_json::Value]) -> Vec<Box<dyn rusqlite::ToSql>> {
    params
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
                serde_json::Value::Object(obj) => {
                    // Handle Buffer-like objects
                    if let Some(serde_json::Value::String(t)) = obj.get("type") {
                        if t == "Buffer" {
                            if let Some(serde_json::Value::Array(data)) = obj.get("data") {
                                let bytes: Vec<u8> = data
                                    .iter()
                                    .filter_map(|v| v.as_u64().map(|n| n as u8))
                                    .collect();
                                return Box::new(bytes);
                            }
                        }
                    }
                    Box::new(rusqlite::types::Null)
                }
                _ => Box::new(rusqlite::types::Null),
            }
        })
        .collect()
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
        ValueRef::Blob(b) => {
            // Return as Buffer-like object for compatibility
            let data: Vec<serde_json::Value> =
                b.iter().map(|&byte| serde_json::json!(byte)).collect();
            serde_json::json!({
                "type": "Buffer",
                "data": data
            })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_creation() {
        let db = Database::new(Some(":memory:".to_string()), None).unwrap();
        assert!(db.open);
        assert!(db.memory);
        assert!(!db.readonly);
    }

    #[test]
    fn test_database_name() {
        let db = Database::new(Some(":memory:".to_string()), None).unwrap();
        assert_eq!(db.name(), ":memory:");
    }
}
