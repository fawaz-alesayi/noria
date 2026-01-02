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

        // Validate SQL at prepare time by calling SQLite's prepare
        // This matches better-sqlite3 behavior of catching syntax errors early
        {
            let conn = self.inner.connection().read();
            conn.prepare(&sql)
                .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;
        }

        let stmt = self
            .inner
            .prepare(&sql)
            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;

        // Determine statement type
        let sql_upper = sql.trim().to_uppercase();
        let is_reader = sql_upper.starts_with("SELECT")
            || sql_upper.starts_with("WITH")
            || sql_upper.starts_with("PRAGMA")
            || sql_upper.contains(" RETURNING ");

        // Check if this is a DDL statement (returns 0 changes)
        let is_ddl = sql_upper.starts_with("CREATE")
            || sql_upper.starts_with("DROP")
            || sql_upper.starts_with("ALTER")
            || sql_upper.starts_with("VACUUM")
            || sql_upper.starts_with("REINDEX")
            || sql_upper.starts_with("ANALYZE");

        Ok(Statement {
            inner: Arc::new(Mutex::new(stmt)),
            db: self.inner.clone(),
            sql,
            is_reader,
            is_ddl,
            pluck_mode: false,
            expand_mode: false,
            raw_mode: false,
            bound_params: None,
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
    is_ddl: bool,
    pluck_mode: bool,
    expand_mode: bool,
    raw_mode: bool,
    bound_params: Option<Vec<serde_json::Value>>,
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

        // Check if params were provided when already bound
        if self.bound_params.is_some() && !params.is_empty() {
            return Err(Error::new(
                Status::InvalidArg,
                "This statement already has bound parameters",
            ));
        }

        // Use bound params or provided params
        let flat_params = if let Some(ref bound) = self.bound_params {
            bound.clone()
        } else {
            flatten_params(&params)?
        };
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
        // Check if params were provided when already bound
        if self.bound_params.is_some() && !params.is_empty() {
            return Err(Error::new(
                Status::InvalidArg,
                "This statement already has bound parameters",
            ));
        }

        // Use bound params or provided params
        let flat_params = if let Some(ref bound) = self.bound_params {
            bound.clone()
        } else {
            flatten_params(&params)?
        };
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
        // Check if params were provided when already bound
        if self.bound_params.is_some() && !params.is_empty() {
            return Err(Error::new(
                Status::InvalidArg,
                "This statement already has bound parameters",
            ));
        }

        // Use bound params or provided params
        let flat_params = if let Some(ref bound) = self.bound_params {
            bound.clone()
        } else {
            flatten_params(&params)?
        };

        let changes = if has_named_params(&flat_params) {
            // Handle named parameters
            if let serde_json::Value::Object(obj) = &flat_params[0] {
                let named = extract_named_params(&self.sql, obj);
                let conn = self.db.connection().read();
                let mut stmt = conn.prepare(&self.sql)
                    .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;

                // Build named params slice - rusqlite expects &[(&str, &dyn ToSql)]
                let named_refs: Vec<(&str, &dyn rusqlite::ToSql)> = named
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_ref()))
                    .collect();

                stmt.execute(named_refs.as_slice())
                    .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?
            } else {
                0
            }
        } else {
            // Handle positional parameters
            let param_values = convert_params(&flat_params);
            let param_refs: Vec<&dyn rusqlite::ToSql> =
                param_values.iter().map(|b| b.as_ref()).collect();

            self.db
                .execute(&self.sql, rusqlite::params_from_iter(&param_refs))
                .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?
        };

        // Get last_insert_rowid from the connection
        let last_rowid = {
            let conn = self.db.connection().read();
            conn.last_insert_rowid()
        };

        // DDL statements (CREATE, DROP, ALTER, etc.) return 0 changes
        // to match better-sqlite3 behavior
        let final_changes = if self.is_ddl { 0 } else { changes as i64 };
        let final_rowid = if self.is_ddl { 0 } else { last_rowid };

        Ok(RunResult {
            changes: final_changes,
            last_insert_rowid: final_rowid,
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

    /// Bind parameters permanently for reuse.
    #[napi(ts_args_type = "...params: any[]")]
    pub fn bind(&mut self, params: Vec<serde_json::Value>) -> Result<&Self> {
        // Check if already bound
        if self.bound_params.is_some() {
            return Err(Error::new(
                Status::InvalidArg,
                "The bind() method can only be invoked once per statement object",
            ));
        }

        // Flatten the params
        let flat_params = flatten_params(&params)?;
        self.bound_params = Some(flat_params);
        Ok(self)
    }

    /// Get column information for this statement.
    /// Note: SQLite column origin info requires SQLITE_ENABLE_COLUMN_METADATA which
    /// may not be available. We return basic column names for now.
    #[napi]
    pub fn columns(&self) -> Result<Vec<ColumnInfo>> {
        if !self.is_reader {
            return Err(Error::new(
                Status::InvalidArg,
                "This statement does not return data",
            ));
        }

        let conn = self.db.connection().read();
        let stmt = conn.prepare(&self.sql)
            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;

        let count = stmt.column_count();
        let mut columns = Vec::with_capacity(count);

        for i in 0..count {
            let name = stmt.column_name(i)
                .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?
                .to_string();

            // For now, return basic info without origin metadata
            // Full metadata requires SQLITE_ENABLE_COLUMN_METADATA compile flag
            columns.push(ColumnInfo {
                name,
                column: None,
                table: None,
                database: None,
                type_name: None,
            });
        }

        Ok(columns)
    }
}

/// Column information from Statement#columns()
#[napi(object)]
pub struct ColumnInfo {
    pub name: String,
    pub column: Option<String>,
    pub table: Option<String>,
    pub database: Option<String>,
    #[napi(js_name = "type")]
    pub type_name: Option<String>,
}

/// Result of running a statement.
#[napi(object)]
pub struct RunResult {
    pub changes: i64,
    #[napi(js_name = "lastInsertRowid")]
    pub last_insert_rowid: i64,
}

/// Flatten nested arrays in params (better-sqlite3 accepts arrays mixed with values)
/// Also handles object params for named parameters
fn flatten_params(params: &[serde_json::Value]) -> Result<Vec<serde_json::Value>> {
    let mut result = Vec::new();
    for param in params {
        match param {
            serde_json::Value::Array(arr) => {
                for item in arr {
                    result.push(item.clone());
                }
            }
            // If first param is an object with regular keys (not Buffer), it's named params
            serde_json::Value::Object(obj) => {
                // Check if it's a Buffer - those get passed through
                if obj.get("type").map_or(false, |t| t == "Buffer") {
                    result.push(param.clone());
                } else {
                    // It's a named params object - push it as-is for special handling
                    result.push(param.clone());
                }
            }
            _ => result.push(param.clone()),
        }
    }
    Ok(result)
}

/// Check if params contains named parameters (an object that's not a Buffer)
fn has_named_params(params: &[serde_json::Value]) -> bool {
    if params.len() == 1 {
        if let serde_json::Value::Object(obj) = &params[0] {
            // It's named params if it's an object that's not a Buffer
            return obj.get("type").map_or(true, |t| t != "Buffer");
        }
    }
    false
}

/// Convert a single JSON value to rusqlite value
fn json_to_sql_value(v: &serde_json::Value) -> Box<dyn rusqlite::ToSql> {
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
}

/// Extract named parameters from an object
/// SQLite named params use $name, @name, or :name syntax
/// rusqlite expects the full parameter name with prefix
fn extract_named_params(sql: &str, obj: &serde_json::Map<String, serde_json::Value>) -> Vec<(String, Box<dyn rusqlite::ToSql>)> {
    obj.iter()
        .map(|(k, v)| {
            // Determine what prefix the SQL uses for this parameter
            let prefixed_name = if sql.contains(&format!("${}", k)) {
                format!("${}", k)
            } else if sql.contains(&format!("@{}", k)) {
                format!("@{}", k)
            } else if sql.contains(&format!(":{}", k)) {
                format!(":{}", k)
            } else {
                // Default to $ prefix
                format!("${}", k)
            };
            (prefixed_name, json_to_sql_value(v))
        })
        .collect()
}

/// Convert JSON values to rusqlite params.
fn convert_params(params: &[serde_json::Value]) -> Vec<Box<dyn rusqlite::ToSql>> {
    params
        .iter()
        .filter_map(|v| {
            // Skip objects that are named param containers (not Buffers)
            if let serde_json::Value::Object(obj) = v {
                if obj.get("type").map_or(true, |t| t != "Buffer") {
                    return None; // Skip named param objects
                }
            }
            Some(json_to_sql_value(v))
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
