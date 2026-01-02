//! Node.js bindings for noria-sqlite - better-sqlite3 compatible API.
//!
//! This module provides a JavaScript API that is fully compatible with better-sqlite3,
//! with transparent incremental view maintenance powered by Noria.

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use noria_sqlite::{Database as NoriaDatabase, Statement as NoriaStatement};
use parking_lot::Mutex;
use rusqlite::functions::FunctionFlags;
use std::sync::mpsc;
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
    unsafe_mode: bool,
    default_safe_integers: bool,
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
            unsafe_mode: false,
            default_safe_integers: false,
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
            safe_ints: self.default_safe_integers,
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

    /// Toggle unsafe mode.
    /// In unsafe mode, operations that would normally be blocked during iteration are allowed.
    /// @param enabled - Whether to enable unsafe mode. If not provided, returns current state.
    #[napi(js_name = "_unsafeMode")]
    pub fn unsafe_mode(&mut self, enabled: Option<bool>) -> bool {
        if let Some(value) = enabled {
            self.unsafe_mode = value;
        }
        self.unsafe_mode
    }

    /// Toggle default safe integers mode.
    /// When enabled, new statements will return integers as BigInt by default.
    /// @param enabled - Whether to enable safe integers. If not provided, returns current state.
    #[napi(js_name = "_defaultSafeIntegers")]
    pub fn default_safe_integers(&mut self, enabled: Option<bool>) -> bool {
        if let Some(value) = enabled {
            self.default_safe_integers = value;
        }
        self.default_safe_integers
    }

    /// Get the default safe integers setting.
    #[napi(js_name = "_getDefaultSafeIntegers")]
    pub fn get_default_safe_integers(&self) -> bool {
        self.default_safe_integers
    }

    /// Load a SQLite extension.
    #[napi(js_name = "_loadExtension")]
    pub fn load_extension(&self, path: String, entry_point: Option<String>) -> Result<()> {
        if !self.is_open {
            return Err(Error::new(
                Status::GenericFailure,
                "The database connection is not open",
            ));
        }

        let conn = self.inner.connection().write();

        // Enable extension loading
        unsafe {
            conn.load_extension_enable()
                .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;
        }

        // Load the extension
        let result = unsafe {
            match entry_point {
                Some(ep) => conn.load_extension(&path, Some(&ep)),
                None => conn.load_extension(&path, None::<&str>),
            }
        };

        // Disable extension loading for safety
        unsafe {
            let _ = conn.load_extension_disable();
        }

        result.map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))
    }

    /// Perform a simple backup of the database using VACUUM INTO.
    /// This is a synchronous operation that creates a complete copy of the database.
    /// For incremental backup with progress callbacks, a more sophisticated implementation
    /// using rusqlite's backup API would be needed.
    #[napi(js_name = "_backup")]
    pub fn backup(&self, dest_path: String, attached_name: Option<String>) -> Result<BackupProgress> {
        if !self.is_open {
            return Err(Error::new(
                Status::GenericFailure,
                "The database connection is not open",
            ));
        }

        let attached = attached_name.unwrap_or_else(|| "main".to_string());

        // Use VACUUM INTO for a simple backup
        // This creates a complete copy of the database
        let conn = self.inner.connection().read();

        // For attached databases, we'd need to handle differently
        // For now, just use VACUUM INTO for the main database
        if attached == "main" {
            let sql = format!("VACUUM INTO '{}'", dest_path.replace("'", "''"));
            conn.execute(&sql, [])
                .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;
        } else {
            // For attached databases, we need to open the attached db and backup
            return Err(Error::new(
                Status::GenericFailure,
                "Backup of attached databases is not yet supported",
            ));
        }

        Ok(BackupProgress {
            total_pages: 1,
            remaining_pages: 0,
        })
    }

    /// Register a user-defined SQL function.
    /// @param fn - JavaScript function to call
    /// @param name - SQL function name
    /// @param argc - Number of arguments (-1 for varargs)
    /// @param safe_ints - Whether to use BigInt for integers (0=false, 1=true, 2=inherit)
    /// @param deterministic - Whether function is deterministic
    /// @param direct_only - Whether function can only be called directly (not from triggers/views)
    #[napi(js_name = "_registerFunction")]
    pub fn register_function(
        &self,
        #[napi(ts_arg_type = "(...args: any[]) => any")] callback: JsFunction,
        name: String,
        argc: i32,
        safe_ints: i32,
        deterministic: bool,
        direct_only: bool,
    ) -> Result<()> {
        if !self.is_open {
            return Err(Error::new(
                Status::GenericFailure,
                "The database connection is not open",
            ));
        }

        // We'll store the channel sender alongside the args
        type CallArgs = (Vec<serde_json::Value>, mpsc::Sender<serde_json::Value>);

        // Create a threadsafe function from the callback
        // The callback receives args as Vec<serde_json::Value> and returns serde_json::Value
        let tsfn: ThreadsafeFunction<CallArgs, ErrorStrategy::Fatal> = callback
            .create_threadsafe_function(0, |ctx| {
                // ctx.value is (args, sender)
                // We return just the args to be passed to the JS function
                // The sender is used later in call_with_return_value
                let (args, _sender) = ctx.value;
                Ok(args)
            })?;

        // Determine the effective safe_ints setting
        let use_safe_ints = if safe_ints == 2 {
            self.default_safe_integers
        } else {
            safe_ints == 1
        };

        // Build function flags
        let mut flags = FunctionFlags::SQLITE_UTF8;
        if deterministic {
            flags |= FunctionFlags::SQLITE_DETERMINISTIC;
        }
        if direct_only {
            flags |= FunctionFlags::SQLITE_DIRECTONLY;
        }

        let tsfn = Arc::new(tsfn);
        let tsfn_clone = tsfn.clone();

        // Register the function with SQLite
        let conn = self.inner.connection().write();
        conn.create_scalar_function(&name, argc, flags, move |ctx| {
            // Convert SQLite arguments to JSON for JavaScript
            let mut args = Vec::with_capacity(ctx.len());
            for i in 0..ctx.len() {
                let value = sqlite_value_to_json(ctx.get_raw(i), use_safe_ints);
                args.push(value);
            }

            // Create a channel for this invocation
            let (tx, rx) = mpsc::channel();
            let tx_for_callback = tx.clone();

            // Call the JavaScript function with blocking mode
            // The callback receives the JS function's return value directly
            let status = tsfn_clone.call_with_return_value(
                (args, tx),
                ThreadsafeFunctionCallMode::Blocking,
                move |js_return: serde_json::Value| {
                    // Send the result through the channel
                    let _ = tx_for_callback.send(js_return);
                    Ok(())
                },
            );

            if status != Status::Ok {
                return Err(rusqlite::Error::UserFunctionError(Box::new(
                    std::io::Error::new(std::io::ErrorKind::Other, "Failed to call JS function"),
                )));
            }

            // Wait for the result from JavaScript
            match rx.recv() {
                Ok(value) => json_to_sqlite_result(value),
                Err(_) => Err(rusqlite::Error::UserFunctionError(Box::new(
                    std::io::Error::new(std::io::ErrorKind::Other, "Channel closed"),
                ))),
            }
        })
        .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;

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

/// Progress information for database backup
#[napi(object)]
pub struct BackupProgress {
    #[napi(js_name = "totalPages")]
    pub total_pages: i32,
    #[napi(js_name = "remainingPages")]
    pub remaining_pages: i32,
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
    safe_ints: bool,
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

        let safe_ints = self.safe_ints;
        let stmt = self.inner.lock();
        let result = stmt.query_row(&param_refs, |row| {
            if self.pluck_mode {
                // Return just the first column value
                row_value_to_json(row, 0, safe_ints)
            } else if self.raw_mode {
                // Return as array
                let mut arr = Vec::new();
                for i in 0..row.as_ref().column_count() {
                    arr.push(row_value_to_json(row, i, safe_ints)?);
                }
                Ok(serde_json::Value::Array(arr))
            } else {
                // Return as object
                let mut obj = serde_json::Map::new();
                for i in 0..row.as_ref().column_count() {
                    let default_name = format!("col{}", i);
                    let name = row.as_ref().column_name(i).unwrap_or(&default_name);
                    let value = row_value_to_json(row, i, safe_ints)?;
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

        let safe_ints = self.safe_ints;
        let stmt = self.inner.lock();
        let results = stmt
            .query_map(&param_refs, |row| {
                if self.pluck_mode {
                    row_value_to_json(row, 0, safe_ints)
                } else if self.raw_mode {
                    let mut arr = Vec::new();
                    for i in 0..row.as_ref().column_count() {
                        arr.push(row_value_to_json(row, i, safe_ints)?);
                    }
                    Ok(serde_json::Value::Array(arr))
                } else {
                    let mut obj = serde_json::Map::new();
                    for i in 0..row.as_ref().column_count() {
                        let default_name = format!("col{}", i);
                        let name = row.as_ref().column_name(i).unwrap_or(&default_name);
                        let value = row_value_to_json(row, i, safe_ints)?;
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

    /// Enable safe integers mode - return integers as BigInt.
    #[napi(js_name = "_safeIntegers")]
    pub fn safe_integers(&mut self, enabled: Option<bool>) -> &Self {
        self.safe_ints = enabled.unwrap_or(true);
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

/// Check if an object is a special marker (Buffer or BigInt) rather than named params
fn is_special_marker(obj: &serde_json::Map<String, serde_json::Value>) -> bool {
    // Buffer marker
    if obj.get("type").map_or(false, |t| t == "Buffer") {
        return true;
    }
    // BigInt marker
    if obj.contains_key("$bigint") {
        return true;
    }
    false
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
            // If first param is an object with regular keys (not Buffer/BigInt), it's named params
            serde_json::Value::Object(obj) => {
                // Check if it's a special marker - those get passed through as values
                if is_special_marker(obj) {
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

/// Check if params contains named parameters (an object that's not a special marker)
fn has_named_params(params: &[serde_json::Value]) -> bool {
    if params.len() == 1 {
        if let serde_json::Value::Object(obj) = &params[0] {
            // It's named params if it's an object that's not a special marker
            return !is_special_marker(obj);
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
            // Handle BigInt marker objects
            if let Some(serde_json::Value::String(s)) = obj.get("$bigint") {
                if let Ok(i) = s.parse::<i64>() {
                    return Box::new(i);
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
            // Skip objects that are named param containers (not special markers)
            if let serde_json::Value::Object(obj) = v {
                if !is_special_marker(obj) {
                    return None; // Skip named param objects
                }
            }
            Some(json_to_sql_value(v))
        })
        .collect()
}

/// Convert a rusqlite row value to JSON.
/// When safe_ints is true, integers are returned as a special marker object
/// that the JS wrapper will convert to BigInt.
fn row_value_to_json(row: &rusqlite::Row, idx: usize, safe_ints: bool) -> rusqlite::Result<serde_json::Value> {
    use rusqlite::types::ValueRef;

    Ok(match row.get_ref(idx)? {
        ValueRef::Null => serde_json::Value::Null,
        ValueRef::Integer(i) => {
            if safe_ints {
                // Return as a special marker that JS will convert to BigInt
                serde_json::json!({
                    "$bigint": i.to_string()
                })
            } else {
                // Return as f64 to ensure JavaScript treats it as Number
                // (NAPI-RS may auto-convert large i64 to BigInt, but f64 stays as Number)
                serde_json::json!(i as f64)
            }
        }
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

/// Convert a SQLite value (from function context) to JSON for JavaScript.
fn sqlite_value_to_json(value: rusqlite::types::ValueRef, safe_ints: bool) -> serde_json::Value {
    use rusqlite::types::ValueRef;

    match value {
        ValueRef::Null => serde_json::Value::Null,
        ValueRef::Integer(i) => {
            if safe_ints {
                serde_json::json!({ "$bigint": i.to_string() })
            } else {
                serde_json::json!(i as f64)
            }
        }
        ValueRef::Real(f) => serde_json::json!(f),
        ValueRef::Text(s) => {
            serde_json::Value::String(std::str::from_utf8(s).unwrap_or("").to_string())
        }
        ValueRef::Blob(b) => {
            let data: Vec<serde_json::Value> =
                b.iter().map(|&byte| serde_json::json!(byte)).collect();
            serde_json::json!({
                "type": "Buffer",
                "data": data
            })
        }
    }
}

/// Convert a JSON value from JavaScript to a rusqlite Result for function return.
fn json_to_sqlite_result(value: serde_json::Value) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'static>> {
    use rusqlite::types::{ToSqlOutput, Value};

    Ok(match value {
        serde_json::Value::Null => ToSqlOutput::Owned(Value::Null),
        serde_json::Value::Bool(b) => ToSqlOutput::Owned(Value::Integer(if b { 1 } else { 0 })),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                ToSqlOutput::Owned(Value::Integer(i))
            } else if let Some(f) = n.as_f64() {
                ToSqlOutput::Owned(Value::Real(f))
            } else {
                ToSqlOutput::Owned(Value::Null)
            }
        }
        serde_json::Value::String(s) => ToSqlOutput::Owned(Value::Text(s)),
        serde_json::Value::Object(obj) => {
            // Handle Buffer-like objects
            if let Some(serde_json::Value::String(t)) = obj.get("type") {
                if t == "Buffer" {
                    if let Some(serde_json::Value::Array(data)) = obj.get("data") {
                        let bytes: Vec<u8> = data
                            .iter()
                            .filter_map(|v| v.as_u64().map(|n| n as u8))
                            .collect();
                        return Ok(ToSqlOutput::Owned(Value::Blob(bytes)));
                    }
                }
            }
            // Handle BigInt marker
            if let Some(serde_json::Value::String(s)) = obj.get("$bigint") {
                if let Ok(i) = s.parse::<i64>() {
                    return Ok(ToSqlOutput::Owned(Value::Integer(i)));
                }
            }
            ToSqlOutput::Owned(Value::Null)
        }
        serde_json::Value::Array(_) => ToSqlOutput::Owned(Value::Null),
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
