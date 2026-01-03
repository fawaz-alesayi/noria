//! Node.js bindings for noria-sqlite - better-sqlite3 compatible API.
//!
//! This module provides a JavaScript API that is fully compatible with better-sqlite3,
//! with transparent incremental view maintenance powered by Noria.

use napi::bindgen_prelude::*;
use napi::sys;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::NapiRaw;
use napi::JsUnknown;
use napi::{JsObject, JsNull};
use napi_derive::napi;
use noria::DataType;
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

        // Check if Noria acceleration should be disabled
        let acceleration_disabled = options
            .noria
            .as_ref()
            .and_then(|n| n.acceleration_disabled)
            .unwrap_or(false);

        // Create config with acceleration_disabled option
        let config = noria_sqlite::Config {
            acceleration_disabled,
            ..Default::default()
        };

        let db = if is_memory {
            NoriaDatabase::open_in_memory_with_config(config)
        } else {
            NoriaDatabase::open_with_config(&filename, config)
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

        // Validate SQL at prepare time and get column names, table origins, and param count
        let (column_names, column_tables, param_count) = {
            let conn = self.inner.connection().read();
            let sqlite_stmt = conn.prepare(&sql)
                .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;

            // Extract column names and table origins using column_metadata feature
            let metadata = sqlite_stmt.columns_with_metadata();
            let names: Vec<String> = metadata.iter()
                .map(|col| col.name().to_string())
                .collect();
            let tables: Vec<Option<String>> = metadata.iter()
                .map(|col| col.table_name().map(|s| s.to_string()))
                .collect();

            // Get parameter count
            let param_count = sqlite_stmt.parameter_count();

            (names, tables, param_count)
        };

        let stmt = self
            .inner
            .prepare(&sql)
            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;

        // Check if the statement has a Noria view (is accelerated)
        let is_cached = stmt.is_cached();

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

        // Pre-allocate CStrings for column names (used by fast path)
        let column_name_cstrs: Vec<std::ffi::CString> = column_names
            .iter()
            .map(|n| std::ffi::CString::new(n.as_str()).unwrap())
            .collect();

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
            column_names,
            column_name_cstrs,
            column_tables,
            is_cached,
            param_count,
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

    /// Get cache statistics from the Noria dataflow engine.
    /// Returns an object with:
    /// - nodeCount: Number of nodes in the dataflow graph
    /// - materializedNodes: Number of materialized (cached) nodes
    /// - totalRows: Total rows across all materialized views
    /// - viewCount: Number of registered views
    /// - cacheHits: Number of cache hits (lookups that found data in cache)
    /// - cacheMisses: Number of cache misses (lookups that required upquery to SQLite)
    #[napi(js_name = "cacheStats")]
    pub fn cache_stats(&self) -> CacheStats {
        let stats = self.inner.cache_stats();
        CacheStats {
            node_count: stats.node_count as i64,
            materialized_nodes: stats.materialized_nodes as i64,
            total_rows: stats.total_rows as i64,
            view_count: stats.view_count as i64,
            cache_hits: stats.cache_hits as i64,
            cache_misses: stats.cache_misses as i64,
        }
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
                Some(ep) => conn.load_extension(path.as_str(), Some(ep.as_str())),
                None => conn.load_extension(path.as_str(), None::<&str>),
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

    /// Serialize the database to a Buffer.
    /// Returns the raw database bytes as a Node.js Buffer.
    #[napi(js_name = "_serialize")]
    pub fn serialize(&self, attached_name: Option<String>) -> Result<Buffer> {
        if !self.is_open {
            return Err(Error::new(
                Status::GenericFailure,
                "The database connection is not open",
            ));
        }

        let attached = attached_name.unwrap_or_else(|| "main".to_string());

        let conn = self.inner.connection().read();

        // Use rusqlite's serialize method with schema name as &str
        let data = conn
            .serialize(attached.as_str())
            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;

        // Data implements Deref<Target = [u8]>, so we can get bytes from it
        let bytes: Vec<u8> = (*data).to_vec();
        Ok(Buffer::from(bytes))
    }

    /// Register a user-defined SQL function.
    /// @param fn - JavaScript function to call
    /// @param name - SQL function name
    /// @param argc - Number of arguments (-1 for varargs)
    /// @param safe_ints - Whether to use BigInt for integers (0=false, 1=true, 2=inherit)
    /// @param deterministic - Whether function is deterministic
    /// @param direct_only - Whether function can only be called directly (not from triggers/views)
    ///
    /// This uses raw NAPI calls similar to how better-sqlite3 uses raw V8 calls.
    /// Since SQLite runs on the main Node.js thread, we can call JS functions
    /// directly without ThreadsafeFunction.
    #[napi(js_name = "_registerFunction")]
    pub fn register_function(
        &self,
        env: Env,
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

        // Store raw NAPI pointers - similar to how better-sqlite3 stores v8::Isolate*
        // This is safe because SQLite callbacks run on the same thread as Node.js
        let raw_env = env.raw();

        // Create a reference to the function so it won't be garbage collected
        let mut fn_ref: sys::napi_ref = std::ptr::null_mut();
        unsafe {
            let status = sys::napi_create_reference(raw_env, callback.raw(), 1, &mut fn_ref);
            if status != sys::Status::napi_ok {
                return Err(Error::new(Status::GenericFailure, "Failed to create function reference"));
            }
        }

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

        // Create a closure context that holds the raw pointers
        // This is similar to better-sqlite3's CustomFunction class
        struct FunctionContext {
            raw_env: sys::napi_env,
            fn_ref: sys::napi_ref,
            safe_ints: bool,
        }

        // SAFETY: These pointers are valid for the lifetime of the database connection
        // because SQLite runs on the same thread as Node.js and we hold a reference
        unsafe impl Send for FunctionContext {}
        unsafe impl Sync for FunctionContext {}

        let ctx = Arc::new(FunctionContext {
            raw_env,
            fn_ref,
            safe_ints: use_safe_ints,
        });

        let ctx_clone = ctx.clone();

        // Register the function with SQLite
        let conn = self.inner.connection().write();
        conn.create_scalar_function(name.as_str(), argc, flags, move |sqlite_ctx| {
            let ctx = &ctx_clone;

            unsafe {
                // Get the function from the reference
                let mut js_fn: sys::napi_value = std::ptr::null_mut();
                let status = sys::napi_get_reference_value(ctx.raw_env, ctx.fn_ref, &mut js_fn);
                if status != sys::Status::napi_ok || js_fn.is_null() {
                    return Err(rusqlite::Error::UserFunctionError(Box::new(
                        std::io::Error::new(std::io::ErrorKind::Other, "Failed to get function reference"),
                    )));
                }

                // Get undefined for 'this' value
                let mut undefined: sys::napi_value = std::ptr::null_mut();
                sys::napi_get_undefined(ctx.raw_env, &mut undefined);

                // Convert SQLite arguments to NAPI values
                let arg_count = sqlite_ctx.len();
                let mut napi_args: Vec<sys::napi_value> = Vec::with_capacity(arg_count);

                for i in 0..arg_count {
                    let napi_val = sqlite_value_to_napi(ctx.raw_env, sqlite_ctx.get_raw(i), ctx.safe_ints)?;
                    napi_args.push(napi_val);
                }

                // Call the JavaScript function directly - like better-sqlite3's fn->Call()
                let mut result: sys::napi_value = std::ptr::null_mut();
                let status = sys::napi_call_function(
                    ctx.raw_env,
                    undefined,
                    js_fn,
                    arg_count,
                    if arg_count > 0 { napi_args.as_ptr() } else { std::ptr::null() },
                    &mut result,
                );

                if status != sys::Status::napi_ok {
                    // Check if there was a JS exception
                    let mut is_pending = false;
                    sys::napi_is_exception_pending(ctx.raw_env, &mut is_pending);
                    if is_pending {
                        // Clear the exception and return error
                        let mut exception: sys::napi_value = std::ptr::null_mut();
                        sys::napi_get_and_clear_last_exception(ctx.raw_env, &mut exception);
                        return Err(rusqlite::Error::UserFunctionError(Box::new(
                            std::io::Error::new(std::io::ErrorKind::Other, "JavaScript function threw an error"),
                        )));
                    }
                    return Err(rusqlite::Error::UserFunctionError(Box::new(
                        std::io::Error::new(std::io::ErrorKind::Other, "Failed to call JavaScript function"),
                    )));
                }

                // Convert the result back to SQLite
                napi_value_to_sqlite_result(ctx.raw_env, result)
            }
        })
        .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;

        Ok(())
    }

    /// Register a user-defined aggregate function.
    /// @param start - Initial accumulator value (or function that returns initial value)
    /// @param step - Function called for each row: step(accumulator, ...values)
    /// @param inverse - Optional function for window functions: inverse(accumulator, ...values)
    /// @param result - Optional function to transform final result: result(accumulator)
    /// @param name - SQL function name
    /// @param argc - Number of arguments (-1 for varargs)
    /// @param safe_ints - Whether to use BigInt for integers (0=false, 1=true, 2=inherit)
    /// @param deterministic - Whether function is deterministic
    /// @param direct_only - Whether function can only be called directly
    #[napi(js_name = "_registerAggregate")]
    pub fn register_aggregate(
        &self,
        env: Env,
        start: JsUnknown,
        #[napi(ts_arg_type = "(acc: any, ...args: any[]) => any")] step: JsFunction,
        inverse: Option<JsFunction>,
        result_fn: Option<JsFunction>,
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

        let raw_env = env.raw();

        // Create references to prevent GC
        let mut step_ref: sys::napi_ref = std::ptr::null_mut();
        let mut inverse_ref: sys::napi_ref = std::ptr::null_mut();
        let mut result_ref: sys::napi_ref = std::ptr::null_mut();
        let mut start_ref: sys::napi_ref = std::ptr::null_mut();
        let mut start_is_function = false;
        let mut start_value: Option<rusqlite::types::Value> = None;

        unsafe {
            // Create reference for step function
            let status = sys::napi_create_reference(raw_env, step.raw(), 1, &mut step_ref);
            if status != sys::Status::napi_ok {
                return Err(Error::new(Status::GenericFailure, "Failed to create step function reference"));
            }

            // Create reference for inverse function if provided
            if let Some(ref inv) = inverse {
                let status = sys::napi_create_reference(raw_env, inv.raw(), 1, &mut inverse_ref);
                if status != sys::Status::napi_ok {
                    return Err(Error::new(Status::GenericFailure, "Failed to create inverse function reference"));
                }
            }

            // Create reference for result function if provided
            if let Some(ref res) = result_fn {
                let status = sys::napi_create_reference(raw_env, res.raw(), 1, &mut result_ref);
                if status != sys::Status::napi_ok {
                    return Err(Error::new(Status::GenericFailure, "Failed to create result function reference"));
                }
            }

            // Check if start is a function or a primitive value
            let mut value_type: sys::napi_valuetype = sys::ValueType::napi_undefined;
            sys::napi_typeof(raw_env, start.raw(), &mut value_type);
            start_is_function = value_type == sys::ValueType::napi_function;

            if start_is_function || value_type == sys::ValueType::napi_object {
                // Functions and objects can have references
                let status = sys::napi_create_reference(raw_env, start.raw(), 1, &mut start_ref);
                if status != sys::Status::napi_ok {
                    return Err(Error::new(Status::GenericFailure, "Failed to create start reference"));
                }
            } else {
                // Primitives (null, undefined, boolean, number, string, bigint) - convert to rusqlite Value
                start_value = Some(napi_value_to_rusqlite_value(raw_env, start.raw())
                    .map_err(|e| Error::new(Status::GenericFailure, format!("Failed to convert start value: {}", e)))?);
            }
        }

        // Determine safe_ints setting
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

        // Context for aggregate - holds all JS function refs
        struct AggregateContext {
            raw_env: sys::napi_env,
            start_ref: sys::napi_ref,       // null if start is primitive
            start_is_function: bool,
            start_primitive: Option<rusqlite::types::Value>,  // Some if start is primitive
            step_ref: sys::napi_ref,
            inverse_ref: sys::napi_ref,  // may be null
            result_ref: sys::napi_ref,   // may be null
            safe_ints: bool,
        }

        unsafe impl Send for AggregateContext {}
        unsafe impl Sync for AggregateContext {}

        // The accumulator is stored as a rusqlite Value (converted to/from JS as needed)
        struct Accumulator {
            value: rusqlite::types::Value,
        }

        struct JsAggregate {
            ctx: Arc<AggregateContext>,
        }

        impl rusqlite::functions::Aggregate<Accumulator, rusqlite::types::Value> for JsAggregate {
            fn init(&self, _: &mut rusqlite::functions::Context<'_>) -> rusqlite::Result<Accumulator> {
                unsafe {
                    let env = self.ctx.raw_env;

                    let init_val = if let Some(ref primitive) = self.ctx.start_primitive {
                        // Start is a primitive value - use it directly
                        primitive.clone()
                    } else {
                        // Get start value from reference (function or object)
                        let mut start_val: sys::napi_value = std::ptr::null_mut();
                        sys::napi_get_reference_value(env, self.ctx.start_ref, &mut start_val);

                        if self.ctx.start_is_function {
                            // Call the start function
                            let mut undefined: sys::napi_value = std::ptr::null_mut();
                            sys::napi_get_undefined(env, &mut undefined);

                            let mut result: sys::napi_value = std::ptr::null_mut();
                            let status = sys::napi_call_function(env, undefined, start_val, 0, std::ptr::null(), &mut result);
                            if status != sys::Status::napi_ok {
                                return Err(rusqlite::Error::UserFunctionError(Box::new(
                                    std::io::Error::new(std::io::ErrorKind::Other, "Failed to call start function"),
                                )));
                            }
                            // Convert the JS result to rusqlite Value
                            napi_value_to_rusqlite_value(env, result)?
                        } else {
                            // Object start value - convert to rusqlite Value
                            napi_value_to_rusqlite_value(env, start_val)?
                        }
                    };

                    Ok(Accumulator { value: init_val })
                }
            }

            fn step(&self, sqlite_ctx: &mut rusqlite::functions::Context<'_>, acc: &mut Accumulator) -> rusqlite::Result<()> {
                unsafe {
                    let env = self.ctx.raw_env;

                    // Get the step function
                    let mut step_fn: sys::napi_value = std::ptr::null_mut();
                    sys::napi_get_reference_value(env, self.ctx.step_ref, &mut step_fn);

                    // Convert current accumulator to NAPI value
                    let acc_napi = rusqlite_value_to_napi(env, &acc.value)?;

                    // Build args: (accumulator, ...sqlite_values)
                    let arg_count = sqlite_ctx.len() + 1;
                    let mut napi_args: Vec<sys::napi_value> = Vec::with_capacity(arg_count);
                    napi_args.push(acc_napi);

                    for i in 0..sqlite_ctx.len() {
                        let napi_val = sqlite_value_to_napi(env, sqlite_ctx.get_raw(i), self.ctx.safe_ints)?;
                        napi_args.push(napi_val);
                    }

                    // Call step(acc, ...values)
                    let mut undefined: sys::napi_value = std::ptr::null_mut();
                    sys::napi_get_undefined(env, &mut undefined);

                    let mut result: sys::napi_value = std::ptr::null_mut();
                    let status = sys::napi_call_function(
                        env,
                        undefined,
                        step_fn,
                        arg_count,
                        napi_args.as_ptr(),
                        &mut result,
                    );

                    if status != sys::Status::napi_ok {
                        let mut is_pending = false;
                        sys::napi_is_exception_pending(env, &mut is_pending);
                        if is_pending {
                            let mut exception: sys::napi_value = std::ptr::null_mut();
                            sys::napi_get_and_clear_last_exception(env, &mut exception);
                        }
                        return Err(rusqlite::Error::UserFunctionError(Box::new(
                            std::io::Error::new(std::io::ErrorKind::Other, "Failed to call step function"),
                        )));
                    }

                    // Update accumulator with new value (if step returned something)
                    let mut value_type: sys::napi_valuetype = sys::ValueType::napi_undefined;
                    sys::napi_typeof(env, result, &mut value_type);

                    if value_type != sys::ValueType::napi_undefined {
                        // Convert result back to rusqlite Value and update accumulator
                        let new_value = napi_value_to_rusqlite_value(env, result)?;
                        acc.value = new_value;
                    }

                    Ok(())
                }
            }

            fn finalize(&self, _: &mut rusqlite::functions::Context<'_>, acc: Option<Accumulator>) -> rusqlite::Result<rusqlite::types::Value> {
                unsafe {
                    let env = self.ctx.raw_env;

                    match acc {
                        Some(a) => {
                            // Get the final accumulator value
                            let final_acc = a.value;

                            // If we have a result function, call it
                            if !self.ctx.result_ref.is_null() {
                                // Convert accumulator to NAPI value
                                let acc_napi = rusqlite_value_to_napi(env, &final_acc)?;

                                let mut result_fn: sys::napi_value = std::ptr::null_mut();
                                sys::napi_get_reference_value(env, self.ctx.result_ref, &mut result_fn);

                                let mut undefined: sys::napi_value = std::ptr::null_mut();
                                sys::napi_get_undefined(env, &mut undefined);

                                let mut result: sys::napi_value = std::ptr::null_mut();
                                let status = sys::napi_call_function(
                                    env,
                                    undefined,
                                    result_fn,
                                    1,
                                    &acc_napi,
                                    &mut result,
                                );

                                if status != sys::Status::napi_ok {
                                    return Err(rusqlite::Error::UserFunctionError(Box::new(
                                        std::io::Error::new(std::io::ErrorKind::Other, "Failed to call result function"),
                                    )));
                                }
                                // Convert result back to rusqlite Value
                                napi_value_to_rusqlite_value(env, result)
                            } else {
                                // Return accumulator directly
                                Ok(final_acc)
                            }
                        }
                        None => {
                            // No rows - return null
                            Ok(rusqlite::types::Value::Null)
                        }
                    }
                }
            }
        }

        // Implement WindowAggregate for window functions (when inverse is provided)
        impl rusqlite::functions::WindowAggregate<Accumulator, rusqlite::types::Value> for JsAggregate {
            fn value(&self, acc: Option<&mut Accumulator>) -> rusqlite::Result<rusqlite::types::Value> {
                unsafe {
                    let env = self.ctx.raw_env;

                    match acc {
                        Some(a) => {
                            let current_acc = &a.value;

                            // If we have a result function, call it
                            if !self.ctx.result_ref.is_null() {
                                let acc_napi = rusqlite_value_to_napi(env, current_acc)?;

                                let mut result_fn: sys::napi_value = std::ptr::null_mut();
                                sys::napi_get_reference_value(env, self.ctx.result_ref, &mut result_fn);

                                let mut undefined: sys::napi_value = std::ptr::null_mut();
                                sys::napi_get_undefined(env, &mut undefined);

                                let mut result: sys::napi_value = std::ptr::null_mut();
                                let status = sys::napi_call_function(
                                    env,
                                    undefined,
                                    result_fn,
                                    1,
                                    &acc_napi,
                                    &mut result,
                                );

                                if status != sys::Status::napi_ok {
                                    return Err(rusqlite::Error::UserFunctionError(Box::new(
                                        std::io::Error::new(std::io::ErrorKind::Other, "Failed to call result function"),
                                    )));
                                }
                                napi_value_to_rusqlite_value(env, result)
                            } else {
                                Ok(current_acc.clone())
                            }
                        }
                        None => Ok(rusqlite::types::Value::Null),
                    }
                }
            }

            fn inverse(&self, sqlite_ctx: &mut rusqlite::functions::Context<'_>, acc: &mut Accumulator) -> rusqlite::Result<()> {
                unsafe {
                    let env = self.ctx.raw_env;

                    // Check if we have an inverse function
                    if self.ctx.inverse_ref.is_null() {
                        return Err(rusqlite::Error::UserFunctionError(Box::new(
                            std::io::Error::new(std::io::ErrorKind::Other, "No inverse function provided"),
                        )));
                    }

                    // Get the inverse function
                    let mut inverse_fn: sys::napi_value = std::ptr::null_mut();
                    sys::napi_get_reference_value(env, self.ctx.inverse_ref, &mut inverse_fn);

                    // Convert current accumulator to NAPI value
                    let acc_napi = rusqlite_value_to_napi(env, &acc.value)?;

                    // Build args: (accumulator, ...sqlite_values)
                    let arg_count = sqlite_ctx.len() + 1;
                    let mut napi_args: Vec<sys::napi_value> = Vec::with_capacity(arg_count);
                    napi_args.push(acc_napi);

                    for i in 0..sqlite_ctx.len() {
                        let napi_val = sqlite_value_to_napi(env, sqlite_ctx.get_raw(i), self.ctx.safe_ints)?;
                        napi_args.push(napi_val);
                    }

                    // Call inverse(acc, ...values)
                    let mut undefined: sys::napi_value = std::ptr::null_mut();
                    sys::napi_get_undefined(env, &mut undefined);

                    let mut result: sys::napi_value = std::ptr::null_mut();
                    let status = sys::napi_call_function(
                        env,
                        undefined,
                        inverse_fn,
                        arg_count,
                        napi_args.as_ptr(),
                        &mut result,
                    );

                    if status != sys::Status::napi_ok {
                        let mut is_pending = false;
                        sys::napi_is_exception_pending(env, &mut is_pending);
                        if is_pending {
                            let mut exception: sys::napi_value = std::ptr::null_mut();
                            sys::napi_get_and_clear_last_exception(env, &mut exception);
                        }
                        return Err(rusqlite::Error::UserFunctionError(Box::new(
                            std::io::Error::new(std::io::ErrorKind::Other, "Failed to call inverse function"),
                        )));
                    }

                    // Update accumulator with new value (if inverse returned something)
                    let mut value_type: sys::napi_valuetype = sys::ValueType::napi_undefined;
                    sys::napi_typeof(env, result, &mut value_type);

                    if value_type != sys::ValueType::napi_undefined {
                        // Convert result back to rusqlite Value and update accumulator
                        let new_value = napi_value_to_rusqlite_value(env, result)?;
                        acc.value = new_value;
                    }

                    Ok(())
                }
            }
        }

        let ctx = Arc::new(AggregateContext {
            raw_env,
            start_ref,
            start_is_function,
            start_primitive: start_value,
            step_ref,
            inverse_ref,
            result_ref,
            safe_ints: use_safe_ints,
        });

        let aggregate = JsAggregate { ctx };

        // Register as window function if inverse is provided, otherwise as regular aggregate
        let conn = self.inner.connection().write();
        if inverse.is_some() {
            conn.create_window_function(name.as_str(), argc, flags, aggregate)
                .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;
        } else {
            conn.create_aggregate_function(name.as_str(), argc, flags, aggregate)
                .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;
        }

        Ok(())
    }

    /// Register a virtual table.
    /// This creates an eponymous virtual table that reads from a JavaScript generator.
    #[napi(js_name = "_registerVirtualTable")]
    pub fn register_virtual_table(
        &self,
        env: Env,
        name: String,
        columns: Vec<String>,
        parameters: Vec<String>,
        #[napi(ts_arg_type = "(...args: any[]) => Generator")] rows_generator: JsFunction,
    ) -> Result<()> {
        use rusqlite::vtab::{
            eponymous_only_module, Context, Filters, IndexInfo, VTab, VTabConfig, VTabConnection, VTabCursor,
        };
        use std::ffi::c_int;

        if !self.is_open {
            return Err(Error::new(
                Status::GenericFailure,
                "The database connection is not open",
            ));
        }

        let raw_env = env.raw();

        // Create a reference to the generator function
        let mut gen_ref: sys::napi_ref = std::ptr::null_mut();
        unsafe {
            let status = sys::napi_create_reference(raw_env, rows_generator.raw(), 1, &mut gen_ref);
            if status != sys::Status::napi_ok {
                return Err(Error::new(Status::GenericFailure, "Failed to create generator reference"));
            }
        }

        // Build the CREATE TABLE schema
        let mut col_defs: Vec<String> = columns.iter().map(|c| format!("{} ANY", c)).collect();
        // Add hidden parameters
        for param in &parameters {
            col_defs.push(format!("{} HIDDEN", param));
        }
        let schema = format!("CREATE TABLE x({})", col_defs.join(", "));
        let column_count = columns.len();
        let param_count = parameters.len();

        // Context for the virtual table
        struct JsVTabAux {
            raw_env: sys::napi_env,
            gen_ref: sys::napi_ref,
            schema: String,
            column_count: usize,
            param_count: usize,
        }

        unsafe impl Send for JsVTabAux {}
        unsafe impl Sync for JsVTabAux {}

        let aux = Box::new(JsVTabAux {
            raw_env,
            gen_ref,
            schema: schema.clone(),
            column_count,
            param_count,
        });

        // Virtual table structure
        #[repr(C)]
        struct JsVTab {
            base: rusqlite::ffi::sqlite3_vtab,
        }

        // Cursor structure - holds the current state
        struct JsVTabCursor<'vtab> {
            base: rusqlite::ffi::sqlite3_vtab_cursor,
            rows: Vec<Vec<rusqlite::types::Value>>,
            row_index: usize,
            phantom: std::marker::PhantomData<&'vtab JsVTab>,
        }

        unsafe impl<'vtab> VTab<'vtab> for JsVTab {
            type Aux = JsVTabAux;
            type Cursor = JsVTabCursor<'vtab>;

            fn connect(
                db: &mut VTabConnection,
                aux: Option<&JsVTabAux>,
                _args: &[&[u8]],
            ) -> rusqlite::Result<(String, Self)> {
                let vtab = Self {
                    base: rusqlite::ffi::sqlite3_vtab::default(),
                };
                db.config(VTabConfig::Innocuous)?;
                let schema = aux.map(|a| a.schema.clone()).unwrap_or_default();
                Ok((schema, vtab))
            }

            fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
                // Simple implementation - just scan all rows
                info.set_estimated_cost(1000000.0);
                info.set_estimated_rows(1000);
                Ok(())
            }

            fn open(&'vtab mut self) -> rusqlite::Result<Self::Cursor> {
                Ok(JsVTabCursor {
                    base: rusqlite::ffi::sqlite3_vtab_cursor::default(),
                    rows: Vec::new(),
                    row_index: 0,
                    phantom: std::marker::PhantomData,
                })
            }
        }

        unsafe impl<'vtab> VTabCursor for JsVTabCursor<'vtab> {
            fn filter(
                &mut self,
                _idx_num: c_int,
                _idx_str: Option<&str>,
                _args: &rusqlite::vtab::Filters<'_>,
            ) -> rusqlite::Result<()> {
                // For now, just use hardcoded test data
                // TODO: Actually call the JavaScript generator
                self.rows = vec![
                    vec![rusqlite::types::Value::Integer(1)],
                    vec![rusqlite::types::Value::Integer(2)],
                    vec![rusqlite::types::Value::Integer(3)],
                ];
                self.row_index = 0;
                Ok(())
            }

            fn next(&mut self) -> rusqlite::Result<()> {
                self.row_index += 1;
                Ok(())
            }

            fn eof(&self) -> bool {
                self.row_index >= self.rows.len()
            }

            fn column(&self, ctx: &mut Context, col: c_int) -> rusqlite::Result<()> {
                if self.row_index < self.rows.len() {
                    let row = &self.rows[self.row_index];
                    if (col as usize) < row.len() {
                        ctx.set_result(&row[col as usize])?;
                    }
                }
                Ok(())
            }

            fn rowid(&self) -> rusqlite::Result<i64> {
                Ok(self.row_index as i64)
            }
        }

        // Register the module
        let conn = self.inner.connection().write();

        // Create the module and register it using the name string slice
        conn.create_module(name.as_str(), eponymous_only_module::<JsVTab>(), Some(*aux))
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
    /// Noria-specific options
    pub noria: Option<NoriaOptions>,
}

/// Noria-specific configuration options
#[napi(object)]
#[derive(Default)]
pub struct NoriaOptions {
    /// Disable Noria acceleration (passthrough to raw SQLite).
    /// Useful for benchmarking raw SQLite performance.
    #[napi(js_name = "accelerationDisabled")]
    pub acceleration_disabled: Option<bool>,
}

/// Progress information for database backup
#[napi(object)]
pub struct BackupProgress {
    #[napi(js_name = "totalPages")]
    pub total_pages: i32,
    #[napi(js_name = "remainingPages")]
    pub remaining_pages: i32,
}

/// Cache statistics from the Noria dataflow engine
#[napi(object)]
pub struct CacheStats {
    #[napi(js_name = "nodeCount")]
    pub node_count: i64,
    #[napi(js_name = "materializedNodes")]
    pub materialized_nodes: i64,
    #[napi(js_name = "totalRows")]
    pub total_rows: i64,
    #[napi(js_name = "viewCount")]
    pub view_count: i64,
    #[napi(js_name = "cacheHits")]
    pub cache_hits: i64,
    #[napi(js_name = "cacheMisses")]
    pub cache_misses: i64,
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
    /// Column names for cached result conversion
    column_names: Vec<String>,
    /// Pre-allocated CStrings for fast NAPI property setting
    column_name_cstrs: Vec<std::ffi::CString>,
    /// Column table names for expand mode (None if no origin table)
    column_tables: Vec<Option<String>>,
    /// Whether this statement has a Noria view (is accelerated)
    is_cached: bool,
    /// Number of parameters expected by this statement
    param_count: usize,
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
    ///
    /// If the query is accelerated by Noria, tries the cache first with upquery fallback.
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
        let expand_mode = self.expand_mode;
        let column_tables = &self.column_tables;

        // Try cached path first if this query is accelerated
        if self.is_cached {
            let stmt = self.inner.lock();
            if let Ok(Some(row)) = stmt.query_row_cached_or_upquery(&param_refs) {
                // Convert cached DataType row to JSON
                let json_value = if self.pluck_mode {
                    // Return just the first column value
                    row.first().map(|dt| datatype_to_json(dt, safe_ints))
                        .unwrap_or(serde_json::Value::Null)
                } else if self.raw_mode {
                    // Return as array
                    cached_row_to_json_array(&row, safe_ints)
                } else if expand_mode {
                    // Return as nested objects grouped by table
                    cached_row_to_json_expanded(&row, &self.column_names, column_tables, safe_ints)
                } else {
                    // Return as object
                    cached_row_to_json_object(&row, &self.column_names, safe_ints)
                };
                return Ok(Some(json_value));
            }
            // If cached path returned None (no rows), return None
            // If it returned an error, fall through to SQLite
        }

        // Capture column_tables for closure
        let column_tables_clone = column_tables.clone();

        // Fall back to SQLite path
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
            } else if expand_mode {
                // Return as nested objects grouped by table
                row_to_json_expanded(row, &column_tables_clone, safe_ints)
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
    ///
    /// If the query is accelerated by Noria, tries the cache first with upquery fallback.
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
        let expand_mode = self.expand_mode;
        let column_tables = &self.column_tables;

        // Try cached path first if this query is accelerated
        if self.is_cached {
            let stmt = self.inner.lock();
            // Try cache lookup; returns Option<Vec<Vec<DataType>>>
            if let Ok(Some(rows)) = stmt.query_map_cached(&param_refs) {
                // Convert all cached rows to JSON
                let json_results: Vec<serde_json::Value> = rows.iter().map(|row| {
                    if self.pluck_mode {
                        row.first().map(|dt| datatype_to_json(dt, safe_ints))
                            .unwrap_or(serde_json::Value::Null)
                    } else if self.raw_mode {
                        cached_row_to_json_array(row, safe_ints)
                    } else if expand_mode {
                        cached_row_to_json_expanded(row, &self.column_names, column_tables, safe_ints)
                    } else {
                        cached_row_to_json_object(row, &self.column_names, safe_ints)
                    }
                }).collect();
                return Ok(json_results);
            }
            // Cache miss - fall through to SQLite (which will populate cache via upquery)
        }

        // Capture column_tables for closure
        let column_tables_clone = column_tables.clone();

        // Fall back to SQLite path
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
                } else if expand_mode {
                    row_to_json_expanded(row, &column_tables_clone, safe_ints)
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

    /// Optimized version of get() that creates JS objects directly without JSON serialization.
    /// This provides better performance for high-throughput scenarios.
    #[napi(js_name = "_getFast")]
    pub fn get_fast(&self, env: Env, params: Vec<serde_json::Value>) -> Result<JsUnknown> {
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
        let raw_env = env.raw();

        // Execute the query and create JS object directly
        let stmt = self.inner.lock();
        let result = stmt.query_row(&param_refs, |row| {
            unsafe {
                if self.pluck_mode {
                    // Return just the first column value
                    sqlite_value_to_napi(raw_env, row.get_ref(0)?, safe_ints)
                } else if self.raw_mode {
                    // Return as array
                    let count = row.as_ref().column_count();
                    let mut arr: sys::napi_value = std::ptr::null_mut();
                    sys::napi_create_array_with_length(raw_env, count, &mut arr);
                    for i in 0..count {
                        let val = sqlite_value_to_napi(raw_env, row.get_ref(i)?, safe_ints)?;
                        sys::napi_set_element(raw_env, arr, i as u32, val);
                    }
                    Ok(arr)
                } else {
                    // Return as object
                    let count = row.as_ref().column_count();
                    let mut obj: sys::napi_value = std::ptr::null_mut();
                    sys::napi_create_object(raw_env, &mut obj);
                    for i in 0..count {
                        let name = row.as_ref().column_name(i).unwrap_or("?");
                        let val = sqlite_value_to_napi(raw_env, row.get_ref(i)?, safe_ints)?;
                        let name_cstr = std::ffi::CString::new(name).unwrap();
                        sys::napi_set_named_property(raw_env, obj, name_cstr.as_ptr(), val);
                    }
                    Ok(obj)
                }
            }
        });

        match result {
            Ok(napi_val) => {
                Ok(unsafe { JsUnknown::from_napi_value(raw_env, napi_val)? })
            }
            Err(noria_sqlite::Error::Sqlite(rusqlite::Error::QueryReturnedNoRows)) => {
                env.get_undefined().map(|u| u.into_unknown())
            }
            Err(e) => Err(Error::new(
                Status::GenericFailure,
                format!("SQLITE_ERROR: {}", e),
            )),
        }
    }

    /// Optimized version of all() that creates JS objects directly without JSON serialization.
    #[napi(js_name = "_allFast")]
    pub fn all_fast(&self, env: Env, params: Vec<serde_json::Value>) -> Result<JsUnknown> {
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
        let raw_env = env.raw();

        // Execute the query directly on the connection (bypass NoriaStatement for speed)
        let conn = self.db.connection().read();
        let mut sqlite_stmt = conn.prepare_cached(&self.sql)
            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;

        // Get column count (use pre-allocated CStrings from self)
        let col_count = sqlite_stmt.column_count();
        let col_name_cstrs = &self.column_name_cstrs;

        // Create result array (pre-allocate with hint if we have one)
        let result_arr = unsafe {
            let mut arr: sys::napi_value = std::ptr::null_mut();
            // Use regular array - it will grow as needed
            sys::napi_create_array(raw_env, &mut arr);
            arr
        };

        let mut row_idx = 0u32;
        let mut rows = sqlite_stmt.query(rusqlite::params_from_iter(&param_refs))
            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;

        while let Some(row) = rows.next()
            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?
        {
            let row_val = unsafe {
                if self.pluck_mode {
                    let ref_val = row.get_ref(0)
                        .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;
                    sqlite_value_to_napi(raw_env, ref_val, safe_ints)
                        .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?
                } else if self.raw_mode {
                    let mut arr: sys::napi_value = std::ptr::null_mut();
                    sys::napi_create_array_with_length(raw_env, col_count, &mut arr);
                    for i in 0..col_count {
                        let ref_val = row.get_ref(i)
                            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;
                        let val = sqlite_value_to_napi(raw_env, ref_val, safe_ints)
                            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;
                        sys::napi_set_element(raw_env, arr, i as u32, val);
                    }
                    arr
                } else {
                    let mut obj: sys::napi_value = std::ptr::null_mut();
                    sys::napi_create_object(raw_env, &mut obj);
                    for i in 0..col_count {
                        let ref_val = row.get_ref(i)
                            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;
                        let val = sqlite_value_to_napi(raw_env, ref_val, safe_ints)
                            .map_err(|e| Error::new(Status::GenericFailure, format!("SQLITE_ERROR: {}", e)))?;
                        // Use pre-allocated CString - no allocation per row
                        sys::napi_set_named_property(raw_env, obj, col_name_cstrs[i].as_ptr(), val);
                    }
                    obj
                }
            };

            unsafe {
                sys::napi_set_element(raw_env, result_arr, row_idx, row_val);
            }
            row_idx += 1;
        }

        Ok(unsafe { JsUnknown::from_napi_value(raw_env, result_arr)? })
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

        // Validate parameter count
        if flat_params.len() != self.param_count {
            return Err(Error::new(
                Status::InvalidArg,
                format!(
                    "Expected {} parameter(s), got {}",
                    self.param_count,
                    flat_params.len()
                ),
            ));
        }

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

/// Convert a noria::DataType to serde_json::Value.
/// This allows returning cached results directly without going through rusqlite.
fn datatype_to_json(dt: &DataType, safe_ints: bool) -> serde_json::Value {
    match dt {
        DataType::None => serde_json::Value::Null,
        DataType::Int(i) => {
            if safe_ints {
                serde_json::json!({ "$bigint": i.to_string() })
            } else {
                serde_json::json!(*i as f64)
            }
        }
        DataType::BigInt(i) => {
            if safe_ints {
                serde_json::json!({ "$bigint": i.to_string() })
            } else {
                serde_json::json!(*i as f64)
            }
        }
        DataType::UnsignedInt(i) => {
            if safe_ints {
                serde_json::json!({ "$bigint": i.to_string() })
            } else {
                serde_json::json!(*i as f64)
            }
        }
        DataType::UnsignedBigInt(i) => {
            if safe_ints {
                serde_json::json!({ "$bigint": i.to_string() })
            } else {
                serde_json::json!(*i as f64)
            }
        }
        DataType::Real(int_part, frac_part) => {
            let f = *int_part as f64 + (*frac_part as f64 / 1_000_000_000.0);
            serde_json::json!(f)
        }
        DataType::Text(_) | DataType::TinyText(_) => {
            let s: &str = dt.into();
            serde_json::Value::String(s.to_string())
        }
        DataType::Timestamp(ts) => {
            serde_json::Value::String(ts.to_string())
        }
    }
}

/// Convert a cached row (Vec<DataType>) to JSON object using column names.
fn cached_row_to_json_object(
    row: &[DataType],
    column_names: &[String],
    safe_ints: bool,
) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (i, dt) in row.iter().enumerate() {
        let name = column_names.get(i)
            .map(|s| s.clone())
            .unwrap_or_else(|| format!("col{}", i));
        obj.insert(name, datatype_to_json(dt, safe_ints));
    }
    serde_json::Value::Object(obj)
}

/// Convert a cached row to JSON array (for raw mode).
fn cached_row_to_json_array(row: &[DataType], safe_ints: bool) -> serde_json::Value {
    let arr: Vec<serde_json::Value> = row.iter().map(|dt| datatype_to_json(dt, safe_ints)).collect();
    serde_json::Value::Array(arr)
}

/// Convert a cached row to JSON with nested objects grouped by table (for expand mode).
fn cached_row_to_json_expanded(
    row: &[DataType],
    column_names: &[String],
    column_tables: &[Option<String>],
    safe_ints: bool,
) -> serde_json::Value {
    let mut tables: std::collections::HashMap<String, serde_json::Map<String, serde_json::Value>> =
        std::collections::HashMap::new();

    for (i, dt) in row.iter().enumerate() {
        let col_name = column_names.get(i)
            .map(|s| s.clone())
            .unwrap_or_else(|| format!("col{}", i));

        let table_name = column_tables.get(i)
            .and_then(|t| t.clone())
            .unwrap_or_else(|| "$".to_string()); // Use "$" for columns without table origin

        let value = datatype_to_json(dt, safe_ints);

        tables.entry(table_name)
            .or_insert_with(serde_json::Map::new)
            .insert(col_name, value);
    }

    // Convert HashMap to JSON object
    let mut obj = serde_json::Map::new();
    for (table, cols) in tables {
        obj.insert(table, serde_json::Value::Object(cols));
    }
    serde_json::Value::Object(obj)
}

/// Convert a rusqlite row to JSON with nested objects grouped by table (for expand mode).
fn row_to_json_expanded(
    row: &rusqlite::Row,
    column_tables: &[Option<String>],
    safe_ints: bool,
) -> rusqlite::Result<serde_json::Value> {
    let mut tables: std::collections::HashMap<String, serde_json::Map<String, serde_json::Value>> =
        std::collections::HashMap::new();

    for i in 0..row.as_ref().column_count() {
        let default_name = format!("col{}", i);
        let col_name = row.as_ref().column_name(i).unwrap_or(&default_name).to_string();

        let table_name = column_tables.get(i)
            .and_then(|t| t.clone())
            .unwrap_or_else(|| "$".to_string()); // Use "$" for columns without table origin

        let value = row_value_to_json(row, i, safe_ints)?;

        tables.entry(table_name)
            .or_insert_with(serde_json::Map::new)
            .insert(col_name, value);
    }

    // Convert HashMap to JSON object
    let mut obj = serde_json::Map::new();
    for (table, cols) in tables {
        obj.insert(table, serde_json::Value::Object(cols));
    }
    Ok(serde_json::Value::Object(obj))
}

/// Convert serde_json params to noria::DataType keys for cache lookup.
fn params_to_datatype_key(params: &[Box<dyn rusqlite::ToSql>]) -> Vec<DataType> {
    params.iter().map(|p| {
        use rusqlite::types::ToSqlOutput;
        match p.to_sql() {
            Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Integer(i))) => DataType::BigInt(i),
            Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Text(s))) => {
                DataType::from(std::str::from_utf8(s).unwrap_or(""))
            }
            Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Real(f))) => DataType::from(f),
            Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Null)) => DataType::None,
            Ok(ToSqlOutput::Owned(rusqlite::types::Value::Integer(i))) => DataType::BigInt(i),
            Ok(ToSqlOutput::Owned(rusqlite::types::Value::Text(s))) => DataType::from(s.as_str()),
            Ok(ToSqlOutput::Owned(rusqlite::types::Value::Real(f))) => DataType::from(f),
            Ok(ToSqlOutput::Owned(rusqlite::types::Value::Null)) => DataType::None,
            _ => DataType::None,
        }
    }).collect()
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

/// Convert a SQLite value to a raw NAPI value for direct function calls.
/// This is similar to how better-sqlite3's Data::GetArgumentsJS works.
#[inline(always)]
unsafe fn sqlite_value_to_napi(
    env: sys::napi_env,
    value: rusqlite::types::ValueRef,
    safe_ints: bool,
) -> rusqlite::Result<sys::napi_value> {
    use rusqlite::types::ValueRef;

    let mut result: sys::napi_value = std::ptr::null_mut();

    match value {
        ValueRef::Null => {
            sys::napi_get_null(env, &mut result);
        }
        ValueRef::Integer(i) => {
            if safe_ints {
                // Create BigInt for safe integers
                let mut lossless = true;
                sys::napi_create_bigint_int64(env, i, &mut result);
            } else {
                // Create number (as f64)
                sys::napi_create_double(env, i as f64, &mut result);
            }
        }
        ValueRef::Real(f) => {
            sys::napi_create_double(env, f, &mut result);
        }
        ValueRef::Text(s) => {
            let text = std::str::from_utf8(s).unwrap_or("");
            sys::napi_create_string_utf8(
                env,
                text.as_ptr() as *const i8,
                text.len(),
                &mut result,
            );
        }
        ValueRef::Blob(b) => {
            // Use napi_create_buffer_copy for efficient Buffer creation
            // This directly creates a Node.js Buffer with a copy of the data
            let mut _buffer_data: *mut std::ffi::c_void = std::ptr::null_mut();
            sys::napi_create_buffer_copy(
                env,
                b.len(),
                b.as_ptr() as *const std::ffi::c_void,
                &mut _buffer_data,
                &mut result,
            );
        }
    }

    Ok(result)
}

/// Convert a raw NAPI value back to SQLite result.
/// This is similar to how better-sqlite3's Data::ResultValueFromJS works.
unsafe fn napi_value_to_sqlite_result(
    env: sys::napi_env,
    value: sys::napi_value,
) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'static>> {
    use rusqlite::types::{ToSqlOutput, Value};

    if value.is_null() {
        return Ok(ToSqlOutput::Owned(Value::Null));
    }

    let mut value_type: sys::napi_valuetype = sys::ValueType::napi_undefined;
    sys::napi_typeof(env, value, &mut value_type);

    match value_type {
        sys::ValueType::napi_null | sys::ValueType::napi_undefined => {
            Ok(ToSqlOutput::Owned(Value::Null))
        }
        sys::ValueType::napi_boolean => {
            let mut bool_val = false;
            sys::napi_get_value_bool(env, value, &mut bool_val);
            Ok(ToSqlOutput::Owned(Value::Integer(if bool_val { 1 } else { 0 })))
        }
        sys::ValueType::napi_number => {
            let mut num_val: f64 = 0.0;
            sys::napi_get_value_double(env, value, &mut num_val);
            // Check if it's an integer
            if num_val.fract() == 0.0 && num_val >= i64::MIN as f64 && num_val <= i64::MAX as f64 {
                Ok(ToSqlOutput::Owned(Value::Integer(num_val as i64)))
            } else {
                Ok(ToSqlOutput::Owned(Value::Real(num_val)))
            }
        }
        sys::ValueType::napi_string => {
            // Get string length
            let mut str_len: usize = 0;
            sys::napi_get_value_string_utf8(env, value, std::ptr::null_mut(), 0, &mut str_len);

            // Allocate buffer and get string
            let mut buf = vec![0u8; str_len + 1];
            let mut copied: usize = 0;
            sys::napi_get_value_string_utf8(
                env,
                value,
                buf.as_mut_ptr() as *mut i8,
                str_len + 1,
                &mut copied,
            );
            buf.truncate(copied);
            let s = String::from_utf8_lossy(&buf).into_owned();
            Ok(ToSqlOutput::Owned(Value::Text(s)))
        }
        sys::ValueType::napi_bigint => {
            let mut int_val: i64 = 0;
            let mut lossless = true;
            sys::napi_get_value_bigint_int64(env, value, &mut int_val, &mut lossless);
            Ok(ToSqlOutput::Owned(Value::Integer(int_val)))
        }
        sys::ValueType::napi_object => {
            // Check if it's a Buffer
            let mut is_buffer = false;
            sys::napi_is_buffer(env, value, &mut is_buffer);
            if is_buffer {
                let mut data: *mut std::ffi::c_void = std::ptr::null_mut();
                let mut len: usize = 0;
                sys::napi_get_buffer_info(env, value, &mut data, &mut len);
                let bytes = std::slice::from_raw_parts(data as *const u8, len).to_vec();
                return Ok(ToSqlOutput::Owned(Value::Blob(bytes)));
            }
            // Other objects become null
            Ok(ToSqlOutput::Owned(Value::Null))
        }
        _ => Ok(ToSqlOutput::Owned(Value::Null)),
    }
}

/// Convert a raw NAPI value back to rusqlite Value (for aggregate finalize).
unsafe fn napi_value_to_rusqlite_value(
    env: sys::napi_env,
    value: sys::napi_value,
) -> rusqlite::Result<rusqlite::types::Value> {
    use rusqlite::types::Value;

    if value.is_null() {
        return Ok(Value::Null);
    }

    let mut value_type: sys::napi_valuetype = sys::ValueType::napi_undefined;
    sys::napi_typeof(env, value, &mut value_type);

    match value_type {
        sys::ValueType::napi_null | sys::ValueType::napi_undefined => {
            Ok(Value::Null)
        }
        sys::ValueType::napi_boolean => {
            let mut bool_val = false;
            sys::napi_get_value_bool(env, value, &mut bool_val);
            Ok(Value::Integer(if bool_val { 1 } else { 0 }))
        }
        sys::ValueType::napi_number => {
            let mut num_val: f64 = 0.0;
            sys::napi_get_value_double(env, value, &mut num_val);
            // Check if it's an integer
            if num_val.fract() == 0.0 && num_val >= i64::MIN as f64 && num_val <= i64::MAX as f64 {
                Ok(Value::Integer(num_val as i64))
            } else {
                Ok(Value::Real(num_val))
            }
        }
        sys::ValueType::napi_string => {
            // Get string length
            let mut str_len: usize = 0;
            sys::napi_get_value_string_utf8(env, value, std::ptr::null_mut(), 0, &mut str_len);

            // Allocate buffer and get string
            let mut buf = vec![0u8; str_len + 1];
            let mut copied: usize = 0;
            sys::napi_get_value_string_utf8(
                env,
                value,
                buf.as_mut_ptr() as *mut i8,
                str_len + 1,
                &mut copied,
            );
            buf.truncate(copied);
            let s = String::from_utf8_lossy(&buf).into_owned();
            Ok(Value::Text(s))
        }
        sys::ValueType::napi_bigint => {
            let mut int_val: i64 = 0;
            let mut lossless = true;
            sys::napi_get_value_bigint_int64(env, value, &mut int_val, &mut lossless);
            Ok(Value::Integer(int_val))
        }
        sys::ValueType::napi_object => {
            // Check if it's a Buffer
            let mut is_buffer = false;
            sys::napi_is_buffer(env, value, &mut is_buffer);
            if is_buffer {
                let mut data: *mut std::ffi::c_void = std::ptr::null_mut();
                let mut len: usize = 0;
                sys::napi_get_buffer_info(env, value, &mut data, &mut len);
                let bytes = std::slice::from_raw_parts(data as *const u8, len).to_vec();
                return Ok(Value::Blob(bytes));
            }
            // Check if it's an Array
            let mut is_array = false;
            sys::napi_is_array(env, value, &mut is_array);
            if is_array {
                // Serialize array to JSON string with special marker
                if let Ok(json) = napi_value_to_json(env, value) {
                    return Ok(Value::Text(format!("\x00__ARRAY__{}", json)));
                }
            }
            // Other objects - serialize to JSON string with special marker
            if let Ok(json) = napi_value_to_json(env, value) {
                return Ok(Value::Text(format!("\x00__OBJECT__{}", json)));
            }
            Ok(Value::Null)
        }
        _ => Ok(Value::Null),
    }
}

/// Convert a NAPI object/array to JSON string.
unsafe fn napi_value_to_json(
    env: sys::napi_env,
    value: sys::napi_value,
) -> std::result::Result<String, ()> {
    // Get the global object
    let mut global: sys::napi_value = std::ptr::null_mut();
    sys::napi_get_global(env, &mut global);

    // Get JSON.stringify
    let mut json_obj: sys::napi_value = std::ptr::null_mut();
    let json_str = "JSON\0";
    sys::napi_get_named_property(env, global, json_str.as_ptr() as *const i8, &mut json_obj);

    let mut stringify_fn: sys::napi_value = std::ptr::null_mut();
    let stringify_str = "stringify\0";
    sys::napi_get_named_property(env, json_obj, stringify_str.as_ptr() as *const i8, &mut stringify_fn);

    // Call JSON.stringify(value)
    let mut result: sys::napi_value = std::ptr::null_mut();
    let mut undefined: sys::napi_value = std::ptr::null_mut();
    sys::napi_get_undefined(env, &mut undefined);

    let status = sys::napi_call_function(env, json_obj, stringify_fn, 1, &value, &mut result);
    if status != sys::Status::napi_ok {
        return Err(());
    }

    // Get the string value
    let mut str_len: usize = 0;
    sys::napi_get_value_string_utf8(env, result, std::ptr::null_mut(), 0, &mut str_len);

    let mut buf = vec![0u8; str_len + 1];
    let mut copied: usize = 0;
    sys::napi_get_value_string_utf8(
        env,
        result,
        buf.as_mut_ptr() as *mut i8,
        str_len + 1,
        &mut copied,
    );
    buf.truncate(copied);
    String::from_utf8(buf).map_err(|_| ())
}

/// Parse a JSON string back to a NAPI value.
unsafe fn json_to_napi_value(
    env: sys::napi_env,
    json: &str,
) -> std::result::Result<sys::napi_value, ()> {
    // Get the global object
    let mut global: sys::napi_value = std::ptr::null_mut();
    sys::napi_get_global(env, &mut global);

    // Get JSON.parse
    let mut json_obj: sys::napi_value = std::ptr::null_mut();
    let json_str = "JSON\0";
    sys::napi_get_named_property(env, global, json_str.as_ptr() as *const i8, &mut json_obj);

    let mut parse_fn: sys::napi_value = std::ptr::null_mut();
    let parse_str = "parse\0";
    sys::napi_get_named_property(env, json_obj, parse_str.as_ptr() as *const i8, &mut parse_fn);

    // Create the JSON string as a NAPI value
    let mut json_val: sys::napi_value = std::ptr::null_mut();
    sys::napi_create_string_utf8(env, json.as_ptr() as *const i8, json.len(), &mut json_val);

    // Call JSON.parse(json)
    let mut result: sys::napi_value = std::ptr::null_mut();
    let status = sys::napi_call_function(env, json_obj, parse_fn, 1, &json_val, &mut result);
    if status != sys::Status::napi_ok {
        return Err(());
    }

    Ok(result)
}

/// Convert a rusqlite Value to a raw NAPI value.
unsafe fn rusqlite_value_to_napi(
    env: sys::napi_env,
    value: &rusqlite::types::Value,
) -> rusqlite::Result<sys::napi_value> {
    use rusqlite::types::Value;

    let mut result: sys::napi_value = std::ptr::null_mut();

    match value {
        Value::Null => {
            sys::napi_get_null(env, &mut result);
        }
        Value::Integer(i) => {
            sys::napi_create_double(env, *i as f64, &mut result);
        }
        Value::Real(f) => {
            sys::napi_create_double(env, *f, &mut result);
        }
        Value::Text(s) => {
            // Check for special markers for serialized objects/arrays
            if s.starts_with("\x00__OBJECT__") {
                let json = &s[11..];
                if let Ok(parsed) = json_to_napi_value(env, json) {
                    return Ok(parsed);
                }
            } else if s.starts_with("\x00__ARRAY__") {
                let json = &s[10..];
                if let Ok(parsed) = json_to_napi_value(env, json) {
                    return Ok(parsed);
                }
            }
            // Regular string
            sys::napi_create_string_utf8(
                env,
                s.as_ptr() as *const i8,
                s.len(),
                &mut result,
            );
        }
        Value::Blob(b) => {
            let mut buffer_data: *mut std::ffi::c_void = std::ptr::null_mut();
            sys::napi_create_buffer_copy(
                env,
                b.len(),
                b.as_ptr() as *const std::ffi::c_void,
                &mut buffer_data,
                &mut result,
            );
        }
    }

    Ok(result)
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
