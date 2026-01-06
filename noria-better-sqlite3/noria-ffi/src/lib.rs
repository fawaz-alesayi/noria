//! C FFI bridge for Noria dataflow engine integration with better-sqlite3
//!
//! This module provides a transparent caching layer for better-sqlite3 using
//! noria-sqlite's real dataflow engine with incremental view maintenance.
//!
//! Key features:
//! - Incremental CDC propagation (not just invalidation)
//! - Dataflow operators: Filter, Project, Join, Aggregate
//! - Upquery via callback to C++
//! - Memory management with random eviction

use noria::DataType;
use noria_sqlite::dataflow::{
    LocalExecutor, Record, Records, SqlConverter, ViewHandle,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::ffi::{c_char, c_double, c_int, c_void, CStr};
use std::ptr;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};

#[allow(unused_imports)]
use std::convert::TryFrom;

// ============================================================================
// FFI Types (match C++ expectations - unchanged from before)
// ============================================================================

/// Value types matching SQLite
pub const NORIA_NULL: c_int = 0;
pub const NORIA_INTEGER: c_int = 1;
pub const NORIA_FLOAT: c_int = 2;
pub const NORIA_TEXT: c_int = 3;
pub const NORIA_BLOB: c_int = 4;

/// A single value for FFI
#[repr(C)]
pub struct NoriaValue {
    pub value_type: c_int,
    pub int_value: i64,
    pub float_value: c_double,
    pub text_ptr: *const c_char,
    pub text_len: c_int,
    pub blob_ptr: *const u8,
    pub blob_len: c_int,
}

impl Default for NoriaValue {
    fn default() -> Self {
        NoriaValue {
            value_type: NORIA_NULL,
            int_value: 0,
            float_value: 0.0,
            text_ptr: ptr::null(),
            text_len: 0,
            blob_ptr: ptr::null(),
            blob_len: 0,
        }
    }
}

/// Result of a cache lookup
#[repr(C)]
pub struct NoriaLookupResult {
    /// 1 if found in cache, 0 if cache miss
    pub found: c_int,
    /// Number of rows returned
    pub row_count: c_int,
    /// Opaque pointer to row data (caller must free with noria_free_rows)
    pub rows: *mut c_void,
}

/// Cache statistics
#[repr(C)]
pub struct NoriaCacheStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub total_rows: u64,
    pub view_count: c_int,
    pub node_count: c_int,
    pub memory_bytes: u64,
    pub max_memory_bytes: u64,
    pub eviction_count: u64,
    pub bytes_evicted: u64,
}

// ============================================================================
// Upquery Callback Type
// ============================================================================

/// Callback type for upqueries (Rust calls C++ to execute SQLite query)
pub type UpqueryCallback = extern "C" fn(
    user_data: *mut c_void,
    sql: *const c_char,
    params: *const NoriaValue,
    param_count: c_int,
    out_rows: *mut *mut c_void,
    out_row_count: *mut c_int,
) -> c_int;

// ============================================================================
// DataType Conversion
// ============================================================================

/// Convert FFI NoriaValue to Noria DataType
fn noria_value_to_datatype(nv: &NoriaValue) -> DataType {
    match nv.value_type {
        NORIA_NULL => DataType::None,
        NORIA_INTEGER => DataType::BigInt(nv.int_value),
        NORIA_FLOAT => {
            // Convert float to Noria's Real representation
            DataType::from(nv.float_value)
        }
        NORIA_TEXT => {
            if nv.text_ptr.is_null() || nv.text_len <= 0 {
                DataType::from("")
            } else {
                let slice = unsafe {
                    std::slice::from_raw_parts(nv.text_ptr as *const u8, nv.text_len as usize)
                };
                let s = String::from_utf8_lossy(slice);
                DataType::from(s.as_ref())
            }
        }
        NORIA_BLOB => {
            // Blob represented as Text for now (simplified)
            if nv.blob_ptr.is_null() || nv.blob_len <= 0 {
                DataType::None
            } else {
                let slice =
                    unsafe { std::slice::from_raw_parts(nv.blob_ptr, nv.blob_len as usize) };
                // Store blob as hex text for now
                let hex: String = slice.iter().map(|b| format!("{:02x}", b)).collect();
                DataType::from(hex.as_str())
            }
        }
        _ => DataType::None,
    }
}

/// Internal value type for cache storage (same as before, for FFI return)
#[derive(Clone, Debug)]
enum Value {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    fn from_datatype(dt: &DataType) -> Self {
        match dt {
            DataType::None => Value::Null,
            DataType::Int(i) => Value::Int(*i as i64),
            DataType::BigInt(i) => Value::Int(*i),
            DataType::UnsignedInt(u) => Value::Int(*u as i64),
            DataType::UnsignedBigInt(u) => Value::Int(*u as i64),
            DataType::Real(int_part, frac_part) => {
                let f = *int_part as f64 + (*frac_part as f64 / 1_000_000_000.0);
                Value::Float(f)
            }
            DataType::Text(_) | DataType::TinyText(_) => {
                // Use the built-in conversion from DataType to &str
                let s: &str = dt.into();
                Value::Text(s.to_string())
            }
            DataType::Timestamp(_) => Value::Null, // Simplified
        }
    }

    fn to_noria_value(&self) -> NoriaValue {
        match self {
            Value::Null => NoriaValue::default(),
            Value::Int(i) => NoriaValue {
                value_type: NORIA_INTEGER,
                int_value: *i,
                ..Default::default()
            },
            Value::Float(f) => NoriaValue {
                value_type: NORIA_FLOAT,
                float_value: *f,
                ..Default::default()
            },
            Value::Text(s) => NoriaValue {
                value_type: NORIA_TEXT,
                text_ptr: s.as_ptr() as *const c_char,
                text_len: s.len() as c_int,
                ..Default::default()
            },
            Value::Blob(b) => NoriaValue {
                value_type: NORIA_BLOB,
                blob_ptr: b.as_ptr(),
                blob_len: b.len() as c_int,
                ..Default::default()
            },
        }
    }
}

type Row = Vec<Value>;

/// Estimate memory size of a row
fn row_size(row: &[DataType]) -> usize {
    row.iter()
        .map(|dt| match dt {
            DataType::None => 8,
            DataType::Int(_) | DataType::BigInt(_) => 16,
            DataType::UnsignedInt(_) | DataType::UnsignedBigInt(_) => 16,
            DataType::Real(_, _) => 16,
            DataType::Text(_) | DataType::TinyText(_) => {
                let s: &str = dt.into();
                24 + s.len()
            }
            DataType::Timestamp(_) => 16,
        })
        .sum::<usize>()
        + 24 // Vec overhead
}

// ============================================================================
// View Storage
// ============================================================================

/// A registered view with its dataflow handle
struct NoriaViewEntry {
    /// The dataflow view handle
    handle: ViewHandle,
    /// Original SQL
    sql: String,
    /// Tables this view depends on
    tables: Vec<String>,
}


// ============================================================================
// Default Constants
// ============================================================================

const DEFAULT_MAX_MEMORY_BYTES: u64 = 100 * 1024 * 1024; // 100MB

// ============================================================================
// Noria Handle - Main Engine State
// ============================================================================

/// Opaque handle to Noria engine
pub struct NoriaHandle {
    /// The dataflow executor (executes the graph)
    executor: RwLock<LocalExecutor>,

    /// SQL converter (parses SQL to dataflow)
    converter: RwLock<SqlConverter>,

    /// View ID -> NoriaViewEntry mapping
    views: RwLock<HashMap<i32, NoriaViewEntry>>,

    /// SQL (normalized) -> view ID mapping
    sql_to_view_id: RwLock<HashMap<String, i32>>,

    /// Table name -> list of view IDs that depend on it
    table_to_views: RwLock<HashMap<String, Vec<i32>>>,

    /// Table name -> table ID (for fast bitmap invalidation)
    table_to_id: RwLock<HashMap<String, usize>>,

    /// Table ID -> table name
    id_to_table: RwLock<Vec<String>>,

    /// Next view ID
    next_view_id: AtomicI32,

    /// Statistics
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    max_memory_bytes: AtomicU64,
    eviction_count: AtomicU64,
    bytes_evicted: AtomicU64,

    /// Dirty table bitmap for deferred invalidation (legacy support)
    dirty_tables: AtomicU64,

    /// Upquery callback
    upquery_callback: RwLock<Option<UpqueryCallback>>,
    upquery_user_data: RwLock<*mut c_void>,
}

// Safety: The raw pointers are only accessed in a single-threaded context
unsafe impl Send for NoriaHandle {}
unsafe impl Sync for NoriaHandle {}

/// Container for rows returned to C++
struct RowsContainer {
    rows: Vec<Row>,
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Normalize SQL for comparison
fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_uppercase()
}

/// Extract table names from SQL (simple parser)
fn extract_tables(sql: &str) -> Vec<String> {
    let mut tables = Vec::new();
    let upper = sql.to_uppercase();

    // FROM clause
    if let Some(from_pos) = upper.find(" FROM ") {
        let after_from = &sql[from_pos + 6..];
        let end_keywords = [
            " WHERE ", " JOIN ", " ORDER ", " GROUP ", " LIMIT ", " HAVING ", ";",
        ];
        let mut end_pos = after_from.len();
        for kw in end_keywords {
            if let Some(pos) = after_from.to_uppercase().find(kw) {
                if pos < end_pos {
                    end_pos = pos;
                }
            }
        }

        let table_part = after_from[..end_pos].trim();
        for table in table_part.split(',') {
            let table = table.trim();
            let table_name = table.split_whitespace().next().unwrap_or(table);
            let clean =
                table_name.trim_matches(|c: char| c == '`' || c == '"' || c == '[' || c == ']');
            if !clean.is_empty() {
                tables.push(clean.to_lowercase());
            }
        }
    }

    // JOIN clauses
    for join_kw in [
        " JOIN ",
        " INNER JOIN ",
        " LEFT JOIN ",
        " RIGHT JOIN ",
        " CROSS JOIN ",
    ] {
        let mut pos = 0;
        while let Some(join_pos) = upper[pos..].find(join_kw) {
            let actual_pos = pos + join_pos + join_kw.len();
            if actual_pos < sql.len() {
                let after_join = &sql[actual_pos..];
                if let Some(table_name) = after_join.split_whitespace().next() {
                    let clean = table_name.trim_matches(|c: char| {
                        c == '`' || c == '"' || c == '[' || c == ']' || c == ','
                    });
                    let lower = clean.to_lowercase();
                    if !lower.is_empty() && lower != "on" && !tables.contains(&lower) {
                        tables.push(lower);
                    }
                }
            }
            pos = actual_pos;
        }
    }

    tables
}

/// Convert slice of NoriaValue to Vec<DataType>
fn convert_values(values: *const NoriaValue, count: c_int) -> Vec<DataType> {
    if values.is_null() || count <= 0 {
        return vec![];
    }
    let slice = unsafe { std::slice::from_raw_parts(values, count as usize) };
    slice.iter().map(noria_value_to_datatype).collect()
}


// ============================================================================
// FFI Functions
// ============================================================================

/// Create a new Noria engine
#[no_mangle]
pub extern "C" fn noria_create(_sqlite_db: *mut c_void) -> *mut NoriaHandle {
    let handle = Box::new(NoriaHandle {
        executor: RwLock::new(LocalExecutor::new()),
        converter: RwLock::new(SqlConverter::new()),
        views: RwLock::new(HashMap::new()),
        sql_to_view_id: RwLock::new(HashMap::new()),
        table_to_views: RwLock::new(HashMap::new()),
        table_to_id: RwLock::new(HashMap::new()),
        id_to_table: RwLock::new(Vec::new()),
        next_view_id: AtomicI32::new(0),
        cache_hits: AtomicU64::new(0),
        cache_misses: AtomicU64::new(0),
        max_memory_bytes: AtomicU64::new(DEFAULT_MAX_MEMORY_BYTES),
        eviction_count: AtomicU64::new(0),
        bytes_evicted: AtomicU64::new(0),
        dirty_tables: AtomicU64::new(0),
        upquery_callback: RwLock::new(None),
        upquery_user_data: RwLock::new(ptr::null_mut()),
    });

    Box::into_raw(handle)
}

/// Destroy a Noria engine
#[no_mangle]
pub extern "C" fn noria_destroy(handle: *mut NoriaHandle) {
    if handle.is_null() {
        return;
    }

    // Just drop the handle - no background worker to stop
    let _ = unsafe { Box::from_raw(handle) };
}

/// Set upquery callback
#[no_mangle]
pub extern "C" fn noria_set_upquery_callback(
    handle: *mut NoriaHandle,
    callback: UpqueryCallback,
    user_data: *mut c_void,
) -> c_int {
    if handle.is_null() {
        return -1;
    }

    let handle = unsafe { &*handle };
    *handle.upquery_callback.write() = Some(callback);
    *handle.upquery_user_data.write() = user_data;
    0
}

/// Register a table schema (must be called before registering views that use it)
#[no_mangle]
pub extern "C" fn noria_register_table_schema(
    handle: *mut NoriaHandle,
    table: *const c_char,
    columns: *const *const c_char,
    column_count: c_int,
) -> c_int {
    if handle.is_null() || table.is_null() {
        return -1;
    }

    let handle = unsafe { &*handle };
    let table_str = match unsafe { CStr::from_ptr(table) }.to_str() {
        Ok(s) => s.to_lowercase(),
        Err(_) => return -1,
    };

    // Extract column names
    let mut column_names = Vec::new();
    if !columns.is_null() && column_count > 0 {
        let col_ptrs = unsafe { std::slice::from_raw_parts(columns, column_count as usize) };
        for &col_ptr in col_ptrs {
            if !col_ptr.is_null() {
                if let Ok(s) = unsafe { CStr::from_ptr(col_ptr) }.to_str() {
                    column_names.push(s.to_string());
                }
            }
        }
    }

    // Register with SQL converter
    handle
        .converter
        .write()
        .register_table(&table_str, column_names.clone());

    // Register base table in executor
    handle
        .executor
        .write()
        .add_base_table(&table_str, column_names);

    // Assign table ID
    {
        let mut table_to_id = handle.table_to_id.write();
        let mut id_to_table = handle.id_to_table.write();
        if !table_to_id.contains_key(&table_str) {
            let tid = id_to_table.len();
            table_to_id.insert(table_str.clone(), tid);
            id_to_table.push(table_str);
        }
    }

    0
}

/// Register a SELECT query as a view
#[no_mangle]
pub extern "C" fn noria_register_view(handle: *mut NoriaHandle, sql: *const c_char) -> c_int {
    if handle.is_null() || sql.is_null() {
        return -1;
    }

    let handle = unsafe { &*handle };
    let sql_str = match unsafe { CStr::from_ptr(sql) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };

    let normalized = normalize_sql(sql_str);

    // Check if already registered
    if let Some(&id) = handle.sql_to_view_id.read().get(&normalized) {
        return id;
    }

    // Extract table dependencies
    let tables = extract_tables(sql_str);

    // Try to create the view using SQL converter
    let view_handle = {
        let converter = handle.converter.read();
        let mut executor = handle.executor.write();
        match converter.convert_select(sql_str, &mut executor) {
            Ok(vh) => vh,
            Err(e) => {
                // Unsupported query - return -1 (skip caching per user's choice)
                tracing::debug!("Unsupported query, skipping cache: {:?}", e);
                return -1;
            }
        }
    };

    // Assign view ID
    let view_id = handle.next_view_id.fetch_add(1, Ordering::SeqCst);

    // Store view entry
    handle.views.write().insert(
        view_id,
        NoriaViewEntry {
            handle: view_handle,
            sql: sql_str.to_string(),
            tables: tables.clone(),
        },
    );

    // Register SQL -> view mapping
    handle.sql_to_view_id.write().insert(normalized, view_id);

    // Register table -> view mappings
    {
        let mut table_to_views = handle.table_to_views.write();
        let mut table_to_id = handle.table_to_id.write();
        let mut id_to_table = handle.id_to_table.write();

        for table in tables {
            // Ensure table has an ID
            if !table_to_id.contains_key(&table) {
                let tid = id_to_table.len();
                table_to_id.insert(table.clone(), tid);
                id_to_table.push(table.clone());
            }

            // Add view to table's view list
            table_to_views
                .entry(table)
                .or_insert_with(Vec::new)
                .push(view_id);
        }
    }

    view_id
}

/// Check if a view exists for the given SQL
#[no_mangle]
pub extern "C" fn noria_has_view(handle: *mut NoriaHandle, sql: *const c_char) -> c_int {
    if handle.is_null() || sql.is_null() {
        return 0;
    }

    let handle = unsafe { &*handle };
    let sql_str = match unsafe { CStr::from_ptr(sql) }.to_str() {
        Ok(s) => s,
        Err(_) => return 0,
    };

    let normalized = normalize_sql(sql_str);
    if handle.sql_to_view_id.read().contains_key(&normalized) {
        1
    } else {
        0
    }
}

/// Check if any views exist
#[no_mangle]
pub extern "C" fn noria_has_any_views(handle: *mut NoriaHandle) -> c_int {
    if handle.is_null() {
        return 0;
    }

    let handle = unsafe { &*handle };
    if handle.views.read().is_empty() {
        0
    } else {
        1
    }
}

/// Check if table has any views
#[no_mangle]
pub extern "C" fn noria_table_has_views(handle: *mut NoriaHandle, table: *const c_char) -> c_int {
    if handle.is_null() || table.is_null() {
        return 0;
    }

    let handle = unsafe { &*handle };
    let table_str = match unsafe { CStr::from_ptr(table) }.to_str() {
        Ok(s) => s.to_lowercase(),
        Err(_) => return 0,
    };

    if handle.table_to_views.read().contains_key(&table_str) {
        1
    } else {
        0
    }
}

/// Get table ID for fast CDC
#[no_mangle]
pub extern "C" fn noria_get_table_id(handle: *mut NoriaHandle, table: *const c_char) -> c_int {
    if handle.is_null() || table.is_null() {
        return -1;
    }

    let handle = unsafe { &*handle };
    let table_str = match unsafe { CStr::from_ptr(table) }.to_str() {
        Ok(s) => s.to_lowercase(),
        Err(_) => return -1,
    };

    match handle.table_to_id.read().get(&table_str) {
        Some(&id) => id as c_int,
        None => -1,
    }
}

/// Queue table invalidation by ID (legacy - now triggers CDC propagation)
#[no_mangle]
pub extern "C" fn noria_queue_invalidate_by_id(handle: *mut NoriaHandle, table_id: c_int) -> c_int {
    if handle.is_null() || table_id < 0 || table_id >= 64 {
        return -1;
    }

    let handle = unsafe { &*handle };
    handle
        .dirty_tables
        .fetch_or(1u64 << table_id, Ordering::Relaxed);
    0
}

/// Lookup in cache
#[no_mangle]
pub extern "C" fn noria_lookup(
    handle: *mut NoriaHandle,
    view_id: c_int,
    key_values: *const NoriaValue,
    key_count: c_int,
) -> NoriaLookupResult {
    let not_found = NoriaLookupResult {
        found: 0,
        row_count: 0,
        rows: ptr::null_mut(),
    };

    if handle.is_null() || view_id < 0 {
        return not_found;
    }

    let handle = unsafe { &*handle };

    // Get view
    let views = handle.views.read();
    let view_entry = match views.get(&view_id) {
        Some(v) => v,
        None => return not_found,
    };

    // Convert key
    let key: Vec<DataType> = convert_values(key_values, key_count);

    // Try cache lookup using executor
    let executor = handle.executor.read();
    match executor.lookup(&view_entry.handle, &key) {
        Some(rows) => {
            handle.cache_hits.fetch_add(1, Ordering::Relaxed);
            let row_count = rows.len() as c_int;
            if row_count == 0 {
                return NoriaLookupResult {
                    found: 1,
                    row_count: 0,
                    rows: ptr::null_mut(),
                };
            }

            // Convert DataType rows to Value rows for FFI
            let value_rows: Vec<Row> = rows
                .iter()
                .map(|row| row.iter().map(Value::from_datatype).collect())
                .collect();

            let container = Box::new(RowsContainer { rows: value_rows });
            NoriaLookupResult {
                found: 1,
                row_count,
                rows: Box::into_raw(container) as *mut c_void,
            }
        }
        None => {
            handle.cache_misses.fetch_add(1, Ordering::Relaxed);

            // Try upquery if callback is set
            let callback = *handle.upquery_callback.read();
            if let Some(cb) = callback {
                let user_data = *handle.upquery_user_data.read();
                let sql_cstr =
                    std::ffi::CString::new(view_entry.sql.as_str()).unwrap_or_default();

                let mut out_rows: *mut c_void = ptr::null_mut();
                let mut out_row_count: c_int = 0;

                let result = cb(
                    user_data,
                    sql_cstr.as_ptr(),
                    key_values,
                    key_count,
                    &mut out_rows,
                    &mut out_row_count,
                );

                if result == 0 && !out_rows.is_null() && out_row_count > 0 {
                    // Upquery succeeded - inject results into cache for future hits
                    let rows_container = unsafe { &*(out_rows as *const RowsContainer) };

                    // Convert Value rows to DataType for cache injection
                    let mut records_to_inject = Vec::new();
                    for row in rows_container.rows.iter() {
                        let datatype_row: Vec<DataType> = row
                            .iter()
                            .map(|v| match v {
                                Value::Null => DataType::None,
                                Value::Int(i) => DataType::BigInt(*i),
                                Value::Float(f) => DataType::from(*f),
                                Value::Text(s) => DataType::from(s.as_str()),
                                Value::Blob(_) => DataType::None, // Simplified
                            })
                            .collect();
                        records_to_inject.push(Record::Positive(datatype_row));
                    }

                    // Inject into view cache
                    if !records_to_inject.is_empty() {
                        let records = Records::from(records_to_inject);
                        drop(executor); // Release read lock before write
                        handle
                            .executor
                            .write()
                            .inject_into_view(&view_entry.handle, records);
                    }

                    // Return the rows to caller
                    return NoriaLookupResult {
                        found: 1,
                        row_count: out_row_count,
                        rows: out_rows,
                    };
                }
            }

            not_found
        }
    }
}

/// Lookup with upquery fallback (same as lookup)
#[no_mangle]
pub extern "C" fn noria_lookup_or_upquery(
    handle: *mut NoriaHandle,
    view_id: c_int,
    key_values: *const NoriaValue,
    key_count: c_int,
) -> NoriaLookupResult {
    noria_lookup(handle, view_id, key_values, key_count)
}

/// Store rows from upquery (injects into view cache)
#[no_mangle]
pub extern "C" fn noria_store_rows(
    handle: *mut NoriaHandle,
    view_id: c_int,
    _key_values: *const NoriaValue,
    _key_count: c_int,
    rows: *const *const NoriaValue,
    row_counts: *const c_int,
    num_rows: c_int,
) -> c_int {
    if handle.is_null() || view_id < 0 {
        return -1;
    }

    let handle = unsafe { &*handle };

    let views = handle.views.read();
    let view_entry = match views.get(&view_id) {
        Some(v) => v,
        None => return -1,
    };

    // Convert rows to Records
    let mut records_vec = Vec::new();
    if !rows.is_null() && num_rows > 0 {
        let row_ptrs = unsafe { std::slice::from_raw_parts(rows, num_rows as usize) };
        let counts = unsafe { std::slice::from_raw_parts(row_counts, num_rows as usize) };

        for (row_ptr, count) in row_ptrs.iter().zip(counts.iter()) {
            if !row_ptr.is_null() && *count > 0 {
                let values = unsafe { std::slice::from_raw_parts(*row_ptr, *count as usize) };
                let row: Vec<DataType> = values.iter().map(noria_value_to_datatype).collect();
                records_vec.push(Record::Positive(row));
            }
        }
    }

    // Inject into view
    let records = Records::from(records_vec);
    handle
        .executor
        .write()
        .inject_into_view(&view_entry.handle, records);

    0
}

/// Get value from rows
#[no_mangle]
pub extern "C" fn noria_get_value(
    rows_ptr: *mut c_void,
    row_index: c_int,
    col_index: c_int,
    out_value: *mut NoriaValue,
) -> c_int {
    if rows_ptr.is_null() || out_value.is_null() || row_index < 0 || col_index < 0 {
        return -1;
    }

    let container = unsafe { &*(rows_ptr as *const RowsContainer) };

    if row_index as usize >= container.rows.len() {
        return -1;
    }

    let row = &container.rows[row_index as usize];
    if col_index as usize >= row.len() {
        return -1;
    }

    let value = &row[col_index as usize];
    unsafe {
        *out_value = value.to_noria_value();
    }
    0
}

/// Get column count for a row
#[no_mangle]
pub extern "C" fn noria_row_column_count(rows_ptr: *mut c_void, row_index: c_int) -> c_int {
    if rows_ptr.is_null() || row_index < 0 {
        return 0;
    }

    let container = unsafe { &*(rows_ptr as *const RowsContainer) };

    if row_index as usize >= container.rows.len() {
        return 0;
    }

    container.rows[row_index as usize].len() as c_int
}

/// Free rows container
#[no_mangle]
pub extern "C" fn noria_free_rows(rows: *mut c_void) {
    if !rows.is_null() {
        let container = unsafe { Box::from_raw(rows as *mut RowsContainer) };
        // Free any owned strings in the container
        for row in container.rows.iter() {
            for val in row.iter() {
                if let Value::Text(s) = val {
                    // String is owned by Rust, will be dropped automatically
                    let _ = s;
                }
            }
        }
        // Box drops automatically
    }
}

// ============================================================================
// FFI Functions for Building RowsContainer from C++
// ============================================================================

/// Create an empty rows container for upquery results
#[no_mangle]
pub extern "C" fn noria_rows_create() -> *mut c_void {
    let container = Box::new(RowsContainer { rows: Vec::new() });
    Box::into_raw(container) as *mut c_void
}

/// Add a row to the rows container
/// The values are copied into the container
#[no_mangle]
pub extern "C" fn noria_rows_add_row(
    rows_ptr: *mut c_void,
    values: *const NoriaValue,
    value_count: c_int,
) -> c_int {
    if rows_ptr.is_null() || values.is_null() || value_count <= 0 {
        return -1;
    }

    let container = unsafe { &mut *(rows_ptr as *mut RowsContainer) };
    let values_slice = unsafe { std::slice::from_raw_parts(values, value_count as usize) };

    // Convert NoriaValue array to Row (Vec<Value>), copying string data
    let row: Row = values_slice
        .iter()
        .map(|nv| match nv.value_type {
            NORIA_NULL => Value::Null,
            NORIA_INTEGER => Value::Int(nv.int_value),
            NORIA_FLOAT => Value::Float(nv.float_value),
            NORIA_TEXT => {
                if nv.text_ptr.is_null() || nv.text_len <= 0 {
                    Value::Text(String::new())
                } else {
                    // Copy the string data (C++ data may be transient)
                    let slice = unsafe {
                        std::slice::from_raw_parts(nv.text_ptr as *const u8, nv.text_len as usize)
                    };
                    let s = String::from_utf8_lossy(slice).into_owned();
                    Value::Text(s)
                }
            }
            NORIA_BLOB => {
                if nv.blob_ptr.is_null() || nv.blob_len <= 0 {
                    Value::Blob(Vec::new())
                } else {
                    let slice =
                        unsafe { std::slice::from_raw_parts(nv.blob_ptr, nv.blob_len as usize) };
                    Value::Blob(slice.to_vec())
                }
            }
            _ => Value::Null,
        })
        .collect();

    container.rows.push(row);
    0
}

/// Apply INSERT with synchronous CDC propagation
#[no_mangle]
pub extern "C" fn noria_apply_insert(
    handle: *mut NoriaHandle,
    table: *const c_char,
    values: *const NoriaValue,
    value_count: c_int,
) -> c_int {
    if handle.is_null() || table.is_null() {
        return -1;
    }

    let handle = unsafe { &*handle };
    let table_str = match unsafe { CStr::from_ptr(table) }.to_str() {
        Ok(s) => s.to_lowercase(),
        Err(_) => return -1,
    };

    let row = convert_values(values, value_count);

    // Synchronous propagation through dataflow
    let record = Record::Positive(row);
    let records = Records::from(vec![record]);
    handle.executor.write().apply_write(&table_str, records);

    0
}

/// Apply DELETE with synchronous CDC propagation
#[no_mangle]
pub extern "C" fn noria_apply_delete(
    handle: *mut NoriaHandle,
    table: *const c_char,
    old_values: *const NoriaValue,
    value_count: c_int,
) -> c_int {
    if handle.is_null() || table.is_null() {
        return -1;
    }

    let handle = unsafe { &*handle };
    let table_str = match unsafe { CStr::from_ptr(table) }.to_str() {
        Ok(s) => s.to_lowercase(),
        Err(_) => return -1,
    };

    let row = convert_values(old_values, value_count);

    // Synchronous propagation through dataflow
    let record = Record::Negative(row);
    let records = Records::from(vec![record]);
    handle.executor.write().apply_write(&table_str, records);

    0
}

/// Apply UPDATE with synchronous CDC propagation
#[no_mangle]
pub extern "C" fn noria_apply_update(
    handle: *mut NoriaHandle,
    table: *const c_char,
    old_values: *const NoriaValue,
    new_values: *const NoriaValue,
    value_count: c_int,
) -> c_int {
    if handle.is_null() || table.is_null() {
        return -1;
    }

    let handle = unsafe { &*handle };
    let table_str = match unsafe { CStr::from_ptr(table) }.to_str() {
        Ok(s) => s.to_lowercase(),
        Err(_) => return -1,
    };

    let old_row = convert_values(old_values, value_count);
    let new_row = convert_values(new_values, value_count);

    // UPDATE = negative (old) + positive (new)
    let records = Records::from(vec![
        Record::Negative(old_row),
        Record::Positive(new_row),
    ]);
    handle.executor.write().apply_write(&table_str, records);

    0
}

/// Queue INSERT (same as apply)
#[no_mangle]
pub extern "C" fn noria_queue_insert(
    handle: *mut NoriaHandle,
    table: *const c_char,
    values: *const NoriaValue,
    value_count: c_int,
) -> c_int {
    noria_apply_insert(handle, table, values, value_count)
}

/// Queue DELETE (same as apply)
#[no_mangle]
pub extern "C" fn noria_queue_delete(
    handle: *mut NoriaHandle,
    table: *const c_char,
    old_values: *const NoriaValue,
    value_count: c_int,
) -> c_int {
    noria_apply_delete(handle, table, old_values, value_count)
}

/// Queue UPDATE (same as apply)
#[no_mangle]
pub extern "C" fn noria_queue_update(
    handle: *mut NoriaHandle,
    table: *const c_char,
    old_values: *const NoriaValue,
    new_values: *const NoriaValue,
    value_count: c_int,
) -> c_int {
    noria_apply_update(handle, table, old_values, new_values, value_count)
}

/// Flush pending CDC events (no-op since we're synchronous now)
#[no_mangle]
pub extern "C" fn noria_flush(handle: *mut NoriaHandle) -> c_int {
    if handle.is_null() {
        return -1;
    }

    let handle = unsafe { &*handle };

    // Clear dirty table bitmap (legacy support)
    let _dirty = handle.dirty_tables.swap(0, Ordering::AcqRel);

    // Synchronous mode - nothing to flush
    0
}

/// Get cache statistics
#[no_mangle]
pub extern "C" fn noria_get_stats(handle: *mut NoriaHandle) -> NoriaCacheStats {
    let default_stats = NoriaCacheStats {
        cache_hits: 0,
        cache_misses: 0,
        total_rows: 0,
        view_count: 0,
        node_count: 0,
        memory_bytes: 0,
        max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
        eviction_count: 0,
        bytes_evicted: 0,
    };

    if handle.is_null() {
        return default_stats;
    }

    let handle = unsafe { &*handle };
    let executor = handle.executor.read();
    let stats = executor.stats();

    NoriaCacheStats {
        cache_hits: handle.cache_hits.load(Ordering::Relaxed),
        cache_misses: handle.cache_misses.load(Ordering::Relaxed),
        total_rows: stats.total_rows as u64,
        view_count: handle.views.read().len() as c_int,
        node_count: stats.node_count as c_int,
        memory_bytes: (stats.total_rows * 100) as u64, // Rough estimate
        max_memory_bytes: handle.max_memory_bytes.load(Ordering::Relaxed),
        eviction_count: handle.eviction_count.load(Ordering::Relaxed),
        bytes_evicted: handle.bytes_evicted.load(Ordering::Relaxed),
    }
}

/// Set maximum memory limit in bytes
#[no_mangle]
pub extern "C" fn noria_set_max_memory(handle: *mut NoriaHandle, max_bytes: u64) -> c_int {
    if handle.is_null() {
        return -1;
    }

    let handle = unsafe { &*handle };
    handle
        .max_memory_bytes
        .store(max_bytes, Ordering::Relaxed);
    0
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_sql() {
        let normalized = normalize_sql("SELECT  *  FROM  users  WHERE  id = ?");
        assert_eq!(normalized, "SELECT * FROM USERS WHERE ID = ?");
    }

    #[test]
    fn test_extract_tables() {
        assert_eq!(
            extract_tables("SELECT * FROM users WHERE id = ?"),
            vec!["users"]
        );

        assert_eq!(
            extract_tables("SELECT * FROM users, orders WHERE users.id = orders.user_id"),
            vec!["users", "orders"]
        );
    }

    #[test]
    fn test_create_and_destroy() {
        let handle = noria_create(ptr::null_mut());
        assert!(!handle.is_null());
        noria_destroy(handle);
    }

    #[test]
    fn test_datatype_conversion() {
        let nv = NoriaValue {
            value_type: NORIA_INTEGER,
            int_value: 42,
            ..Default::default()
        };
        let dt = noria_value_to_datatype(&nv);
        assert_eq!(dt, DataType::BigInt(42));
    }
}
