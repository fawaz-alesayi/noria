//! # C FFI Bridge for Noria Dataflow Engine
//!
//! This module provides the FFI layer between C++ (better-sqlite3) and Rust
//! (noria-core), enabling transparent query caching with incremental view
//! maintenance.
//!
//! ## Architecture Overview
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     JavaScript (Node.js)                        │
//! │                   stmt.get(1), db.prepare()                     │
//! └─────────────────────────────────────────────────────────────────┘
//!                                │
//!                                ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    C++ (better-sqlite3)                         │
//! │   Statement::TryNoriaGet() ──▶ noria->LookupOrUpquery()        │
//! │   Database::Exec()         ──▶ preupdate hook ──▶ CDC queue    │
//! └─────────────────────────────────────────────────────────────────┘
//!                                │
//!                                ▼ FFI boundary (this module)
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Rust FFI Layer (noria-ffi)                   │
//! │  ┌──────────────┐ ┌──────────────┐ ┌────────────────────────┐  │
//! │  │ noria_lookup │ │ noria_queue_ │ │ noria_lookup_int_key   │  │
//! │  │ _or_upquery  │ │ insert/del/up│ │ (integer fast path)    │  │
//! │  └──────────────┘ └──────────────┘ └────────────────────────┘  │
//! │                          │                                      │
//! │                          ▼                                      │
//! │  ┌──────────────────────────────────────────────────────────┐  │
//! │  │                    NoriaHandle                            │  │
//! │  │  - executor: LocalExecutor (dataflow DAG)                 │  │
//! │  │  - views: ArcSwap<HashMap> (lock-free view registry)      │  │
//! │  │  - write_queue: Vec<PendingWrite> (async CDC batch)       │  │
//! │  └──────────────────────────────────────────────────────────┘  │
//! └─────────────────────────────────────────────────────────────────┘
//!                                │
//!                                ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    noria-core (Rust)                            │
//! │  LocalExecutor, DynamicState, SqlConverter, etc.                │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Hot Path vs Cold Path
//!
//! The FFI layer distinguishes between:
//!
//! **Hot Path (Cache Hit)** - Optimized for speed:
//! 1. `noria_lookup_int_key()` - Direct integer key lookup (no NoriaValue conversion)
//! 2. ArcSwap load (lock-free) to get view entry
//! 3. State lookup returns `Arc<Vec<DataType>>` (O(1) clone)
//! 4. DataRowsContainer stores Arcs directly (zero-copy)
//!
//! **Cold Path (Cache Miss)** - Calls back to C++:
//! 1. `noria_lookup_or_upquery()` detects miss via `LookupResult::Missing`
//! 2. Invokes upquery callback to execute SQL in SQLite
//! 3. Results injected into cache for future hits
//!
//! ## Key Optimizations
//!
//! See `OPTIMIZATIONS.md` for benchmarks and detailed analysis.
//!
//! | Optimization | Function | Impact |
//! |--------------|----------|--------|
//! | Integer key fast path | `noria_lookup_int_key()` | +12% single-key reads |
//! | Zero-copy rows | `DataRowsContainer` | +10-21% reads |
//! | Batch value extraction | `noria_get_row()` | Reduces FFI calls |
//! | ArcSwap views | `views` field | Lock-free reads |
//! | Async batch CDC | `noria_flush()` | 4x faster writes |
//!
//! ## Async Batch Processing
//!
//! Implements the async batch processing from the Noria paper (Section 4.3):
//!
//! ```text
//! INSERT/UPDATE/DELETE → noria_queue_*() → write_queue
//!                                              │
//!                        Transaction COMMIT ───┘
//!                                              │
//!                        noria_flush() ───────▶ Batch propagate
//! ```
//!
//! Benefits: Single lock acquisition per table, aggregate batching,
//! reduced retraction overhead.
//!
//! ## Memory Management
//!
//! - Result containers are heap-allocated and must be freed via `noria_free_rows()`
//! - Views support random eviction when memory exceeds `max_memory_bytes`
//! - String data in upquery results is copied (C++ data may be transient)
//!
//! ## Paper Reference
//!
//! <https://pdos.csail.mit.edu/papers/noria:osdi18.pdf>

use noria::DataType;
use noria_core::dataflow::{
    LocalExecutor, Record, Records, ViewHandle,
    Row as ArcRow,  // Arc<Vec<DataType>> for O(1) cloning
};
use noria_core::SqlConverter;
use arc_swap::ArcSwap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::ffi::{c_char, c_double, c_int, c_void, CStr};
use std::ptr;
use std::sync::Arc;
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

/// Result of a batch cache lookup
#[repr(C)]
pub struct NoriaBatchLookupResult {
    /// Number of results (one per key)
    pub count: c_int,
    /// Opaque pointer to result array (caller must free with noria_free_batch_results)
    pub results: *mut c_void,
}

/// Maximum columns supported by NoriaRowData fixed-size array
pub const NORIA_MAX_ROW_COLUMNS: usize = 32;

/// Row data returned by noria_get_row - all values in a single FFI call
/// Uses fixed-size array to avoid heap allocation for common cases (≤32 columns)
#[repr(C)]
pub struct NoriaRowData {
    /// Number of columns in this row
    pub col_count: c_int,
    /// Fixed-size array of values (only first col_count entries are valid)
    pub values: [NoriaValue; NORIA_MAX_ROW_COLUMNS],
}

impl Default for NoriaRowData {
    fn default() -> Self {
        // Safe initialization with zeroed memory for the array
        NoriaRowData {
            col_count: 0,
            values: unsafe { std::mem::zeroed() },
        }
    }
}

/// Container for batch results
struct BatchResultsContainer {
    results: Vec<(c_int, c_int, Option<Box<DataRowsContainer>>)>, // (found, row_count, rows)
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

/// Convert DataType directly to NoriaValue WITHOUT intermediate Value allocation
/// This is the zero-copy fast path
fn datatype_to_noria_value(dt: &DataType, out: &mut NoriaValue) {
    match dt {
        DataType::None => {
            out.value_type = NORIA_NULL;
        }
        DataType::Int(i) => {
            out.value_type = NORIA_INTEGER;
            out.int_value = *i as i64;
        }
        DataType::BigInt(i) => {
            out.value_type = NORIA_INTEGER;
            out.int_value = *i;
        }
        DataType::UnsignedInt(u) => {
            out.value_type = NORIA_INTEGER;
            out.int_value = *u as i64;
        }
        DataType::UnsignedBigInt(u) => {
            out.value_type = NORIA_INTEGER;
            out.int_value = *u as i64;
        }
        DataType::Real(int_part, frac_part) => {
            out.value_type = NORIA_FLOAT;
            out.float_value = *int_part as f64 + (*frac_part as f64 / 1_000_000_000.0);
        }
        DataType::Text(_) | DataType::TinyText(_) => {
            // Get pointer directly into the DataType's string storage
            let s: &str = dt.into();
            out.value_type = NORIA_TEXT;
            out.text_ptr = s.as_ptr() as *const c_char;
            out.text_len = s.len() as c_int;
        }
        DataType::Timestamp(_) => {
            out.value_type = NORIA_NULL;
        }
    }
}

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
#[derive(Clone)]
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

/// Pending write for batch processing
#[derive(Clone)]
struct PendingWrite {
    table: String,
    record: Record,
}

/// Main Noria engine state, managing the dataflow graph and view registry.
///
/// ## Thread Safety
///
/// This struct is designed for safe concurrent access:
/// - `views`: Uses `ArcSwap` for lock-free read access (OPTIMIZATIONS.md #7)
/// - `executor`, `converter`: Protected by `RwLock` (writes are rare)
/// - `upquery_callback`: Protected by `RwLock`, only modified during setup
/// - Atomic counters for statistics
///
/// ## Key Fields
///
/// - `executor`: The dataflow graph engine (processes CDC events)
/// - `views`: Lock-free registry mapping view IDs to handles
/// - `write_queue`: Batches CDC events for async propagation (Noria paper §4.3)
pub struct NoriaHandle {
    /// Dataflow executor - propagates CDC events through operator DAG
    executor: RwLock<LocalExecutor>,

    /// SQL-to-dataflow converter
    converter: RwLock<SqlConverter>,

    /// View registry: ID → entry. Uses ArcSwap for lock-free reads.
    /// OPTIMIZATION: Avoids RwLock overhead on hot read path (see OPTIMIZATIONS.md #7)
    views: ArcSwap<HashMap<i32, NoriaViewEntry>>,

    /// Reverse lookup: normalized SQL → view ID
    sql_to_view_id: RwLock<HashMap<String, i32>>,

    /// Table dependencies: table name → dependent view IDs
    table_to_views: RwLock<HashMap<String, Vec<i32>>>,

    /// Table ID mapping (for bitmap operations)
    table_to_id: RwLock<HashMap<String, usize>>,
    id_to_table: RwLock<Vec<String>>,

    /// Auto-incrementing view ID
    next_view_id: AtomicI32,

    /// Performance statistics (atomic for lock-free updates)
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    max_memory_bytes: AtomicU64,
    eviction_count: AtomicU64,
    bytes_evicted: AtomicU64,

    /// Legacy: dirty table bitmap for deferred invalidation
    dirty_tables: AtomicU64,

    /// Upquery callback: invoked on cache miss to fetch from SQLite
    upquery_callback: RwLock<Option<UpqueryCallback>>,
    upquery_user_data: RwLock<*mut c_void>,

    /// CDC event queue for batch processing (from Noria paper)
    write_queue: RwLock<Vec<PendingWrite>>,
}

// SAFETY: All fields are either:
// - Atomic types (inherently thread-safe)
// - Protected by RwLock (synchronized access)
// - ArcSwap (designed for concurrent access)
// The raw pointer `upquery_user_data` is only accessed while holding the
// `upquery_callback` lock, preventing data races.
unsafe impl Send for NoriaHandle {}
unsafe impl Sync for NoriaHandle {}

// ============================================================================
// Container Types for FFI Result Returns
//
// Two container types support different result sources:
// - DataRowsContainer: Cache hits (zero-copy, Arc-wrapped rows)
// - RowsContainer: Upquery results (converted from C++ data)
//
// Both use #[repr(C)] with container_type at offset 0 so C++ can detect
// which type to use via noria_get_value() / noria_get_row().
// ============================================================================

/// Type discriminant at offset 0 of both container types
const CONTAINER_TYPE_VALUES: u8 = 0;  // Legacy RowsContainer (upquery results)
const CONTAINER_TYPE_DIRECT: u8 = 1;  // Zero-copy DataRowsContainer (cache hits)

/// Container for upquery results (rows from C++ callback).
///
/// Used when cache misses require fetching from SQLite. The `Value` type
/// holds owned string data (copied from C++ since that data may be transient).
#[repr(C)]
struct RowsContainer {
    /// Discriminant for C++ type detection (always CONTAINER_TYPE_VALUES)
    container_type: u8,
    /// Rows with owned data
    rows: Vec<Row>,
}

impl RowsContainer {
    fn new(rows: Vec<Row>) -> Self {
        Self {
            container_type: CONTAINER_TYPE_VALUES,
            rows,
        }
    }
}

/// Zero-copy container for cache hit results (OPTIMIZATIONS.md #9).
///
/// Stores `Arc<Vec<DataType>>` directly - lookups just clone Arc references
/// (O(1) ref count increment) rather than deep-copying row data.
///
/// ## Why Two Container Types?
///
/// Cache hits return Arc-wrapped rows from state, avoiding allocation.
/// Upquery results come from C++ and need owned storage.
/// The discriminant at offset 0 lets `noria_get_value()` handle both.
#[repr(C)]
struct DataRowsContainer {
    /// Discriminant for C++ type detection (always CONTAINER_TYPE_DIRECT)
    container_type: u8,
    /// Arc-wrapped rows - cloning is O(1), no data copying
    rows: Vec<ArcRow>,
}

impl DataRowsContainer {
    fn new(rows: Vec<ArcRow>) -> Self {
        Self {
            container_type: CONTAINER_TYPE_DIRECT,
            rows,
        }
    }
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
        views: ArcSwap::from_pointee(HashMap::new()),
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
        write_queue: RwLock::new(Vec::new()),
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

    // Store view entry (clone-modify-store for ArcSwap)
    {
        // load() returns Guard<Arc<T>>, dereference twice to clone inner HashMap
        let mut views = (**handle.views.load()).clone();
        views.insert(
            view_id,
            NoriaViewEntry {
                handle: view_handle,
                sql: sql_str.to_string(),
                tables: tables.clone(),
            },
        );
        handle.views.store(Arc::new(views));
    }

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
    if handle.views.load().is_empty() {
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

    // Get view handle only - avoid cloning sql/tables on hot path (only needed for upquery)
    let view_handle = {
        let views = handle.views.load();
        match views.get(&view_id) {
            Some(v) => v.handle.clone(),
            None => return not_found,
        }
    };

    // Convert key
    let key: Vec<DataType> = {
        convert_values(key_values, key_count)
    };

    // Try cache lookup using executor
    let lookup_result = {
        let executor = handle.executor.read();
        executor.lookup(&view_handle, &key)
    };

    match lookup_result {
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

            // ZERO-COPY: Store DataType rows directly without conversion
            // The conversion to NoriaValue happens on-demand in noria_get_value
            let container = {
                Box::new(DataRowsContainer::new(rows))
            };
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

                // Only clone sql now (on cache miss path) - not on hot cache hit path
                let sql = {
                    let views = handle.views.load();
                    match views.get(&view_id) {
                        Some(v) => v.sql.clone(),
                        None => return not_found,
                    }
                };
                let sql_cstr = std::ffi::CString::new(sql.as_str()).unwrap_or_default();

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
                        handle
                            .executor
                            .write()
                            .inject_into_view(&view_handle, records);
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

/// Fast path lookup for single integer key - skips NoriaValue conversion overhead
///
/// This is an optimization for the common case where the key is a single integer
/// (e.g., primary key lookup). By taking an i64 directly, we avoid:
/// - NoriaValue struct construction in C++
/// - noria_value_to_datatype conversion in Rust
/// - Vec allocation for the key
#[no_mangle]
pub extern "C" fn noria_lookup_int_key(
    handle: *mut NoriaHandle,
    view_id: c_int,
    key: i64,
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

    // Get view handle only - avoid cloning sql/tables on hot path
    let view_handle = {
        let views = handle.views.load();
        match views.get(&view_id) {
            Some(v) => v.handle.clone(),
            None => return not_found,
        }
    };

    // Create key directly - no NoriaValue conversion needed
    let key_dt = [DataType::BigInt(key)];

    // Try cache lookup using executor
    let lookup_result = {
        let executor = handle.executor.read();
        executor.lookup(&view_handle, &key_dt)
    };

    match lookup_result {
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

            // ZERO-COPY: Store DataType rows directly
            let container = Box::new(DataRowsContainer::new(rows));
            NoriaLookupResult {
                found: 1,
                row_count,
                rows: Box::into_raw(container) as *mut c_void,
            }
        }
        None => {
            // DON'T count miss here - the caller will fall back to regular lookup
            // which will count the miss there if needed. This avoids double-counting.
            not_found
        }
    }
}

/// Batch lookup - lookup multiple keys in a single FFI call
/// This amortizes lock acquisition and FFI crossing overhead
#[no_mangle]
pub extern "C" fn noria_lookup_batch(
    handle: *mut NoriaHandle,
    view_id: c_int,
    keys: *const *const NoriaValue,
    key_counts: *const c_int,
    num_keys: c_int,
) -> NoriaBatchLookupResult {

    let empty_result = NoriaBatchLookupResult {
        count: 0,
        results: ptr::null_mut(),
    };

    if handle.is_null() || view_id < 0 || keys.is_null() || num_keys <= 0 {
        return empty_result;
    }

    let handle = unsafe { &*handle };
    let key_ptrs = unsafe { std::slice::from_raw_parts(keys, num_keys as usize) };
    let counts = unsafe { std::slice::from_raw_parts(key_counts, num_keys as usize) };

    // Get view handle only - no need for sql/tables in batch lookup (no upquery)
    let view_handle = {
        let views = handle.views.load();
        match views.get(&view_id) {
            Some(v) => v.handle.clone(),
            None => return empty_result,
        }
    };

    // Perform all lookups with single lock acquisition
    let mut results = Vec::with_capacity(num_keys as usize);
    {
        let executor = handle.executor.read();

        for (key_ptr, &key_count) in key_ptrs.iter().zip(counts.iter()) {
            if key_ptr.is_null() || key_count <= 0 {
                results.push((0, 0, None));
                continue;
            }

            let key = convert_values(*key_ptr, key_count);
            match executor.lookup(&view_handle, &key) {
                Some(rows) => {
                    handle.cache_hits.fetch_add(1, Ordering::Relaxed);
                    let row_count = rows.len() as c_int;
                    if row_count == 0 {
                        results.push((1, 0, None));
                    } else {
                        let container = Box::new(DataRowsContainer::new(rows));
                        results.push((1, row_count, Some(container)));
                    }
                }
                None => {
                    handle.cache_misses.fetch_add(1, Ordering::Relaxed);
                    results.push((0, 0, None));
                }
            }
        }
    }

    let container = Box::new(BatchResultsContainer { results });
    NoriaBatchLookupResult {
        count: num_keys,
        results: Box::into_raw(container) as *mut c_void,
    }
}

/// Get a single result from batch lookup
#[no_mangle]
pub extern "C" fn noria_batch_get_result(
    batch_ptr: *mut c_void,
    index: c_int,
    out_found: *mut c_int,
    out_row_count: *mut c_int,
) -> *mut c_void {
    if batch_ptr.is_null() || out_found.is_null() || out_row_count.is_null() || index < 0 {
        return ptr::null_mut();
    }

    let container = unsafe { &*(batch_ptr as *const BatchResultsContainer) };

    if index as usize >= container.results.len() {
        return ptr::null_mut();
    }

    let (found, row_count, ref rows) = container.results[index as usize];
    unsafe {
        *out_found = found;
        *out_row_count = row_count;
    }

    match rows {
        Some(rows_box) => rows_box.as_ref() as *const DataRowsContainer as *mut c_void,
        None => ptr::null_mut(),
    }
}

/// Free batch lookup results
#[no_mangle]
pub extern "C" fn noria_free_batch_results(batch_ptr: *mut c_void) {
    if !batch_ptr.is_null() {
        let _ = unsafe { Box::from_raw(batch_ptr as *mut BatchResultsContainer) };
    }
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

    let views = handle.views.load();
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

/// Get value from rows - handles both container types (zero-copy and legacy)
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

    // Read container type from first byte (both structs have it first)
    let container_type = unsafe { *(rows_ptr as *const u8) };

    if container_type == CONTAINER_TYPE_DIRECT {
        // ZERO-COPY PATH: DataRowsContainer with DataType
        let container = unsafe { &*(rows_ptr as *const DataRowsContainer) };

        if row_index as usize >= container.rows.len() {
            return -1;
        }

        let row = &container.rows[row_index as usize];
        if col_index as usize >= row.len() {
            return -1;
        }

        // Direct conversion without intermediate Value allocation
        let dt = &row[col_index as usize];
        unsafe {
            // Zero the output first
            *out_value = NoriaValue::default();
            datatype_to_noria_value(dt, &mut *out_value);
        }
        0
    } else {
        // LEGACY PATH: RowsContainer with Value (for upquery results)
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
}

/// Get all values for a row in a single FFI call - batch optimization
/// This eliminates N FFI calls (one per column) with a single call that fills
/// a fixed-size array. For rows with ≤32 columns, this avoids all heap allocation.
///
/// Returns: 0 on success, -1 on error, -2 if row has more columns than NORIA_MAX_ROW_COLUMNS
#[no_mangle]
pub extern "C" fn noria_get_row(
    rows_ptr: *mut c_void,
    row_index: c_int,
    out_data: *mut NoriaRowData,
) -> c_int {
    if rows_ptr.is_null() || out_data.is_null() || row_index < 0 {
        return -1;
    }

    // Read container type from first byte
    let container_type = unsafe { *(rows_ptr as *const u8) };

    if container_type == CONTAINER_TYPE_DIRECT {
        // ZERO-COPY PATH: DataRowsContainer with DataType
        let container = unsafe { &*(rows_ptr as *const DataRowsContainer) };

        if row_index as usize >= container.rows.len() {
            return -1;
        }

        let row = &container.rows[row_index as usize];
        let col_count = row.len();

        // Check if row fits in fixed-size array
        if col_count > NORIA_MAX_ROW_COLUMNS {
            return -2; // Too many columns - caller should fall back to noria_get_value
        }

        unsafe {
            (*out_data).col_count = col_count as c_int;

            // Convert all columns in one pass
            for (i, dt) in row.iter().enumerate() {
                datatype_to_noria_value(dt, &mut (*out_data).values[i]);
            }
        }
        0
    } else {
        // LEGACY PATH: RowsContainer with Value (for upquery results)
        let container = unsafe { &*(rows_ptr as *const RowsContainer) };

        if row_index as usize >= container.rows.len() {
            return -1;
        }

        let row = &container.rows[row_index as usize];
        let col_count = row.len();

        if col_count > NORIA_MAX_ROW_COLUMNS {
            return -2;
        }

        unsafe {
            (*out_data).col_count = col_count as c_int;

            for (i, value) in row.iter().enumerate() {
                (*out_data).values[i] = value.to_noria_value();
            }
        }
        0
    }
}

/// Get column count for a row - handles both container types
#[no_mangle]
pub extern "C" fn noria_row_column_count(rows_ptr: *mut c_void, row_index: c_int) -> c_int {
    if rows_ptr.is_null() || row_index < 0 {
        return 0;
    }

    // Read container type from first byte
    let container_type = unsafe { *(rows_ptr as *const u8) };

    if container_type == CONTAINER_TYPE_DIRECT {
        let container = unsafe { &*(rows_ptr as *const DataRowsContainer) };
        if row_index as usize >= container.rows.len() {
            return 0;
        }
        container.rows[row_index as usize].len() as c_int
    } else {
        let container = unsafe { &*(rows_ptr as *const RowsContainer) };
        if row_index as usize >= container.rows.len() {
            return 0;
        }
        container.rows[row_index as usize].len() as c_int
    }
}

/// Free rows container - handles both container types
#[no_mangle]
pub extern "C" fn noria_free_rows(rows: *mut c_void) {
    if rows.is_null() {
        return;
    }

    // Read container type from first byte
    let container_type = unsafe { *(rows as *const u8) };

    if container_type == CONTAINER_TYPE_DIRECT {
        // Zero-copy container - just drop the DataType vectors
        let _ = unsafe { Box::from_raw(rows as *mut DataRowsContainer) };
    } else {
        // Legacy container - drop Value vectors
        let _ = unsafe { Box::from_raw(rows as *mut RowsContainer) };
    }
    // Box drops automatically, cleaning up all owned data
}

// ============================================================================
// FFI Functions for Building RowsContainer from C++
// ============================================================================

/// Create an empty rows container for upquery results (legacy path)
#[no_mangle]
pub extern "C" fn noria_rows_create() -> *mut c_void {
    let container = Box::new(RowsContainer::new(Vec::new()));
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

/// Queue INSERT for batch processing
#[no_mangle]
pub extern "C" fn noria_queue_insert(
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
    let pending = PendingWrite {
        table: table_str,
        record: Record::Positive(row),
    };

    handle.write_queue.write().push(pending);
    0
}

/// Queue DELETE for batch processing
#[no_mangle]
pub extern "C" fn noria_queue_delete(
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
    let pending = PendingWrite {
        table: table_str,
        record: Record::Negative(row),
    };

    handle.write_queue.write().push(pending);
    0
}

/// Queue UPDATE for batch processing (DELETE old + INSERT new)
#[no_mangle]
pub extern "C" fn noria_queue_update(
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

    let mut queue = handle.write_queue.write();
    queue.push(PendingWrite {
        table: table_str.clone(),
        record: Record::Negative(old_row),
    });
    queue.push(PendingWrite {
        table: table_str,
        record: Record::Positive(new_row),
    });

    0
}

/// Flush pending CDC events - batch process all queued writes
///
/// This implements the async batch processing from the original Noria paper.
/// Instead of propagating each write individually, we batch writes by table
/// and propagate them together. This is more efficient because:
/// 1. Single lock acquisition per table instead of per-write
/// 2. Aggregate operators can batch updates for the same group key
/// 3. Reduces retraction overhead (emit old/new only once per final state)
#[no_mangle]
pub extern "C" fn noria_flush(handle: *mut NoriaHandle) -> c_int {

    if handle.is_null() {
        return -1;
    }

    let handle = unsafe { &*handle };

    // Clear dirty table bitmap (legacy support)
    let _dirty = handle.dirty_tables.swap(0, Ordering::AcqRel);

    // Take all pending writes from the queue
    let pending: Vec<PendingWrite> = {
        let mut queue = handle.write_queue.write();
        std::mem::take(&mut *queue)
    };

    if pending.is_empty() {
        return 0;
    }

    // Group writes by table for batch processing
    let writes_by_table: HashMap<String, Vec<Record>> = {
        let mut map: HashMap<String, Vec<Record>> = HashMap::new();
        for pw in pending {
            map.entry(pw.table)
                .or_insert_with(Vec::new)
                .push(pw.record);
        }
        map
    };

    // Process each table's writes as a batch
    {
        let mut executor = handle.executor.write();
        for (table, records) in writes_by_table {
            let batch = Records::from(records);
            executor.apply_write(&table, batch);
        }
    }

    0
}

/// Clear the write queue (used on transaction rollback to discard pending CDC events)
#[no_mangle]
pub extern "C" fn noria_clear_queue(handle: *mut NoriaHandle) {
    if handle.is_null() {
        return;
    }

    let handle = unsafe { &*handle };

    // Clear dirty table bitmap
    handle.dirty_tables.swap(0, Ordering::AcqRel);

    // Clear pending writes without processing them
    let mut queue = handle.write_queue.write();
    queue.clear();
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
        view_count: handle.views.load().len() as c_int,
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
