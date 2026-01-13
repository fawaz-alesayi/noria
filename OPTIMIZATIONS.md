# Noria-SQLite Performance Optimizations

This document tracks all performance optimizations implemented and future opportunities identified through profiling analysis.

## Summary

| Optimization | Status | Impact |
|--------------|--------|--------|
| Zero-copy result returns | Complete | ~15% speedup |
| Batch lookup API (FFI) | Complete | Reduces FFI overhead for bulk ops |
| Direct integer array indexing | Complete | O(1) vs O(log n) for integer PKs |
| DynamicState auto-detection | Complete | Auto-selects optimal state impl |
| noria-sqlite uses noria-core state | Complete | +0.19x speedup |
| `#[repr(C)]` container layout | Complete | Fixed type detection across FFI |
| Compile-time conditional profiling | Complete | Zero overhead when disabled |
| LTO (Link-Time Optimization) | Complete | Cross-crate inlining |
| C++/JS Batch API (getMany) | Complete | 1.1-1.2x speedup for batch lookups |
| Lock-free views with ArcSwap | Complete | Wait-free reads, ~1% improvement |
| Pre-update hook CDC | Complete | 4x faster writes (was session extension) |
| Arc-wrapped rows in state | **Complete** | +10-21% speedup (eliminates deep copy) |
| Integer Key Fast Path | **Complete** | +12% single-key reads (skips NoriaValue) |

## Current Performance (Lobsters Benchmark - January 2026)

| Scenario | better-sqlite3 | noria | Speedup |
|----------|----------------|-------|---------|
| Single-key read | 323,500 | 995,975 | **3.08x** |
| Read-only mixed | 301,825 | 577,805 | **1.91x** |
| Read 99/1 | 205,988 | 372,488 | **1.81x** |
| Read 95/5 | 114,400 | 146,647 | **1.28x** |
| Read 90/10 | 67,492 | 53,390 | 0.79x |

Cache throughput: **~1M ops/sec**

---

## Completed Optimizations

### 1. Zero-Copy Result Returns

**Files**: `noria-ffi/src/lib.rs`

**Problem**: The lookup path was allocating intermediate `Value` types:
```
executor.lookup() → Vec<Vec<DataType>>
                  → convert to Vec<Vec<Value>>  ← WASTEFUL
                  → RowsContainer
                  → C++ calls noria_get_value
```

**Solution**: Store `DataType` directly without intermediate allocation:
- Added `DataRowsContainer` with type discriminant
- `noria_get_value()` converts `DataType` → `NoriaValue` on-demand
- Used `#[repr(C)]` to ensure predictable memory layout for type detection

```rust
#[repr(C)]
struct DataRowsContainer {
    container_type: u8,  // CONTAINER_TYPE_DIRECT = 1
    rows: Vec<Vec<DataType>>,
}

fn datatype_to_noria_value(dt: &DataType, out: &mut NoriaValue) {
    // Direct conversion without intermediate Value allocation
}
```

### 2. Batch Lookup API

**Files**: `noria-ffi/src/lib.rs`, `noria-ffi/noria.h`

**Problem**: Each lookup incurs FFI crossing overhead and lock acquisition.

**Solution**: Batch multiple lookups in single FFI call:
```rust
pub extern "C" fn noria_lookup_batch(
    handle: *mut NoriaHandle,
    view_id: c_int,
    keys: *const *const NoriaValue,
    key_counts: *const c_int,
    num_keys: c_int,
) -> NoriaBatchLookupResult
```

Benefits:
- Single lock acquisition for all lookups
- Amortized FFI crossing overhead
- Pre-allocated result vector

### 3. Direct Integer Array Indexing (IntegerArrayState)

**Files**: `noria-core/src/dataflow/state.rs`

**Problem**: HashMap lookup has O(log n) average case with hash computation overhead.

**Solution**: For single integer primary keys, use direct array indexing:
```rust
pub struct IntegerArrayState {
    key_column: usize,
    key_offset: i64,
    data: Vec<Option<Vec<Vec<DataType>>>>,  // data[key - offset] = rows
    row_count: usize,
}

fn lookup(&self, key: &[DataType]) -> LookupResult {
    let idx = key_val - self.key_offset;
    // O(1) direct array access
    match &self.data[idx as usize] {
        Some(rows) => LookupResult::Some(rows.iter().map(|r| r.as_slice()).collect()),
        None => LookupResult::Missing,
    }
}
```

Benefits:
- O(1) lookup vs O(log n) HashMap
- No hash computation
- Better cache locality

### 4. DynamicState Auto-Detection

**Files**: `noria-core/src/dataflow/state.rs`

**Problem**: Need to choose optimal state implementation based on key type.

**Solution**: Auto-detect on first record:
```rust
pub struct DynamicState {
    key_columns: Vec<usize>,
    inner: DynamicStateInner,
}

enum DynamicStateInner {
    Uninitialized,
    IntegerArray(IntegerArrayState),  // For integer PKs
    HashMap(MemoryState),              // For other keys
}
```

### 5. Compile-Time Conditional Profiling

**Files**: `noria-core/src/profiling.rs`, various `Cargo.toml`

**Problem**: Instrumented profiling had 59.6% overhead.

**Solution**: Make profiling compile-time conditional:
```rust
#[cfg(feature = "profiling")]
mod enabled { /* full implementation */ }

impl ProfileGuard {
    #[inline(always)]
    pub fn new(_name: &str) -> Self {
        #[cfg(feature = "profiling")]
        { Profiler::enter(_name); Self { _active: true } }
        #[cfg(not(feature = "profiling"))]
        Self {}  // Zero overhead when disabled
    }
}
```

### 6. LTO (Link-Time Optimization)

**Files**: `noria-ffi/Cargo.toml`

```toml
[profile.release]
lto = "fat"
codegen-units = 1
panic = "abort"
opt-level = 3
```

### 7. Lock-Free Views with ArcSwap

**Files**: `noria-ffi/src/lib.rs`, `noria-ffi/Cargo.toml`

**Problem**: RwLock acquisition overhead for every view lookup (~1% overhead from perf profiling).

**Solution**: Use `arc-swap` crate for wait-free reads on the views HashMap:
```rust
use arc_swap::ArcSwap;
use std::sync::Arc;

pub struct NoriaHandle {
    executor: RwLock<LocalExecutor>,  // Still needs RwLock for writes
    views: ArcSwap<HashMap<i32, NoriaViewEntry>>,  // Wait-free reads
}
```

### 8. Pre-Update Hook CDC (Replacing Session Extension)

**Files**: `noria.cpp`, `noria-ffi/src/lib.rs`, `noria.h`

**Problem**: Session extension CDC had 43% overhead due to:
- SQL parsing on every write (`sessionSelectStmt` - 14.93%)
- Schema queries (`sessionReinitTable` - 9.84%)

**Solution**: Replace with SQLite pre-update hook:
```cpp
void EnablePreUpdateHook() {
    sqlite3_preupdate_hook(db_, PreUpdateHookCallback, this);
    sqlite3_rollback_hook(db_, RollbackHookCallback, this);
}

void ProcessPreUpdateChange(const char* table_name, int op) {
    // Direct value extraction - no SQL parsing
    sqlite3_preupdate_old(db_, i, &val);
    sqlite3_preupdate_new(db_, i, &val);
    // Queue CDC event directly
}
```

**Results**:
- Write performance: 45K → 178K ops/sec (**4x faster**)
- Write overhead: 7x → 1.7x slower than vanilla SQLite
- 95/5 workload: parity → **1.26x speedup**

### 9. Arc-Wrapped Rows in State

**Files**: `noria-core/src/dataflow/state.rs`, `noria-sqlite/src/dataflow/*.rs`, `noria-ffi/src/lib.rs`

**Problem**: Every lookup was deep-copying row data:
```rust
// Old code - expensive .to_vec() on every lookup
fn lookup(&self, key: &[DataType]) -> LookupResult {
    match self.data.get(key) {
        Some(rows) => LookupResult::Some(rows.iter().map(|r| r.to_vec()).collect()),
        ...
    }
}
```

Profiling showed `spec_from_iter_nested` (Vec allocation from iterators) taking ~2.8% of execution time.

**Solution**: Store rows as `Arc<Vec<DataType>>` so lookups clone Arc references (O(1) ref count increment) instead of deep copying:
```rust
/// A row stored in state - Arc-wrapped for cheap cloning on lookup.
pub type Row = Arc<Vec<DataType>>;

// Now cloning is O(1) - just increment reference count
fn lookup(&self, key: &[DataType]) -> LookupResult {
    match self.data.get(key) {
        Some(rows) => LookupResult::Some(rows.iter().cloned().collect()),
        ...
    }
}
```

**Results**:
- Single-key read: 800K → 884K ops/sec (+10%)
- Read 99/1: 290K → 351K ops/sec (+21%)
- Read 95/5: 110K → 143K ops/sec (+30%)
- Eliminated `spec_from_iter_nested` from hot path in profiling

### 10. Integer Key Fast Path

**Files**: `noria-ffi/src/lib.rs`, `noria-ffi/noria.h`, `src/util/noria.cpp`, `src/objects/statement.cpp`

**Problem**: Most database lookups use integer primary keys, but the FFI path converts through generic `NoriaValue`:
```cpp
// Old path - always goes through NoriaValue
std::vector<NoriaValue> keys(param_count);
keys[0] = V8ToNoriaKey(isolate, arg, string_storage[0]);  // Struct construction
result = noria->LookupOrUpquery(view_id, keys.data(), 1); // FFI call
// Rust side: noria_value_to_datatype() conversion
```

**Solution**: Dedicated integer key FFI function that takes `i64` directly:
```rust
#[no_mangle]
pub extern "C" fn noria_lookup_int_key(
    handle: *mut NoriaHandle,
    view_id: c_int,
    key: i64,  // Direct integer, no NoriaValue conversion
) -> NoriaLookupResult {
    let key_dt = [DataType::BigInt(key)];  // Stack allocation, no Vec
    // ... lookup
}
```

```cpp
// C++ fast path for integer keys
if (param_count == 1 && arg->IsInt32()) {
    int64_t int_key = arg.As<v8::Int32>()->Value();
    result = noria->LookupIntKey(view_id, int_key);  // Fast path
    if (result.found) return true;  // Cache hit
    // Fall through to regular path only on miss (for upquery)
}
```

**Results**:
- Single-key read: 884K → 996K ops/sec (+12.7%)
- Speedup improved from 2.67x to **3.08x**
- `noria_lookup_int_key` visible in profiling at 2.25% (confirming usage)

---

## C++/JS Batch API Integration

### stmt.getMany() - Batch Lookup

**Files**: `statement.cpp`, `statement.hpp`, `noria.cpp`, `statement.js`

**Feature**: Batch lookup for multiple keys in a single FFI call:
```javascript
const stmt = db.prepare('SELECT * FROM users WHERE id = ?');

// Single FFI call for multiple lookups
const users = stmt.getMany([1, 2, 3, 4, 5]);
// Returns: [{ id: 1, ... }, { id: 2, ... }, ...]

// With fresh option
const freshUsers = stmt.getMany([1, 2, 3], { fresh: true });
```

**Benefits**:
- Single lock acquisition for all lookups
- Amortized FFI crossing overhead
- ~1.1-1.2x speedup over individual `get()` calls (when cache is warm)

---

## Future Optimization Opportunities

### Current Hot Path Profile (95/5 workload, January 2026)

| Area | % Time | Functions | Description |
|------|--------|-----------|-------------|
| V8 Object Creation | ~20% | `NoriaRowToJS`, `v8::Object::New`, `NameDictionary::Add` | Creating JavaScript objects from cache results |
| Memory allocation | ~7.4% | `malloc`, `cfree` | Vec allocations, V8 object heap allocations |
| Rust cloning/iteration | ~5% | `.to_vec()`, `.collect()`, iterator folding | Data copying in lookup path |
| FFI overhead | ~3% | `noria_lookup`, `noria_get_value` | Multiple FFI boundary crossings per row |
| SQLite operations | ~22% | `sqlite3VdbeExec`, `sqlite3BtreeInsert` | Actual database work (unavoidable baseline) |

### Current Data Flow (Cache Hit Path)

```
JS stmt.get(key)
    │
    ▼
C++ TryNoriaGet()
    │
    ▼ FFI call #1
Rust noria_lookup()
    │
    ▼ RwLock acquire (parking_lot)
    Rust executor.lookup()
        │
        ▼ key conversion: Vec<DataType> allocation
        State::lookup()
            │
            ▼ HashMap/Array lookup
            returns LookupResult::Some(Vec<&[DataType]>)
        │
        ▼ .to_vec() CLONES EACH ROW - First copy
    │
    ▼ return Vec<Vec<DataType>>
    ▼ Box::new(DataRowsContainer) - Heap allocation
    ▼ RwLock release
    │
C++ receives rows_ptr
    │
    ▼ for each column (N iterations):
        │
        ▼ FFI call #2..N+1
        noria_get_value() - extracts single value
    │
    ▼ NoriaRowToJS()
        │
        ▼ v8::Object::New(isolate)
        ▼ for each column:
            ▼ NameDictionary::Add() - V8 hidden class transition
            ▼ String creation for column name
            ▼ Value conversion (NoriaValue → v8::Value)
    │
    ▼
JS receives { col1: val1, col2: val2, ... }
```

### Key Problems Identified

1. **Data copied 3 times**: Rust state → `Vec<Vec<DataType>>` → V8 Object properties
2. **N+1 FFI calls per row**: 1 lookup call + N `noria_get_value` calls
3. **V8 object creation per query**: Even for identical schemas, full object construction
4. **Lock on every read**: RwLock acquisition adds latency, limits concurrency
5. **No schema reuse**: Column names re-processed on every result

---

## Detailed Future Optimization Ideas

### 1. V8 Object Templates (Pre-compiled Object Shapes)

**Priority**: HIGH | **Impact**: ~20% faster reads | **Complexity**: Medium

#### Problem
For a given query, the result schema (column names, types) is always identical. Yet we create objects from scratch every time, triggering V8's NameDictionary operations and hidden class transitions.

#### Current Code
```cpp
// In NoriaRowToJS():
v8::Local<v8::Object> row = v8::Object::New(isolate);
for (int i = 0; i < col_count; ++i) {
    row->Set(ctx, column_names[i], value).FromJust();  // Triggers hidden class transition
}
```

#### Proposed Solution
Pre-create an `ObjectTemplate` for each view at registration time. V8's ObjectTemplate allows pre-defining property names, enabling fast instantiation without dictionary operations.

```cpp
// At view registration (once):
class ViewTemplate {
    v8::Global<v8::ObjectTemplate> template_;
    std::vector<std::string> column_names_;

public:
    void Initialize(v8::Isolate* isolate, const std::vector<std::string>& columns) {
        v8::Local<v8::ObjectTemplate> tmpl = v8::ObjectTemplate::New(isolate);
        tmpl->SetInternalFieldCount(1);  // For fast property access

        for (size_t i = 0; i < columns.size(); i++) {
            // Pre-define all properties with indexed accessors
            tmpl->SetAccessor(
                v8::String::NewFromUtf8(isolate, columns[i].c_str()).ToLocalChecked(),
                PropertyGetter,  // Fast native getter
                nullptr,         // Read-only
                v8::Integer::New(isolate, i)  // Column index as data
            );
        }
        template_.Reset(isolate, tmpl);
        column_names_ = columns;
    }

    v8::Local<v8::Object> NewInstance(v8::Isolate* isolate, void* row_data) {
        auto tmpl = template_.Get(isolate);
        auto obj = tmpl->NewInstance(isolate->GetCurrentContext()).ToLocalChecked();
        obj->SetAlignedPointerInInternalField(0, row_data);  // Store data pointer
        return obj;
    }
};

// Property getter - called lazily on property access
static void PropertyGetter(v8::Local<v8::Name> property,
                          const v8::PropertyCallbackInfo<v8::Value>& info) {
    int col_idx = info.Data().As<v8::Integer>()->Value();
    void* row_data = info.Holder()->GetAlignedPointerFromInternalField(0);
    // Extract value from row_data[col_idx]
    info.GetReturnValue().Set(ExtractValue(row_data, col_idx));
}
```

#### Alternative: Fast Object Construction with Pre-internalized Strings
```cpp
// Cache internalized strings for column names (per view)
class CachedColumnNames {
    std::vector<v8::Global<v8::Name>> names_;

public:
    void Initialize(v8::Isolate* isolate, const std::vector<std::string>& columns) {
        for (const auto& col : columns) {
            auto str = v8::String::NewFromUtf8(isolate, col.c_str()).ToLocalChecked();
            auto name = str->ToName(isolate->GetCurrentContext()).ToLocalChecked();
            names_.emplace_back(isolate, name);
        }
    }
};

// Usage - single Object::New call with pre-internalized names:
v8::Local<v8::Object> obj = v8::Object::New(
    isolate,
    v8::Null(isolate),       // prototype
    cached_names.data(),      // Pre-internalized names
    values.data(),            // Values array
    col_count
);
```

#### Expected Gains
- Eliminate NameDictionary::Add calls (~4% of profile)
- Reduce string interning overhead
- Enable V8 to use fast-path object creation
- **Estimated: 15-25% faster cache hit reads**

---

### 2. Batch FFI / Serialized Result Return

**Priority**: HIGH | **Impact**: ~10% faster | **Complexity**: Low

#### Problem
Currently we make N+1 FFI calls per row:
1. `noria_lookup()` - returns opaque pointer
2. `noria_get_value()` × N - called for each column

FFI calls have overhead: argument marshaling, stack setup, potential cache invalidation.

#### Proposed Solution A: Bulk Value Extraction
Return all values in a single FFI call.

```rust
// New FFI function - returns all values for a row at once
#[repr(C)]
pub struct NoriaRowData {
    col_count: c_int,
    values: [NoriaValue; 32],  // Fixed-size array, covers most queries
}

#[no_mangle]
pub extern "C" fn noria_get_row(
    rows_ptr: *mut c_void,
    row_index: c_int,
    out_data: *mut NoriaRowData,
) -> c_int {
    // Fill all values in one call
    let container = unsafe { &*(rows_ptr as *const DataRowsContainer) };
    let row = &container.rows[row_index as usize];

    unsafe {
        (*out_data).col_count = row.len() as c_int;
        for (i, val) in row.iter().enumerate().take(32) {
            (*out_data).values[i] = datatype_to_noria_value(val);
        }
    }
    0
}
```

```cpp
// C++ usage - single FFI call per row:
NoriaRowData row_data;
noria_get_row(result.rows, row_index, &row_data);  // Single FFI call
for (int col = 0; col < row_data.col_count; col++) {
    // Use row_data.values[col] directly
}
```

#### Proposed Solution B: Serialized Binary Format
Serialize entire result to binary, deserialize in C++.

```rust
#[repr(C, packed)]
struct SerializedResult {
    row_count: u32,
    col_count: u32,
    // Followed by: [row_0_values...][row_1_values...]
    // Each value: [type: u8][data: variable]
}

#[no_mangle]
pub extern "C" fn noria_lookup_serialized(
    handle: *mut NoriaHandle,
    view_id: c_int,
    key_values: *const NoriaValue,
    key_count: c_int,
    out_buffer: *mut u8,
    buffer_size: c_int,
) -> c_int {
    // Returns bytes written, or -1 if buffer too small
    let result = do_lookup(...);
    serialize_to_buffer(result, out_buffer, buffer_size)
}
```

```cpp
// C++ - pre-allocated buffer, single FFI call
thread_local std::vector<uint8_t> lookup_buffer(64 * 1024);  // 64KB thread-local

int bytes = noria_lookup_serialized(handle, view_id, keys, key_count,
                                     lookup_buffer.data(), lookup_buffer.size());
// Deserialize directly to V8 objects
```

#### Expected Gains
- Reduce FFI calls from N+1 to 1-2
- Better CPU cache utilization (single call vs scattered)
- **Estimated: 5-15% faster cache hit reads**

---

### 3. Lock-Free State with Epoch-Based Reclamation

**Priority**: MEDIUM-HIGH | **Impact**: 10-30% for mixed workloads | **Complexity**: Medium

#### Problem
Every read acquires an RwLock:
```rust
let executor = handle.executor.read();  // Lock acquisition
executor.lookup(&view_handle, &key)
// Lock released
```

Even with `parking_lot`'s efficient implementation, this adds latency and limits scalability under write contention.

#### Proposed Solution: Epoch-Based Reclamation
Use `crossbeam-epoch` for truly lock-free reads.

```rust
use crossbeam_epoch::{self as epoch, Atomic, Owned, Shared};

struct LockFreeExecutor {
    // Atomic pointer to current state snapshot
    current: Atomic<ExecutorSnapshot>,
}

struct ExecutorSnapshot {
    nodes: Vec<Node>,
    // ... immutable snapshot of executor state
}

impl LockFreeExecutor {
    fn lookup(&self, view: &ViewHandle, key: &[DataType]) -> Option<Vec<Vec<DataType>>> {
        // Pin current epoch - prevents reclamation while we're reading
        let guard = epoch::pin();

        // Load current snapshot (lock-free atomic load)
        let snapshot = self.current.load(Ordering::Acquire, &guard);
        let snapshot_ref = unsafe { snapshot.as_ref()? };

        // Perform lookup on immutable snapshot
        snapshot_ref.lookup(view, key)

        // guard dropped here - epoch unpinned
    }

    fn apply_write(&self, table: &str, records: Records) {
        // Create new snapshot with modifications
        let guard = epoch::pin();
        let old = self.current.load(Ordering::Acquire, &guard);
        let old_ref = unsafe { old.as_ref().unwrap() };

        // Clone-on-write: create modified copy
        let mut new_snapshot = old_ref.clone();
        new_snapshot.apply_write_internal(table, records);

        // Atomically swap to new snapshot
        let new_owned = Owned::new(new_snapshot);
        let old = self.current.swap(new_owned, Ordering::AcqRel, &guard);

        // Defer reclamation of old snapshot
        unsafe {
            guard.defer_destroy(old);
        }
    }
}
```

#### Alternative: ArcSwap with Copy-on-Write (Simpler)
```rust
use arc_swap::ArcSwap;

struct CowExecutor {
    state: ArcSwap<ExecutorState>,
}

impl CowExecutor {
    fn lookup(&self, view: &ViewHandle, key: &[DataType]) -> Option<...> {
        // Lock-free load of Arc
        let state = self.state.load();
        state.lookup(view, key)
    }

    fn apply_write(&self, table: &str, records: Records) {
        // Read-modify-write with retry loop
        loop {
            let old = self.state.load();
            let mut new = (**old).clone();  // Clone state
            new.apply_write_internal(table, records);

            // Try to swap atomically
            if self.state.compare_and_swap(&old, Arc::new(new)).ptr_eq(&old) {
                break;
            }
            // Retry if another write happened concurrently
        }
    }
}
```

#### Expected Gains
- Zero lock overhead on reads
- Better scalability under concurrent load
- Reduced latency variance (no lock contention spikes)
- **Estimated: 10-30% faster in write-heavy scenarios**

---

### 4. Columnar Storage Format

**Priority**: MEDIUM | **Impact**: 2-3x lookup speed | **Complexity**: High

#### Problem
Current storage is row-oriented: `Vec<Vec<DataType>>`. This causes:
- Poor cache locality (each row is separate allocation)
- Can't use SIMD operations
- Cloning requires per-row allocation

#### Proposed Solution: Columnar Storage
Store data column-by-column with typed arrays.

```rust
/// Columnar storage for a single key's rows
struct ColumnarRows {
    row_count: usize,
    columns: Vec<TypedColumn>,
}

enum TypedColumn {
    /// Contiguous i64 array - excellent cache locality
    Integers(Vec<i64>),

    /// Contiguous f64 array
    Floats(Vec<f64>),

    /// String data with arena allocation
    Strings {
        /// Concatenated string data
        data: Vec<u8>,
        /// (offset, length) pairs for each row
        offsets: Vec<(u32, u32)>,
    },

    /// Blob data with arena allocation
    Blobs {
        data: Vec<u8>,
        offsets: Vec<(u32, u32)>,
    },

    /// Nullable wrapper
    Nullable {
        inner: Box<TypedColumn>,
        null_bitmap: BitVec,
    },
}

impl ColumnarRows {
    /// Get a single row (for compatibility)
    fn get_row(&self, row_idx: usize) -> Vec<DataType> {
        self.columns.iter()
            .map(|col| col.get(row_idx))
            .collect()
    }

    /// Get a single column value (very fast)
    fn get_value(&self, row_idx: usize, col_idx: usize) -> DataType {
        self.columns[col_idx].get(row_idx)
    }

    /// Iterate over all rows (zero-copy for fixed-size types)
    fn iter(&self) -> impl Iterator<Item = RowRef<'_>> {
        (0..self.row_count).map(move |i| RowRef { storage: self, row: i })
    }
}

/// Zero-copy row reference
struct RowRef<'a> {
    storage: &'a ColumnarRows,
    row: usize,
}
```

#### Benefits for SIMD Operations
```rust
impl TypedColumn {
    /// Sum all integers in column (SIMD-friendly)
    fn sum_integers(&self) -> i64 {
        match self {
            TypedColumn::Integers(data) => {
                // Auto-vectorized by LLVM
                data.iter().sum()
            }
            _ => panic!("Not an integer column"),
        }
    }
}
```

#### Expected Gains
- Cache-friendly memory layout
- Zero-copy row access (return references)
- SIMD-friendly operations for aggregates
- Reduced memory fragmentation
- **Estimated: 2-3x faster lookups, 2-5x faster aggregations**

---

### 5. Direct V8 Object Storage

**Priority**: LOW (complexity) | **Impact**: 3-5x faster reads | **Complexity**: Very High

#### Radical Idea
Store V8 objects directly in the cache. On lookup, return a clone of the stored object (very fast in V8).

```cpp
class V8ObjectCache {
    v8::Isolate* isolate_;
    std::unordered_map<std::string, v8::Global<v8::Object>> cache_;

public:
    v8::Local<v8::Object> Lookup(int view_id, const std::string& key) {
        auto it = cache_.find(MakeKey(view_id, key));
        if (it != cache_.end()) {
            // Return a copy of the cached object - very fast!
            return it->second.Get(isolate_)->Clone();
        }
        return v8::Local<v8::Object>();
    }
};
```

#### Challenges
1. **Isolate Affinity**: V8 objects are tied to a single Isolate
2. **Memory Management**: Persistent handles prevent GC
3. **Invalidation Complexity**: CDC events must invalidate correct cache entries

#### Expected Gains
- Near-zero conversion overhead on cache hits
- **Estimated: 3-5x faster cache hit reads**

---

### 6. JIT Compilation for Queries

**Priority**: LOW (experimental) | **Impact**: 5-10x for simple queries | **Complexity**: Very High

#### Concept
The dataflow graph structure is known at view registration time. Compile specialized native code for each view's lookup operation.

```rust
/// At view registration, generate specialized lookup function
fn compile_integer_key_single_row_lookup(
    view: &View,
) -> Box<dyn Fn(&IntegerArrayState, i64) -> Option<&[DataType]>> {
    Box::new(move |state: &IntegerArrayState, key: i64| {
        // Direct array access - no HashMap, no key conversion
        let idx = (key - state.base_key) as usize;
        state.data.get(idx)?.as_ref().map(|rows| rows[0].as_slice())
    })
}
```

#### Expected Gains
- Eliminate all abstraction overhead
- CPU branch prediction friendly
- **Estimated: 5-10x faster for simple queries**

---

### 7. JSON Serialization Path

**Priority**: MEDIUM | **Impact**: Variable (benchmark needed) | **Complexity**: Low

#### Concept
V8's native JSON.parse() is highly optimized. It might be faster to serialize results to JSON in Rust (using simd-json) and let V8 parse natively.

```rust
use simd_json;

#[no_mangle]
pub extern "C" fn noria_lookup_json(
    handle: *mut NoriaHandle,
    view_id: c_int,
    key_values: *const NoriaValue,
    key_count: c_int,
    out_json: *mut c_char,
    json_buffer_size: c_int,
) -> c_int {
    let result = do_lookup(...)?;
    let json = simd_json::to_string(&result).unwrap();
    // Copy to output buffer
}
```

```javascript
const jsonStr = noria.lookupJson(viewId, key);
const result = JSON.parse(jsonStr);  // V8's native JSON parser
```

#### Expected Gains
- Leverages V8's highly optimized JSON parser
- Single FFI call
- **Must benchmark to confirm**

---

### 8. Deferred Write Propagation

**Priority**: MEDIUM | **Impact**: High for write-heavy | **Complexity**: Medium

#### Current Behavior
```
INSERT → CDC captured → Queue → Commit → noria_flush() → Propagate (synchronous, blocks)
```

#### Proposed: Background Propagation Thread
```
INSERT → CDC captured → Queue → Commit → Return immediately
                                        ↓
                          Background thread: propagate events
                                        ↓
                          Update version counter
```

```rust
struct AsyncPropagator {
    queue: crossbeam_channel::Sender<CdcEvent>,
    version: AtomicU64,
}
```

#### API for Fresh Reads
```javascript
// Default: may return slightly stale data (fast)
const result = stmt.get(1);

// Force fresh read (waits for propagation)
const result = stmt.get(1, { fresh: true });
```

#### Expected Gains
- Write operations complete faster
- **Estimated: 2-5x faster writes in write-heavy workloads**

---

### 9. Append-Only Log Architecture

**Priority**: LOW (major rework) | **Impact**: High scalability | **Complexity**: Very High

#### Concept
Replace in-place state updates with append-only log + periodic compaction.

```
┌─────────────────────────────────────────────────────────┐
│                    Append-Only Log                       │
│  [INSERT users (1,'Alice')][UPDATE users (1,'Bob')]...  │
└─────────────────────────────────────────────────────────┘
                           │
                           │ Async compaction (background)
                           ▼
┌─────────────────────────────────────────────────────────┐
│              Materialized View Snapshot                  │
│  (Immutable - readers see consistent point-in-time)     │
└─────────────────────────────────────────────────────────┘
```

#### Benefits
- Readers never block (always read from immutable snapshot)
- Writers just append (very fast)
- Natural point-in-time queries
- **Near-linear scalability with concurrent readers**

---

### 10. Specialized Integer Key Fast Path

**Priority**: HIGH | **Impact**: 2x for integer keys | **Complexity**: Low

#### Observation
Most database primary keys are integers. We already have `IntegerArrayState` but the FFI path still converts through generic `NoriaValue`.

#### Proposed: Dedicated Integer Key FFI
```rust
#[no_mangle]
pub extern "C" fn noria_lookup_int_key(
    handle: *mut NoriaHandle,
    view_id: c_int,
    key: i64,  // Direct integer, no NoriaValue conversion
) -> NoriaLookupResult {
    let key_dt = DataType::BigInt(key);
    // ... rest of lookup
}
```

```cpp
// C++ fast path for integer keys
if (key_count == 1 && IsInteger(keys[0])) {
    return noria_lookup_int_key(handle, view_id, GetIntValue(keys[0]));
}
```

#### Expected Gains
- Skip NoriaValue conversion for most queries
- **Estimated: 10-20% faster for integer key lookups**

---

## Implementation Priority Matrix

| # | Optimization | Impact | Complexity | Status |
|---|-------------|--------|------------|--------|
| 10 | Integer Key Fast Path | +12% single-key | Low | ✅ Complete |
| 2 | Batch FFI (noria_get_row) | ~10% | Low | ✅ Complete |
| 1 | V8 Object Templates | ~20% faster reads | Medium | **Next** |
| 3 | Lock-Free State | Medium-High (mixed) | Medium | Planned |
| 7 | JSON Serialization | Variable | Low | Benchmark first |
| 8 | Deferred Propagation | High (writes) | Medium | Planned |
| 4 | Columnar Storage | High (2-3x) | High | Future |
| 5 | V8 Object Storage | Very High (3-5x) | Very High | Research |
| 6 | JIT Compilation | Very High (5-10x) | Very High | Research |
| 9 | Append-Only Log | High scalability | Very High | Future |

---

## Profiling Notes

### Hardware-Sampled Profiling (perf)

Use `perf` for accurate profiling with <1% overhead:
```bash
sudo perf record -g --call-graph dwarf -F 999 -- node benchmark.js
sudo perf report -f --stdio --no-children -g none --percent-limit 1
```

### Benchmarking Methodology

1. **Micro-benchmarks**: Isolate specific operations
   ```javascript
   for (let i = 0; i < 1000000; i++) {
       stmt.get(i % 100);
   }
   ```

2. **Lobsters benchmark**: Realistic mixed workload
   ```bash
   npm run bench:lobsters
   ```

3. **Memory tracking**:
   ```bash
   valgrind --tool=massif node benchmark.js
   ```

---

## Test Results

- **Node.js Tests**: 348 passing
- **Rust Tests**: All passing

---

## Historical Performance Progression

| Date | Single-key Speedup | Notes |
|------|-------------------|-------|
| Initial | 1.85x | Before optimizations |
| +Profiling disabled | 2.03x | Broke 2x barrier |
| +Zero-copy | 2.37x | Eliminated Value allocation |
| +IntegerArrayState | 2.46x | O(1) integer lookups (noria-core only) |
| +noria-sqlite integration | 2.65x | Fixed: noria-sqlite now uses noria-core's DynamicState |
| +ArcSwap views | 2.63x | Lock-free view lookups (within margin of error) |
| +Pre-update hook CDC | 2.79x | Replaced session extension, 4x faster writes |
| +Arc-wrapped rows | 2.67x | O(1) row cloning instead of deep copy |
| +Integer key fast path | **3.08x** | Skips NoriaValue conversion for integer PKs |

---

## References

- [V8 Object Templates Documentation](https://v8.dev/docs/embed#templates)
- [crossbeam-epoch: Epoch-based memory reclamation](https://docs.rs/crossbeam-epoch)
- [simd-json: High-performance JSON](https://github.com/simd-lite/simd-json)
- [Apache Arrow: Columnar format](https://arrow.apache.org/)
- [Original Noria Paper (OSDI'18)](https://www.usenix.org/conference/osdi18/presentation/gjengset)
