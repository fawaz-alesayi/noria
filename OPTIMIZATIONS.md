# Noria-SQLite Performance Optimizations

This document tracks all performance optimizations implemented to maximize Noria's caching speedup.

## Summary

| Optimization | Status | Impact |
|--------------|--------|--------|
| Zero-copy result returns | Complete | ~15% speedup |
| Batch lookup API (FFI) | Complete | Reduces FFI overhead for bulk ops |
| Direct integer array indexing | Complete | O(1) vs O(log n) for integer PKs |
| DynamicState auto-detection | Complete | Auto-selects optimal state impl |
| noria-sqlite uses noria-core state | **Fixed** | +0.19x speedup (was not integrated) |
| `#[repr(C)]` container layout | Complete | Fixed type detection across FFI |
| Compile-time conditional profiling | Complete | Zero overhead when disabled |
| LTO (Link-Time Optimization) | Complete | Cross-crate inlining |
| C++/JS Batch API (getMany) | **Complete** | 1.1-1.2x speedup for batch lookups |
| Lock-free views with ArcSwap | **Complete** | Wait-free reads, ~1% improvement |

## Current Performance (Lobsters Benchmark)

| Scenario | better-sqlite3 | noria | Speedup |
|----------|----------------|-------|---------|
| Single-key read | 298,559 | 790,217 | **2.65x** |
| Read-only mixed | 296,168 | 558,192 | **1.88x** |
| Read 99/1 | 204,648 | 307,950 | **1.50x** |
| Read 95/5 | 110,909 | 115,983 | **1.05x** |
| Read 90/10 | 63,466 | 54,226 | 0.85x |

Cache throughput: **>800K ops/sec**

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

---

## Completed: C++/JS Batch API Integration

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

**Implementation**:
- Falls back to SQLite if any key misses the cache
- Returns `undefined` for non-existent keys

---

### 7. Lock-Free Views with ArcSwap

**Files**: `noria-ffi/src/lib.rs`, `noria-ffi/Cargo.toml`

**Problem**: RwLock acquisition overhead for every view lookup (~1% overhead from perf profiling).

**Solution**: Use `arc-swap` crate for wait-free reads on the views HashMap:
```rust
use arc_swap::ArcSwap;
use std::sync::Arc;

#[derive(Clone)]
struct NoriaViewEntry {
    handle: ViewHandle,
    sql: String,
    tables: Vec<String>,
}

pub struct NoriaHandle {
    executor: RwLock<LocalExecutor>,  // Still needs RwLock for writes
    views: ArcSwap<HashMap<i32, NoriaViewEntry>>,  // Wait-free reads
    // ...
}
```

**Read path** (wait-free):
```rust
let views = handle.views.load();  // No lock, just atomic load
if let Some(entry) = views.get(&view_id) {
    // Fast path - no contention
}
```

**Write path** (clone-modify-store):
```rust
let mut views = (**handle.views.load()).clone();
views.insert(view_id, NoriaViewEntry { ... });
handle.views.store(Arc::new(views));
```

**Benefits**:
- Wait-free reads (no lock contention under concurrent load)
- Atomic pointer swap for writes (no writer starvation)
- ~1% improvement in single-threaded scenarios; more significant under concurrent read load

---

## Pending Optimizations

(No pending optimizations at this time)

---

## Profiling Notes

### Hardware-Sampled Profiling (perf)

Use `perf` for accurate profiling with <1% overhead:
```bash
sudo perf record -g -F 999 -- node benchmark.js
sudo perf report --stdio --no-children --percent-limit=1
```

### Current Bottleneck Breakdown (perf)

| Category | Overhead | Notes |
|----------|----------|-------|
| SQLite | ~11.5% | VdbeExec, BtreeMoveto, WAL checksums |
| V8/JS | ~8.0% | Object::New, StringTable, AllocateRaw |
| Kernel | ~7.4% | syscalls, copy_from_user, kmem_cache |
| Noria | ~3.4% | LocalExecutor::lookup, DynamicState, FFI |
| malloc/free | ~3.0% | Shared across all components |

**Key observations**:
- No RwLock overhead visible (ArcSwap eliminated it)
- Noria cache path is very efficient (~3.4% total)
- V8 object creation is significant overhead for JS integration
- SQLite dominates when upqueries/writes are involved

---

## Test Results

- **Node.js Tests**: 348 passing
- **Rust Tests**: All passing including new IntegerArrayState tests

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
