# Noria-SQLite

Embed Noria's differential dataflow engine as a transparent, in-process caching layer for SQLite.

## Project Vision

- **Drop-in better-sqlite3 replacement** with automatic query acceleration
- **Zero configuration**: No external processes, no schema files, no manual view definitions
- **Database-agnostic core**: `noria-core` enables future support for MySQL, Postgres, and more
- **Future targets**: rqlite (distributed SQLite), litestream (SQLite replication)

## Core Principles

1. **Single Source of Truth**: Database owns disk, Noria owns RAM cache
2. **Eventual Consistency**: Async batch CDC propagation (matches original Noria paper)
3. **`{ fresh: true }` Escape Hatch**: Strong reads when needed
4. **Fail-Safe**: Falls back to database on errors or unsupported queries

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            Application                                   │
│                   (Node.js via noria-better-sqlite3)                    │
├─────────────────────────────────────────────────────────────────────────┤
│  db.prepare("SELECT * FROM users WHERE id = ?")                         │
│       ↓                                                                 │
│  ┌─────────────────────┐      ┌─────────────────────────────────┐      │
│  │   Dynamic View      │      │     Statement Execution         │      │
│  │   Synthesis         │      │  ┌─────────┐    ┌───────────┐   │      │
│  │  (on first prepare) │      │  │ Cache   │ OR │ Upquery   │   │      │
│  │                     │      │  │ Hit O(1)│    │ (SQLite)  │   │      │
│  └─────────────────────┘      │  └─────────┘    └───────────┘   │      │
├─────────────────────────────────────────────────────────────────────────┤
│                     C++ Noria Wrapper (noria.cpp)                       │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────────┐     │
│  │ View Registration│  │ Cache Lookup     │  │ Batch CDC Queue   │     │
│  │ (RegisterView)   │  │ (LookupOrUpquery)│  │ (queue + flush)   │     │
│  └──────────────────┘  └──────────────────┘  └───────────────────┘     │
├─────────────────────────────────────────────────────────────────────────┤
│                      Rust FFI Layer (noria-ffi)                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  Write queue with batch processing (async CDC from Noria paper)  │  │
│  │  noria_queue_insert/delete/update → noria_flush()                │  │
│  └──────────────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────────┤
│                         noria-core (Rust)                               │
│  ┌────────────────┐ ┌────────────────┐ ┌────────────────────────────┐  │
│  │ LocalExecutor  │ │ SqlConverter   │ │ DatabaseAdapter trait      │  │
│  │ (dataflow DAG) │ │ (sqlparser-rs) │ │ CdcSource trait            │  │
│  ├────────────────┤ ├────────────────┤ ├────────────────────────────┤  │
│  │ Operators:     │ │ Multi-dialect: │ │ Implementations:           │  │
│  │ Filter,Project │ │ SQLite,Postgres│ │ - SqliteAdapter (current)  │  │
│  │ Join,Aggregate │ │ MySQL,Generic  │ │ - PostgresAdapter (future) │  │
│  └────────────────┘ └────────────────┘ │ - MySqlAdapter (future)    │  │
│                                        └────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────────┤
│                    Database (Source of Truth)                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  SQLite: Preupdate hook captures INSERT/UPDATE/DELETE (in C++)  │   │
│  │  Postgres (future): Logical replication / pg_notify             │   │
│  │  MySQL (future): Binlog replication                             │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

**Write Path (Async Batch CDC)**:
```
db.run("INSERT...") → Database execute → Queue CDC event → Transaction commit
                                                         → noria_flush() batches all events
                                                         → Propagate through dataflow
```

**Read Path**:
```
stmt.get(key) → Check view cache → Hit: return O(1) | Miss: upquery DB → populate cache → return
```

**Transaction Path**:
```
BEGIN → statements execute (CDC queued) → COMMIT → flush queue → batch propagate
                                        → ROLLBACK → discard queue → cache unchanged
```

---

## Crate Architecture

```
noria/
├── noria-core/                    # Database-agnostic dataflow engine
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                 # Public exports, prelude
│       ├── adapter.rs             # DatabaseAdapter, CdcSource traits
│       ├── view_cache.rs          # evmap-backed caching
│       ├── dataflow/
│       │   ├── mod.rs             # Record, Records types
│       │   ├── executor.rs        # LocalExecutor (DAG propagation)
│       │   ├── ops.rs             # Filter, Project, Join, Aggregate
│       │   └── state.rs           # MemoryState, StateKey, IntegerArrayState
│       └── sql/
│           └── mod.rs             # SqlConverter (sqlparser-rs, multi-dialect)
│
├── noria-better-sqlite3/          # Node.js bindings (C++ FFI)
│   ├── noria-ffi/                 # Rust FFI layer
│   │   └── src/lib.rs             # Write queue, batch flush, imports noria-core
│   ├── src/
│   │   ├── util/noria.cpp         # C++ wrapper, preupdate hook CDC
│   │   └── objects/               # Database/Statement integration
│   └── lib/                       # JavaScript API
│
└── noria/                         # Original Noria DataType crate
```

**Note**: The `noria-sqlite` crate was removed as it duplicated `noria-core` functionality.
CDC is handled directly in C++ via SQLite's preupdate hook, not the session extension.

---

## Database Adapter Traits

The `noria-core` crate defines traits for pluggable database backends:

```rust
// noria-core/src/adapter.rs

/// Trait for database adapters (schema discovery, upqueries)
pub trait DatabaseAdapter {
    type Error: std::error::Error;

    fn table_schema(&self, table: &str) -> Result<Option<TableSchema>, Self::Error>;
    fn upquery(&self, sql: &str, params: &[DataType]) -> Result<Vec<Vec<DataType>>, Self::Error>;
    fn list_tables(&self) -> Result<Vec<String>, Self::Error>;
}

/// Trait for Change Data Capture sources
pub trait CdcSource {
    fn poll(&mut self) -> Vec<CdcEvent>;
    fn track_table(&mut self, table: &str);
    fn is_active(&self) -> bool;
    fn reset(&mut self);
}

/// CDC event types
pub enum CdcEvent {
    Insert { table: String, row: Vec<DataType> },
    Delete { table: String, row: Vec<DataType> },
    Update { table: String, old: Vec<DataType>, new: Vec<DataType> },
}
```

### Database Support

| Database | CDC Mechanism | Status |
|----------|---------------|--------|
| SQLite | Preupdate hook (C++) | **Implemented** |
| PostgreSQL | Logical replication / LISTEN/NOTIFY | Planned |
| MySQL | Binlog replication | Planned |

---

## Key Files Reference

| File | Purpose | Key Functions |
|------|---------|---------------|
| `noria-core/src/dataflow/executor.rs` | Graph execution | `propagate()`, `apply_write()`, `lookup()` |
| `noria-core/src/dataflow/ops.rs` | Operators | `FilterOp`, `JoinOp`, `AggregateOp` |
| `noria-core/src/dataflow/state.rs` | State storage | `MemoryState`, `IntegerArrayState`, `Row` (Arc-wrapped) |
| `noria-core/src/sql/mod.rs` | SQL parsing | `SqlConverter`, `SqlDialect` |
| `noria-ffi/src/lib.rs` | Rust FFI layer | `noria_queue_*()`, `noria_flush()`, write queue |
| `noria.cpp` | C++ wrapper | `RegisterView()`, `LookupOrUpquery()`, preupdate hook CDC |

---

## Async Batch Processing

Implements the async batch processing from the original Noria paper (OSDI'18):

```
┌─────────────────────────────────────────────────────────────────┐
│                     Write Queue (per transaction)                │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐               │
│  │ INSERT  │ │ INSERT  │ │ UPDATE  │ │ DELETE  │  ...          │
│  │ votes   │ │ votes   │ │ users   │ │ comments│               │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ noria_flush()
┌─────────────────────────────────────────────────────────────────┐
│                     Batch by Table                               │
│  votes: [+row1, +row2]    users: [-old, +new]    comments: [-r] │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ Single propagate() per table
┌─────────────────────────────────────────────────────────────────┐
│                     Dataflow Propagation                         │
│  Aggregate operators batch updates for same group key            │
│  Reduces retraction overhead (emit old/new once per final state) │
└─────────────────────────────────────────────────────────────────┘
```

**Benefits**:
- Single lock acquisition per table (not per write)
- Aggregate operators batch updates for same group key
- Reduces retraction overhead

---

## Performance

### Lobsters Benchmark (vs better-sqlite3)

Simulates a link aggregator (HN/Lobsters) with stories, users, votes, comments.
- **Reads**: story lookup, vote count (aggregate), user profile, story+author (join)
- **Writes**: add vote, add comment (triggers aggregate view updates)

| Scenario | better-sqlite3 | noria | Speedup |
|----------|----------------|-------|---------|
| Single-key read | 323,500 ops/sec | 995,975 ops/sec | **3.1x** |
| Read-only mixed | 301,825 ops/sec | 577,805 ops/sec | **1.9x** |
| Read 99% / Write 1% | 205,988 ops/sec | 372,488 ops/sec | **1.8x** |
| Read 95% / Write 5% | 114,400 ops/sec | 146,647 ops/sec | **1.3x** |
| Read 90% / Write 10% | 67,492 ops/sec | 53,390 ops/sec | 0.8x |

**Trade-offs**: Beneficial for read-heavy workloads (95%+ reads). At 90/10, write overhead dominates. Cache throughput approaches **~1M ops/sec** for single-key lookups.

### Alignment with Original Noria Paper

| Metric | Original Noria | Noria-SQLite | Notes |
|--------|---------------|--------------|-------|
| Target workload | 95%+ reads | 95%+ reads | Same |
| Speedup vs baseline | 5-7x vs MySQL | 1.9-3.1x vs SQLite | SQLite baseline is faster |
| Design | Distributed | In-process | Simpler, lower latency |

---

## Test Methodology

```bash
# Build noria-better-sqlite3
cd noria-better-sqlite3
npm run build-release

# Run all tests (348 passing)
npm test

# Run Lobsters benchmark
npm run bench:lobsters

# Rust tests
cargo test --package noria-core --package noria-ffi
```

---

## Profiling

Use Linux `perf` for accurate performance profiling. It provides hardware-sampled profiling with <1% overhead, which is far more accurate than instrumented profiling.

### Quick Start

```bash
cd noria-better-sqlite3

# Record a profiling session
sudo perf record -g -F 999 -- node benchmark/lobsters.js

# Generate a report (top functions)
sudo perf report --stdio --no-children --percent-limit=0.5

# Clean up
rm perf.data
```

### Profiling Tips

1. **Use the lobsters benchmark** for realistic workloads:
   ```bash
   npm run bench:lobsters
   ```

2. **Use perf-target.js** for focused cache profiling:
   ```bash
   sudo perf record -g -F 999 -- node benchmark/perf-target.js
   ```

3. **Interpret the results** by category:
   - `sqlite3*` functions: SQLite execution (expected for writes/upqueries)
   - `v8::*` functions: V8/JavaScript overhead (object creation, string handling)
   - `noria_*` / Rust functions: Noria cache operations
   - `malloc`/`free`: Memory allocation overhead

### Current Bottleneck Breakdown

| Category | Overhead | Notes |
|----------|----------|-------|
| SQLite | ~11% | VdbeExec, BtreeMoveto, WAL checksums |
| V8/JS | ~8% | Object::New, StringTable, AllocateRaw |
| Kernel | ~7% | syscalls, memory operations |
| Noria | ~3% | Cache lookups, state management |
| malloc | ~3% | Shared across all components |

### Runtime Metrics

Use `db.cacheStats()` for runtime cache metrics (no overhead):

```javascript
const stats = db.cacheStats();
// {
//   cacheHits: 1000,
//   cacheMisses: 5,
//   totalRows: 500,
//   viewCount: 3,
//   nodeCount: 10
// }
```

---

## Current Status (January 2026)

### Completed
- **Simplified architecture**: Two Rust crates (`noria-core`, `noria`) + C++ FFI
- **noria-core**: Database-agnostic dataflow engine with multi-dialect SQL parser
- **Preupdate hook CDC**: Efficient change capture directly in C++ (replaced session extension)
- **Async batch processing**: Write queue + flush (from Noria paper)
- **Core operators**: Filter, Project, Join, Aggregate with incremental maintenance
- **Optimized state**: `IntegerArrayState` for O(1) integer key lookups, Arc-wrapped rows
- **better-sqlite3 compatibility**: Full API compatibility via C++ FFI

### Test Results
- **Node.js Tests**: 348 passing
- **Rust Tests**: 44 passing (noria-core + noria-ffi)

### Not Yet Implemented
- Subqueries and window functions
- PostgreSQL adapter (logical replication)
- MySQL adapter (binlog)

---

## Design Decisions

### 1. noria-core Separation
- **Decision**: Extract database-agnostic code into `noria-core` crate
- **Rationale**: Enables future support for Postgres, MySQL without duplicating dataflow logic
- **Key abstractions**: `DatabaseAdapter` trait, `CdcSource` trait, `SqlDialect` enum

### 2. Async Batch Processing
- **Decision**: Queue CDC events, batch-process on flush
- **Rationale**: Matches original Noria paper; reduces lock contention and aggregate overhead
- **Implementation**: `noria_queue_*()` → `noria_flush()` in FFI layer

### 3. C++ FFI over NAPI-RS
- **Decision**: Use C++ wrapper calling Rust FFI
- **Rationale**: Integrates with existing better-sqlite3 codebase; simpler build
- **Trade-off**: More complex FFI boundary; manual memory management

### 4. Preupdate Hook for SQLite CDC
- **Decision**: Use SQLite's `sqlite3_preupdate_hook` for change capture in C++
- **Rationale**: Simpler than session extension; captures old/new values; no extra compile flags
- **Benefit**: No need for `SQLITE_ENABLE_SESSION`; works with stock SQLite

### 5. Arc-Wrapped Rows for Zero-Copy Lookups
- **Decision**: State stores `Arc<Vec<DataType>>` rows; lookups return Arc clones
- **Rationale**: O(1) cloning on lookup (ref count increment only); no data copying
- **Impact**: Significant performance improvement for read-heavy workloads

---

## Theoretical Background

### Noria's Partially-Stateful Dataflow
- **Push-based**: Writes propagate through DAG, not pulled on read
- **Partial materialization**: Views can be sparse; upqueries fill holes
- **Differential updates**: Positive (insert) and negative (retraction) records
- **Async batch processing**: Queue writes, propagate in batches

### Storage Architecture
| Layer | Original Noria | Noria-SQLite |
|-------|---------------|--------------|
| Materialized Views | evmap | HashMap with Arc rows |
| Base Table Storage | RocksDB | SQLite (or Postgres/MySQL) |
| Upquery Source | RocksDB | Database via callback |
| CDC | Custom | Preupdate hook (SQLite) |

---

## Usage

```javascript
const Database = require('noria-better-sqlite3');

// Create database - Noria enabled by default
const db = new Database(':memory:');

db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

// Prepare creates a Noria view automatically
const stmt = db.prepare('SELECT * FROM users WHERE id = ?');

// Insert data - CDC queued, propagates on flush
db.exec("INSERT INTO users VALUES (1, 'Alice')");

// Cache hit - O(1) lookup
const user = stmt.get(1);  // { id: 1, name: 'Alice' }

// Update propagates through CDC
db.exec("UPDATE users SET name = 'Bob' WHERE id = 1");
stmt.get(1);  // { id: 1, name: 'Bob' }

// Cache stats
const stats = db.cacheStats();
// { cacheHits: 2, cacheMisses: 0, totalRows: 1, viewCount: 1, nodeCount: 2 }
```
