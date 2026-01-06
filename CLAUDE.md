# Noria-SQLite

Embed Noria's differential dataflow engine as a transparent, in-process caching layer for SQLite.

## Project Vision

- **Drop-in better-sqlite3 replacement** with automatic query acceleration
- **Zero configuration**: No external processes, no schema files, no manual view definitions
- **Future targets**: rqlite (distributed SQLite) and litestream (SQLite replication)

## Core Principles

1. **Single Source of Truth**: SQLite owns disk, Noria owns RAM cache
2. **Eventual Consistency**: Async CDC propagation (matches distributed target architecture)
3. **`{ fresh: true }` Escape Hatch**: Strong reads when needed
4. **Fail-Safe**: Falls back to SQLite on errors or unsupported queries

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Application                                 │
│                    (Node.js via noria-better-sqlite3)               │
├─────────────────────────────────────────────────────────────────────┤
│  db.prepare("SELECT * FROM users WHERE id = ?")                     │
│       ↓                                                             │
│  ┌─────────────────────┐      ┌─────────────────────────────────┐  │
│  │   Dynamic View      │      │     Statement Execution         │  │
│  │   Synthesis         │      │  ┌─────────┐    ┌───────────┐   │  │
│  │  (on first prepare) │      │  │ Cache   │ OR │ Upquery   │   │  │
│  │                     │      │  │ Hit O(1)│    │ (SQLite)  │   │  │
│  └─────────────────────┘      │  └─────────┘    └───────────┘   │  │
├─────────────────────────────────────────────────────────────────────┤
│                   C++ Noria Wrapper (noria.cpp)                     │
│  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐   │
│  │ View Registration│  │ Cache Lookup     │  │ CDC Processing  │   │
│  │ (RegisterView)   │  │ (LookupOrUpquery)│  │ (NotifyChange)  │   │
│  └──────────────────┘  └──────────────────┘  └─────────────────┘   │
├─────────────────────────────────────────────────────────────────────┤
│                    Rust FFI Layer (noria-ffi)                       │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  noria-sqlite dataflow engine with evmap-backed views        │  │
│  │  Filter, Project, Join, Aggregate operators                  │  │
│  │  Incremental view maintenance via CDC                        │  │
│  └──────────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────┤
│                    SQLite (Source of Truth)                         │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  Session Extension captures INSERT/UPDATE/DELETE changes    │   │
│  │  Transaction-aware: changes only applied after COMMIT       │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow

**Write Path (CDC)**:
```
db.run("INSERT...") → SQLite execute → Session changeset → propagate through dataflow → return
```

**Read Path**:
```
stmt.get(key) → Check view cache → Hit: return O(1) | Miss: upquery SQLite → populate cache → return
```

**Transaction Path**:
```
BEGIN → statements execute (CDC deferred) → COMMIT → process changeset → cache updated
                                          → ROLLBACK → changeset discarded → cache unchanged
```

---

## Directory Structure

```
noria/
├── CLAUDE.md                    # This file (context for Claude sessions)
├── noria-sqlite/               # Core Rust dataflow library
│   ├── src/
│   │   ├── lib.rs              # Public API, Config struct
│   │   ├── database.rs         # Connection wrapper
│   │   ├── statement.rs        # Prepared statement handling
│   │   ├── error.rs            # Error types
│   │   └── dataflow/
│   │       ├── engine.rs       # NoriaEngine: views, upqueries, CDC apply
│   │       ├── executor.rs     # LocalExecutor: graph propagation
│   │       ├── sql.rs          # SQL parser → dataflow graph
│   │       ├── ops.rs          # Operators: Filter, Project, Join, Aggregate
│   │       └── state.rs        # State trait, MemoryState (evmap-backed)
│   └── tests/
│       ├── integration.rs      # Rust integration tests
│       └── differential.rs     # Differential correctness tests
├── noria-better-sqlite3/       # PRIMARY: C++ FFI integration with better-sqlite3
│   ├── src/
│   │   ├── better_sqlite3.cpp  # Main entry point
│   │   ├── objects/
│   │   │   ├── database.cpp    # Database class with Noria integration
│   │   │   └── statement.cpp   # Statement with cache lookup + CDC
│   │   └── util/
│   │       └── noria.cpp       # Noria C++ wrapper class
│   ├── noria-ffi/
│   │   └── src/lib.rs          # Rust FFI: wraps noria-sqlite for C++ access
│   ├── lib/                    # JavaScript API (better-sqlite3 compatible)
│   ├── test/
│   │   └── 60.noria-acceleration.js  # Noria-specific tests
│   └── binding.gyp             # Build configuration
└── better-sqlite3/             # Reference implementation for API compatibility
```

---

## Key Files Reference

| File | Purpose | Key Functions |
|------|---------|---------------|
| `noria-better-sqlite3/src/util/noria.cpp` | C++ Noria wrapper | `RegisterView()`, `LookupOrUpquery()`, `ProcessSessionChangeset()` |
| `noria-better-sqlite3/src/objects/database.cpp` | Database with Noria | `JS_exec()`, `JS_prepare()`, `CloseHandles()` |
| `noria-better-sqlite3/src/objects/statement.cpp` | Statement + CDC | `JS_get()`, `JS_run()`, `NotifyCdc()` |
| `noria-better-sqlite3/noria-ffi/src/lib.rs` | Rust FFI layer | `noria_create()`, `noria_lookup()`, `noria_apply_insert()` |
| `noria-sqlite/src/dataflow/engine.rs` | Dataflow engine | `create_view()`, `lookup_or_upquery()`, `apply_insert_row()` |
| `noria-sqlite/src/dataflow/executor.rs` | Graph execution | `propagate()`, `apply_write()`, `lookup()` |
| `noria-sqlite/src/dataflow/ops.rs` | Operators | `FilterOp`, `JoinOp`, `AggregateOp` |
| `noria-sqlite/src/dataflow/state.rs` | View state storage | `StateKey`, `MemoryState`, `lookup()` |

---

## Test Methodology

```bash
# Build noria-better-sqlite3
cd noria-better-sqlite3
npm run build-release

# Run all tests (348 passing)
npm test

# Run specific Noria tests
npm test -- --grep "Noria"

# Rust tests
cd noria-sqlite && cargo test

# FFI tests
cd noria-better-sqlite3/noria-ffi && cargo test
```

### Key Test Files
- `noria-better-sqlite3/test/60.noria-acceleration.js` - Noria cache behavior tests
- `noria-better-sqlite3/test/99.benchmark.js` - Performance comparison vs better-sqlite3
- `noria-sqlite/tests/integration.rs` - Rust integration tests
- `noria-sqlite/tests/differential.rs` - Correctness tests for dataflow

---

## Current Status (January 2026)

### Completed
- **Core Dataflow Engine**: Filter, Project, Join, Aggregate operators
- **Session-Based CDC**: INSERT/UPDATE/DELETE captured with transaction semantics
- **better-sqlite3 Integration**: Full API compatibility via C++ FFI
- **Incremental Propagation**: Changes flow through graph to update views
- **Partial Materialization**: O(1) cache hits, upqueries on miss
- **Dynamic View Synthesis**: `prepare()` auto-creates Noria views
- **Transaction-Aware CDC**: Events buffered until COMMIT, discarded on ROLLBACK
- **Cache Statistics**: `cacheHits`, `cacheMisses`, `totalRows` tracking
- **Memory Management**: Configurable limits with random eviction
- **JOIN CDC**: Base table changes propagate to JOIN views
- **Aggregate CDC**: Incremental aggregate updates (COUNT, SUM, AVG)
- **StateKey Optimization**: Single-column keys avoid Vec allocation overhead

### Test Results
- **Node.js Tests**: 348 passing
- **Rust Tests**: 127 passing
- **FFI Tests**: 4 passing

### Not Yet Implemented
- Subqueries and window functions
- Named parameter CDC path optimization

---

## Design Decisions

### 1. C++ FFI over NAPI-RS
- **Decision**: Use C++ wrapper calling Rust FFI instead of direct NAPI-RS bindings
- **Rationale**: Integrates with existing better-sqlite3 codebase; simpler build; better performance
- **Trade-off**: More complex FFI boundary; manual memory management

### 2. Session Extension for CDC
- **Decision**: Use `sqlite3session` for change capture
- **Rationale**: Transaction-aware; captures old values; efficient
- **Requirement**: SQLite compiled with `-DSQLITE_ENABLE_SESSION`
- **Note**: Pre-update hook conflicts with session; rely on session alone

### 3. Transaction-Aware Processing
- **Decision**: Only process CDC changeset when autocommit=1 (not in transaction)
- **Rationale**: Ensures ROLLBACK properly discards changes from cache
- **Implementation**: Check `sqlite3_get_autocommit()` before processing

### 4. Session Recreation
- **Decision**: Recreate session after extracting changeset
- **Rationale**: Session transitions to FINISHED state after `sqlite3session_changeset()`
- **Implementation**: Delete and recreate session to continue recording

---

## Introspection API

```javascript
const Database = require('noria-better-sqlite3');
const db = new Database('app.db');

const stats = db.cacheStats();
// {
//   cacheHits: 1000,      // Reads from cache
//   cacheMisses: 50,      // Upqueries triggered
//   totalRows: 150,       // Total rows across views
//   viewCount: 2,         // Number of registered views
//   nodeCount: 5          // Nodes in dataflow graph
// }

const hitRate = stats.cacheHits / (stats.cacheHits + stats.cacheMisses);
console.log(`Cache hit rate: ${(hitRate * 100).toFixed(1)}%`);
```

---

## Usage

```javascript
const Database = require('noria-better-sqlite3');

// Create database - Noria enabled by default
const db = new Database(':memory:');

db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

// Prepare creates a Noria view automatically
const stmt = db.prepare('SELECT * FROM users WHERE id = ?');

// Insert data - CDC propagates to view
db.exec("INSERT INTO users VALUES (1, 'Alice')");

// Cache hit - O(1) lookup
const user = stmt.get(1);  // { id: 1, name: 'Alice' }

// Update propagates through CDC
db.exec("UPDATE users SET name = 'Bob' WHERE id = 1");
stmt.get(1);  // { id: 1, name: 'Bob' }

// Delete removes from cache
db.exec("DELETE FROM users WHERE id = 1");
stmt.get(1);  // undefined
```

---

## Theoretical Background

### Noria's Partially-Stateful Dataflow
- **Push-based**: Writes propagate through DAG, not pulled on read
- **Partial materialization**: Views can be sparse; upqueries fill holes
- **Differential updates**: Positive (insert) and negative (retraction) records

### SQLite Integration
- **Consistency**: SQLite is strongly consistent; Noria provides eventual consistency for reads
- **CDC**: SQLite Session Extension captures changes with transaction semantics

### Storage Architecture
| Layer | Original Noria | Noria-SQLite |
|-------|---------------|--------------|
| Materialized Views | evmap | evmap (same) |
| Base Table Storage | RocksDB | SQLite |
| Upquery Source | RocksDB | SQLite |

This eliminates RocksDB entirely—SQLite serves as both user database AND Noria's base table backend.

---

## Performance

### Benchmark Results (vs better-sqlite3)

| Operation | Improvement |
|-----------|-------------|
| Read single row (cache hit) | **+42%** |
| Read 100 rows | +2% |
| Insert single row | -3% (CDC overhead) |
| Insert 100 in transaction | -2% |
| Cache hit throughput | **>1M ops/sec** |
| Cache hit rate | 100% (for repeated queries) |

Run benchmarks: `npm test -- --grep benchmark`

### Performance Characteristics
- **Read-heavy workloads**: Significant speedup from cache hits
- **Write-heavy workloads**: Small overhead from CDC propagation
- **Mixed workloads**: Net positive for typical 90% read / 10% write patterns
