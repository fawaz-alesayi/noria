# noria-better-sqlite3

Fork of [better-sqlite3](https://github.com/WiseLibs/better-sqlite3) with integrated Noria dataflow caching.

## Overview

noria-better-sqlite3 adds a transparent in-memory caching layer to better-sqlite3, inspired by the [Noria](https://pdos.csail.mit.edu/papers/noria:osdi18.pdf) research system. Views are automatically materialized for SELECT queries, and cache invalidation happens asynchronously via SQLite's session extension for CDC (Change Data Capture).

**Key features:**
- Drop-in replacement for better-sqlite3 (100% API compatible)
- Zero configuration required
- No performance penalty when cache is not used (104% of better-sqlite3 baseline)
- Async CDC with eventual consistency model
- `{ fresh: true }` option for consistent reads when needed

## Architecture

```
Application
     │
     ▼
┌─────────────────────────────────────────┐
│         noria-better-sqlite3            │
│  ┌─────────────┐    ┌───────────────┐   │
│  │ View Cache  │    │ SQLite Engine │   │
│  │ (HashMap)   │◄──►│ (fallback)    │   │
│  └─────────────┘    └───────────────┘   │
│         ▲                    │          │
│         │   CDC Events       │          │
│         └────────────────────┘          │
└─────────────────────────────────────────┘
```

**Read path:** Check cache → hit: return O(1) | miss: query SQLite → populate cache

**Write path:** Execute SQLite → session captures changes → async invalidation

## Installation

```bash
npm install noria-better-sqlite3
```

## Usage

```javascript
const Database = require('noria-better-sqlite3');

const db = new Database('mydb.sqlite');
db.pragma('journal_mode = WAL');

// Prepare a SELECT - automatically registers as a Noria view
const getUser = db.prepare('SELECT * FROM users WHERE id = ?');

// Reads are served from cache after first query
const user = getUser.get(123);

// For consistent reads after writes, use { fresh: true }
db.prepare('UPDATE users SET name = ? WHERE id = ?').run('Alice', 123);
const freshUser = getUser.get(123, { fresh: true });
```

## Cache Statistics

```javascript
const stats = db.cacheStats();
// {
//   cacheHits: 1000,
//   cacheMisses: 50,
//   totalRows: 150,
//   viewCount: 5,
//   nodeCount: 5
// }
```

## Performance

| Operation | vs better-sqlite3 |
|-----------|------------------|
| Single row read (no cache) | 104% |
| Multi-row read (cached) | 10x faster |
| Transaction insert (with views) | ~95% |

Performance tested on Node.js v22 with 100,000 iterations.

## How it works

1. **View Registration:** When you call `prepare()` on a SELECT, it's registered as a Noria view
2. **Cache Population:** On first read, results are cached by query parameters
3. **CDC Tracking:** SQLite session extension tracks INSERT/UPDATE/DELETE
4. **Async Invalidation:** Cache entries are invalidated when dependent tables change
5. **Eventual Consistency:** Reads may return stale data briefly after writes (use `{ fresh: true }` for consistency)

## Project Structure

```
noria-better-sqlite3/
├── noria-ffi/          # Rust FFI library (cache engine)
│   └── src/lib.rs      # HashMap-based view cache with async CDC
├── src/
│   ├── util/noria.cpp  # C++ wrapper for Noria FFI
│   └── objects/        # Statement integration
└── test/               # Comprehensive test suite (311 tests)
```

## Original better-sqlite3

This project is based on [better-sqlite3](https://github.com/WiseLibs/better-sqlite3) by Joshua Wise.

For the original better-sqlite3 documentation:
- [API documentation](./docs/api.md)
- [Performance](./docs/performance.md)
- [64-bit integer support](./docs/integer.md)

## License

[MIT](./LICENSE)
