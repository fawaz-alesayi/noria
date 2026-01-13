# noria-better-sqlite3

Fork of [better-sqlite3](https://github.com/WiseLibs/better-sqlite3) with a dataflow caching layer.

**Experimental** — This is a research project. Not recommended for production.

## What it does

When you call `prepare()` on a SELECT, it registers a materialized view. Reads check the cache first. Writes are captured by SQLite's session extension and flow through the dataflow graph to update the views.

The API is identical to better-sqlite3. Swap out the require and you're done.

Consistency model is eventual. After a write, reads might briefly return stale data. Use `{ fresh: true }` if you need the latest value.

## Installation

Not published to npm yet. Build from source:

```bash
git clone https://github.com/mit-pdos/noria.git
cd noria/noria-better-sqlite3
npm install
npm run build-release
```

## Usage

```javascript
const Database = require('noria-better-sqlite3');

const db = new Database('mydb.sqlite');
db.pragma('journal_mode = WAL');

const getUser = db.prepare('SELECT * FROM users WHERE id = ?');

// first call hits SQLite, subsequent calls hit cache
const user = getUser.get(123);

// writes update the cache via CDC
db.prepare('UPDATE users SET name = ? WHERE id = ?').run('Alice', 123);

// force a fresh read from SQLite
const freshUser = getUser.get(123, { fresh: true });
```

## Cache stats

```javascript
const stats = db.cacheStats();
// { cacheHits: 1000, cacheMisses: 50, totalRows: 150, viewCount: 5, nodeCount: 12 }
```

## Performance

Tested on OCI VM.Standard.A1.Flex (4 OCPU ARM, 24GB RAM), Ubuntu 22.04.

### Noria benefit (cache hits)

| Benchmark | better-sqlite3 | noria-better-sqlite3 | Difference |
|-----------|----------------|----------------------|------------|
| Hot path (same key) | 323,000 ops/sec | 996,000 ops/sec | **+208%** |
| Random keys (warm cache) | 302,000 ops/sec | 578,000 ops/sec | **+91%** |

### Parity (operations that bypass cache)

| Benchmark | better-sqlite3 | noria-better-sqlite3 | Difference |
|-----------|----------------|----------------------|------------|
| Range query (100 rows) | 16,600 ops/sec | 16,100 ops/sec | -3% |
| Insert single row | 461,000 ops/sec | 441,000 ops/sec | -4% |
| Insert 100 rows (txn) | 7,100 ops/sec | 6,800 ops/sec | -4% |

Reads are faster. Writes have ~4% CDC overhead.

### Lobsters benchmark

Simulates a link aggregator (HN/Lobsters style) with stories, users, votes, and comments.

```
npm run bench:lobsters
```

- **Reads**: story lookup, vote count (aggregate), user profile, story+author (join)
- **Writes**: add vote, add comment (triggers aggregate view updates)

| Scenario | better-sqlite3 | noria-better-sqlite3 | Speedup |
|----------|----------------|----------------------|---------|
| Single-key read | 323,500 ops/sec | 995,975 ops/sec | **3.1x** |
| Read-only mixed | 301,825 ops/sec | 577,805 ops/sec | **1.9x** |
| Read 99% / Write 1% | 205,988 ops/sec | 372,488 ops/sec | **1.8x** |
| Read 95% / Write 5% | 114,400 ops/sec | 146,647 ops/sec | **1.3x** |
| Read 90% / Write 10% | 67,492 ops/sec | 53,390 ops/sec | 0.8x |

**Trade-offs**: Beneficial for read-heavy workloads (95%+ reads). At 90/10, write overhead dominates. Cache throughput approaches **1M ops/sec** for single-key lookups.

## What's supported

Works:
- Single-table SELECTs with WHERE
- JOINs (INNER, LEFT)
- Aggregates (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY
- Parameterized queries

Doesn't work yet:
- Subqueries
- Window functions
- UNION/INTERSECT/EXCEPT

## Project layout

```
noria-better-sqlite3/
├── noria-ffi/          # Rust FFI (dataflow engine)
├── src/
│   ├── util/noria.cpp  # C++ wrapper
│   └── objects/        # Database/Statement integration
├── lib/                # JS API
└── test/               # 348 tests
```

## better-sqlite3 docs

This is a fork of [better-sqlite3](https://github.com/WiseLibs/better-sqlite3) by Joshua Wise.

- [API](./docs/api.md)
- [Performance](./docs/performance.md)
- [64-bit integers](./docs/integer.md)

## Contributing

This is a personal research project. I'm not actively seeking contributions or providing support. Issues are disabled. You're welcome to fork it. If you open a PR and it looks good, I might merge it when I have time.

## License

MIT or Apache-2.0, at your option.
