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

vs better-sqlite3 (100k iterations):

| Operation | Difference |
|-----------|------------|
| Read single row (cache hit) | +42% |
| Read 100 rows | +2% |
| Insert single row | -3% |
| Bulk insert (100 rows) | -2% |

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
