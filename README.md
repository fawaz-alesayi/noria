# Noria-SQLite

In-process caching layer for SQLite using differential dataflow.

**Experimental** — This is a research project. Not recommended for production.

## What is this?

Noria-SQLite puts a dataflow engine in front of SQLite. When you `prepare()` a SELECT, it creates a materialized view. Reads hit an O(1) cache. Writes flow through the dataflow graph to keep views up to date.

It's a drop-in replacement for better-sqlite3, based on the [Noria research system](https://pdos.csail.mit.edu/papers/noria:osdi18.pdf) from MIT PDOS. The original Noria uses ZooKeeper and distributed workers; this version runs entirely in-process.

## Architecture

```
Application (Node.js)
        |
        v
+---------------------+
| noria-better-sqlite3|  <-- better-sqlite3 API
+---------------------+
        |
        v
+---------------------+
|   noria-sqlite      |  <-- Rust dataflow engine
+---------------------+
        |
        v
+---------------------+
|     SQLite          |  <-- source of truth
+---------------------+
```

Writes: SQLite executes -> session extension captures changes -> dataflow updates views

Reads: check cache -> hit: return | miss: query SQLite, populate cache, return

## Packages

| Package | Description |
|---------|-------------|
| [noria-better-sqlite3](./noria-better-sqlite3) | Node.js bindings |
| [noria-sqlite](./noria-sqlite) | Rust dataflow library |

## Quick Start

See [noria-better-sqlite3](./noria-better-sqlite3) for installation.

```javascript
const Database = require('noria-better-sqlite3');
const db = new Database(':memory:');

db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
const stmt = db.prepare('SELECT * FROM users WHERE id = ?');

db.exec("INSERT INTO users VALUES (1, 'Alice')");
stmt.get(1);  // cache hit
```

## Performance

vs better-sqlite3 (100k iterations):

| Operation | Difference |
|-----------|------------|
| Read single row (cache hit) | +42% |
| Read 100 rows | +2% |
| Insert single row | -3% |
| Bulk insert (100 rows) | -2% |

Reads are faster. Writes have CDC overhead.

## Papers

- [Noria: dynamic, partially-stateful data-flow for high-performance web applications](https://pdos.csail.mit.edu/papers/noria:osdi18.pdf) (OSDI'18)
- [Jon Gjengset's PhD Thesis](https://jon.thesquareplanet.com/papers/phd-thesis.pdf)

## Contributing

This is a personal research project. I'm not actively seeking contributions or providing support. Issues are disabled. You're welcome to fork it. If you open a PR and it looks good, I might merge it when I have time.

## License

MIT or Apache-2.0, at your option.
