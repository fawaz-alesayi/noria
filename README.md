# Noria-SQLite

In-process caching layer for SQLite using differential dataflow.

**Experimental** — This is a research project. Not recommended for production.

## What is this?

Noria-SQLite puts a dataflow engine in front of SQLite. When you `prepare()` a SELECT, it creates a materialized view. Reads hit an O(1) cache. Writes flow through the dataflow graph to keep views up to date.

It's a drop-in replacement for better-sqlite3, based on the [Noria research system](https://pdos.csail.mit.edu/papers/noria:osdi18.pdf) from MIT PDOS. The original Noria uses ZooKeeper and distributed workers; this version runs entirely in-process.

## Why

SQLite works well for small web apps. Most web apps are read-heavy. Eventually your app gets slow because SQLite can't keep up with reads.

The usual answer is "add caching." So you either roll your own or bring in Redis. Both are complicated. You're writing cache invalidation logic instead of building features.

Noria handles this for you. Swap your require, and reads get served from a cache that stays in sync with your data. The goal is 5-10x read throughput without thinking about caching. Thanks to [Jon Gjengset](https://thesquareplanet.com/) for the research that made this possible.

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

Tested on OCI VM.Standard.A1.Flex (4 OCPU ARM, 24GB RAM), Ubuntu 22.04.

### Lobsters benchmark

Simulates a link aggregator (HN/Lobsters style) with stories, users, votes, and comments.
- **Reads**: story lookup, vote count (aggregate), user profile, story+author (join)
- **Writes**: add vote, add comment (triggers aggregate view updates)

| Scenario | better-sqlite3 | noria-better-sqlite3 | Speedup |
|----------|----------------|----------------------|---------|
| Single-key read | 325,000 ops/sec | 800,000 ops/sec | **2.5x** |
| Read-only mixed | 295,000 ops/sec | 510,000 ops/sec | **1.7x** |
| Read 99% / Write 1% | 200,000 ops/sec | 290,000 ops/sec | **1.4x** |
| Read 95% / Write 5% | 110,000 ops/sec | 110,000 ops/sec | 1.0x |
| Read 90% / Write 10% | 63,000 ops/sec | 57,000 ops/sec | 0.9x |

**Trade-offs**: Beneficial for read-heavy workloads (99%+ reads). At 95/5, performance is at parity. Write-heavy workloads with aggregate views are slower due to incremental maintenance overhead.

### Parity (operations that bypass cache)

| Benchmark | better-sqlite3 | noria-better-sqlite3 | Difference |
|-----------|----------------|----------------------|------------|
| Range query (100 rows) | 16,600 ops/sec | 16,100 ops/sec | -3% |
| Insert single row | 461,000 ops/sec | 441,000 ops/sec | -4% |
| Insert 100 rows (txn) | 7,100 ops/sec | 6,800 ops/sec | -4% |

Reads are faster. Writes have ~4% CDC overhead.

## Papers

- [Noria: dynamic, partially-stateful data-flow for high-performance web applications](https://pdos.csail.mit.edu/papers/noria:osdi18.pdf) (OSDI'18)
- [Jon Gjengset's PhD Thesis](https://jon.thesquareplanet.com/papers/phd-thesis.pdf)

## Contributing

This is a personal research project. I'm not actively seeking contributions or providing support. Issues are disabled. You're welcome to fork it. If you open a PR and it looks good, I might merge it when I have time.

## License

MIT or Apache-2.0, at your option.
