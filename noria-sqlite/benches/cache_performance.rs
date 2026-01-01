//! Benchmarks for noria-sqlite cache performance
//!
//! Run with: cargo bench -p noria-sqlite

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use noria_sqlite::{Config, Database};

/// Set up a test database with sample data
fn setup_benchmark_db(row_count: usize) -> Database {
    let db = Database::open_in_memory().expect("Failed to create database");

    db.execute_batch(
        r#"
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX idx_users_email ON users(email);
        "#,
    )
    .expect("Failed to create schema");

    // Insert test data
    for i in 0..row_count {
        db.execute(
            "INSERT INTO users (id, name, email, age, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                i as i64,
                format!("User {}", i),
                format!("user{}@example.com", i),
                20 + (i % 50) as i32,
                "2024-01-01 00:00:00",
            ),
        )
        .expect("Failed to insert data");
    }

    db
}

/// Benchmark: Cache hit vs SQLite query for single row lookup
fn bench_single_row_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_row_lookup");

    for size in [100, 1000, 10000].iter() {
        let db = setup_benchmark_db(*size);

        // Prepare statement (creates view)
        let stmt = db
            .prepare("SELECT name, email, age FROM users WHERE id = ?")
            .unwrap();

        // Warm up cache by querying once
        let _ = stmt.query_row([50i64], |row| {
            let _: String = row.get(0)?;
            let _: String = row.get(1)?;
            let _: i32 = row.get(2)?;
            Ok(())
        });

        group.throughput(Throughput::Elements(1));

        // Benchmark cache hit (via direct API)
        group.bench_with_input(
            BenchmarkId::new("cache_hit_direct", size),
            size,
            |b, _| {
                b.iter(|| {
                    let result = stmt.query_row_cached(black_box([50i64]));
                    black_box(result)
                })
            },
        );

        // Benchmark SQLite query (always fresh)
        group.bench_with_input(
            BenchmarkId::new("sqlite_query", size),
            size,
            |b, _| {
                b.iter(|| {
                    let result = stmt.query_row(black_box([50i64]), |row| {
                        let name: String = row.get(0)?;
                        let email: String = row.get(1)?;
                        let age: i32 = row.get(2)?;
                        Ok((name, email, age))
                    });
                    black_box(result)
                })
            },
        );

        // Benchmark raw rusqlite for comparison
        let conn = db.connection();
        group.bench_with_input(
            BenchmarkId::new("raw_rusqlite", size),
            size,
            |b, _| {
                b.iter(|| {
                    let conn = conn.read();
                    let result: rusqlite::Result<(String, String, i32)> = conn.query_row(
                        "SELECT name, email, age FROM users WHERE id = ?",
                        [50i64],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    );
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark: Multiple queries with different keys (simulates real workload)
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");

    let db = setup_benchmark_db(1000);
    let stmt = db
        .prepare("SELECT name, email FROM users WHERE id = ?")
        .unwrap();

    // Pre-populate cache with some keys
    for i in 0..100 {
        let _ = stmt.query_row([i as i64], |row| {
            let _: String = row.get(0)?;
            let _: String = row.get(1)?;
            Ok(())
        });
    }

    group.throughput(Throughput::Elements(100));

    // Benchmark: 100 queries, mix of cache hits and misses
    group.bench_function("100_queries_50pct_hit", |b| {
        b.iter(|| {
            for i in 0..100 {
                // Keys 0-49 are in cache, 50-99 are not (then wrap)
                let key = (i % 100) as i64;
                let _ = stmt.query_row(black_box([key]), |row| {
                    let _: String = row.get(0)?;
                    let _: String = row.get(1)?;
                    Ok(())
                });
            }
        })
    });

    // Benchmark: 100 queries, all cache hits (same key)
    group.bench_function("100_queries_same_key", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let _ = stmt.query_row(black_box([42i64]), |row| {
                    let _: String = row.get(0)?;
                    let _: String = row.get(1)?;
                    Ok(())
                });
            }
        })
    });

    group.finish();
}

/// Benchmark: Cache population (upquery) performance
fn bench_cache_population(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_population");

    for size in [100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("upquery", size), size, |b, &size| {
            b.iter_with_setup(
                || {
                    // Setup: create fresh database
                    let db = setup_benchmark_db(size);
                    let stmt = db
                        .prepare("SELECT name, email, age FROM users WHERE id = ?")
                        .unwrap();
                    (db, stmt)
                },
                |(_db, stmt)| {
                    // Measure: populate cache for 10 keys
                    for i in 0..10 {
                        let _ = stmt.query_row(black_box([i as i64]), |row| {
                            let _: String = row.get(0)?;
                            let _: String = row.get(1)?;
                            let _: i32 = row.get(2)?;
                            Ok(())
                        });
                    }
                },
            )
        });
    }

    group.finish();
}

/// Benchmark: Consistency guard overhead
fn bench_consistency_guard(c: &mut Criterion) {
    let mut group = c.benchmark_group("consistency_guard");

    // With consistency guard enabled (default)
    let db_with_guard = setup_benchmark_db(1000);
    let stmt_with = db_with_guard
        .prepare("SELECT name FROM users WHERE id = ?")
        .unwrap();

    // Without consistency guard
    let mut config = Config::default();
    config.enable_consistency_guard = false;
    let db_without_guard =
        Database::open_in_memory_with_config(config).expect("Failed to create database");
    db_without_guard
        .execute_batch(
            r#"
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
        INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
        "#,
        )
        .unwrap();
    let stmt_without = db_without_guard
        .prepare("SELECT name FROM users WHERE id = ?")
        .unwrap();

    // Warm up caches
    let _ = stmt_with.query_row([1i64], |r| r.get::<_, String>(0));
    let _ = stmt_without.query_row([1i64], |r| r.get::<_, String>(0));

    group.bench_function("with_guard", |b| {
        b.iter(|| {
            let _ = stmt_with.query_row(black_box([1i64]), |r| r.get::<_, String>(0));
        })
    });

    group.bench_function("without_guard", |b| {
        b.iter(|| {
            let _ = stmt_without.query_row(black_box([1i64]), |r| r.get::<_, String>(0));
        })
    });

    group.finish();
}

/// Benchmark: Memory estimation
fn bench_memory_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory");

    group.bench_function("cache_stats", |b| {
        let db = setup_benchmark_db(1000);
        let stmt = db
            .prepare("SELECT name, email FROM users WHERE id = ?")
            .unwrap();

        // Populate cache
        for i in 0..100 {
            let _ = stmt.query_row([i as i64], |row| {
                let _: String = row.get(0)?;
                let _: String = row.get(1)?;
                Ok(())
            });
        }

        b.iter(|| {
            let stats = db.cache_stats();
            black_box(stats)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_row_lookup,
    bench_mixed_workload,
    bench_cache_population,
    bench_consistency_guard,
    bench_memory_overhead,
);

criterion_main!(benches);
