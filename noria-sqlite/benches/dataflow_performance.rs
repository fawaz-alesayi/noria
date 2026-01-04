//! Benchmarks for noria-sqlite dataflow engine performance
//!
//! This benchmark compares the local dataflow executor against raw SQLite
//! for filtering, projection, aggregation, and incremental updates.
//!
//! Run with: cargo bench -p noria-sqlite --bench dataflow_performance

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use noria::DataType;
use noria_sqlite::dataflow::{
    AggregateFunc, AggregateOp, FilterCondition, FilterOp, LocalExecutor, NoriaEngine,
    OperatorType, Records,
};
use parking_lot::RwLock;
use rusqlite::Connection;
use std::sync::Arc;

/// Set up test data for benchmarking
fn create_test_records(count: usize) -> Records {
    let records: Vec<Vec<DataType>> = (0..count)
        .map(|i| {
            vec![
                DataType::Int(i as i32),
                DataType::from(format!("User{}", i).as_str()),
                DataType::Int((20 + i % 50) as i32),
            ]
        })
        .collect();
    Records::from(records)
}

/// Benchmark: Filter operation through dataflow vs direct state lookup
fn bench_filter_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_throughput");

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        // Setup dataflow with filter
        let mut executor = LocalExecutor::new();
        let users =
            executor.add_base_table("users", vec!["id".into(), "name".into(), "age".into()]);

        let filter = executor.add_operator(
            "adults",
            OperatorType::Filter(FilterOp::new(
                FilterCondition::Gt(2, DataType::Int(25)), // age > 25
                3,
                vec![0],
            )),
            vec![users],
            vec!["id".into(), "name".into(), "age".into()],
        );
        let view = executor.materialize(filter, vec![0]);

        // Create test data
        let records = create_test_records(*size);

        // Benchmark: Apply writes through dataflow
        group.bench_with_input(
            BenchmarkId::new("dataflow_filter", size),
            &records,
            |b, records| {
                b.iter_with_setup(
                    || {
                        // Clone executor for each iteration
                        let mut exec = LocalExecutor::new();
                        let base = exec.add_base_table(
                            "users",
                            vec!["id".into(), "name".into(), "age".into()],
                        );
                        let filter = exec.add_operator(
                            "adults",
                            OperatorType::Filter(FilterOp::new(
                                FilterCondition::Gt(2, DataType::Int(25)),
                                3,
                                vec![0],
                            )),
                            vec![base],
                            vec!["id".into(), "name".into(), "age".into()],
                        );
                        exec.materialize(filter, vec![0]);
                        (exec, records.clone())
                    },
                    |(mut exec, recs)| {
                        exec.apply_write("users", recs);
                        black_box(exec.stats())
                    },
                )
            },
        );

        // Benchmark: Raw SQLite filter
        group.bench_with_input(BenchmarkId::new("sqlite_filter", size), size, |b, &sz| {
            b.iter_with_setup(
                || {
                    let conn = Connection::open_in_memory().unwrap();
                    conn.execute_batch(
                        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
                    )
                    .unwrap();
                    conn
                },
                |conn| {
                    // Insert data
                    for i in 0..sz {
                        conn.execute(
                            "INSERT INTO users VALUES (?, ?, ?)",
                            (i as i64, format!("User{}", i), (20 + i % 50) as i64),
                        )
                        .unwrap();
                    }
                    // Query with filter
                    let mut stmt = conn
                        .prepare("SELECT * FROM users WHERE age > 25")
                        .unwrap();
                    let count = stmt.query_map([], |_| Ok(())).unwrap().count();
                    black_box(count)
                },
            )
        });
    }

    group.finish();
}

/// Benchmark: Aggregation (COUNT) through dataflow vs SQLite
fn bench_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregation");

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));


        // Benchmark: Dataflow aggregation
        group.bench_with_input(
            BenchmarkId::new("dataflow_count", size),
            size,
            |b, &sz| {
                b.iter_with_setup(
                    || {
                        let mut exec = LocalExecutor::new();
                        let votes = exec.add_base_table("votes", vec!["article_id".into()]);
                        let agg = exec.add_operator(
                            "vote_count",
                            OperatorType::Aggregate(AggregateOp::new(
                                vec![0],
                                AggregateFunc::Count,
                                vec![0],
                            )),
                            vec![votes],
                            vec!["article_id".into(), "count".into()],
                        );
                        exec.materialize(agg, vec![0]);

                        // Create votes for 10 articles
                        let records: Records = (0..sz)
                            .map(|i| vec![DataType::Int((i % 10) as i32)])
                            .collect::<Vec<_>>()
                            .into();

                        (exec, records)
                    },
                    |(mut exec, recs)| {
                        exec.apply_write("votes", recs);
                        black_box(exec.stats())
                    },
                )
            },
        );

        // Benchmark: SQLite aggregation
        group.bench_with_input(BenchmarkId::new("sqlite_count", size), size, |b, &sz| {
            b.iter_with_setup(
                || {
                    let conn = Connection::open_in_memory().unwrap();
                    conn.execute_batch("CREATE TABLE votes (article_id INTEGER)")
                        .unwrap();
                    conn
                },
                |conn| {
                    // Insert votes
                    for i in 0..sz {
                        conn.execute("INSERT INTO votes VALUES (?)", [(i % 10) as i64])
                            .unwrap();
                    }
                    // Query with aggregation
                    let mut stmt = conn
                        .prepare("SELECT article_id, COUNT(*) FROM votes GROUP BY article_id")
                        .unwrap();
                    let count = stmt.query_map([], |_| Ok(())).unwrap().count();
                    black_box(count)
                },
            )
        });
    }

    group.finish();
}

/// Benchmark: Incremental update performance
fn bench_incremental_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("incremental_update");

    for batch_size in [1, 10, 100].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));


        // Pre-populate with data, then measure incremental update cost
        group.bench_with_input(
            BenchmarkId::new("dataflow_insert", batch_size),
            batch_size,
            |b, &sz| {
                b.iter_with_setup(
                    || {
                        let mut exec = LocalExecutor::new();
                        let users = exec.add_base_table(
                            "users",
                            vec!["id".into(), "name".into(), "age".into()],
                        );
                        let filter = exec.add_operator(
                            "adults",
                            OperatorType::Filter(FilterOp::new(
                                FilterCondition::Gt(2, DataType::Int(25)),
                                3,
                                vec![0],
                            )),
                            vec![users],
                            vec!["id".into(), "name".into(), "age".into()],
                        );
                        let agg = exec.add_operator(
                            "adult_count",
                            OperatorType::Aggregate(AggregateOp::new(
                                vec![2], // group by age
                                AggregateFunc::Count,
                                vec![0],
                            )),
                            vec![filter],
                            vec!["age".into(), "count".into()],
                        );
                        exec.materialize(agg, vec![0]);

                        // Pre-populate with 1000 users
                        let initial = create_test_records(1000);
                        exec.apply_write("users", initial);

                        // Prepare incremental batch
                        let batch: Records = (1000..1000 + sz)
                            .map(|i| {
                                vec![
                                    DataType::Int(i as i32),
                                    DataType::from(format!("User{}", i).as_str()),
                                    DataType::Int((20 + i % 50) as i32),
                                ]
                            })
                            .collect::<Vec<_>>()
                            .into();

                        (exec, batch)
                    },
                    |(mut exec, batch)| {
                        exec.apply_write("users", batch);
                        black_box(exec.stats())
                    },
                )
            },
        );
    }

    group.finish();
}

/// Benchmark: Lookup performance after data population
fn bench_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup");

    // Pre-populate and measure lookup speed
    let mut executor = LocalExecutor::new();
    let users = executor.add_base_table("users", vec!["id".into(), "name".into(), "age".into()]);
    let view = executor.materialize(users, vec![0]);

    // Populate with 10000 users
    let records = create_test_records(10000);
    executor.apply_write("users", records);

    // Lookup benchmark
    group.bench_function("dataflow_lookup", |b| {
        b.iter(|| {
            let result = executor.lookup(&view, black_box(&[DataType::Int(5000)]));
            black_box(result)
        })
    });

    // Compare with SQLite
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);",
    )
    .unwrap();
    for i in 0..10000 {
        conn.execute(
            "INSERT INTO users VALUES (?, ?, ?)",
            (i as i64, format!("User{}", i), (20 + i % 50) as i64),
        )
        .unwrap();
    }

    group.bench_function("sqlite_lookup", |b| {
        b.iter(|| {
            let result: rusqlite::Result<(i64, String, i64)> = conn.query_row(
                "SELECT * FROM users WHERE id = ?",
                [5000i64],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            );
            black_box(result)
        })
    });

    group.finish();
}

/// Benchmark: NoriaEngine end-to-end
fn bench_noria_engine(c: &mut Criterion) {
    let mut group = c.benchmark_group("noria_engine");

    group.bench_function("create_view_and_lookup", |b| {
        b.iter_with_setup(
            || {
                let conn = Connection::open_in_memory().unwrap();
                conn.execute_batch(
                    "
                    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
                    INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);
                    ",
                )
                .unwrap();
                Arc::new(RwLock::new(conn))
            },
            |conn| {
                let engine = NoriaEngine::new(conn);
                engine.register_table("users").unwrap();
                engine.load_table("users").unwrap();

                let view = engine
                    .create_view("SELECT * FROM users WHERE age = 30")
                    .unwrap();

                // Lookup should find Alice
                let result = engine.lookup(&view, &[DataType::Int(1)]);
                black_box(result)
            },
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_filter_throughput,
    bench_aggregation,
    bench_incremental_update,
    bench_lookup,
    bench_noria_engine,
);

criterion_main!(benches);
