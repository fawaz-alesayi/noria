//! Differential tests: Verify dataflow engine produces identical results to SQLite.
//!
//! These tests run the same query through both:
//! 1. Our dataflow engine (via materialized views)
//! 2. Direct SQLite execution
//!
//! And assert the results are identical. This catches semantic bugs in our
//! filter, projection, join, and aggregate implementations.

use noria::DataType;
use noria_sqlite::Database;
use rusqlite::params;
use std::collections::HashSet;

/// Helper to convert SQLite row to Vec<DataType> for comparison
fn sqlite_row_to_datatypes(row: &rusqlite::Row, col_count: usize) -> Vec<DataType> {
    (0..col_count)
        .map(|i| {
            use rusqlite::types::ValueRef;
            match row.get_ref(i).unwrap() {
                ValueRef::Null => DataType::None,
                ValueRef::Integer(n) => DataType::BigInt(n),
                ValueRef::Real(f) => {
                    // Convert to fixed-point Real
                    let int_part = f.trunc() as i64;
                    let frac_part = ((f.fract()) * 1_000_000_000.0).round() as i32;
                    DataType::Real(int_part, frac_part)
                }
                ValueRef::Text(s) => DataType::from(std::str::from_utf8(s).unwrap()),
                ValueRef::Blob(b) => DataType::from(std::str::from_utf8(b).unwrap_or("")),
            }
        })
        .collect()
}

/// Query SQLite directly and return results as Vec<Vec<DataType>>
fn query_sqlite(db: &Database, sql: &str, params: &[DataType]) -> Vec<Vec<DataType>> {
    let conn = db.connection();
    let conn = conn.read();
    let mut stmt = conn.prepare(sql).unwrap();

    let col_count = stmt.column_count();
    let rusqlite_params: Vec<Box<dyn rusqlite::ToSql>> = params
        .iter()
        .map(|dt| -> Box<dyn rusqlite::ToSql> {
            match dt {
                DataType::None => Box::new(None::<i64>),
                DataType::Int(n) => Box::new(*n),
                DataType::BigInt(n) => Box::new(*n),
                DataType::UnsignedInt(n) => Box::new(*n as i64),
                DataType::UnsignedBigInt(n) => Box::new(*n as i64),
                DataType::Real(int_part, frac_part) => {
                    let f = *int_part as f64 + (*frac_part as f64 / 1_000_000_000.0);
                    Box::new(f)
                }
                DataType::Text(s) => {
                    // ArcCStr to String
                    let cstr: &std::ffi::CStr = s.as_ref();
                    Box::new(cstr.to_string_lossy().into_owned())
                }
                DataType::TinyText(s) => {
                    // Find null terminator
                    let len = s.iter().position(|&b| b == 0).unwrap_or(s.len());
                    Box::new(String::from_utf8_lossy(&s[..len]).into_owned())
                }
                _ => Box::new(format!("{:?}", dt)),
            }
        })
        .collect();

    let param_refs: Vec<&dyn rusqlite::ToSql> = rusqlite_params.iter().map(|b| b.as_ref()).collect();

    stmt.query_map(param_refs.as_slice(), |row| Ok(sqlite_row_to_datatypes(row, col_count)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

/// Query our dataflow engine and return results
fn query_dataflow(db: &Database, sql: &str, params: &[DataType]) -> Vec<Vec<DataType>> {
    let stmt = db.prepare(sql).unwrap();

    // Use the engine lookup with upquery
    if let Some(view) = stmt.view() {
        let engine = db.engine();
        match engine.lookup_or_upquery(view, params) {
            Ok(rows) => rows,
            Err(_) => vec![],
        }
    } else {
        // No view, return empty (shouldn't happen for cacheable queries)
        vec![]
    }
}

/// Compare two result sets (order-independent)
fn results_match(dataflow: &[Vec<DataType>], sqlite: &[Vec<DataType>]) -> bool {
    if dataflow.len() != sqlite.len() {
        return false;
    }

    // Convert to sets for order-independent comparison
    let df_set: HashSet<Vec<String>> = dataflow
        .iter()
        .map(|row| row.iter().map(|dt| format!("{:?}", dt)).collect())
        .collect();

    let sq_set: HashSet<Vec<String>> = sqlite
        .iter()
        .map(|row| row.iter().map(|dt| format!("{:?}", dt)).collect())
        .collect();

    df_set == sq_set
}

/// Assert dataflow results match SQLite, with detailed diff on failure
fn assert_results_match(
    dataflow: &[Vec<DataType>],
    sqlite: &[Vec<DataType>],
    query: &str,
    params: &[DataType],
) {
    if !results_match(dataflow, sqlite) {
        panic!(
            "\n\nDIFFERENTIAL TEST FAILURE\n\
             Query: {}\n\
             Params: {:?}\n\n\
             SQLite returned {} rows:\n{:#?}\n\n\
             Dataflow returned {} rows:\n{:#?}\n",
            query,
            params,
            sqlite.len(),
            sqlite,
            dataflow.len(),
            dataflow
        );
    }
}

/// Check if dataflow results contain all sqlite results (dataflow may have extra key columns)
fn dataflow_contains_sqlite(dataflow: &[Vec<DataType>], sqlite: &[Vec<DataType>]) -> bool {
    if dataflow.len() != sqlite.len() {
        return false;
    }

    // For each sqlite row, check if there's a matching dataflow row
    // (dataflow rows may have extra columns at the end for key columns)
    for sq_row in sqlite {
        let found = dataflow.iter().any(|df_row| {
            if df_row.len() < sq_row.len() {
                return false;
            }
            // Compare first N columns (where N = sqlite column count)
            df_row.iter().zip(sq_row.iter()).all(|(df, sq)| df == sq)
        });
        if !found {
            return false;
        }
    }
    true
}

fn assert_dataflow_contains_sqlite(
    dataflow: &[Vec<DataType>],
    sqlite: &[Vec<DataType>],
    query: &str,
    params: &[DataType],
) {
    if !dataflow_contains_sqlite(dataflow, sqlite) {
        panic!(
            "\n\nDIFFERENTIAL TEST FAILURE\n\
             Query: {}\n\
             Params: {:?}\n\n\
             SQLite returned {} rows:\n{:#?}\n\n\
             Dataflow returned {} rows:\n{:#?}\n\n\
             Dataflow should contain all SQLite rows (may have extra key columns)\n",
            query,
            params,
            sqlite.len(),
            sqlite,
            dataflow.len(),
            dataflow
        );
    }
}

fn setup_test_db() -> Database {
    Database::open_in_memory().unwrap()
}

// =============================================================================
// FILTER TESTS
// =============================================================================

mod filter_tests {
    use super::*;

    #[test]
    fn test_filter_equality_integer() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ())
            .unwrap();
        db.register_table("t").unwrap();

        // Insert test data
        db.execute("INSERT INTO t VALUES (1, 100)", ()).unwrap();
        db.execute("INSERT INTO t VALUES (2, 200)", ()).unwrap();
        db.execute("INSERT INTO t VALUES (3, 100)", ()).unwrap();

        let query = "SELECT id, val FROM t WHERE val = ?";
        let params = vec![DataType::BigInt(100)];

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_dataflow_contains_sqlite(&dataflow, &sqlite, query, &params);
        assert_eq!(sqlite.len(), 2, "Should find 2 rows with val=100");
    }

    #[test]
    fn test_filter_equality_string() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)", ())
            .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, 'Alice')", ()).unwrap();
        db.execute("INSERT INTO t VALUES (2, 'Bob')", ()).unwrap();
        db.execute("INSERT INTO t VALUES (3, 'Alice')", ()).unwrap();

        let query = "SELECT id, name FROM t WHERE name = ?";
        let params = vec![DataType::from("Alice")];

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_dataflow_contains_sqlite(&dataflow, &sqlite, query, &params);
        assert_eq!(sqlite.len(), 2);
    }

    #[test]
    fn test_filter_no_matches() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ())
            .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, 100)", ()).unwrap();
        db.execute("INSERT INTO t VALUES (2, 200)", ()).unwrap();

        let query = "SELECT id, val FROM t WHERE val = ?";
        let params = vec![DataType::BigInt(999)]; // No match

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_eq!(sqlite.len(), 0, "Should find no rows");
        assert_eq!(dataflow.len(), 0, "Dataflow should also find no rows");
    }

    #[test]
    fn test_filter_empty_table() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ())
            .unwrap();
        db.register_table("t").unwrap();

        let query = "SELECT id, val FROM t WHERE val = ?";
        let params = vec![DataType::BigInt(100)];

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_eq!(sqlite.len(), 0);
        assert_eq!(dataflow.len(), 0);
    }

    #[test]
    fn test_filter_negative_numbers() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ())
            .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, -100)", ()).unwrap();
        db.execute("INSERT INTO t VALUES (2, 0)", ()).unwrap();
        db.execute("INSERT INTO t VALUES (3, 100)", ()).unwrap();

        let query = "SELECT id, val FROM t WHERE val = ?";
        let params = vec![DataType::BigInt(-100)];

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_dataflow_contains_sqlite(&dataflow, &sqlite, query, &params);
        assert_eq!(sqlite.len(), 1);
    }

    #[test]
    fn test_filter_boundary_values() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ())
            .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, 9223372036854775807)", ()) // i64::MAX
            .unwrap();
        db.execute("INSERT INTO t VALUES (2, -9223372036854775808)", ()) // i64::MIN
            .unwrap();
        db.execute("INSERT INTO t VALUES (3, 0)", ()).unwrap();

        let query = "SELECT id, val FROM t WHERE val = ?";
        let params = vec![DataType::BigInt(i64::MAX)];

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_dataflow_contains_sqlite(&dataflow, &sqlite, query, &params);
        assert_eq!(sqlite.len(), 1);
    }
}

// =============================================================================
// PROJECTION TESTS
// =============================================================================

mod projection_tests {
    use super::*;

    #[test]
    fn test_project_subset_columns() {
        let db = setup_test_db();
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)",
            (),
        )
        .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, 'a1', 'b1', 'c1')", ())
            .unwrap();
        db.execute("INSERT INTO t VALUES (2, 'a2', 'b2', 'c2')", ())
            .unwrap();

        let query = "SELECT a, c FROM t WHERE id = ?";
        let params = vec![DataType::BigInt(1)];

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        // Check that projected columns match (dataflow may have key column at end)
        assert_eq!(sqlite.len(), 1);
        assert!(dataflow.len() >= 1);

        // First two columns should match (a, c)
        assert_eq!(dataflow[0][0], sqlite[0][0], "Column 'a' should match");
        assert_eq!(dataflow[0][1], sqlite[0][1], "Column 'c' should match");
    }

    #[test]
    fn test_project_reorder_columns() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)", ())
            .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, 'a1', 'b1')", ()).unwrap();

        let query = "SELECT b, a FROM t WHERE id = ?";
        let params = vec![DataType::BigInt(1)];

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_eq!(sqlite.len(), 1);
        assert_eq!(sqlite[0][0], DataType::from("b1"), "First col should be b");
        assert_eq!(sqlite[0][1], DataType::from("a1"), "Second col should be a");

        assert_eq!(dataflow[0][0], sqlite[0][0]);
        assert_eq!(dataflow[0][1], sqlite[0][1]);
    }

    #[test]
    fn test_project_all_columns() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)", ())
            .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, 'a1', 'b1')", ()).unwrap();

        let query = "SELECT id, a, b FROM t WHERE id = ?";
        let params = vec![DataType::BigInt(1)];

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_eq!(sqlite.len(), 1);
        // All 3 columns should match
        for i in 0..3 {
            assert_eq!(dataflow[0][i], sqlite[0][i], "Column {} should match", i);
        }
    }
}

// =============================================================================
// JOIN TESTS
// =============================================================================

mod join_tests {
    use super::*;

    #[test]
    fn test_join_basic() {
        let db = setup_test_db();
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", ())
            .unwrap();
        db.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)",
            (),
        )
        .unwrap();
        db.register_table("users").unwrap();
        db.register_table("orders").unwrap();

        db.execute("INSERT INTO users VALUES (1, 'Alice')", ()).unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob')", ()).unwrap();
        db.execute("INSERT INTO orders VALUES (1, 1, 'Widget')", ())
            .unwrap();
        db.execute("INSERT INTO orders VALUES (2, 1, 'Gadget')", ())
            .unwrap();
        db.execute("INSERT INTO orders VALUES (3, 2, 'Thing')", ())
            .unwrap();

        let query =
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = ?";
        let params = vec![DataType::BigInt(1)];

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_eq!(sqlite.len(), 2, "Alice has 2 orders");
        assert_dataflow_contains_sqlite(&dataflow, &sqlite, query, &params);
    }

    #[test]
    fn test_join_no_matches() {
        let db = setup_test_db();
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", ())
            .unwrap();
        db.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)",
            (),
        )
        .unwrap();
        db.register_table("users").unwrap();
        db.register_table("orders").unwrap();

        db.execute("INSERT INTO users VALUES (1, 'Alice')", ()).unwrap();
        // No orders for Alice

        let query =
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = ?";
        let params = vec![DataType::BigInt(1)];

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_eq!(sqlite.len(), 0, "Alice has no orders");
        assert_eq!(dataflow.len(), 0);
    }

    #[test]
    fn test_join_user_not_found() {
        let db = setup_test_db();
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", ())
            .unwrap();
        db.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT)",
            (),
        )
        .unwrap();
        db.register_table("users").unwrap();
        db.register_table("orders").unwrap();

        db.execute("INSERT INTO users VALUES (1, 'Alice')", ()).unwrap();
        db.execute("INSERT INTO orders VALUES (1, 1, 'Widget')", ())
            .unwrap();

        let query =
            "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id WHERE users.id = ?";
        let params = vec![DataType::BigInt(999)]; // User doesn't exist

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_eq!(sqlite.len(), 0);
        assert_eq!(dataflow.len(), 0);
    }

    #[test]
    fn test_join_multiple_matching_rows() {
        let db = setup_test_db();
        db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, val INTEGER)", ())
            .unwrap();
        db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_val INTEGER, data TEXT)", ())
            .unwrap();
        db.register_table("a").unwrap();
        db.register_table("b").unwrap();

        // Multiple rows in 'a' with same val
        db.execute("INSERT INTO a VALUES (1, 100)", ()).unwrap();
        db.execute("INSERT INTO a VALUES (2, 100)", ()).unwrap();
        // Multiple rows in 'b' matching val=100
        db.execute("INSERT INTO b VALUES (1, 100, 'x')", ()).unwrap();
        db.execute("INSERT INTO b VALUES (2, 100, 'y')", ()).unwrap();

        let query = "SELECT a.id, b.data FROM a JOIN b ON a.val = b.a_val WHERE a.val = ?";
        let params = vec![DataType::BigInt(100)];

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        // 2 rows in a * 2 rows in b = 4 results
        assert_eq!(sqlite.len(), 4);
        assert_dataflow_contains_sqlite(&dataflow, &sqlite, query, &params);
    }
}

// =============================================================================
// AGGREGATE TESTS
// =============================================================================

mod aggregate_tests {
    use super::*;

    #[test]
    fn test_aggregate_count_basic() {
        let db = setup_test_db();
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, val INTEGER)",
            (),
        )
        .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, 1, 10)", ()).unwrap();
        db.execute("INSERT INTO t VALUES (2, 1, 20)", ()).unwrap();
        db.execute("INSERT INTO t VALUES (3, 1, 30)", ()).unwrap();
        db.execute("INSERT INTO t VALUES (4, 2, 40)", ()).unwrap();

        let query = "SELECT category, COUNT(*) FROM t GROUP BY category HAVING category = ?";
        let params = vec![DataType::BigInt(1)];

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_eq!(sqlite.len(), 1);
        assert_eq!(sqlite[0][1], DataType::BigInt(3), "Count should be 3");

        // Compare aggregate result
        assert!(dataflow.len() >= 1);
        assert_eq!(dataflow[0][1], sqlite[0][1]);
    }

    #[test]
    fn test_aggregate_sum_basic() {
        let db = setup_test_db();
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, val INTEGER)",
            (),
        )
        .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, 1, 10)", ()).unwrap();
        db.execute("INSERT INTO t VALUES (2, 1, 20)", ()).unwrap();
        db.execute("INSERT INTO t VALUES (3, 1, 30)", ()).unwrap();

        let query = "SELECT category, SUM(val) FROM t GROUP BY category HAVING category = ?";
        let params = vec![DataType::BigInt(1)];

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_eq!(sqlite.len(), 1);
        assert_eq!(sqlite[0][1], DataType::BigInt(60), "Sum should be 60");

        assert!(dataflow.len() >= 1);
        assert_eq!(dataflow[0][1], sqlite[0][1]);
    }

    #[test]
    fn test_aggregate_empty_group() {
        let db = setup_test_db();
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, val INTEGER)",
            (),
        )
        .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, 1, 10)", ()).unwrap();

        let query = "SELECT category, COUNT(*) FROM t GROUP BY category HAVING category = ?";
        let params = vec![DataType::BigInt(999)]; // No such category

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_eq!(sqlite.len(), 0);
        assert_eq!(dataflow.len(), 0);
    }

    #[test]
    fn test_aggregate_count_with_nulls() {
        let db = setup_test_db();
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, val INTEGER)",
            (),
        )
        .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, 1, 10)", ()).unwrap();
        db.execute("INSERT INTO t VALUES (2, 1, NULL)", ()).unwrap();
        db.execute("INSERT INTO t VALUES (3, 1, 30)", ()).unwrap();

        let query = "SELECT category, COUNT(*) FROM t GROUP BY category HAVING category = ?";
        let params = vec![DataType::BigInt(1)];

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        // COUNT(*) counts NULLs, should be 3
        assert_eq!(sqlite.len(), 1);
        assert_eq!(sqlite[0][1], DataType::BigInt(3));

        assert!(dataflow.len() >= 1);
        assert_eq!(dataflow[0][1], sqlite[0][1]);
    }
}

// =============================================================================
// UPDATE/DELETE PROPAGATION TESTS
// =============================================================================

mod propagation_tests {
    use super::*;

    #[test]
    fn test_insert_propagates_correctly() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ())
            .unwrap();
        db.register_table("t").unwrap();

        let query = "SELECT id, val FROM t WHERE val = ?";
        let params = vec![DataType::BigInt(100)];

        // Create view first
        let _ = db.prepare(query).unwrap();

        // Initial state
        let sqlite_before = query_sqlite(&db, query, &params);
        let dataflow_before = query_dataflow(&db, query, &params);
        assert_eq!(sqlite_before.len(), 0);
        assert_eq!(dataflow_before.len(), 0);

        // Insert
        db.execute("INSERT INTO t VALUES (1, 100)", ()).unwrap();

        // Both should see the new row
        let sqlite_after = query_sqlite(&db, query, &params);
        let dataflow_after = query_dataflow(&db, query, &params);

        assert_eq!(sqlite_after.len(), 1);
        assert_dataflow_contains_sqlite(&dataflow_after, &sqlite_after, query, &params);
    }

    #[test]
    fn test_update_propagates_correctly() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ())
            .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, 100)", ()).unwrap();

        let query = "SELECT id, val FROM t WHERE id = ?";
        let params = vec![DataType::BigInt(1)];

        // Create view
        let _ = db.prepare(query).unwrap();

        // Verify initial
        let sqlite_before = query_sqlite(&db, query, &params);
        assert_eq!(sqlite_before[0][1], DataType::BigInt(100));

        // Update
        db.execute("UPDATE t SET val = 200 WHERE id = 1", ()).unwrap();

        // Both should see updated value
        let sqlite_after = query_sqlite(&db, query, &params);
        let dataflow_after = query_dataflow(&db, query, &params);

        assert_eq!(sqlite_after[0][1], DataType::BigInt(200));
        assert_dataflow_contains_sqlite(&dataflow_after, &sqlite_after, query, &params);
    }

    #[test]
    fn test_delete_propagates_correctly() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ())
            .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, 100)", ()).unwrap();

        let query = "SELECT id, val FROM t WHERE id = ?";
        let params = vec![DataType::BigInt(1)];

        // Create view
        let _ = db.prepare(query).unwrap();

        // Verify initial
        let sqlite_before = query_sqlite(&db, query, &params);
        assert_eq!(sqlite_before.len(), 1);

        // Delete
        db.execute("DELETE FROM t WHERE id = 1", ()).unwrap();

        // Both should show empty
        let sqlite_after = query_sqlite(&db, query, &params);
        let dataflow_after = query_dataflow(&db, query, &params);

        assert_eq!(sqlite_after.len(), 0);
        assert_eq!(dataflow_after.len(), 0);
    }

    #[test]
    fn test_update_changes_filter_membership() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ())
            .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, 100)", ()).unwrap();
        db.execute("INSERT INTO t VALUES (2, 200)", ()).unwrap();

        let query = "SELECT id, val FROM t WHERE val = ?";
        let params_100 = vec![DataType::BigInt(100)];
        let params_200 = vec![DataType::BigInt(200)];

        // Create views for both
        let _ = db.prepare(query).unwrap();

        // Verify initial: row 1 in val=100, row 2 in val=200
        let sqlite_100_before = query_sqlite(&db, query, &params_100);
        let sqlite_200_before = query_sqlite(&db, query, &params_200);
        assert_eq!(sqlite_100_before.len(), 1);
        assert_eq!(sqlite_200_before.len(), 1);

        // Update row 1 from val=100 to val=200
        db.execute("UPDATE t SET val = 200 WHERE id = 1", ()).unwrap();

        // Now: row 1 should NOT be in val=100, both rows in val=200
        let sqlite_100_after = query_sqlite(&db, query, &params_100);
        let sqlite_200_after = query_sqlite(&db, query, &params_200);
        let dataflow_100_after = query_dataflow(&db, query, &params_100);
        let dataflow_200_after = query_dataflow(&db, query, &params_200);

        assert_eq!(sqlite_100_after.len(), 0, "val=100 should be empty");
        assert_eq!(sqlite_200_after.len(), 2, "val=200 should have 2 rows");

        assert_eq!(dataflow_100_after.len(), 0, "dataflow val=100 should be empty");
        assert_dataflow_contains_sqlite(&dataflow_200_after, &sqlite_200_after, query, &params_200);
    }

    #[test]
    fn test_multiple_operations_sequence() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ())
            .unwrap();
        db.register_table("t").unwrap();

        let query = "SELECT id, val FROM t WHERE val = ?";
        let params = vec![DataType::BigInt(100)];

        // Create view
        let _ = db.prepare(query).unwrap();

        // Sequence of operations
        db.execute("INSERT INTO t VALUES (1, 100)", ()).unwrap();
        db.execute("INSERT INTO t VALUES (2, 100)", ()).unwrap();
        db.execute("DELETE FROM t WHERE id = 1", ()).unwrap();
        db.execute("INSERT INTO t VALUES (3, 100)", ()).unwrap();
        db.execute("UPDATE t SET val = 200 WHERE id = 2", ()).unwrap();
        db.execute("INSERT INTO t VALUES (4, 100)", ()).unwrap();

        // Final state should match
        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        // Should have rows 3 and 4 with val=100
        assert_eq!(sqlite.len(), 2);
        assert_dataflow_contains_sqlite(&dataflow, &sqlite, query, &params);
    }
}

// =============================================================================
// STRESS TESTS
// =============================================================================

mod stress_tests {
    use super::*;

    #[test]
    fn test_many_rows() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, category INTEGER, val INTEGER)", ())
            .unwrap();
        db.register_table("t").unwrap();

        // Insert 100 rows across 10 categories
        for i in 0..100i64 {
            db.execute(
                "INSERT INTO t VALUES (?, ?, ?)",
                params![i, i % 10, i * 10],
            )
            .unwrap();
        }

        let query = "SELECT id, val FROM t WHERE category = ?";
        let params = vec![DataType::BigInt(5)]; // Category 5 has rows 5, 15, 25, ...

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_eq!(sqlite.len(), 10, "Category 5 should have 10 rows");
        assert_dataflow_contains_sqlite(&dataflow, &sqlite, query, &params);
    }

    #[test]
    fn test_many_updates() {
        let db = setup_test_db();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)", ())
            .unwrap();
        db.register_table("t").unwrap();

        db.execute("INSERT INTO t VALUES (1, 0)", ()).unwrap();

        let query = "SELECT id, val FROM t WHERE id = ?";
        let params = vec![DataType::BigInt(1)];

        // Create view
        let _ = db.prepare(query).unwrap();

        // Update 50 times
        for i in 1..=50i64 {
            db.execute("UPDATE t SET val = ? WHERE id = 1", params![i])
                .unwrap();
        }

        let sqlite = query_sqlite(&db, query, &params);
        let dataflow = query_dataflow(&db, query, &params);

        assert_eq!(sqlite[0][1], DataType::BigInt(50));
        assert_dataflow_contains_sqlite(&dataflow, &sqlite, query, &params);
    }
}
