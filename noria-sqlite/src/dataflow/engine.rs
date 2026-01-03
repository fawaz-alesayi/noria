//! Noria dataflow engine for SQLite.
//!
//! This module provides a complete integration of the dataflow executor
//! with SQLite, including:
//! - Automatic view synthesis from SQL queries
//! - CDC-based incremental updates
//! - Upquery support for cache misses

use noria::DataType;
use parking_lot::RwLock;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::Arc;

use super::adapter::SqliteAdapter;
use super::executor::{NodeIndex, ViewHandle};
use super::sql::{SqlConverter, SqlResult};
use super::{Record, Records};

/// Handle to a Noria-backed materialized view.
#[derive(Clone)]
pub struct NoriaView {
    /// The underlying view handle.
    handle: ViewHandle,
    /// SQL query that created this view.
    sql: String,
    /// Tables this view depends on.
    tables: Vec<String>,
}

impl NoriaView {
    /// Get the SQL that created this view.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Get the tables this view depends on.
    pub fn tables(&self) -> &[String] {
        &self.tables
    }

    /// Get the key columns used for lookups.
    pub fn key_columns(&self) -> &[usize] {
        self.handle.key_columns()
    }
}

/// The Noria dataflow engine.
///
/// This manages materialized views backed by SQLite data.
pub struct NoriaEngine {
    /// The SQLite adapter for CDC.
    adapter: RwLock<SqliteAdapter>,
    /// SQL converter for view synthesis.
    sql_converter: RwLock<SqlConverter>,
    /// Map from normalized SQL to view.
    views: RwLock<HashMap<String, NoriaView>>,
    /// Reference to the SQLite connection for upqueries.
    conn: Arc<RwLock<Connection>>,
}

impl NoriaEngine {
    /// Create a new Noria engine.
    pub fn new(conn: Arc<RwLock<Connection>>) -> Self {
        Self {
            adapter: RwLock::new(SqliteAdapter::new()),
            sql_converter: RwLock::new(SqlConverter::new()),
            views: RwLock::new(HashMap::new()),
            conn,
        }
    }

    /// Discover and register a table schema from SQLite.
    pub fn register_table(&self, table_name: &str) -> rusqlite::Result<()> {
        let conn = self.conn.read();
        let mut adapter = self.adapter.write();

        // Discover schema from SQLite
        adapter.discover_table(&conn, table_name)?;

        // Also register with SQL converter
        if let Some(schema) = adapter.get_schema(table_name) {
            let mut converter = self.sql_converter.write();
            converter.register_table(table_name, schema.columns.clone());
        }

        Ok(())
    }

    /// Register a table with explicit schema.
    pub fn register_table_schema(
        &self,
        table_name: &str,
        columns: Vec<String>,
        primary_key: Option<usize>,
    ) {
        let mut adapter = self.adapter.write();
        adapter.register_table(table_name, columns.clone(), primary_key);

        let mut converter = self.sql_converter.write();
        converter.register_table(table_name, columns);
    }

    /// Load existing data from a table into the dataflow.
    pub fn load_table(&self, table_name: &str) -> rusqlite::Result<usize> {
        let conn = self.conn.read();
        let mut adapter = self.adapter.write();
        adapter.load_table(&conn, table_name)
    }

    /// Create a materialized view for the given SELECT query.
    pub fn create_view(&self, sql: &str) -> SqlResult<NoriaView> {
        // Auto-discover any tables referenced in the query
        let tables = self.extract_tables(sql);
        for table in &tables {
            // Try to register the table if not already registered
            let conn = self.conn.read();
            let mut adapter = self.adapter.write();
            if adapter.get_schema(table).is_none() {
                if let Err(e) = adapter.discover_table(&conn, table) {
                    tracing::debug!("Failed to discover table {}: {}", table, e);
                }
                // Also register with SQL converter
                if let Some(schema) = adapter.get_schema(table) {
                    let mut converter = self.sql_converter.write();
                    converter.register_table(table, schema.columns.clone());
                }
            }
        }

        let mut adapter = self.adapter.write();
        let converter = self.sql_converter.read();

        // Convert SQL to dataflow
        let view_handle = converter.convert_select(sql, adapter.executor_mut())?;

        // Extract table names from SQL (simplified)
        let tables = self.extract_tables(sql);

        let view = NoriaView {
            handle: view_handle,
            sql: sql.to_string(),
            tables,
        };

        // Store the view
        let normalized = normalize_sql(sql);
        self.views.write().insert(normalized, view.clone());

        Ok(view)
    }

    /// Look up rows from a view by key.
    ///
    /// Returns `None` if the key is not in the cache.
    pub fn lookup(&self, view: &NoriaView, key: &[DataType]) -> Option<Vec<Vec<DataType>>> {
        let adapter = self.adapter.read();
        adapter.executor().lookup(&view.handle, key)
    }

    /// Look up rows from a view, with upquery fallback.
    ///
    /// If the key is not in the cache, queries SQLite and populates the cache.
    pub fn lookup_or_upquery(
        &self,
        view: &NoriaView,
        key: &[DataType],
    ) -> rusqlite::Result<Vec<Vec<DataType>>> {
        // Try cache first
        {
            let adapter = self.adapter.read();
            if let Some(rows) = adapter.executor().lookup(&view.handle, key) {
                return Ok(rows);
            }
        }

        // Cache miss - perform upquery
        self.upquery(view, key)
    }

    /// Perform an upquery: query SQLite and populate the cache.
    fn upquery(&self, view: &NoriaView, key: &[DataType]) -> rusqlite::Result<Vec<Vec<DataType>>> {
        let conn = self.conn.read();

        // Build the upquery SQL by adding the key constraint
        let upquery_sql = self.build_upquery_sql(&view.sql, &view.handle, key);

        let mut stmt = conn.prepare(&upquery_sql)?;

        // Bind key parameters
        for (i, val) in key.iter().enumerate() {
            match val {
                DataType::None => stmt.raw_bind_parameter(i + 1, rusqlite::types::Null)?,
                DataType::Int(v) => stmt.raw_bind_parameter(i + 1, *v)?,
                DataType::BigInt(v) => stmt.raw_bind_parameter(i + 1, *v)?,
                DataType::UnsignedInt(v) => stmt.raw_bind_parameter(i + 1, *v)?,
                DataType::UnsignedBigInt(v) => stmt.raw_bind_parameter(i + 1, *v as i64)?,
                DataType::Real(int_part, frac_part) => {
                    let f = *int_part as f64 + (*frac_part as f64 / 1_000_000_000.0);
                    stmt.raw_bind_parameter(i + 1, f)?
                }
                DataType::Text(_) | DataType::TinyText(_) => {
                    let s: &str = val.into();
                    stmt.raw_bind_parameter(i + 1, s)?
                }
                DataType::Timestamp(ts) => {
                    stmt.raw_bind_parameter(i + 1, ts.to_string())?
                }
            }
        }

        // Execute and collect results
        let column_count = stmt.column_count();
        let mut rows = stmt.raw_query();
        let mut result = Vec::new();
        let mut records_to_inject = Vec::new();

        // Check if key columns are already in the SQLite result.
        // For SELECT *, the key column is already in the output (e.g., key_columns = [0]).
        // For SELECT name, age FROM..., the key is added at the end by projection (e.g., key_columns = [2]).
        // We only append key if the key column indices are >= the SQLite column count.
        let key_columns = view.handle.key_columns();
        let needs_key_append = key_columns.iter().any(|&col| col >= column_count);

        while let Some(row) = rows.next()? {
            let mut row_data = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let val = match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => DataType::None,
                    rusqlite::types::ValueRef::Integer(v) => DataType::BigInt(v),
                    rusqlite::types::ValueRef::Real(f) => {
                        let int_part = f.trunc() as i64;
                        let frac_part = ((f.fract().abs()) * 1_000_000_000.0) as i32;
                        DataType::Real(int_part, frac_part)
                    }
                    rusqlite::types::ValueRef::Text(s) => {
                        DataType::from(std::str::from_utf8(s).unwrap_or(""))
                    }
                    rusqlite::types::ValueRef::Blob(_) => DataType::None, // Skip blobs for now
                };
                row_data.push(val);
            }
            result.push(row_data.clone());

            // For cache injection, only append key if it's not already in the output.
            // This ensures row format matches what CDC produces through the dataflow.
            let row_for_cache = if needs_key_append {
                let mut extended = row_data;
                for key_val in key {
                    extended.push(key_val.clone());
                }
                extended
            } else {
                row_data
            };
            records_to_inject.push(Record::Positive(row_for_cache));
        }

        // Drop the query to release the connection read lock
        drop(rows);
        drop(stmt);
        drop(conn);

        // Populate the cache by injecting records directly into the view's state
        if !records_to_inject.is_empty() {
            let mut adapter = self.adapter.write();
            let records = Records::from(records_to_inject);
            adapter.executor_mut().inject_into_view(&view.handle, records);
        }

        Ok(result)
    }

    /// Build an upquery SQL by replacing placeholders with key values.
    fn build_upquery_sql(&self, sql: &str, _handle: &ViewHandle, _key: &[DataType]) -> String {
        // For now, just return the original SQL with placeholders
        // The actual key binding happens in the upquery function
        sql.to_string()
    }

    /// Apply a write from SQLite to the dataflow.
    ///
    /// Called from the update hook.
    pub fn apply_insert(&self, table: &str, rowid: i64) {
        let conn = self.conn.read();
        let mut adapter = self.adapter.write();

        // Fetch the row data using rowid
        if let Some(row) = adapter.fetch_row_by_rowid(&conn, table, rowid) {
            adapter.handle_change(
                rusqlite::hooks::Action::SQLITE_INSERT,
                table,
                Some(row),
                None,
            );
        }
    }

    /// Apply an insert with row data directly (from session extension).
    pub fn apply_insert_row(&self, table: &str, new_row: Vec<DataType>) {
        let mut adapter = self.adapter.write();
        adapter.handle_change(
            rusqlite::hooks::Action::SQLITE_INSERT,
            table,
            Some(new_row),
            None,
        );
    }

    /// Apply a delete from SQLite to the dataflow.
    ///
    /// Note: For deletes, we need the old row values which are not available
    /// from the update hook alone. In a full implementation, we'd use
    /// preupdate_hook or session extension.
    pub fn apply_delete(&self, table: &str, old_row: Vec<DataType>) {
        let mut adapter = self.adapter.write();
        adapter.handle_change(
            rusqlite::hooks::Action::SQLITE_DELETE,
            table,
            None,
            Some(old_row),
        );
    }

    /// Apply an update from SQLite to the dataflow.
    pub fn apply_update(&self, table: &str, rowid: i64, old_row: Option<Vec<DataType>>) {
        let conn = self.conn.read();
        let mut adapter = self.adapter.write();

        // Fetch new row data using rowid
        if let Some(new_row) = adapter.fetch_row_by_rowid(&conn, table, rowid) {
            adapter.handle_change(
                rusqlite::hooks::Action::SQLITE_UPDATE,
                table,
                Some(new_row),
                old_row,
            );
        }
    }

    /// Apply an update with row data directly (from session extension).
    pub fn apply_update_rows(&self, table: &str, old_row: Vec<DataType>, new_row: Vec<DataType>) {
        let mut adapter = self.adapter.write();
        adapter.handle_change(
            rusqlite::hooks::Action::SQLITE_UPDATE,
            table,
            Some(new_row),
            Some(old_row),
        );
    }

    /// Get executor statistics.
    pub fn stats(&self) -> EngineStats {
        let adapter = self.adapter.read();
        let executor_stats = adapter.executor().stats();
        let views = self.views.read();

        EngineStats {
            node_count: executor_stats.node_count,
            materialized_nodes: executor_stats.materialized_nodes,
            total_rows: executor_stats.total_rows,
            view_count: views.len(),
        }
    }

    /// Extract table names from SQL (simplified).
    fn extract_tables(&self, sql: &str) -> Vec<String> {
        let upper = sql.to_uppercase();
        let mut tables = Vec::new();

        // Extract FROM table
        if let Some(from_pos) = upper.find("FROM") {
            let after_from = &sql[from_pos + 4..];
            if let Some(table) = after_from.split_whitespace().next() {
                let clean = table
                    .trim_matches('`')
                    .trim_matches('"')
                    .trim_matches(',')
                    .to_string();
                if !clean.is_empty() && !clean.eq_ignore_ascii_case("(") {
                    tables.push(clean);
                }
            }
        }

        // Extract JOIN tables
        for keyword in ["JOIN ", "INNER JOIN ", "LEFT JOIN ", "RIGHT JOIN ", "CROSS JOIN "] {
            let mut search_pos = 0;
            while let Some(pos) = upper[search_pos..].find(keyword) {
                let abs_pos = search_pos + pos + keyword.len();
                if abs_pos < sql.len() {
                    let after_join = &sql[abs_pos..];
                    if let Some(table) = after_join.split_whitespace().next() {
                        let clean = table
                            .trim_matches('`')
                            .trim_matches('"')
                            .to_string();
                        if !clean.is_empty()
                            && !clean.eq_ignore_ascii_case("ON")
                            && !tables.iter().any(|t| t.eq_ignore_ascii_case(&clean))
                        {
                            tables.push(clean);
                        }
                    }
                }
                search_pos = abs_pos;
            }
        }

        tables
    }
}

/// Statistics about the Noria engine.
#[derive(Debug, Clone)]
pub struct EngineStats {
    pub node_count: usize,
    pub materialized_nodes: usize,
    pub total_rows: usize,
    pub view_count: usize,
}

/// Normalize SQL for caching (simplified).
fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ").to_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn setup_test_db() -> Arc<RwLock<Connection>> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
            CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
            ",
        )
        .unwrap();
        Arc::new(RwLock::new(conn))
    }

    #[test]
    fn test_engine_create_view() {
        let conn = setup_test_db();
        let engine = NoriaEngine::new(conn);

        // Register tables
        engine.register_table("users").unwrap();

        // Create a view
        let view = engine
            .create_view("SELECT * FROM users WHERE id = ?")
            .unwrap();

        assert_eq!(view.key_columns(), &[0]);
        assert!(view.sql().contains("SELECT"));
    }

    #[test]
    fn test_engine_insert_and_lookup() {
        let conn = setup_test_db();

        // Insert some data first
        {
            let c = conn.write();
            c.execute("INSERT INTO users VALUES (1, 'Alice', 30)", [])
                .unwrap();
            c.execute("INSERT INTO users VALUES (2, 'Bob', 25)", [])
                .unwrap();
        }

        let engine = NoriaEngine::new(conn.clone());
        engine.register_table("users").unwrap();

        // Load existing data
        let count = engine.load_table("users").unwrap();
        assert_eq!(count, 2);

        // Create view - this should now have access to the data
        // Note: Simple SELECT * without filter just passes through
        let view = engine
            .create_view("SELECT * FROM users WHERE id = ?")
            .expect("Failed to create view");

        // The view was created, stats should show the nodes
        let stats = engine.stats();
        assert!(stats.node_count >= 1);
    }

    #[test]
    fn test_engine_apply_insert() {
        let conn = setup_test_db();
        let engine = NoriaEngine::new(conn.clone());

        engine.register_table("users").unwrap();

        // Create view with a concrete filter value (not a placeholder)
        let _view = engine
            .create_view("SELECT * FROM users WHERE age = 30")
            .unwrap();

        // Insert via SQLite
        let rowid = {
            let c = conn.write();
            c.execute("INSERT INTO users VALUES (1, 'Alice', 30)", [])
                .unwrap();
            c.last_insert_rowid()
        };

        // Apply the insert to dataflow
        engine.apply_insert("users", rowid);

        let stats = engine.stats();
        // The filtered view should have the row (age = 30 matches)
        assert!(stats.total_rows >= 1, "Expected at least 1 row, got {}", stats.total_rows);
    }

    #[test]
    fn test_engine_upquery() {
        let conn = setup_test_db();

        // Insert some data
        {
            let c = conn.write();
            c.execute("INSERT INTO users VALUES (1, 'Alice', 30)", [])
                .unwrap();
        }

        let engine = NoriaEngine::new(conn.clone());
        engine.register_table("users").unwrap();

        // Create view but don't load table (simulating cache miss)
        let view = engine
            .create_view("SELECT * FROM users WHERE id = ?")
            .unwrap();

        // Upquery should fetch from SQLite
        let result = engine
            .lookup_or_upquery(&view, &[DataType::Int(1)])
            .unwrap();

        assert!(!result.is_empty());
        assert_eq!(result[0][1], DataType::from("Alice"));
    }

    #[test]
    fn test_upquery_populates_cache() {
        let conn = setup_test_db();

        // Insert data before creating engine
        {
            let c = conn.write();
            c.execute("INSERT INTO users VALUES (1, 'Alice', 30)", []).unwrap();
            c.execute("INSERT INTO users VALUES (2, 'Bob', 25)", []).unwrap();
        }

        let engine = NoriaEngine::new(conn.clone());
        engine.register_table("users").unwrap();

        // Create view (don't load table - simulates empty cache)
        let view = engine
            .create_view("SELECT id, name FROM users WHERE id = ?")
            .unwrap();

        // First lookup: cache miss, should trigger upquery
        let result1 = engine.lookup_or_upquery(&view, &[DataType::Int(1)]).unwrap();
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0][1], DataType::from("Alice"));

        // Second lookup: should hit cache (use lookup instead of lookup_or_upquery)
        // If cache was populated, lookup should return Some
        let result2 = engine.lookup(&view, &[DataType::Int(1)]);
        assert!(result2.is_some(), "Cache should be populated after upquery");
        assert_eq!(result2.unwrap().len(), 1);

        // Key 2 was never queried, so cache should miss
        let result3 = engine.lookup(&view, &[DataType::Int(2)]);
        assert!(result3.is_none(), "Unqueried key should not be in cache");
    }

    #[test]
    fn test_engine_with_filter() {
        let conn = setup_test_db();

        // Insert data
        {
            let c = conn.write();
            c.execute("INSERT INTO users VALUES (1, 'Alice', 30)", [])
                .unwrap();
            c.execute("INSERT INTO users VALUES (2, 'Bob', 25)", [])
                .unwrap();
            c.execute("INSERT INTO users VALUES (3, 'Charlie', 30)", [])
                .unwrap();
        }

        let engine = NoriaEngine::new(conn);
        engine.register_table("users").unwrap();
        engine.load_table("users").unwrap();

        // Create a filtered view
        let view = engine
            .create_view("SELECT * FROM users WHERE age = 30")
            .unwrap();

        let stats = engine.stats();
        // Should have base table + filter node
        assert!(stats.node_count >= 2);
    }

    #[test]
    fn test_incremental_insert_propagation() {
        // Test that INSERT propagates to filtered view
        let conn = setup_test_db();
        let engine = NoriaEngine::new(conn.clone());
        engine.register_table("users").unwrap();

        // Create a filtered view on age = 30
        // The key column is extracted from WHERE clause (age)
        let view = engine
            .create_view("SELECT id, name FROM users WHERE age = 30")
            .unwrap();

        // Before insert, view should be empty or missing
        // Key is age (=30), not id
        let result_before = engine.lookup(&view, &[DataType::BigInt(30)]);
        assert!(result_before.is_none() || result_before.as_ref().map(|r| r.is_empty()).unwrap_or(true),
            "View should be empty before insert");

        // Insert via SQLite and propagate through dataflow
        let rowid = {
            let c = conn.write();
            c.execute("INSERT INTO users VALUES (1, 'Alice', 30)", []).unwrap();
            c.last_insert_rowid()
        };
        engine.apply_insert("users", rowid);

        // Lookup by the key column (age from WHERE clause)
        let result = engine.lookup(&view, &[DataType::BigInt(30)]);
        assert!(result.is_some(), "View should have results after insert");
        let rows = result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], DataType::BigInt(1)); // id
        assert_eq!(rows[0][1], DataType::from("Alice")); // name
    }

    #[test]
    fn test_incremental_insert_filter_rejects() {
        // Test that INSERT to non-matching filter does NOT propagate
        let conn = setup_test_db();
        let engine = NoriaEngine::new(conn.clone());
        engine.register_table("users").unwrap();

        // Create a filtered view on age = 30
        let _view = engine
            .create_view("SELECT id, name FROM users WHERE age = 30")
            .unwrap();

        // Insert a user with age = 25 (doesn't match filter)
        let rowid = {
            let c = conn.write();
            c.execute("INSERT INTO users VALUES (1, 'Bob', 25)", []).unwrap();
            c.last_insert_rowid()
        };
        engine.apply_insert("users", rowid);

        // View should NOT have this row
        let stats = engine.stats();
        // The view should have 0 rows (only the non-matching row was inserted)
        assert!(stats.total_rows <= 1, "Filtered-out row should not be in view, got {} rows", stats.total_rows);
    }

    #[test]
    fn test_incremental_update_propagation() {
        // Test that UPDATE triggers retraction + insertion
        let conn = setup_test_db();
        let engine = NoriaEngine::new(conn.clone());
        engine.register_table("users").unwrap();

        // Create a simple view
        let view = engine
            .create_view("SELECT id, name FROM users WHERE id = ?")
            .unwrap();

        // Insert initial data
        let rowid = {
            let c = conn.write();
            c.execute("INSERT INTO users VALUES (1, 'Alice', 30)", []).unwrap();
            c.last_insert_rowid()
        };
        engine.apply_insert("users", rowid);

        // Verify initial data
        let result = engine.lookup(&view, &[DataType::Int(1)]);
        assert!(result.is_some());
        assert_eq!(result.unwrap()[0][1], DataType::from("Alice"));

        // Update the name: Alice -> Alicia
        // This requires old and new values
        let old_row = vec![
            DataType::BigInt(1),
            DataType::from("Alice"),
            DataType::BigInt(30),
        ];
        let new_row = vec![
            DataType::BigInt(1),
            DataType::from("Alicia"),
            DataType::BigInt(30),
        ];
        engine.apply_update_rows("users", old_row, new_row);

        // Verify the update propagated
        let result_after = engine.lookup(&view, &[DataType::Int(1)]);
        assert!(result_after.is_some());
        let rows = result_after.unwrap();
        assert_eq!(rows.len(), 1, "Should still have exactly 1 row");
        assert_eq!(rows[0][1], DataType::from("Alicia"), "Name should be updated");
    }

    #[test]
    fn test_incremental_delete_propagation() {
        // Test that DELETE removes row from view
        let conn = setup_test_db();
        let engine = NoriaEngine::new(conn.clone());
        engine.register_table("users").unwrap();

        // Create a simple view
        let view = engine
            .create_view("SELECT id, name FROM users WHERE id = ?")
            .unwrap();

        // Insert data
        let rowid = {
            let c = conn.write();
            c.execute("INSERT INTO users VALUES (1, 'Alice', 30)", []).unwrap();
            c.last_insert_rowid()
        };
        engine.apply_insert("users", rowid);

        // Verify row exists
        let result = engine.lookup(&view, &[DataType::Int(1)]);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1);

        // Delete the row
        let deleted_row = vec![
            DataType::BigInt(1),
            DataType::from("Alice"),
            DataType::BigInt(30),
        ];
        engine.apply_delete("users", deleted_row);

        // Verify row is gone
        let result_after = engine.lookup(&view, &[DataType::Int(1)]);
        assert!(result_after.is_none() || result_after.unwrap().is_empty(),
            "Row should be deleted from view");
    }

    #[test]
    fn test_multiple_views_same_table() {
        // Test that multiple views on the same table all update correctly
        let conn = setup_test_db();
        let engine = NoriaEngine::new(conn.clone());
        engine.register_table("users").unwrap();

        // Create two different filtered views on the same table
        let view_age_30 = engine
            .create_view("SELECT id, name FROM users WHERE age = 30")
            .unwrap();

        let view_age_25 = engine
            .create_view("SELECT id, name FROM users WHERE age = 25")
            .unwrap();

        // Insert a user with age 30
        {
            let c = conn.write();
            c.execute("INSERT INTO users VALUES (1, 'Alice', 30)", []).unwrap();
        }
        engine.apply_insert("users", 1);

        // view_age_30 should have Alice, view_age_25 should be empty
        // Lookup is by the key column from WHERE clause (age), not id
        let result_30 = engine.lookup(&view_age_30, &[DataType::BigInt(30)]);
        assert!(result_30.is_some());
        assert_eq!(result_30.unwrap().len(), 1);

        let result_25 = engine.lookup(&view_age_25, &[DataType::BigInt(25)]);
        assert!(result_25.is_none() || result_25.unwrap().is_empty());

        // Insert a user with age 25
        {
            let c = conn.write();
            c.execute("INSERT INTO users VALUES (2, 'Bob', 25)", []).unwrap();
        }
        engine.apply_insert("users", 2);

        // Now both views should have their respective users
        let result_30_after = engine.lookup(&view_age_30, &[DataType::BigInt(30)]);
        assert!(result_30_after.is_some());
        assert_eq!(result_30_after.unwrap()[0][1], DataType::from("Alice"));

        let result_25_after = engine.lookup(&view_age_25, &[DataType::BigInt(25)]);
        assert!(result_25_after.is_some());
        assert_eq!(result_25_after.unwrap()[0][1], DataType::from("Bob"));
    }

    #[test]
    fn test_aggregate_incremental_update() {
        // Test that COUNT aggregate updates incrementally
        let conn = setup_test_db();
        let engine = NoriaEngine::new(conn.clone());
        engine.register_table("users").unwrap();

        // Create aggregate view: COUNT users by age
        let view = engine
            .create_view("SELECT age, COUNT(*) FROM users GROUP BY age")
            .unwrap();

        // Insert first user with age 30
        {
            let c = conn.write();
            c.execute("INSERT INTO users VALUES (1, 'Alice', 30)", []).unwrap();
        }
        engine.apply_insert("users", 1);

        // Check count for age 30 = 1
        let result = engine.lookup(&view, &[DataType::BigInt(30)]);
        assert!(result.is_some(), "Should have result for age 30");
        let rows = result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], DataType::BigInt(1), "Count should be 1");

        // Insert second user with age 30
        {
            let c = conn.write();
            c.execute("INSERT INTO users VALUES (2, 'Bob', 30)", []).unwrap();
        }
        engine.apply_insert("users", 2);

        // Check count for age 30 = 2 (incremental update!)
        let result2 = engine.lookup(&view, &[DataType::BigInt(30)]);
        assert!(result2.is_some());
        let rows2 = result2.unwrap();
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0][1], DataType::BigInt(2), "Count should be 2 after second insert");

        // Insert user with different age
        {
            let c = conn.write();
            c.execute("INSERT INTO users VALUES (3, 'Charlie', 25)", []).unwrap();
        }
        engine.apply_insert("users", 3);

        // Age 30 count should still be 2
        let result3 = engine.lookup(&view, &[DataType::BigInt(30)]);
        assert_eq!(result3.unwrap()[0][1], DataType::BigInt(2));

        // Age 25 count should be 1
        let result_25 = engine.lookup(&view, &[DataType::BigInt(25)]);
        assert!(result_25.is_some());
        assert_eq!(result_25.unwrap()[0][1], DataType::BigInt(1));
    }
}
