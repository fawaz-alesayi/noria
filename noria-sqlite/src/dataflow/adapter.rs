//! SQLite adapter for CDC to dataflow Records conversion.
//!
//! This module bridges SQLite changes (INSERT/UPDATE/DELETE) to the
//! dataflow executor by converting them to Records.

use noria::DataType;
use rusqlite::hooks::Action;
use rusqlite::{Connection, Row};
use std::collections::HashMap;

use super::executor::{LocalExecutor, NodeIndex};
use super::{Record, Records};

/// Schema information for a table.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Table name.
    pub name: String,
    /// Column names.
    pub columns: Vec<String>,
    /// Primary key column index (if known).
    pub primary_key: Option<usize>,
}

/// Adapter that connects SQLite changes to the dataflow executor.
pub struct SqliteAdapter {
    /// Known table schemas (table_name -> schema).
    schemas: HashMap<String, TableSchema>,
    /// The dataflow executor.
    executor: LocalExecutor,
    /// Map from table name to base node index.
    base_nodes: HashMap<String, NodeIndex>,
}

impl SqliteAdapter {
    /// Create a new SQLite adapter with an executor.
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            executor: LocalExecutor::new(),
            base_nodes: HashMap::new(),
        }
    }

    /// Register a table schema.
    ///
    /// This must be called before any writes to the table can be processed.
    pub fn register_table(&mut self, name: &str, columns: Vec<String>, primary_key: Option<usize>) {
        // Store schema
        self.schemas.insert(
            name.to_string(),
            TableSchema {
                name: name.to_string(),
                columns: columns.clone(),
                primary_key,
            },
        );

        // Create base table in executor
        let node_idx = self.executor.add_base_table(name, columns);
        self.base_nodes.insert(name.to_string(), node_idx);
    }

    /// Auto-discover table schema from SQLite.
    ///
    /// Queries the sqlite_master and pragma to get table info.
    pub fn discover_table(&mut self, conn: &Connection, table_name: &str) -> rusqlite::Result<()> {
        let mut columns = Vec::new();
        let mut primary_key = None;

        // Use PRAGMA table_info to get column information
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table_name))?;
        let column_iter = stmt.query_map([], |row| {
            let cid: i64 = row.get(0)?;
            let name: String = row.get(1)?;
            let pk: bool = row.get(5)?;
            Ok((cid as usize, name, pk))
        })?;

        for col_result in column_iter {
            let (cid, name, pk) = col_result?;
            if pk {
                primary_key = Some(cid);
            }
            columns.push(name);
        }

        if !columns.is_empty() {
            self.register_table(table_name, columns, primary_key);
        }

        Ok(())
    }

    /// Get access to the executor.
    pub fn executor(&self) -> &LocalExecutor {
        &self.executor
    }

    /// Get mutable access to the executor.
    pub fn executor_mut(&mut self) -> &mut LocalExecutor {
        &mut self.executor
    }

    /// Handle a CDC event from SQLite.
    ///
    /// This is called from the update hook or after detecting a change.
    /// For INSERT/UPDATE, you need to also query the current row values.
    pub fn handle_change(
        &mut self,
        action: Action,
        table: &str,
        new_row: Option<Vec<DataType>>,
        old_row: Option<Vec<DataType>>,
    ) {
        // Skip if we don't know this table
        if !self.schemas.contains_key(table) {
            return;
        }

        let records = match action {
            Action::SQLITE_INSERT => {
                if let Some(row) = new_row {
                    Records::from(vec![Record::Positive(row)])
                } else {
                    return;
                }
            }
            Action::SQLITE_DELETE => {
                if let Some(row) = old_row {
                    Records::from(vec![Record::Negative(row)])
                } else {
                    return;
                }
            }
            Action::SQLITE_UPDATE => {
                // For updates, we emit negative for old and positive for new
                let mut recs = Vec::new();
                if let Some(old) = old_row {
                    recs.push(Record::Negative(old));
                }
                if let Some(new) = new_row {
                    recs.push(Record::Positive(new));
                }
                if recs.is_empty() {
                    return;
                }
                Records::from(recs)
            }
            _ => return,
        };

        // Apply to executor
        self.executor.apply_write(table, records);
    }

    /// Fetch row by rowid and convert to DataType vector.
    ///
    /// This is used after an INSERT/UPDATE to get the new row values.
    pub fn fetch_row_by_rowid(
        &self,
        conn: &Connection,
        table: &str,
        rowid: i64,
    ) -> Option<Vec<DataType>> {
        let schema = self.schemas.get(table)?;

        let sql = format!("SELECT * FROM {} WHERE rowid = ?", table);
        let mut stmt = conn.prepare(&sql).ok()?;

        let result = stmt.query_row([rowid], |row| Self::row_to_datatypes(row, schema.columns.len()));

        result.ok()
    }

    /// Convert a rusqlite Row to a Vec<DataType>.
    fn row_to_datatypes(row: &Row, num_cols: usize) -> rusqlite::Result<Vec<DataType>> {
        let mut result = Vec::with_capacity(num_cols);

        for i in 0..num_cols {
            let value = match row.get_ref(i)? {
                rusqlite::types::ValueRef::Null => DataType::None,
                rusqlite::types::ValueRef::Integer(i) => DataType::BigInt(i),
                rusqlite::types::ValueRef::Real(f) => {
                    // Convert f64 to fixed-point representation (integer, fractional)
                    // DataType::Real uses (i64, i32) where i32 is fractional part
                    let int_part = f.trunc() as i64;
                    // Get fractional part scaled to fit in i32 (up to 9 digits)
                    let frac_part = ((f.fract().abs()) * 1_000_000_000.0) as i32;
                    DataType::Real(int_part, frac_part)
                }
                rusqlite::types::ValueRef::Text(s) => {
                    DataType::from(std::str::from_utf8(s).unwrap_or(""))
                }
                rusqlite::types::ValueRef::Blob(b) => {
                    // Store blob as text for now (base64 would be better)
                    DataType::from(format!("BLOB:{}", b.len()).as_str())
                }
            };
            result.push(value);
        }

        Ok(result)
    }

    /// Bulk load all rows from a table into the executor.
    ///
    /// This is useful for initial cache population.
    pub fn load_table(&mut self, conn: &Connection, table: &str) -> rusqlite::Result<usize> {
        let schema = match self.schemas.get(table) {
            Some(s) => s.clone(),
            None => return Ok(0),
        };

        let sql = format!("SELECT * FROM {}", table);
        let mut stmt = conn.prepare(&sql)?;
        let num_cols = schema.columns.len();

        let mut count = 0;
        let mut records = Vec::new();

        let rows = stmt.query_map([], |row| Self::row_to_datatypes(row, num_cols))?;

        for row_result in rows {
            if let Ok(row_data) = row_result {
                records.push(Record::Positive(row_data));
                count += 1;
            }
        }

        if !records.is_empty() {
            self.executor.apply_write(table, Records::from(records));
        }

        Ok(count)
    }

    /// Get table schema.
    pub fn get_schema(&self, table: &str) -> Option<&TableSchema> {
        self.schemas.get(table)
    }
}

impl Default for SqliteAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_adapter_basic() {
        let mut adapter = SqliteAdapter::new();
        adapter.register_table(
            "users",
            vec!["id".into(), "name".into(), "age".into()],
            Some(0),
        );

        // Simulate INSERT
        adapter.handle_change(
            Action::SQLITE_INSERT,
            "users",
            Some(vec![
                DataType::Int(1),
                DataType::from("Alice"),
                DataType::Int(30),
            ]),
            None,
        );

        let stats = adapter.executor().stats();
        assert_eq!(stats.node_count, 1); // base table
    }

    #[test]
    fn test_adapter_update() {
        let mut adapter = SqliteAdapter::new();
        adapter.register_table("users", vec!["id".into(), "name".into()], Some(0));

        // Materialize the base table so we can check state
        {
            let base_node = *adapter.base_nodes.get("users").unwrap();
            adapter.executor_mut().materialize(base_node, vec![0]);
        }

        // INSERT
        adapter.handle_change(
            Action::SQLITE_INSERT,
            "users",
            Some(vec![DataType::Int(1), DataType::from("Alice")]),
            None,
        );

        // UPDATE (old name -> new name)
        adapter.handle_change(
            Action::SQLITE_UPDATE,
            "users",
            Some(vec![DataType::Int(1), DataType::from("Alicia")]),
            Some(vec![DataType::Int(1), DataType::from("Alice")]),
        );

        // Check state - should have updated name
        let stats = adapter.executor().stats();
        assert_eq!(stats.total_rows, 1);
    }

    #[test]
    fn test_adapter_delete() {
        let mut adapter = SqliteAdapter::new();
        adapter.register_table("users", vec!["id".into(), "name".into()], Some(0));

        // Materialize
        {
            let base_node = *adapter.base_nodes.get("users").unwrap();
            adapter.executor_mut().materialize(base_node, vec![0]);
        }

        // INSERT
        adapter.handle_change(
            Action::SQLITE_INSERT,
            "users",
            Some(vec![DataType::Int(1), DataType::from("Alice")]),
            None,
        );

        let stats = adapter.executor().stats();
        assert_eq!(stats.total_rows, 1);

        // DELETE
        adapter.handle_change(
            Action::SQLITE_DELETE,
            "users",
            None,
            Some(vec![DataType::Int(1), DataType::from("Alice")]),
        );

        let stats = adapter.executor().stats();
        assert_eq!(stats.total_rows, 0);
    }

    #[test]
    fn test_discover_and_load_table() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
            INSERT INTO users VALUES (1, 'Alice', 30);
            INSERT INTO users VALUES (2, 'Bob', 25);
        ",
        )
        .unwrap();

        let mut adapter = SqliteAdapter::new();
        adapter.discover_table(&conn, "users").unwrap();

        // Materialize
        {
            let base_node = *adapter.base_nodes.get("users").unwrap();
            adapter.executor_mut().materialize(base_node, vec![0]);
        }

        // Load existing data
        let count = adapter.load_table(&conn, "users").unwrap();
        assert_eq!(count, 2);

        let stats = adapter.executor().stats();
        assert_eq!(stats.total_rows, 2);
    }

    #[test]
    fn test_fetch_row_by_rowid() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO users VALUES (1, 'Alice');
        ",
        )
        .unwrap();

        let mut adapter = SqliteAdapter::new();
        adapter.discover_table(&conn, "users").unwrap();

        // The first insert has rowid = 1
        let row = adapter.fetch_row_by_rowid(&conn, "users", 1);
        assert!(row.is_some());
        let row = row.unwrap();
        assert_eq!(row[0], DataType::BigInt(1));
        assert_eq!(row[1], DataType::from("Alice"));
    }
}
