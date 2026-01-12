//! Implementations of noria-core adapter traits for SQLite.
//!
//! This module provides SQLite-specific implementations of the database-agnostic
//! traits defined in noria-core, enabling noria-sqlite to work with the shared
//! dataflow engine.

use noria::DataType;
use noria_core::adapter::{CdcEvent as CoreCdcEvent, CdcSource, DatabaseAdapter, TableSchema as CoreTableSchema};
use rusqlite::types::ValueRef;
use rusqlite::Connection;
use std::collections::HashSet;
use std::sync::Arc;
use parking_lot::RwLock;

use super::session::{CdcEvent, SessionTracker};

/// SQLite implementation of the DatabaseAdapter trait.
///
/// Provides schema discovery and upquery functionality for SQLite databases.
pub struct SqliteDatabaseAdapter {
    /// Database connection (shared).
    conn: Arc<RwLock<Connection>>,
}

impl SqliteDatabaseAdapter {
    /// Create a new SQLite database adapter.
    pub fn new(conn: Arc<RwLock<Connection>>) -> Self {
        Self { conn }
    }
}

impl DatabaseAdapter for SqliteDatabaseAdapter {
    type Error = rusqlite::Error;

    fn table_schema(&self, table: &str) -> Result<Option<CoreTableSchema>, Self::Error> {
        let conn = self.conn.read();
        let mut columns = Vec::new();
        let mut primary_key = Vec::new();

        // Use PRAGMA table_info to get column information
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table))?;
        let column_iter = stmt.query_map([], |row| {
            let cid: i64 = row.get(0)?;
            let name: String = row.get(1)?;
            let pk: bool = row.get(5)?;
            Ok((cid as usize, name, pk))
        })?;

        for col_result in column_iter {
            let (cid, name, pk) = col_result?;
            if pk {
                primary_key.push(cid);
            }
            columns.push(name);
        }

        if columns.is_empty() {
            return Ok(None);
        }

        Ok(Some(
            CoreTableSchema::new(table, columns).with_primary_key(primary_key),
        ))
    }

    fn upquery(&self, sql: &str, params: &[DataType]) -> Result<Vec<Vec<DataType>>, Self::Error> {
        let conn = self.conn.read();
        let mut stmt = conn.prepare(sql)?;

        // Convert DataType params to rusqlite values
        let rusqlite_params: Vec<Box<dyn rusqlite::ToSql>> = params
            .iter()
            .map(|dt| datatype_to_tosql(dt))
            .collect();

        let param_refs: Vec<&dyn rusqlite::ToSql> = rusqlite_params.iter().map(|b| b.as_ref()).collect();

        let num_cols = stmt.column_count();
        let mut rows = Vec::new();

        let mut query_rows = stmt.query(param_refs.as_slice())?;
        while let Some(row) = query_rows.next()? {
            let mut row_data = Vec::with_capacity(num_cols);
            for i in 0..num_cols {
                let value = match row.get_ref(i)? {
                    ValueRef::Null => DataType::None,
                    ValueRef::Integer(i) => DataType::BigInt(i),
                    ValueRef::Real(f) => {
                        let int_part = f.trunc() as i64;
                        let frac_part = ((f.fract().abs()) * 1_000_000_000.0) as i32;
                        DataType::Real(int_part, frac_part)
                    }
                    ValueRef::Text(s) => {
                        DataType::from(std::str::from_utf8(s).unwrap_or(""))
                    }
                    ValueRef::Blob(b) => {
                        DataType::from(format!("BLOB:{}", b.len()).as_str())
                    }
                };
                row_data.push(value);
            }
            rows.push(row_data);
        }

        Ok(rows)
    }

    fn list_tables(&self) -> Result<Vec<String>, Self::Error> {
        let conn = self.conn.read();
        let mut stmt = conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        )?;

        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(tables)
    }
}

/// SQLite CDC source using the session extension.
///
/// Implements the CdcSource trait from noria-core, providing synchronous
/// change data capture through SQLite's session extension.
pub struct SqliteCdcSource<'conn> {
    tracker: SessionTracker<'conn>,
    tracked_tables: HashSet<String>,
    pending_events: Vec<CoreCdcEvent>,
}

impl<'conn> SqliteCdcSource<'conn> {
    /// Create a new SQLite CDC source for the given connection.
    pub fn new(conn: &'conn Connection) -> rusqlite::Result<Self> {
        let tracker = SessionTracker::new(conn)?;
        Ok(Self {
            tracker,
            tracked_tables: HashSet::new(),
            pending_events: Vec::new(),
        })
    }

    /// Extract events from the session and convert to noria-core format.
    pub fn extract_and_convert(&mut self) -> rusqlite::Result<Vec<CoreCdcEvent>> {
        let changeset = self.tracker.changeset()?;
        let events = SessionTracker::extract_events(&changeset)?;

        Ok(events.into_iter().map(sqlite_cdc_to_core).collect())
    }
}

impl<'conn> CdcSource for SqliteCdcSource<'conn> {
    fn poll(&mut self) -> Vec<CoreCdcEvent> {
        // Return any pending events first
        if !self.pending_events.is_empty() {
            return std::mem::take(&mut self.pending_events);
        }

        // Try to extract new events from the session
        match self.extract_and_convert() {
            Ok(events) => events,
            Err(_) => Vec::new(),
        }
    }

    fn track_table(&mut self, table: &str) {
        if !self.tracked_tables.contains(table) {
            if self.tracker.attach_table(table).is_ok() {
                self.tracked_tables.insert(table.to_string());
            }
        }
    }

    fn is_active(&self) -> bool {
        !self.tracked_tables.is_empty()
    }

    fn reset(&mut self) {
        self.pending_events.clear();
        // Note: Can't easily reset the session without recreating it
    }
}

/// Convert a SQLite CDC event to a noria-core CDC event.
fn sqlite_cdc_to_core(event: CdcEvent) -> CoreCdcEvent {
    match event {
        CdcEvent::Insert { table, new_row } => CoreCdcEvent::Insert { table, row: new_row },
        CdcEvent::Delete { table, old_row } => CoreCdcEvent::Delete { table, row: old_row },
        CdcEvent::Update {
            table,
            old_row,
            new_row,
        } => CoreCdcEvent::Update {
            table,
            old: old_row,
            new: new_row,
        },
    }
}

/// Convert a DataType to a boxed ToSql value for rusqlite.
fn datatype_to_tosql(dt: &DataType) -> Box<dyn rusqlite::ToSql> {
    match dt {
        DataType::None => Box::new(rusqlite::types::Null),
        DataType::Int(i) => Box::new(*i),
        DataType::UnsignedInt(u) => Box::new(*u as i64),
        DataType::BigInt(i) => Box::new(*i),
        DataType::UnsignedBigInt(u) => Box::new(*u as i64),
        DataType::Real(int, frac) => {
            let f = *int as f64 + (*frac as f64 / 1_000_000_000.0);
            Box::new(f)
        }
        DataType::Text(s) => Box::new(s.to_str().unwrap_or("").to_string()),
        DataType::TinyText(arr) => {
            let s = std::str::from_utf8(arr).unwrap_or("").trim_end_matches('\0');
            Box::new(s.to_string())
        }
        DataType::Timestamp(ts) => Box::new(ts.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_db() -> Arc<RwLock<Connection>> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
            INSERT INTO users VALUES (1, 'Alice', 30);
            INSERT INTO users VALUES (2, 'Bob', 25);
            ",
        )
        .unwrap();
        Arc::new(RwLock::new(conn))
    }

    #[test]
    fn test_database_adapter_table_schema() {
        let conn = setup_test_db();
        let adapter = SqliteDatabaseAdapter::new(conn);

        let schema = adapter.table_schema("users").unwrap();
        assert!(schema.is_some());

        let schema = schema.unwrap();
        assert_eq!(schema.name, "users");
        assert_eq!(schema.columns, vec!["id", "name", "age"]);
        assert_eq!(schema.primary_key, vec![0]);
    }

    #[test]
    fn test_database_adapter_table_not_found() {
        let conn = setup_test_db();
        let adapter = SqliteDatabaseAdapter::new(conn);

        let schema = adapter.table_schema("nonexistent").unwrap();
        assert!(schema.is_none());
    }

    #[test]
    fn test_database_adapter_upquery() {
        let conn = setup_test_db();
        let adapter = SqliteDatabaseAdapter::new(conn);

        let rows = adapter
            .upquery("SELECT * FROM users WHERE id = ?", &[DataType::BigInt(1)])
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], DataType::BigInt(1));
        assert_eq!(rows[0][1], DataType::from("Alice"));
        assert_eq!(rows[0][2], DataType::BigInt(30));
    }

    #[test]
    fn test_database_adapter_upquery_multiple_rows() {
        let conn = setup_test_db();
        let adapter = SqliteDatabaseAdapter::new(conn);

        let rows = adapter.upquery("SELECT * FROM users", &[]).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_database_adapter_list_tables() {
        let conn = setup_test_db();
        let adapter = SqliteDatabaseAdapter::new(conn);

        let tables = adapter.list_tables().unwrap();
        assert!(tables.contains(&"users".to_string()));
    }

    #[test]
    fn test_cdc_event_conversion() {
        let insert = CdcEvent::Insert {
            table: "users".to_string(),
            new_row: vec![DataType::BigInt(1), DataType::from("Alice")],
        };

        let core_event = sqlite_cdc_to_core(insert);
        match core_event {
            CoreCdcEvent::Insert { table, row } => {
                assert_eq!(table, "users");
                assert_eq!(row.len(), 2);
            }
            _ => panic!("Expected Insert event"),
        }

        let delete = CdcEvent::Delete {
            table: "posts".to_string(),
            old_row: vec![DataType::BigInt(42)],
        };

        let core_event = sqlite_cdc_to_core(delete);
        match core_event {
            CoreCdcEvent::Delete { table, row } => {
                assert_eq!(table, "posts");
                assert_eq!(row.len(), 1);
            }
            _ => panic!("Expected Delete event"),
        }

        let update = CdcEvent::Update {
            table: "users".to_string(),
            old_row: vec![DataType::BigInt(1), DataType::from("Alice")],
            new_row: vec![DataType::BigInt(1), DataType::from("Alicia")],
        };

        let core_event = sqlite_cdc_to_core(update);
        match core_event {
            CoreCdcEvent::Update { table, old, new } => {
                assert_eq!(table, "users");
                assert_eq!(old[1], DataType::from("Alice"));
                assert_eq!(new[1], DataType::from("Alicia"));
            }
            _ => panic!("Expected Update event"),
        }
    }
}
