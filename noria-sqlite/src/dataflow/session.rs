//! Session-based Change Data Capture for SQLite.
//!
//! This module uses SQLite's session extension to capture changes (INSERT, UPDATE, DELETE)
//! with full row data, including old values for DELETE and UPDATE operations.
//!
//! The session extension is superior to update_hook for CDC because:
//! - It provides old row values for DELETE (update_hook only gives rowid)
//! - It provides both old AND new values for UPDATE
//! - Changes are captured in a changeset that can be processed in batch

use fallible_streaming_iterator::FallibleStreamingIterator;
use noria::DataType;
use rusqlite::hooks::Action;
use rusqlite::session::{Changeset, Session};
use rusqlite::types::ValueRef;
use rusqlite::Connection;
use std::collections::HashSet;

use super::{Record, Records};

/// A CDC event captured from a session changeset.
#[derive(Debug, Clone)]
pub enum CdcEvent {
    /// A row was inserted.
    Insert {
        table: String,
        new_row: Vec<DataType>,
    },
    /// A row was updated.
    Update {
        table: String,
        old_row: Vec<DataType>,
        new_row: Vec<DataType>,
    },
    /// A row was deleted.
    Delete {
        table: String,
        old_row: Vec<DataType>,
    },
}

impl CdcEvent {
    /// Get the table name for this event.
    pub fn table(&self) -> &str {
        match self {
            CdcEvent::Insert { table, .. } => table,
            CdcEvent::Update { table, .. } => table,
            CdcEvent::Delete { table, .. } => table,
        }
    }

    /// Convert this event to dataflow Records.
    pub fn to_records(&self) -> Records {
        match self {
            CdcEvent::Insert { new_row, .. } => {
                Records::from(vec![Record::Positive(new_row.clone())])
            }
            CdcEvent::Update { old_row, new_row, .. } => Records::from(vec![
                Record::Negative(old_row.clone()),
                Record::Positive(new_row.clone()),
            ]),
            CdcEvent::Delete { old_row, .. } => {
                Records::from(vec![Record::Negative(old_row.clone())])
            }
        }
    }
}

/// Session-based CDC tracker.
///
/// Wraps SQLite's session extension to capture changes with full row data.
pub struct SessionTracker<'conn> {
    session: Session<'conn>,
    attached_tables: HashSet<String>,
}

impl<'conn> SessionTracker<'conn> {
    /// Create a new session tracker for the given connection.
    pub fn new(conn: &'conn Connection) -> rusqlite::Result<Self> {
        let session = Session::new(conn)?;
        Ok(Self {
            session,
            attached_tables: HashSet::new(),
        })
    }

    /// Attach a table to track changes.
    pub fn attach_table(&mut self, table: &str) -> rusqlite::Result<()> {
        self.session.attach(Some(table))?;
        self.attached_tables.insert(table.to_string());
        Ok(())
    }

    /// Attach all tables in the database.
    pub fn attach_all(&mut self) -> rusqlite::Result<()> {
        self.session.attach(None)?;
        Ok(())
    }

    /// Check if a table is being tracked.
    pub fn is_tracking(&self, table: &str) -> bool {
        self.attached_tables.contains(table)
    }

    /// Get the changeset of all changes since the session was created.
    ///
    /// Note: This consumes the session's recorded changes.
    pub fn changeset(&mut self) -> rusqlite::Result<Changeset> {
        self.session.changeset()
    }

    /// Extract CDC events from a changeset.
    pub fn extract_events(changeset: &Changeset) -> rusqlite::Result<Vec<CdcEvent>> {
        let mut events = Vec::new();
        let mut iter = changeset.iter()?;

        while let Some(item) = iter.next()? {
            let op = item.op()?;
            let table = op.table_name().to_string();
            let num_cols = op.number_of_columns() as usize;

            match op.code() {
                Action::SQLITE_INSERT => {
                    let mut new_row = Vec::with_capacity(num_cols);
                    for i in 0..num_cols {
                        let val = item.new_value(i)?;
                        new_row.push(valueref_to_datatype(val));
                    }
                    events.push(CdcEvent::Insert { table, new_row });
                }
                Action::SQLITE_DELETE => {
                    let mut old_row = Vec::with_capacity(num_cols);
                    for i in 0..num_cols {
                        let val = item.old_value(i)?;
                        old_row.push(valueref_to_datatype(val));
                    }
                    events.push(CdcEvent::Delete { table, old_row });
                }
                Action::SQLITE_UPDATE => {
                    let mut old_row = Vec::with_capacity(num_cols);
                    let mut new_row = Vec::with_capacity(num_cols);
                    for i in 0..num_cols {
                        // For UPDATE, only changed columns and PK columns have values
                        // Other columns return InvalidColumnIndex error
                        let old_val = match item.old_value(i) {
                            Ok(v) => valueref_to_datatype(v),
                            Err(_) => DataType::None, // Column not in changeset
                        };
                        let new_val = match item.new_value(i) {
                            Ok(v) => valueref_to_datatype(v),
                            Err(_) => DataType::None, // Column not in changeset
                        };
                        old_row.push(old_val);
                        new_row.push(new_val);
                    }
                    // For unchanged columns (None in new_row), copy from old_row
                    // This ensures we have complete row data for dataflow processing
                    for i in 0..num_cols {
                        if new_row[i] == DataType::None && old_row[i] != DataType::None {
                            new_row[i] = old_row[i].clone();
                        }
                    }
                    events.push(CdcEvent::Update {
                        table,
                        old_row,
                        new_row,
                    });
                }
                _ => {}
            }
        }

        Ok(events)
    }
}

/// Convert a rusqlite ValueRef to DataType.
fn valueref_to_datatype(val: ValueRef<'_>) -> DataType {
    match val {
        ValueRef::Null => DataType::None,
        ValueRef::Integer(i) => DataType::BigInt(i),
        ValueRef::Real(f) => {
            let int_part = f.trunc() as i64;
            let frac_part = ((f.fract().abs()) * 1_000_000_000.0) as i32;
            DataType::Real(int_part, frac_part)
        }
        ValueRef::Text(s) => DataType::from(std::str::from_utf8(s).unwrap_or("")),
        ValueRef::Blob(_) => DataType::None, // Skip blobs for now
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
            CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
            ",
        )
        .unwrap();
        conn
    }

    #[test]
    fn test_session_tracker_creation() {
        let conn = setup_test_db();
        let tracker = SessionTracker::new(&conn);
        assert!(tracker.is_ok());
    }

    #[test]
    fn test_session_attach_table() {
        let conn = setup_test_db();
        let mut tracker = SessionTracker::new(&conn).unwrap();

        tracker.attach_table("users").unwrap();
        assert!(tracker.is_tracking("users"));
        assert!(!tracker.is_tracking("posts"));
    }

    #[test]
    fn test_session_captures_insert() {
        let conn = setup_test_db();
        let mut tracker = SessionTracker::new(&conn).unwrap();
        tracker.attach_table("users").unwrap();

        // Perform insert
        conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)", [])
            .unwrap();

        // Get changeset and extract events
        let changeset = tracker.changeset().unwrap();
        let events = SessionTracker::extract_events(&changeset).unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            CdcEvent::Insert { table, new_row } => {
                assert_eq!(table, "users");
                assert_eq!(new_row[0], DataType::BigInt(1));
                assert_eq!(new_row[1], DataType::from("Alice"));
                assert_eq!(new_row[2], DataType::BigInt(30));
            }
            _ => panic!("Expected Insert event"),
        }
    }

    #[test]
    fn test_session_captures_delete() {
        let conn = setup_test_db();

        // Insert first (before tracking)
        conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)", [])
            .unwrap();

        // Now start tracking
        let mut tracker = SessionTracker::new(&conn).unwrap();
        tracker.attach_table("users").unwrap();

        // Delete the row
        conn.execute("DELETE FROM users WHERE id = 1", []).unwrap();

        // Get changeset and extract events
        let changeset = tracker.changeset().unwrap();
        let events = SessionTracker::extract_events(&changeset).unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            CdcEvent::Delete { table, old_row } => {
                assert_eq!(table, "users");
                // Session extension provides old values for DELETE!
                assert_eq!(old_row[0], DataType::BigInt(1));
                assert_eq!(old_row[1], DataType::from("Alice"));
                assert_eq!(old_row[2], DataType::BigInt(30));
            }
            _ => panic!("Expected Delete event"),
        }
    }

    #[test]
    fn test_session_captures_update() {
        let conn = setup_test_db();

        // Insert first
        conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)", [])
            .unwrap();

        // Now start tracking
        let mut tracker = SessionTracker::new(&conn).unwrap();
        tracker.attach_table("users").unwrap();

        // Update the row
        conn.execute(
            "UPDATE users SET name = 'Alicia', age = 31 WHERE id = 1",
            [],
        )
        .unwrap();

        // Get changeset and extract events
        let changeset = tracker.changeset().unwrap();
        let events = SessionTracker::extract_events(&changeset).unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            CdcEvent::Update {
                table,
                old_row,
                new_row,
            } => {
                assert_eq!(table, "users");
                // Note: Session extension only stores values for CHANGED columns
                // PK is only stored in old_value for identification, not in new_value
                // So we check the changed columns (name at index 1, age at index 2)
                assert_eq!(old_row[1], DataType::from("Alice"));
                assert_eq!(new_row[1], DataType::from("Alicia"));
                assert_eq!(old_row[2], DataType::BigInt(30));
                assert_eq!(new_row[2], DataType::BigInt(31));
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_session_multiple_operations() {
        let conn = setup_test_db();
        let mut tracker = SessionTracker::new(&conn).unwrap();
        tracker.attach_table("users").unwrap();

        // Note: Session extension COALESCES changes to the same row!
        // INSERT + UPDATE on same row = INSERT with final values
        // INSERT + DELETE on same row = no change at all

        // So let's test with operations on different rows
        conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)", [])
            .unwrap();
        conn.execute("INSERT INTO users VALUES (2, 'Bob', 25)", [])
            .unwrap();
        conn.execute("INSERT INTO users VALUES (3, 'Charlie', 35)", [])
            .unwrap();

        // Get changeset
        let changeset = tracker.changeset().unwrap();
        let events = SessionTracker::extract_events(&changeset).unwrap();

        // Should have 3 insert events (no coalescing since they're different rows)
        assert_eq!(events.len(), 3);

        let insert_count = events
            .iter()
            .filter(|e| matches!(e, CdcEvent::Insert { .. }))
            .count();

        assert_eq!(insert_count, 3);
    }

    #[test]
    fn test_session_coalesces_changes() {
        let conn = setup_test_db();
        let mut tracker = SessionTracker::new(&conn).unwrap();
        tracker.attach_table("users").unwrap();

        // INSERT then UPDATE same row - should coalesce to single INSERT
        conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)", [])
            .unwrap();
        conn.execute("UPDATE users SET age = 31 WHERE id = 1", [])
            .unwrap();

        let changeset = tracker.changeset().unwrap();
        let events = SessionTracker::extract_events(&changeset).unwrap();

        // Session coalesces INSERT + UPDATE into single INSERT with final values
        assert_eq!(events.len(), 1);
        match &events[0] {
            CdcEvent::Insert { new_row, .. } => {
                // Should have the FINAL values after update
                assert_eq!(new_row[0], DataType::BigInt(1));
                assert_eq!(new_row[1], DataType::from("Alice"));
                assert_eq!(new_row[2], DataType::BigInt(31)); // Updated age
            }
            _ => panic!("Expected coalesced Insert event"),
        }
    }

    #[test]
    fn test_session_insert_delete_cancels() {
        let conn = setup_test_db();
        let mut tracker = SessionTracker::new(&conn).unwrap();
        tracker.attach_table("users").unwrap();

        // INSERT then DELETE same row - should cancel out
        conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)", [])
            .unwrap();
        conn.execute("DELETE FROM users WHERE id = 1", [])
            .unwrap();

        let changeset = tracker.changeset().unwrap();
        let events = SessionTracker::extract_events(&changeset).unwrap();

        // Session coalesces INSERT + DELETE into nothing (net zero change)
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn test_cdc_event_to_records() {
        let insert_event = CdcEvent::Insert {
            table: "users".to_string(),
            new_row: vec![DataType::BigInt(1), DataType::from("Alice")],
        };

        let records = insert_event.to_records();
        assert_eq!(records.len(), 1);

        let update_event = CdcEvent::Update {
            table: "users".to_string(),
            old_row: vec![DataType::BigInt(1), DataType::from("Alice")],
            new_row: vec![DataType::BigInt(1), DataType::from("Alicia")],
        };

        let records = update_event.to_records();
        assert_eq!(records.len(), 2); // negative + positive

        let delete_event = CdcEvent::Delete {
            table: "users".to_string(),
            old_row: vec![DataType::BigInt(1), DataType::from("Alice")],
        };

        let records = delete_event.to_records();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn test_session_attach_all() {
        let conn = setup_test_db();
        let mut tracker = SessionTracker::new(&conn).unwrap();
        tracker.attach_all().unwrap();

        // Insert into both tables
        conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)", [])
            .unwrap();
        conn.execute("INSERT INTO posts VALUES (1, 1, 'Hello')", [])
            .unwrap();

        let changeset = tracker.changeset().unwrap();
        let events = SessionTracker::extract_events(&changeset).unwrap();

        assert_eq!(events.len(), 2);

        let tables: Vec<_> = events.iter().map(|e| e.table()).collect();
        assert!(tables.contains(&"users"));
        assert!(tables.contains(&"posts"));
    }

    #[test]
    fn test_session_with_transaction_rollback() {
        // This test verifies what the Session Extension does when a transaction is rolled back.
        // Key question: Does the session still record changes that were rolled back?
        let conn = setup_test_db();
        let mut tracker = SessionTracker::new(&conn).unwrap();
        tracker.attach_all().unwrap();

        // Start a transaction
        conn.execute("BEGIN", []).unwrap();

        // Insert a row
        conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)", [])
            .unwrap();

        // Rollback the transaction
        conn.execute("ROLLBACK", []).unwrap();

        // Get the changeset - does it contain the rolled-back INSERT?
        let changeset = tracker.changeset().unwrap();
        let events = SessionTracker::extract_events(&changeset).unwrap();

        // Print for debugging
        println!("Events after ROLLBACK: {:?}", events);
        println!("Event count: {}", events.len());

        // The critical question: is events.len() == 0 or == 1?
        // If 0: Session Extension handles rollback automatically (good!)
        // If 1: Session Extension does NOT handle rollback (we need to handle it)
    }

    #[test]
    fn test_session_with_transaction_commit() {
        // Verify session records changes when transaction commits
        let conn = setup_test_db();
        let mut tracker = SessionTracker::new(&conn).unwrap();
        tracker.attach_all().unwrap();

        // Start a transaction
        conn.execute("BEGIN", []).unwrap();

        // Insert a row
        conn.execute("INSERT INTO users VALUES (1, 'Alice', 30)", [])
            .unwrap();

        // Commit the transaction
        conn.execute("COMMIT", []).unwrap();

        // Get the changeset
        let changeset = tracker.changeset().unwrap();
        let events = SessionTracker::extract_events(&changeset).unwrap();

        println!("Events after COMMIT: {:?}", events);
        println!("Event count: {}", events.len());

        // Should have 1 event (the committed INSERT)
        assert_eq!(events.len(), 1);
    }
}
