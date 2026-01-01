//! State management for materialized views.
//!
//! This module provides in-memory state storage for dataflow operators,
//! supporting indexed lookups by key columns.

use std::collections::HashMap;
use noria::DataType;
use super::{Record, Records};

/// Result of a state lookup.
#[derive(Debug)]
pub enum LookupResult<'a> {
    /// Found matching rows.
    Some(Vec<&'a [DataType]>),
    /// Key exists but has no rows (empty result).
    Empty,
    /// Key not found (hole in partial state).
    Missing,
}

/// Trait for stateful storage of materialized data.
pub trait State: Send {
    /// Add an index on the given key columns.
    fn add_key(&mut self, columns: Vec<usize>);

    /// Insert or remove records into state.
    fn process_records(&mut self, records: &mut Records);

    /// Look up rows by key.
    fn lookup(&self, key: &[DataType]) -> LookupResult;

    /// Get the number of rows.
    fn len(&self) -> usize;

    /// Check if empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all state.
    fn clear(&mut self);

    /// Create a read-only snapshot of this state.
    /// Used for join lookups where we need to avoid borrow conflicts.
    fn snapshot(&self) -> Box<dyn State>;
}

/// In-memory state with hash-based indexing.
pub struct MemoryState {
    /// The key columns for the primary index.
    key_columns: Vec<usize>,
    /// Data stored by key.
    data: HashMap<Vec<DataType>, Vec<Vec<DataType>>>,
    /// Total row count.
    row_count: usize,
}

/// A read-only snapshot of state data, used for join lookups.
/// This owns its data so it can be passed across borrow boundaries.
pub struct StateSnapshot {
    data: HashMap<Vec<DataType>, Vec<Vec<DataType>>>,
}

impl StateSnapshot {
    /// Create an empty snapshot.
    pub fn empty() -> Self {
        Self {
            data: HashMap::new(),
        }
    }
}

impl State for StateSnapshot {
    fn add_key(&mut self, _columns: Vec<usize>) {
        // No-op for snapshots
    }

    fn process_records(&mut self, _records: &mut Records) {
        // Snapshots are read-only
    }

    fn lookup(&self, key: &[DataType]) -> LookupResult {
        match self.data.get(key) {
            Some(rows) if rows.is_empty() => LookupResult::Empty,
            Some(rows) => LookupResult::Some(rows.iter().map(|r| r.as_slice()).collect()),
            None => LookupResult::Missing,
        }
    }

    fn len(&self) -> usize {
        self.data.values().map(|v| v.len()).sum()
    }

    fn clear(&mut self) {
        self.data.clear();
    }

    fn snapshot(&self) -> Box<dyn State> {
        Box::new(StateSnapshot {
            data: self.data.clone(),
        })
    }
}

impl MemoryState {
    /// Create a new MemoryState with the given key columns.
    pub fn new(key_columns: Vec<usize>) -> Self {
        Self {
            key_columns,
            data: HashMap::new(),
            row_count: 0,
        }
    }

    /// Create a read-only snapshot of this state.
    pub fn snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            data: self.data.clone(),
        }
    }

    /// Extract key from a row.
    fn extract_key(&self, row: &[DataType]) -> Vec<DataType> {
        self.key_columns
            .iter()
            .map(|&col| row.get(col).cloned().unwrap_or(DataType::None))
            .collect()
    }
}

impl State for MemoryState {
    fn add_key(&mut self, columns: Vec<usize>) {
        // For simplicity, we only support a single key index for now
        self.key_columns = columns;
    }

    fn process_records(&mut self, records: &mut Records) {
        for record in &*records {
            let key = self.extract_key(record.row());
            let row = record.row().to_vec();
            let is_positive = record.is_positive();

            if is_positive {
                let entry = self.data.entry(key).or_insert_with(Vec::new);
                entry.push(row);
                self.row_count += 1;
            } else {
                if let Some(rows) = self.data.get_mut(&key) {
                    if let Some(pos) = rows.iter().position(|r| r == &row) {
                        rows.remove(pos);
                        self.row_count -= 1;
                        if rows.is_empty() {
                            self.data.remove(&key);
                        }
                    }
                }
            }
        }
    }

    fn lookup(&self, key: &[DataType]) -> LookupResult {
        match self.data.get(key) {
            Some(rows) if rows.is_empty() => LookupResult::Empty,
            Some(rows) => LookupResult::Some(rows.iter().map(|r| r.as_slice()).collect()),
            None => LookupResult::Missing,
        }
    }

    fn len(&self) -> usize {
        self.row_count
    }

    fn clear(&mut self) {
        self.data.clear();
        self.row_count = 0;
    }

    fn snapshot(&self) -> Box<dyn State> {
        Box::new(StateSnapshot {
            data: self.data.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_state_insert_lookup() {
        let mut state = MemoryState::new(vec![0]); // Key on first column

        let mut records: Records = vec![
            vec![DataType::Int(1), DataType::from("Alice")],
            vec![DataType::Int(2), DataType::from("Bob")],
        ].into();

        state.process_records(&mut records);

        assert_eq!(state.len(), 2);

        // Lookup by key
        match state.lookup(&[DataType::Int(1)]) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], DataType::from("Alice"));
            }
            _ => panic!("Expected Some"),
        }

        // Missing key
        match state.lookup(&[DataType::Int(99)]) {
            LookupResult::Missing => {}
            _ => panic!("Expected Missing"),
        }
    }

    #[test]
    fn test_memory_state_delete() {
        let mut state = MemoryState::new(vec![0]);

        let mut records: Records = vec![
            vec![DataType::Int(1), DataType::from("Alice")],
        ].into();
        state.process_records(&mut records);

        assert_eq!(state.len(), 1);

        // Delete the record
        let mut del_records = Records::from(vec![
            Record::Negative(vec![DataType::Int(1), DataType::from("Alice")]),
        ]);
        state.process_records(&mut del_records);

        assert_eq!(state.len(), 0);
        match state.lookup(&[DataType::Int(1)]) {
            LookupResult::Missing => {}
            _ => panic!("Expected Missing after delete"),
        }
    }

    #[test]
    fn test_memory_state_multiple_rows_same_key() {
        let mut state = MemoryState::new(vec![0]); // Key on first column

        let mut records: Records = vec![
            vec![DataType::Int(1), DataType::from("Alice")],
            vec![DataType::Int(1), DataType::from("Alicia")], // Same key, different value
        ].into();

        state.process_records(&mut records);

        assert_eq!(state.len(), 2);

        match state.lookup(&[DataType::Int(1)]) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Some with 2 rows"),
        }
    }
}
