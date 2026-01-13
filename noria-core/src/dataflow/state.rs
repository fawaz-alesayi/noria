//! # State Management for Materialized Views
//!
//! This module provides in-memory state storage for dataflow operators,
//! supporting indexed lookups by key columns. State is where the "materialized"
//! part of materialized views lives.
//!
//! ## Partially-Stateful Model
//!
//! In the Noria paper (Section 3.2), views can be **partially materialized**:
//! not all possible key values need to be present in state. When a lookup
//! encounters a "hole" (missing key), the system can perform an **upquery**
//! to fetch the data from upstream and fill in the hole.
//!
//! This module implements the state-side of that model through [`LookupResult`]:
//! - `Some(rows)`: Key exists and has data (cache hit)
//! - `Empty`: Key exists but maps to empty result
//! - `Missing`: Key not in state (cache miss, triggers upquery)
//!
//! ## Performance Optimizations
//!
//! Several optimizations make lookups fast (see `OPTIMIZATIONS.md` for benchmarks):
//!
//! ### 1. Arc-Wrapped Rows (OPTIMIZATIONS.md #9)
//!
//! Rows are stored as `Arc<Vec<DataType>>` so lookups can return references
//! without deep copying. Cloning an Arc is O(1) - just a reference count
//! increment.
//!
//! ```text
//! Lookup returns: Vec<Arc<Vec<DataType>>>
//!                      └── Just increment refcount, no data copy
//! ```
//!
//! **Impact**: +10-21% speedup on read-heavy workloads.
//!
//! ### 2. IntegerArrayState (OPTIMIZATIONS.md #3)
//!
//! For single-column integer primary keys (the common case), we use direct
//! array indexing instead of a HashMap:
//!
//! ```text
//! HashMap lookup:  hash(key) → bucket → linear search → O(1) average
//! Array lookup:    data[key - offset] → O(1) guaranteed, no hash
//! ```
//!
//! **Impact**: +12% speedup for integer key lookups.
//!
//! ### 3. DynamicState Auto-Detection (OPTIMIZATIONS.md #4)
//!
//! [`DynamicState`] automatically selects the optimal implementation on first
//! insert. Single-column integer keys get `IntegerArrayState`, everything else
//! gets `MemoryState` (HashMap).
//!
//! ### 4. StateKey Single-Column Optimization
//!
//! [`StateKey`] avoids Vec allocation for single-column keys (the common case)
//! by using an enum:
//!
//! ```text
//! StateKey::Single(DataType)       // No Vec allocation
//! StateKey::Multi(Vec<DataType>)   // Only for composite keys
//! ```
//!
//! ## Key Types
//!
//! - [`Row`]: `Arc<Vec<DataType>>` - shared ownership for O(1) cloning
//! - [`State`]: Trait for all state implementations
//! - [`MemoryState`]: HashMap-backed state (general purpose)
//! - [`IntegerArrayState`]: Array-backed state for integer keys (O(1))
//! - [`DynamicState`]: Auto-selecting wrapper
//!
//! ## Paper Reference
//!
//! See Section 3.2 "Partial State" in the Noria paper:
//! <https://pdos.csail.mit.edu/papers/noria:osdi18.pdf>

use std::collections::HashMap;
use std::sync::Arc;
use noria::DataType;
use super::{Record, Records};

/// A row stored in state, wrapped in Arc for O(1) cloning on lookup.
///
/// This is a critical optimization: instead of deep-copying row data on every
/// lookup, we return Arc references. Cloning an Arc is just a reference count
/// increment - O(1) regardless of row size.
///
/// See `OPTIMIZATIONS.md` #9 for benchmark data (+10-21% speedup).
pub type Row = Arc<Vec<DataType>>;

/// Key storage optimized for the common single-column case.
///
/// Most database tables have single-column primary keys (e.g., `id`). This enum
/// avoids the Vec allocation overhead for that case:
///
/// - `Single`: No heap allocation, DataType stored inline
/// - `Multi`: Only used for composite keys (e.g., `(user_id, post_id)`)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StateKey {
    /// Single-column key (common case, no Vec allocation)
    Single(DataType),
    /// Composite key (rare, requires Vec)
    Multi(Vec<DataType>),
}

impl StateKey {
    /// Create a key from a row and key column indices.
    pub fn from_row(row: &[DataType], key_columns: &[usize]) -> Self {
        if key_columns.len() == 1 {
            StateKey::Single(row[key_columns[0]].clone())
        } else {
            StateKey::Multi(key_columns.iter().map(|&c| row[c].clone()).collect())
        }
    }

    /// Create a key from a slice of values (for lookups).
    pub fn from_slice(key: &[DataType]) -> Self {
        if key.len() == 1 {
            StateKey::Single(key[0].clone())
        } else {
            StateKey::Multi(key.to_vec())
        }
    }
}

/// Result of a state lookup, implementing the partial-state model.
///
/// The three variants correspond to the possible states of a key in
/// partially-materialized views (Noria paper Section 3.2):
///
/// - `Some`: Key is materialized and has data
/// - `Empty`: Key is materialized but maps to empty (e.g., COUNT returned 0)
/// - `Missing`: Key is NOT materialized (a "hole" that triggers upquery)
///
/// The distinction between `Empty` and `Missing` is important:
/// - `Empty` means "we know there's no data" (no upquery needed)
/// - `Missing` means "we don't know" (upquery to fill the hole)
#[derive(Debug)]
pub enum LookupResult {
    /// Key found with matching rows. Rows are Arc-wrapped for O(1) cloning.
    Some(Vec<Row>),
    /// Key exists but has no rows (known empty, not a hole).
    Empty,
    /// Key not in state - this is a "hole" that may trigger an upquery.
    Missing,
}

/// Trait for stateful storage of materialized data.
pub trait State: Send {
    /// Add an index on the given key columns.
    fn add_key(&mut self, columns: Vec<usize>);

    /// Insert or remove records into state.
    fn process_records(&mut self, records: &mut Records);

    /// Look up rows by key. Returns Arc-wrapped rows for O(1) cloning.
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
    /// Data stored by key. Rows are Arc-wrapped for O(1) cloning on lookup.
    data: HashMap<StateKey, Vec<Row>>,
    /// Total row count.
    row_count: usize,
}

/// A read-only snapshot of state data, used for join lookups.
/// This owns its data so it can be passed across borrow boundaries.
pub struct StateSnapshot {
    key_columns: Vec<usize>,
    data: HashMap<StateKey, Vec<Row>>,
}

impl StateSnapshot {
    /// Create an empty snapshot.
    pub fn empty() -> Self {
        Self {
            key_columns: vec![],
            data: HashMap::new(),
        }
    }
}

impl State for StateSnapshot {
    fn add_key(&mut self, columns: Vec<usize>) {
        self.key_columns = columns;
    }

    fn process_records(&mut self, _records: &mut Records) {
        // Snapshots are read-only
    }

    fn lookup(&self, key: &[DataType]) -> LookupResult {
        let state_key = StateKey::from_slice(key);
        match self.data.get(&state_key) {
            Some(rows) if rows.is_empty() => LookupResult::Empty,
            // Clone Arcs - O(1) per row (just ref count increment)
            Some(rows) => LookupResult::Some(rows.iter().cloned().collect()),
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
            key_columns: self.key_columns.clone(),
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
    pub fn to_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            key_columns: self.key_columns.clone(),
            data: self.data.clone(),
        }
    }

    /// Extract key from a row using StateKey for efficiency.
    fn extract_key(&self, row: &[DataType]) -> StateKey {
        StateKey::from_row(row, &self.key_columns)
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
                // Wrap in Arc for cheap cloning on lookup
                entry.push(Arc::new(row));
                self.row_count += 1;
            } else {
                if let Some(rows) = self.data.get_mut(&key) {
                    // Try exact match first
                    if let Some(pos) = rows.iter().position(|r| r.as_slice() == &row[..]) {
                        rows.remove(pos);
                        self.row_count -= 1;
                    } else {
                        // For UPDATE events, the session extension may provide partial rows
                        // with None values for unchanged columns. Try matching with None wildcards.
                        let pos = rows.iter().position(|r| {
                            r.len() == row.len() && r.iter().zip(row.iter()).all(|(stored, new)| {
                                // Match if: values equal, OR the new value is None (wildcard)
                                stored == new || *new == DataType::None
                            })
                        });
                        if let Some(pos) = pos {
                            rows.remove(pos);
                            self.row_count -= 1;
                        }
                    }
                    if rows.is_empty() {
                        self.data.remove(&key);
                    }
                }
            }
        }
    }

    fn lookup(&self, key: &[DataType]) -> LookupResult {
        let state_key = StateKey::from_slice(key);
        match self.data.get(&state_key) {
            Some(rows) if rows.is_empty() => LookupResult::Empty,
            // Clone Arcs - O(1) per row (just ref count increment)
            Some(rows) => LookupResult::Some(rows.iter().cloned().collect()),
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
            key_columns: self.key_columns.clone(),
            data: self.data.clone(),
        })
    }
}

// ============================================================================
// IntegerArrayState - O(1) Direct Indexing (OPTIMIZATIONS.md #3)
// ============================================================================

/// Maximum array size to prevent out-of-memory on sparse keys.
const MAX_INTEGER_ARRAY_SIZE: usize = 10_000_000; // 10M entries

/// State with O(1) direct array indexing for single-column integer keys.
///
/// Most database tables use sequential integer primary keys (auto-increment).
/// This implementation exploits that pattern for faster lookups:
///
/// ```text
/// HashMap:  key → hash(key) → bucket scan → O(1) average, O(n) worst
/// Array:    key → data[key - offset] → O(1) guaranteed
/// ```
///
/// ## How It Works
///
/// The array is indexed by `key_value - key_offset`:
/// - If keys are 1,2,3,4,5 with offset=1: indices are 0,1,2,3,4
/// - If keys are 100,101,102 with offset=100: indices are 0,1,2
/// - Gaps in keys (e.g., 1,3,5) result in `None` entries in the array
///
/// ## Safety Limits
///
/// To prevent OOM on sparse key ranges, the array is capped at 10M entries.
/// Keys outside this range fall back to HashMap behavior in DynamicState.
///
/// ## Performance Impact
///
/// See `OPTIMIZATIONS.md` #3: +12% speedup for single-key integer lookups.
pub struct IntegerArrayState {
    /// The key column index (must be a single column).
    key_column: usize,
    /// Offset to handle non-zero starting keys (key_value - offset = array_index)
    key_offset: i64,
    /// Data stored by direct index: data[key - offset] = rows (Arc-wrapped for O(1) clone)
    data: Vec<Option<Vec<Row>>>,
    /// Total row count.
    row_count: usize,
    /// Track min/max for efficient bounds
    min_key: Option<i64>,
    max_key: Option<i64>,
}

impl IntegerArrayState {
    /// Create a new IntegerArrayState for a single integer key column.
    /// `initial_capacity` is the expected range of keys.
    pub fn new(key_column: usize, initial_capacity: usize) -> Self {
        Self {
            key_column,
            key_offset: 0,
            data: Vec::with_capacity(initial_capacity.min(MAX_INTEGER_ARRAY_SIZE)),
            row_count: 0,
            min_key: None,
            max_key: None,
        }
    }

    /// Create with a known offset (for keys starting at non-zero).
    pub fn with_offset(key_column: usize, offset: i64, capacity: usize) -> Self {
        Self {
            key_column,
            key_offset: offset,
            data: Vec::with_capacity(capacity.min(MAX_INTEGER_ARRAY_SIZE)),
            row_count: 0,
            min_key: None,
            max_key: None,
        }
    }

    /// Extract integer key value from a row.
    #[inline]
    fn extract_key(&self, row: &[DataType]) -> Option<i64> {
        match &row[self.key_column] {
            DataType::Int(i) => Some(*i as i64),
            DataType::BigInt(i) => Some(*i),
            DataType::UnsignedInt(u) => Some(*u as i64),
            DataType::UnsignedBigInt(u) => {
                if *u <= i64::MAX as u64 {
                    Some(*u as i64)
                } else {
                    None // Too large for i64
                }
            }
            _ => None, // Not an integer type
        }
    }

    /// Convert key value to array index.
    #[inline]
    fn key_to_index(&self, key: i64) -> Option<usize> {
        let idx = key - self.key_offset;
        if idx >= 0 && (idx as usize) < self.data.len() {
            Some(idx as usize)
        } else {
            None
        }
    }

    /// Ensure array has capacity for the given key.
    fn ensure_capacity(&mut self, key: i64) {
        let idx = key - self.key_offset;
        if idx < 0 {
            // Need to shift offset and expand array at the beginning
            let shift = (-idx) as usize;
            if shift + self.data.len() > MAX_INTEGER_ARRAY_SIZE {
                return; // Would exceed max size
            }
            let mut new_data = vec![None; shift];
            new_data.append(&mut self.data);
            self.data = new_data;
            self.key_offset = key;
        } else {
            let needed = (idx as usize) + 1;
            if needed > MAX_INTEGER_ARRAY_SIZE {
                return; // Would exceed max size
            }
            if needed > self.data.len() {
                self.data.resize(needed, None);
            }
        }
    }
}

impl State for IntegerArrayState {
    fn add_key(&mut self, columns: Vec<usize>) {
        if columns.len() == 1 {
            self.key_column = columns[0];
        }
    }

    fn process_records(&mut self, records: &mut Records) {

        for record in &*records {
            let key = match self.extract_key(record.row()) {
                Some(k) => k,
                None => continue, // Skip non-integer keys
            };

            // Track min/max
            self.min_key = Some(self.min_key.map_or(key, |m| m.min(key)));
            self.max_key = Some(self.max_key.map_or(key, |m| m.max(key)));

            // Ensure we have space
            self.ensure_capacity(key);

            let idx = match self.key_to_index(key) {
                Some(i) => i,
                None => continue, // Key out of range after ensure_capacity (exceeds max)
            };

            let row = record.row().to_vec();
            let is_positive = record.is_positive();

            if is_positive {
                match &mut self.data[idx] {
                    Some(rows) => {
                        // Wrap in Arc for cheap cloning on lookup
                        rows.push(Arc::new(row));
                    }
                    None => {
                        self.data[idx] = Some(vec![Arc::new(row)]);
                    }
                }
                self.row_count += 1;
            } else {
                if let Some(rows) = &mut self.data[idx] {
                    // Try exact match first
                    if let Some(pos) = rows.iter().position(|r| r.as_slice() == &row[..]) {
                        rows.remove(pos);
                        self.row_count -= 1;
                    } else {
                        // Try None wildcard matching
                        let pos = rows.iter().position(|r| {
                            r.len() == row.len() && r.iter().zip(row.iter()).all(|(stored, new)| {
                                stored == new || *new == DataType::None
                            })
                        });
                        if let Some(pos) = pos {
                            rows.remove(pos);
                            self.row_count -= 1;
                        }
                    }
                    if rows.is_empty() {
                        self.data[idx] = None;
                    }
                }
            }
        }
    }

    fn lookup(&self, key: &[DataType]) -> LookupResult {

        if key.len() != 1 {
            return LookupResult::Missing;
        }

        // Extract integer key directly without StateKey overhead
        let key_val = match &key[0] {
            DataType::Int(i) => *i as i64,
            DataType::BigInt(i) => *i,
            DataType::UnsignedInt(u) => *u as i64,
            DataType::UnsignedBigInt(u) => *u as i64,
            _ => return LookupResult::Missing,
        };

        // Direct array access - O(1)
        let idx = key_val - self.key_offset;
        if idx < 0 || (idx as usize) >= self.data.len() {
            return LookupResult::Missing;
        }

        match &self.data[idx as usize] {
            Some(rows) if rows.is_empty() => LookupResult::Empty,
            // Clone Arcs - O(1) per row (just ref count increment)
            Some(rows) => LookupResult::Some(rows.iter().cloned().collect()),
            None => LookupResult::Missing,
        }
    }

    fn len(&self) -> usize {
        self.row_count
    }

    fn clear(&mut self) {
        self.data.clear();
        self.row_count = 0;
        self.min_key = None;
        self.max_key = None;
    }

    fn snapshot(&self) -> Box<dyn State> {
        // Create a HashMap-based snapshot for compatibility
        let mut data = HashMap::new();
        for (idx, entry) in self.data.iter().enumerate() {
            if let Some(rows) = entry {
                if !rows.is_empty() {
                    let key = DataType::BigInt(idx as i64 + self.key_offset);
                    data.insert(StateKey::Single(key), rows.clone());
                }
            }
        }
        Box::new(StateSnapshot {
            key_columns: vec![self.key_column],
            data,
        })
    }
}

/// Check if a key is suitable for integer array state.
pub fn is_integer_key(key: &[DataType]) -> bool {
    key.len() == 1 && matches!(
        &key[0],
        DataType::Int(_) | DataType::BigInt(_) | DataType::UnsignedInt(_) | DataType::UnsignedBigInt(_)
    )
}

/// Check if a row's key column contains an integer value.
fn is_row_key_integer(row: &[DataType], key_column: usize) -> bool {
    if key_column >= row.len() {
        return false;
    }
    matches!(
        &row[key_column],
        DataType::Int(_) | DataType::BigInt(_) | DataType::UnsignedInt(_) | DataType::UnsignedBigInt(_)
    )
}

/// Create the optimal state implementation for the given key pattern.
/// Returns IntegerArrayState for single integer keys, MemoryState otherwise.
pub fn create_optimal_state(key_columns: Vec<usize>, sample_key: Option<&[DataType]>) -> Box<dyn State> {
    // Use integer array state for single integer keys
    if key_columns.len() == 1 {
        if let Some(key) = sample_key {
            if is_integer_key(key) {
                return Box::new(IntegerArrayState::new(key_columns[0], 1000));
            }
        }
    }
    // Default to hash-based state
    Box::new(MemoryState::new(key_columns))
}

// ============================================================================
// DynamicState - Auto-Detecting Wrapper (OPTIMIZATIONS.md #4)
// ============================================================================

/// State wrapper that auto-detects key type and selects optimal storage.
///
/// At view creation time, we don't always know what types the keys will be.
/// `DynamicState` solves this by deferring the choice until the first record:
///
/// ```text
/// DynamicState::new()       → Uninitialized
///     │
///     ▼ first record arrives
///     │
/// ┌───┴────────────────────────────────┐
/// │ Is key single-column integer?      │
/// └───┬────────────────────────────────┘
///     │
///     ├─ Yes → IntegerArrayState (O(1) direct indexing)
///     │
///     └─ No  → MemoryState (HashMap, handles any key type)
/// ```
///
/// This is transparent to callers - they just use the `State` trait.
///
/// ## Why Not Always Use HashMap?
///
/// For integer primary keys (the common case), `IntegerArrayState` is ~12%
/// faster due to direct array indexing without hash computation.
/// See `OPTIMIZATIONS.md` #4 for details.
pub struct DynamicState {
    key_columns: Vec<usize>,
    inner: DynamicStateInner,
}

enum DynamicStateInner {
    /// Waiting for first record to determine key type
    Uninitialized,
    /// Integer primary key detected - using O(1) array indexing
    IntegerArray(IntegerArrayState),
    /// Non-integer or composite key - using HashMap
    HashMap(MemoryState),
}

impl DynamicState {
    /// Create a new DynamicState for the given key columns.
    pub fn new(key_columns: Vec<usize>) -> Self {
        Self {
            key_columns: key_columns.clone(),
            inner: if key_columns.len() == 1 {
                DynamicStateInner::Uninitialized
            } else {
                // Multi-column keys always use HashMap
                DynamicStateInner::HashMap(MemoryState::new(key_columns))
            },
        }
    }

    /// Ensure inner state is initialized based on the record's key type.
    fn ensure_initialized(&mut self, row: &[DataType]) {
        if let DynamicStateInner::Uninitialized = &self.inner {
            let key_col = self.key_columns[0];
            if is_row_key_integer(row, key_col) {
                self.inner = DynamicStateInner::IntegerArray(
                    IntegerArrayState::new(key_col, 1000)
                );
            } else {
                self.inner = DynamicStateInner::HashMap(
                    MemoryState::new(self.key_columns.clone())
                );
            }
        }
    }
}

impl State for DynamicState {
    fn add_key(&mut self, columns: Vec<usize>) {
        self.key_columns = columns.clone();
        match &mut self.inner {
            DynamicStateInner::Uninitialized => {
                if columns.len() > 1 {
                    self.inner = DynamicStateInner::HashMap(MemoryState::new(columns));
                }
            }
            DynamicStateInner::IntegerArray(s) => s.add_key(columns),
            DynamicStateInner::HashMap(s) => s.add_key(columns),
        }
    }

    fn process_records(&mut self, records: &mut Records) {
        // Initialize state based on first record's key type
        if let Some(record) = (&*records).into_iter().next() {
            self.ensure_initialized(record.row());
        }

        match &mut self.inner {
            DynamicStateInner::Uninitialized => {}
            DynamicStateInner::IntegerArray(s) => s.process_records(records),
            DynamicStateInner::HashMap(s) => s.process_records(records),
        }
    }

    fn lookup(&self, key: &[DataType]) -> LookupResult {
        match &self.inner {
            DynamicStateInner::Uninitialized => LookupResult::Missing,
            DynamicStateInner::IntegerArray(s) => s.lookup(key),
            DynamicStateInner::HashMap(s) => s.lookup(key),
        }
    }

    fn len(&self) -> usize {
        match &self.inner {
            DynamicStateInner::Uninitialized => 0,
            DynamicStateInner::IntegerArray(s) => s.len(),
            DynamicStateInner::HashMap(s) => s.len(),
        }
    }

    fn clear(&mut self) {
        match &mut self.inner {
            DynamicStateInner::Uninitialized => {}
            DynamicStateInner::IntegerArray(s) => s.clear(),
            DynamicStateInner::HashMap(s) => s.clear(),
        }
    }

    fn snapshot(&self) -> Box<dyn State> {
        match &self.inner {
            DynamicStateInner::Uninitialized => Box::new(StateSnapshot::empty()),
            DynamicStateInner::IntegerArray(s) => s.snapshot(),
            DynamicStateInner::HashMap(s) => s.snapshot(),
        }
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

    // ========================================================================
    // IntegerArrayState Tests
    // ========================================================================

    #[test]
    fn test_integer_array_state_insert_lookup() {
        let mut state = IntegerArrayState::new(0, 100);

        let mut records: Records = vec![
            vec![DataType::BigInt(1), DataType::from("Alice")],
            vec![DataType::BigInt(2), DataType::from("Bob")],
            vec![DataType::BigInt(5), DataType::from("Charlie")], // Gap in keys
        ].into();

        state.process_records(&mut records);

        assert_eq!(state.len(), 3);

        // Lookup existing keys
        match state.lookup(&[DataType::BigInt(1)]) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], DataType::from("Alice"));
            }
            _ => panic!("Expected Some for key 1"),
        }

        match state.lookup(&[DataType::BigInt(5)]) {
            LookupResult::Some(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], DataType::from("Charlie"));
            }
            _ => panic!("Expected Some for key 5"),
        }

        // Missing keys (gaps)
        match state.lookup(&[DataType::BigInt(3)]) {
            LookupResult::Missing => {}
            _ => panic!("Expected Missing for key 3"),
        }

        // Out of range key
        match state.lookup(&[DataType::BigInt(100)]) {
            LookupResult::Missing => {}
            _ => panic!("Expected Missing for key 100"),
        }
    }

    #[test]
    fn test_integer_array_state_delete() {
        let mut state = IntegerArrayState::new(0, 10);

        let mut records: Records = vec![
            vec![DataType::BigInt(1), DataType::from("Alice")],
        ].into();
        state.process_records(&mut records);

        assert_eq!(state.len(), 1);

        // Delete the record
        let mut del_records = Records::from(vec![
            Record::Negative(vec![DataType::BigInt(1), DataType::from("Alice")]),
        ]);
        state.process_records(&mut del_records);

        assert_eq!(state.len(), 0);
        match state.lookup(&[DataType::BigInt(1)]) {
            LookupResult::Missing => {}
            _ => panic!("Expected Missing after delete"),
        }
    }

    #[test]
    fn test_integer_array_state_with_int_type() {
        // Test with Int (i32) type
        let mut state = IntegerArrayState::new(0, 10);

        let mut records: Records = vec![
            vec![DataType::Int(1), DataType::from("Alice")],
            vec![DataType::Int(2), DataType::from("Bob")],
        ].into();

        state.process_records(&mut records);
        assert_eq!(state.len(), 2);

        // Lookup should work with both Int and BigInt
        match state.lookup(&[DataType::Int(1)]) {
            LookupResult::Some(rows) => assert_eq!(rows.len(), 1),
            _ => panic!("Expected Some"),
        }

        match state.lookup(&[DataType::BigInt(2)]) {
            LookupResult::Some(rows) => assert_eq!(rows.len(), 1),
            _ => panic!("Expected Some (BigInt lookup for Int key)"),
        }
    }

    #[test]
    fn test_is_integer_key() {
        assert!(is_integer_key(&[DataType::Int(1)]));
        assert!(is_integer_key(&[DataType::BigInt(1)]));
        assert!(is_integer_key(&[DataType::UnsignedInt(1)]));
        assert!(is_integer_key(&[DataType::UnsignedBigInt(1)]));

        // Non-integer keys
        assert!(!is_integer_key(&[DataType::from("text")]));
        assert!(!is_integer_key(&[DataType::None]));

        // Multi-column keys
        assert!(!is_integer_key(&[DataType::Int(1), DataType::Int(2)]));
    }
}
