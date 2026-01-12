//! View cache backed by evmap for lock-free reads
//!
//! This module provides the core caching infrastructure using evmap,
//! a lock-free, eventually consistent concurrent map.

use noria::DataType;
use parking_lot::RwLock;
use std::collections::hash_map::RandomState;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Statistics about the evmap-based view cache.
#[derive(Debug, Clone, Default)]
pub struct ViewCacheStats {
    /// Number of cache hits
    pub hits: u64,
    /// Number of cache misses
    pub misses: u64,
    /// Number of views currently cached
    pub view_count: usize,
    /// Approximate memory usage in bytes
    pub memory_bytes: usize,
}

/// A cached row stored in evmap.
///
/// Each row is a vector of DataType values corresponding to the
/// columns selected in the query.
#[derive(Debug, Clone, PartialEq)]
pub struct CachedRow {
    /// Column values for this row
    pub values: Vec<DataType>,
}

impl CachedRow {
    /// Create a new cached row from column values.
    pub fn new(values: Vec<DataType>) -> Self {
        Self { values }
    }

    /// Get a value by column index.
    pub fn get(&self, index: usize) -> Option<&DataType> {
        self.values.get(index)
    }

    /// Number of columns in this row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the row is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Serialized row data for storage in evmap.
/// We use Vec<u8> because evmap 11.0 requires StableHashEq which is sealed.
type SerializedRow = Box<[u8]>;

/// Inner state of a view, shared across clones.
struct ViewInner {
    /// evmap read handle (String keys, serialized row values)
    reader: evmap::handles::ReadHandle<String, SerializedRow, (), RandomState>,

    /// evmap write handle (wrapped for thread safety)
    writer: RwLock<evmap::handles::WriteHandle<String, SerializedRow, (), RandomState>>,

    /// Track approximate memory usage
    memory_bytes: AtomicU64,
}

/// Handle to a specific materialized view.
///
/// This wraps an evmap reader and provides lookup functionality.
/// The handle can be cheaply cloned and shared across threads.
#[derive(Clone)]
pub struct ViewHandle {
    /// The evmap reader for this view
    inner: Arc<ViewInner>,

    /// Tables this view depends on (for consistency guard)
    pub tables: Vec<String>,

    /// Column indices that form the lookup key
    pub key_columns: Vec<usize>,

    /// Column names in the result (for debugging)
    pub result_columns: Vec<String>,
}

impl ViewHandle {
    /// Create a new view handle with evmap backing.
    pub fn new(tables: Vec<String>, key_columns: Vec<usize>, result_columns: Vec<String>) -> Self {
        // evmap 11.0: construct() returns (WriteHandle, ReadHandle)
        // SAFETY: with_hasher is unsafe because the hasher must be deterministic.
        // std::collections::hash_map::RandomState produces deterministic hashes per instance.
        let (writer, reader) = unsafe {
            evmap::Options::default()
                .with_hasher(RandomState::new())
        }
        .construct();

        Self {
            inner: Arc::new(ViewInner {
                reader,
                writer: RwLock::new(writer),
                memory_bytes: AtomicU64::new(0),
            }),
            tables,
            key_columns,
            result_columns,
        }
    }

    /// Serialize a key (Vec<DataType>) to a deterministic String.
    fn serialize_key(key: &[DataType]) -> String {
        let mut parts = Vec::with_capacity(key.len());
        for dt in key {
            parts.push(serialize_datatype(dt));
        }
        parts.join("\x00")
    }

    /// Serialize a row (Vec<DataType>) to bytes.
    fn serialize_row(row: &[DataType]) -> SerializedRow {
        // Simple format: count (u32) + each value serialized
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(row.len() as u32).to_le_bytes());
        for dt in row {
            let s = serialize_datatype(dt);
            bytes.extend_from_slice(&(s.len() as u32).to_le_bytes());
            bytes.extend_from_slice(s.as_bytes());
        }
        bytes.into_boxed_slice()
    }

    /// Deserialize a row from bytes.
    fn deserialize_row(bytes: &[u8]) -> Vec<DataType> {
        if bytes.len() < 4 {
            return Vec::new();
        }

        let count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let mut result = Vec::with_capacity(count);
        let mut pos = 4;

        for _ in 0..count {
            if pos + 4 > bytes.len() {
                break;
            }
            let len = u32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]) as usize;
            pos += 4;

            if pos + len > bytes.len() {
                break;
            }
            let s = std::str::from_utf8(&bytes[pos..pos + len]).unwrap_or("");
            result.push(deserialize_datatype(s));
            pos += len;
        }

        result
    }

    /// Look up rows by key.
    ///
    /// Returns `None` if the key is not in the cache (cache miss).
    /// Returns `Some(vec)` with matching rows if found (cache hit).
    /// An empty vector means the key exists but has no matching rows.
    pub fn lookup(&self, key: &[DataType]) -> Option<Vec<CachedRow>> {
        let key_str = Self::serialize_key(key);

        // evmap 11.0: use enter() instead of read()
        self.inner.reader.enter().and_then(|map| {
            map.get(&key_str).map(|values| {
                values
                    .iter()
                    .map(|bytes| CachedRow::new(Self::deserialize_row(bytes)))
                    .collect()
            })
        })
    }

    /// Look up a single row by key.
    ///
    /// Returns the first matching row, or `None` if not found.
    pub fn lookup_one(&self, key: &[DataType]) -> Option<CachedRow> {
        self.lookup(key).and_then(|rows| rows.into_iter().next())
    }

    /// Check if a key exists in the cache.
    pub fn contains_key(&self, key: &[DataType]) -> bool {
        let key_str = Self::serialize_key(key);
        self.inner
            .reader
            .enter()
            .map(|map| map.contains_key(&key_str))
            .unwrap_or(false)
    }

    /// Insert a row into the view.
    ///
    /// The key is extracted from the row based on key_columns.
    pub fn insert(&self, key: Vec<DataType>, row: Vec<DataType>) {
        let row_size = estimate_row_size(&row);
        let key_str = Self::serialize_key(&key);
        let row_bytes = Self::serialize_row(&row);

        let mut writer = self.inner.writer.write();
        writer.insert(key_str, row_bytes);
        // evmap 11.0: use publish() instead of refresh()
        writer.publish();
        self.inner
            .memory_bytes
            .fetch_add(row_size as u64, Ordering::Relaxed);
    }

    /// Insert a row, extracting the key from the row itself.
    pub fn insert_row(&self, row: Vec<DataType>) {
        let key: Vec<DataType> = self
            .key_columns
            .iter()
            .filter_map(|&i| row.get(i).cloned())
            .collect();
        self.insert(key, row);
    }

    /// Remove all rows matching a key.
    pub fn remove(&self, key: &[DataType]) {
        let key_str = Self::serialize_key(key);
        let mut writer = self.inner.writer.write();
        // evmap 11.0: use remove_entry() instead of empty()
        writer.remove_entry(key_str);
        writer.publish();
    }

    /// Clear all cached data.
    pub fn clear(&self) {
        let mut writer = self.inner.writer.write();
        writer.purge();
        writer.publish();
        self.inner.memory_bytes.store(0, Ordering::Relaxed);
    }

    /// Estimate memory usage.
    pub fn memory_estimate(&self) -> usize {
        self.inner.memory_bytes.load(Ordering::Relaxed) as usize
    }

    /// Get the number of cached keys.
    pub fn len(&self) -> usize {
        self.inner.reader.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Mark a key as "filled" (exists but empty).
    ///
    /// This is used for partial materialization to distinguish between
    /// "key not queried yet" and "key queried but has no results".
    pub fn mark_filled(&self, key: Vec<DataType>) {
        let key_str = Self::serialize_key(&key);
        let mut writer = self.inner.writer.write();
        // Insert an empty marker - the key exists but has no values
        // We use clear() to ensure the key exists with empty values
        writer.clear(key_str);
        writer.publish();
    }

    /// Check if a key is marked as a "hole" (not yet materialized).
    ///
    /// Returns true if the key has never been queried/filled.
    pub fn is_hole(&self, key: &[DataType]) -> bool {
        let key_str = Self::serialize_key(key);
        self.inner
            .reader
            .enter()
            .map(|map| !map.contains_key(&key_str))
            .unwrap_or(true)
    }
}

/// Serialize a DataType to a string for use in keys.
fn serialize_datatype(dt: &DataType) -> String {
    match dt {
        DataType::None => "N".to_string(),
        DataType::Int(i) => format!("I{}", i),
        DataType::UnsignedInt(u) => format!("U{}", u),
        DataType::BigInt(i) => format!("B{}", i),
        DataType::UnsignedBigInt(u) => format!("G{}", u),
        DataType::Real(int, frac) => format!("R{}_{}", int, frac),
        DataType::Text(s) => format!("T{}", s.to_str().unwrap_or("")),
        DataType::TinyText(arr) => {
            let s = std::str::from_utf8(arr).unwrap_or("").trim_end_matches('\0');
            format!("t{}", s)
        }
        DataType::Timestamp(ts) => format!("S{}", ts.and_utc().timestamp_nanos_opt().unwrap_or(0)),
    }
}

/// Deserialize a DataType from a string.
fn deserialize_datatype(s: &str) -> DataType {
    if s.is_empty() {
        return DataType::None;
    }
    let (tag, rest) = s.split_at(1);
    match tag {
        "N" => DataType::None,
        "I" => rest.parse::<i32>().map(DataType::Int).unwrap_or(DataType::None),
        "U" => rest.parse::<u32>().map(DataType::UnsignedInt).unwrap_or(DataType::None),
        "B" => rest.parse::<i64>().map(DataType::BigInt).unwrap_or(DataType::None),
        "G" => rest.parse::<u64>().map(DataType::UnsignedBigInt).unwrap_or(DataType::None),
        "R" => {
            let parts: Vec<&str> = rest.split('_').collect();
            if parts.len() == 2 {
                if let (Ok(int), Ok(frac)) = (parts[0].parse::<i64>(), parts[1].parse::<i32>()) {
                    return DataType::Real(int, frac);
                }
            }
            DataType::None
        }
        "T" | "t" => DataType::from(rest),
        "S" => {
            if let Ok(nanos) = rest.parse::<i64>() {
                use chrono::{DateTime, Utc};
                if let Some(dt) = DateTime::<Utc>::from_timestamp(nanos / 1_000_000_000, (nanos % 1_000_000_000) as u32) {
                    return DataType::Timestamp(dt.naive_utc());
                }
            }
            DataType::None
        }
        _ => DataType::None,
    }
}

/// Estimate the memory size of a row.
fn estimate_row_size(row: &[DataType]) -> usize {
    use std::mem::size_of;

    let base = size_of::<Vec<DataType>>();
    let values: usize = row
        .iter()
        .map(|dt| match dt {
            DataType::None => size_of::<DataType>(),
            DataType::Int(_) => size_of::<DataType>(),
            DataType::UnsignedInt(_) => size_of::<DataType>(),
            DataType::BigInt(_) => size_of::<DataType>(),
            DataType::UnsignedBigInt(_) => size_of::<DataType>(),
            DataType::Real(_, _) => size_of::<DataType>(),
            DataType::Text(ref s) => {
                // ArcCStr: get length via to_str()
                let str_len = s.to_str().map(|s| s.len()).unwrap_or(0);
                size_of::<DataType>() + str_len
            }
            DataType::TinyText(ref arr) => size_of::<DataType>() + arr.len(),
            DataType::Timestamp(_) => size_of::<DataType>(),
        })
        .sum();

    base + values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_view_handle_insert_and_lookup() {
        let handle = ViewHandle::new(
            vec!["users".to_string()],
            vec![0],
            vec!["id".to_string(), "name".to_string()],
        );

        // Insert a row
        let key = vec![DataType::from(1i64)];
        let row = vec![DataType::from(1i64), DataType::from("Alice")];
        handle.insert(key.clone(), row.clone());

        // Lookup should find it
        let result = handle.lookup(&key);
        assert!(result.is_some());
        let rows = result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, row);
    }

    #[test]
    fn test_view_handle_multiple_rows_per_key() {
        let handle = ViewHandle::new(
            vec!["posts".to_string()],
            vec![0],
            vec!["user_id".to_string(), "title".to_string()],
        );

        let key = vec![DataType::from(1i64)];

        // Insert multiple rows with same key
        handle.insert(key.clone(), vec![DataType::from(1i64), DataType::from("Post 1")]);
        handle.insert(key.clone(), vec![DataType::from(1i64), DataType::from("Post 2")]);

        let result = handle.lookup(&key);
        assert!(result.is_some());
        let rows = result.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_view_handle_cache_miss() {
        let handle = ViewHandle::new(vec!["users".to_string()], vec![0], vec!["id".to_string()]);

        let key = vec![DataType::from(999i64)];
        let result = handle.lookup(&key);
        assert!(result.is_none()); // Cache miss
    }

    #[test]
    fn test_view_handle_clear() {
        let handle = ViewHandle::new(vec!["users".to_string()], vec![0], vec!["id".to_string()]);

        let key = vec![DataType::from(1i64)];
        handle.insert(key.clone(), vec![DataType::from(1i64)]);
        assert!(!handle.is_empty());

        handle.clear();
        assert!(handle.is_empty());
        assert!(handle.lookup(&key).is_none());
    }

    #[test]
    fn test_cached_row_access() {
        let row = CachedRow::new(vec![
            DataType::from(42i64),
            DataType::from("test"),
            DataType::from(3.14f64),
        ]);

        assert_eq!(row.len(), 3);
        assert!(!row.is_empty());
        assert_eq!(row.get(0), Some(&DataType::from(42i64)));
        assert_eq!(row.get(1), Some(&DataType::from("test")));
        assert_eq!(row.get(3), None);
    }

    #[test]
    fn test_serialization_roundtrip() {
        // Test various data types
        let values = vec![
            DataType::None,
            DataType::Int(42),
            DataType::UnsignedInt(100),
            DataType::BigInt(-999),
            DataType::UnsignedBigInt(12345),
            DataType::from("hello world"),
        ];

        for original in values {
            let serialized = serialize_datatype(&original);
            let deserialized = deserialize_datatype(&serialized);
            assert_eq!(original, deserialized, "Failed for {:?}", original);
        }
    }
}
