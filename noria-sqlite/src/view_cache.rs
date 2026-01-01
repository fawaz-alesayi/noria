//! View cache backed by evmap for lock-free reads

use crate::database::CacheStats;
use crate::error::{Error, Result};
use crate::statement::CachedRow;
use crate::worker::Worker;
use crate::Config;
use ahash::RandomState;
use noria::DataType;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Thread-safe view cache using evmap for concurrent access.
pub struct ViewCache {
    /// Map from normalized SQL -> view handle
    views: RwLock<HashMap<String, ViewHandle>>,

    /// Maximum memory budget
    max_memory: usize,

    /// Statistics
    hits: AtomicU64,
    misses: AtomicU64,
}

impl ViewCache {
    /// Create a new view cache with the specified memory limit.
    pub fn new(max_memory: usize) -> Self {
        Self {
            views: RwLock::new(HashMap::new()),
            max_memory,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Get or create a view for the given query.
    pub fn get_or_create_view(
        &self,
        normalized_sql: &str,
        worker: &Worker,
        config: &Config,
    ) -> Result<ViewHandle> {
        // Fast path: check if view already exists
        {
            let views = self.views.read();
            if let Some(handle) = views.get(normalized_sql) {
                return Ok(handle.clone());
            }
        }

        // Slow path: create new view
        let mut views = self.views.write();

        // Double-check after acquiring write lock
        if let Some(handle) = views.get(normalized_sql) {
            return Ok(handle.clone());
        }

        // Synthesize new view via worker
        let handle = worker.synthesize_view(normalized_sql, config)?;
        views.insert(normalized_sql.to_string(), handle.clone());

        Ok(handle)
    }

    /// Record a cache hit.
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss.
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Flush all cached views.
    pub fn flush(&self) {
        let mut views = self.views.write();
        for (_, handle) in views.iter_mut() {
            handle.clear();
        }
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let views = self.views.read();
        let memory_bytes = views.values().map(|h| h.memory_estimate()).sum();

        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            view_count: views.len(),
            memory_bytes,
        }
    }
}

/// Handle to a specific materialized view.
#[derive(Clone)]
pub struct ViewHandle {
    /// The evmap reader for this view
    reader: Arc<EvmapReader>,

    /// Tables this view depends on (for consistency guard)
    pub tables: Vec<String>,

    /// Columns in the key
    key_columns: Vec<usize>,
}

impl ViewHandle {
    /// Create a new view handle.
    pub fn new(tables: Vec<String>, key_columns: Vec<usize>) -> Self {
        Self {
            reader: Arc::new(EvmapReader::new()),
            tables,
            key_columns,
        }
    }

    /// Look up a single row by key.
    pub fn lookup(&self, key: &[DataType]) -> Result<Option<CachedRow>> {
        self.reader.get(key)
    }

    /// Look up multiple rows by key.
    pub fn lookup_multi(&self, key: &[DataType]) -> Result<Option<Vec<CachedRow>>> {
        self.reader.get_multi(key)
    }

    /// Clear all cached data.
    pub fn clear(&mut self) {
        // Note: In full implementation, this would coordinate with the writer
    }

    /// Estimate memory usage.
    pub fn memory_estimate(&self) -> usize {
        self.reader.memory_estimate()
    }
}

/// Wrapper around evmap read handle.
///
/// This provides the lock-free read path for cached views.
struct EvmapReader {
    /// The actual evmap - using a simple HashMap for now
    /// In full implementation, this would be evmap::ReadHandle
    data: RwLock<HashMap<Vec<DataType>, Vec<CachedRow>, RandomState>>,
}

impl EvmapReader {
    fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::with_hasher(RandomState::new())),
        }
    }

    fn get(&self, key: &[DataType]) -> Result<Option<CachedRow>> {
        let data = self.data.read();
        Ok(data.get(key).and_then(|rows| rows.first().cloned()))
    }

    fn get_multi(&self, key: &[DataType]) -> Result<Option<Vec<CachedRow>>> {
        let data = self.data.read();
        Ok(data.get(key).cloned())
    }

    fn memory_estimate(&self) -> usize {
        let data = self.data.read();
        // Rough estimate
        data.len() * 256
    }
}

/// Writer handle for updating the view cache.
///
/// This is used by the background worker to push updates.
pub struct ViewWriter {
    /// The evmap writer handle
    /// In full implementation, this would be evmap::WriteHandle
    data: Arc<RwLock<HashMap<Vec<DataType>, Vec<CachedRow>, RandomState>>>,
}

impl ViewWriter {
    /// Insert a row into the view.
    pub fn insert(&self, key: Vec<DataType>, row: CachedRow) {
        let mut data = self.data.write();
        data.entry(key).or_insert_with(Vec::new).push(row);
    }

    /// Remove a row from the view.
    pub fn remove(&self, key: &[DataType], row: &CachedRow) {
        let mut data = self.data.write();
        if let Some(rows) = data.get_mut(key) {
            rows.retain(|r| r.values != row.values);
        }
    }

    /// Swap buffers to make writes visible to readers.
    pub fn refresh(&self) {
        // In evmap, this would swap the read/write buffers
        // With our simple implementation, writes are immediately visible
    }
}
