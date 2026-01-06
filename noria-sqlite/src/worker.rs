//! Background worker for dataflow processing and CDC

use crate::error::Result;
use crate::view_cache::{ViewCache, ViewHandle};
use crate::Config;
use parking_lot::RwLock;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Thread-safe tracker for recently written tables.
///
/// This is separated from Worker to allow sharing with the update hook
/// without requiring the full Worker to be Send+Sync.
pub struct WriteTracker {
    /// Map from table name to last write time
    recent_writes: RwLock<HashMap<String, Instant>>,
}

impl WriteTracker {
    /// Create a new write tracker.
    pub fn new() -> Self {
        Self {
            recent_writes: RwLock::new(HashMap::new()),
        }
    }

    /// Record a write to a table.
    ///
    /// Called by the update hook when any INSERT/UPDATE/DELETE occurs.
    pub fn record_write(&self, table: &str) {
        let mut recent = self.recent_writes.write();
        recent.insert(table.to_string(), Instant::now());

        // Clean up old entries (older than 60 seconds)
        let cutoff = Instant::now() - std::time::Duration::from_secs(60);
        recent.retain(|_, &mut v| v > cutoff);
    }

    /// Check if any of the given tables were recently modified.
    pub fn is_recently_modified(&self, tables: &[String], window_ms: u64) -> bool {
        let recent = self.recent_writes.read();
        let cutoff = Instant::now() - std::time::Duration::from_millis(window_ms);

        for table in tables {
            if let Some(&write_time) = recent.get(table) {
                if write_time > cutoff {
                    return true;
                }
            }
        }

        false
    }
}

impl Default for WriteTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Background worker that manages:
/// - CDC observation via SQLite update hooks
/// - Dataflow propagation
/// - View synthesis
pub struct Worker {
    /// Reference to the SQLite connection
    #[allow(dead_code)]
    conn: Arc<RwLock<Connection>>,

    /// Reference to the view cache
    #[allow(dead_code)]
    view_cache: Arc<ViewCache>,

    /// Shared write tracker (also used by update hook)
    write_tracker: Arc<WriteTracker>,

    /// Synthesized views and their metadata
    views: RwLock<HashMap<String, ViewMetadata>>,
}

/// Metadata about a synthesized view
#[derive(Clone)]
struct ViewMetadata {
    /// Tables this view depends on
    tables: Vec<String>,

    /// Columns used as the lookup key
    key_columns: Vec<usize>,

    /// Column names in the result
    result_columns: Vec<String>,

    /// The SQL used to create this view
    #[allow(dead_code)]
    sql: String,
}

impl Worker {
    /// Create a new background worker.
    pub fn new(conn: Arc<RwLock<Connection>>, view_cache: Arc<ViewCache>) -> Result<Self> {
        Self::new_with_tracker(conn, view_cache, Arc::new(WriteTracker::new()))
    }

    /// Create a new background worker with an existing write tracker.
    ///
    /// This is used when the write tracker is shared with an update hook.
    pub fn new_with_tracker(
        conn: Arc<RwLock<Connection>>,
        view_cache: Arc<ViewCache>,
        write_tracker: Arc<WriteTracker>,
    ) -> Result<Self> {
        let worker = Self {
            conn,
            view_cache,
            write_tracker,
            views: RwLock::new(HashMap::new()),
        };

        Ok(worker)
    }

    /// Synthesize a new view for the given query.
    ///
    /// This parses the SQL, extracts table dependencies, and creates
    /// the necessary dataflow operators.
    pub fn synthesize_view(&self, normalized_sql: &str, _config: &Config) -> Result<ViewHandle> {
        // Parse the SQL to extract metadata
        let metadata = self.parse_query(normalized_sql)?;

        // Store metadata
        {
            let mut views = self.views.write();
            views.insert(normalized_sql.to_string(), metadata.clone());
        }

        // Create the view handle with evmap backing
        let handle = ViewHandle::new(
            metadata.tables,
            metadata.key_columns,
            metadata.result_columns,
        );

        // TODO: In full implementation:
        // 1. Convert SQL to Noria dataflow graph segment
        // 2. Register with the controller
        // 3. Wait for activation
        // 4. Return handle connected to the evmap

        Ok(handle)
    }

    /// Parse a query to extract metadata.
    fn parse_query(&self, sql: &str) -> Result<ViewMetadata> {
        // Simple parsing - in production this would use nom-sql
        let tables = self.extract_tables(sql)?;
        let key_columns = self.extract_key_columns(sql)?;
        let result_columns = self.extract_result_columns(sql)?;

        Ok(ViewMetadata {
            tables,
            key_columns,
            result_columns,
            sql: sql.to_string(),
        })
    }

    /// Extract table names from a query.
    fn extract_tables(&self, sql: &str) -> Result<Vec<String>> {
        // Simplified table extraction
        // In production, this would parse the SQL properly
        let mut tables = Vec::new();
        let upper = sql.to_uppercase();

        // Look for FROM clause
        if let Some(from_pos) = upper.find("FROM") {
            let after_from = &sql[from_pos + 4..];
            // Get the first word after FROM
            if let Some(table) = after_from.split_whitespace().next() {
                // Clean up backticks and other quotes
                let clean_table = table
                    .trim_matches('`')
                    .trim_matches('"')
                    .trim_matches(',')
                    .to_string();
                if !clean_table.is_empty() {
                    tables.push(clean_table);
                }
            }
        }

        // Look for JOIN clauses
        for keyword in ["JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN"] {
            let mut search_pos = 0;
            while let Some(pos) = upper[search_pos..].find(keyword) {
                let abs_pos = search_pos + pos + keyword.len();
                if abs_pos < sql.len() {
                    let after_join = &sql[abs_pos..];
                    if let Some(table) = after_join.split_whitespace().next() {
                        let clean_table = table
                            .trim_matches('`')
                            .trim_matches('"')
                            .to_string();
                        if !clean_table.is_empty() && !tables.contains(&clean_table) {
                            tables.push(clean_table);
                        }
                    }
                }
                search_pos = abs_pos;
            }
        }

        Ok(tables)
    }

    /// Extract key columns from WHERE clause parameters.
    fn extract_key_columns(&self, sql: &str) -> Result<Vec<usize>> {
        // Count the number of ? or $N parameters - these become the key columns
        let param_count = sql.matches('?').count()
            + (1..=10)
                .filter(|n| sql.contains(&format!("${}", n)))
                .count();

        // For now, assume key columns are 0..param_count
        Ok((0..param_count).collect())
    }

    /// Extract result column names from SELECT clause.
    fn extract_result_columns(&self, sql: &str) -> Result<Vec<String>> {
        let upper = sql.to_uppercase();

        // Find SELECT ... FROM portion
        let select_pos = upper.find("SELECT").unwrap_or(0) + 6;
        let from_pos = upper.find("FROM").unwrap_or(sql.len());

        if select_pos >= from_pos {
            return Ok(vec![]);
        }

        let select_clause = &sql[select_pos..from_pos];

        // Parse column names (simplified - doesn't handle all cases)
        let columns: Vec<String> = select_clause
            .split(',')
            .map(|col| {
                let col = col.trim();
                // Handle aliases (AS name)
                if let Some(as_pos) = col.to_uppercase().find(" AS ") {
                    col[as_pos + 4..].trim().to_string()
                } else {
                    // Get last part after any dots (table.column -> column)
                    col.split('.')
                        .last()
                        .unwrap_or(col)
                        .trim()
                        .trim_matches('`')
                        .trim_matches('"')
                        .to_string()
                }
            })
            .filter(|s| !s.is_empty() && s != "*")
            .collect();

        Ok(columns)
    }

    /// Check if any of the given tables were recently modified.
    ///
    /// Delegates to the shared WriteTracker.
    pub fn is_recently_modified(&self, tables: &[String], window_ms: u64) -> bool {
        self.write_tracker.is_recently_modified(tables, window_ms)
    }

    /// Record a write to a table.
    ///
    /// Delegates to the shared WriteTracker.
    pub fn record_write(&self, table: &str) {
        self.write_tracker.record_write(table);
    }

    /// Process a CDC changeset and update views.
    #[allow(dead_code)]
    pub fn process_changeset(&self, _changeset: &[u8]) -> Result<()> {
        // TODO: Implement changeset parsing and propagation
        // 1. Parse the binary changeset from sqlite3session
        // 2. Convert to Noria Modification packets
        // 3. Inject into the dataflow graph
        // 4. Let propagation update the evmap views
        Ok(())
    }
}
