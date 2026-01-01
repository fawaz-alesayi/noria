//! Background worker for dataflow processing and CDC

use crate::error::{Error, Result};
use crate::view_cache::{ViewCache, ViewHandle};
use crate::Config;
use parking_lot::RwLock;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

/// Background worker that manages:
/// - CDC observation via SQLite Session Extension
/// - Dataflow propagation
/// - View synthesis
pub struct Worker {
    /// Reference to the SQLite connection
    conn: Arc<RwLock<Connection>>,

    /// Reference to the view cache
    view_cache: Arc<ViewCache>,

    /// Track recently modified tables for consistency guard
    recent_writes: RwLock<HashMap<String, Instant>>,

    /// Synthesized views and their metadata
    views: RwLock<HashMap<String, ViewMetadata>>,
}

/// Metadata about a synthesized view
struct ViewMetadata {
    /// Tables this view depends on
    tables: Vec<String>,

    /// Columns used as the lookup key
    key_columns: Vec<usize>,

    /// The SQL used to create this view
    sql: String,
}

impl Worker {
    /// Create a new background worker.
    pub fn new(conn: Arc<RwLock<Connection>>, view_cache: Arc<ViewCache>) -> Result<Self> {
        let worker = Self {
            conn,
            view_cache,
            recent_writes: RwLock::new(HashMap::new()),
            views: RwLock::new(HashMap::new()),
        };

        // TODO: Initialize CDC session observer
        // This requires FFI bindings to sqlite3session_create, etc.

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

        // Create the view handle
        let handle = ViewHandle::new(metadata.tables, metadata.key_columns);

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

        Ok(ViewMetadata {
            tables,
            key_columns,
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

    /// Record a write to a table.
    pub fn record_write(&self, table: &str) {
        let mut recent = self.recent_writes.write();
        recent.insert(table.to_string(), Instant::now());

        // Clean up old entries
        let cutoff = Instant::now() - std::time::Duration::from_secs(60);
        recent.retain(|_, &mut v| v > cutoff);
    }

    /// Process a CDC changeset and update views.
    pub fn process_changeset(&self, _changeset: &[u8]) -> Result<()> {
        // TODO: Implement changeset parsing and propagation
        // 1. Parse the binary changeset from sqlite3session
        // 2. Convert to Noria Modification packets
        // 3. Inject into the dataflow graph
        // 4. Let propagation update the evmap views
        Ok(())
    }
}

impl Clone for ViewMetadata {
    fn clone(&self) -> Self {
        Self {
            tables: self.tables.clone(),
            key_columns: self.key_columns.clone(),
            sql: self.sql.clone(),
        }
    }
}
