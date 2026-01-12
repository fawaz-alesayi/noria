//! Production-safe metrics with minimal overhead.
//!
//! These atomic counters are always compiled in but have ~10ns overhead per increment.
//! Use for dashboards, alerting, and understanding cache behavior.
//!
//! For "where is time spent" profiling, use `perf record` instead.

use std::sync::atomic::{AtomicU64, Ordering};

/// Global metrics for Noria operations.
///
/// Thread-safe atomic counters with relaxed ordering for minimal overhead.
/// These are suitable for production use.
#[derive(Debug, Default)]
pub struct Metrics {
    // Cache behavior
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub upqueries: AtomicU64,

    // Operation counts
    pub lookups: AtomicU64,
    pub inserts: AtomicU64,
    pub updates: AtomicU64,
    pub deletes: AtomicU64,
    pub propagations: AtomicU64,

    // Aggregate timing (for averages, not per-call)
    pub total_lookup_ns: AtomicU64,
    pub total_propagate_ns: AtomicU64,
}

impl Metrics {
    pub const fn new() -> Self {
        Self {
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            upqueries: AtomicU64::new(0),
            lookups: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            updates: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            propagations: AtomicU64::new(0),
            total_lookup_ns: AtomicU64::new(0),
            total_propagate_ns: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn inc_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn inc_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn inc_upquery(&self) {
        self.upqueries.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn inc_lookup(&self) {
        self.lookups.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn inc_insert(&self) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn inc_update(&self) {
        self.updates.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn inc_delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn inc_propagation(&self) {
        self.propagations.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn add_lookup_time(&self, ns: u64) {
        self.total_lookup_ns.fetch_add(ns, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn add_propagate_time(&self, ns: u64) {
        self.total_propagate_ns.fetch_add(ns, Ordering::Relaxed);
    }

    /// Get a snapshot of all metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            upqueries: self.upqueries.load(Ordering::Relaxed),
            lookups: self.lookups.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            updates: self.updates.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            propagations: self.propagations.load(Ordering::Relaxed),
            total_lookup_ns: self.total_lookup_ns.load(Ordering::Relaxed),
            total_propagate_ns: self.total_propagate_ns.load(Ordering::Relaxed),
        }
    }

    /// Reset all metrics to zero.
    pub fn reset(&self) {
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.upqueries.store(0, Ordering::Relaxed);
        self.lookups.store(0, Ordering::Relaxed);
        self.inserts.store(0, Ordering::Relaxed);
        self.updates.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
        self.propagations.store(0, Ordering::Relaxed);
        self.total_lookup_ns.store(0, Ordering::Relaxed);
        self.total_propagate_ns.store(0, Ordering::Relaxed);
    }
}

/// Point-in-time snapshot of metrics (non-atomic, for reporting).
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub upqueries: u64,
    pub lookups: u64,
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub propagations: u64,
    pub total_lookup_ns: u64,
    pub total_propagate_ns: u64,
}

impl MetricsSnapshot {
    /// Cache hit rate as a percentage (0.0 to 100.0).
    pub fn hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            (self.cache_hits as f64 / total as f64) * 100.0
        }
    }

    /// Average lookup latency in nanoseconds.
    pub fn avg_lookup_ns(&self) -> u64 {
        if self.lookups == 0 {
            0
        } else {
            self.total_lookup_ns / self.lookups
        }
    }

    /// Average propagation latency in nanoseconds.
    pub fn avg_propagate_ns(&self) -> u64 {
        if self.propagations == 0 {
            0
        } else {
            self.total_propagate_ns / self.propagations
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_basic() {
        let m = Metrics::new();
        m.inc_cache_hit();
        m.inc_cache_hit();
        m.inc_cache_miss();

        let snap = m.snapshot();
        assert_eq!(snap.cache_hits, 2);
        assert_eq!(snap.cache_misses, 1);
        assert!((snap.hit_rate() - 66.666).abs() < 0.01);
    }

    #[test]
    fn test_metrics_reset() {
        let m = Metrics::new();
        m.inc_lookup();
        m.inc_lookup();
        assert_eq!(m.snapshot().lookups, 2);

        m.reset();
        assert_eq!(m.snapshot().lookups, 0);
    }
}
