//! Prometheus metrics for `snapstore-server`.

use prometheus::{
    register_counter_vec_with_registry, register_histogram_with_registry,
    register_int_counter_with_registry, register_int_gauge_vec_with_registry,
    register_int_gauge_with_registry, CounterVec, Histogram, IntCounter, IntGauge, IntGaugeVec,
    Registry,
};

/// All server-level Prometheus metrics collected in one struct.
#[derive(Clone)]
pub struct Metrics {
    /// `snapstore_pages_ingested_total{dedup="new"|"dup"}`
    pub pages_ingested: CounterVec,
    /// `snapstore_commit_seconds` — histogram around `put_snapshot`.
    pub commit_seconds: Histogram,
    /// `snapstore_resolve_seconds` — histogram around `resolve_pages` calls.
    pub resolve_seconds: Histogram,
    /// `snapstore_flatten_depth` — histogram of chain depth during flatten.
    pub flatten_depth: Histogram,
    /// `snapstore_meta_txn_seconds` — histogram around meta write calls.
    pub meta_txn_seconds: Histogram,
    /// `snapstore_nodes{status}` — gauge updated on stats/create.
    pub nodes: IntGaugeVec,
    /// `snapstore_integrity_errors_total` — counter incremented on startup
    /// reconciliation failures and bad-footer manifest removals.
    pub integrity_errors: IntCounter,

    // ── page channel ─────────────────────────────────────────────────────────
    /// `snapstore_page_channel_bytes_total{dir="in"|"out"}` — bytes transferred
    /// on the SEQPACKET page channel (page payload bytes, not wire overhead).
    pub page_channel_bytes: CounterVec,
    /// `snapstore_page_channel_batches_total{op="put"|"get"}` — completed
    /// batches by operation type.
    pub page_channel_batches: CounterVec,
    /// `snapstore_page_channel_clients` — current number of connected page-channel
    /// clients (gauge).
    pub page_channel_clients: IntGauge,
    /// `snapstore_page_channel_crosscheck_mismatch_total` — server-side hook
    /// counter for the corrupt-cross-check test path (see
    /// [`crate::config::PageChannelConfig::corrupt_cross_check_for_test`]).
    /// Under normal operation this must never advance.
    pub page_channel_crosscheck_mismatch: IntCounter,
}

impl Metrics {
    /// Register all metrics with the given registry.
    pub fn new(registry: &Registry) -> Self {
        let pages_ingested = register_counter_vec_with_registry!(
            "snapstore_pages_ingested_total",
            "Total pages ingested, partitioned by dedup result",
            &["dedup"],
            registry
        )
        .expect("metrics registration");

        let commit_seconds = register_histogram_with_registry!(
            "snapstore_commit_seconds",
            "Seconds to complete put_snapshot (including group-commit wait)",
            vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
            registry
        )
        .expect("metrics registration");

        let resolve_seconds = register_histogram_with_registry!(
            "snapstore_resolve_seconds",
            "Seconds to complete a full ResolvePages stream",
            vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0],
            registry
        )
        .expect("metrics registration");

        let flatten_depth = register_histogram_with_registry!(
            "snapstore_flatten_depth",
            "Manifest parent-chain depth during a flatten operation",
            vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0],
            registry
        )
        .expect("metrics registration");

        let meta_txn_seconds = register_histogram_with_registry!(
            "snapstore_meta_txn_seconds",
            "Seconds to complete a meta write call (create_node, update_nodes, etc.)",
            vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
            registry
        )
        .expect("metrics registration");

        // Histogram buckets for PutPages batch size aren't needed per spec —
        // the plan only calls for the above.

        let nodes = register_int_gauge_vec_with_registry!(
            "snapstore_nodes",
            "Current node count by status",
            &["status"],
            registry
        )
        .expect("metrics registration");

        let integrity_errors = register_int_counter_with_registry!(
            "snapstore_integrity_errors_total",
            "Total integrity errors detected at startup (bad-footer manifests + orphan snapshot_refs)",
            registry
        )
        .expect("metrics registration");

        let page_channel_bytes = register_counter_vec_with_registry!(
            "snapstore_page_channel_bytes_total",
            "Page-channel payload bytes transferred, by direction",
            &["dir"],
            registry
        )
        .expect("metrics registration");

        let page_channel_batches = register_counter_vec_with_registry!(
            "snapstore_page_channel_batches_total",
            "Page-channel completed batches, by operation",
            &["op"],
            registry
        )
        .expect("metrics registration");

        let page_channel_clients = register_int_gauge_with_registry!(
            "snapstore_page_channel_clients",
            "Current number of connected page-channel clients",
            registry
        )
        .expect("metrics registration");

        let page_channel_crosscheck_mismatch = register_int_counter_with_registry!(
            "snapstore_page_channel_crosscheck_mismatch_total",
            "Server-side corrupt-cross-check hook counter (test-only path; must stay at 0 in production)",
            registry
        )
        .expect("metrics registration");

        // Pre-initialise label combinations so they appear in /metrics even when 0.
        let _ = pages_ingested.with_label_values(&["new"]);
        let _ = pages_ingested.with_label_values(&["dup"]);
        for status in ["frontier", "expanded", "pruned", "goal"] {
            nodes.with_label_values(&[status]).set(0);
        }
        let _ = page_channel_bytes.with_label_values(&["in"]);
        let _ = page_channel_bytes.with_label_values(&["out"]);
        let _ = page_channel_batches.with_label_values(&["put"]);
        let _ = page_channel_batches.with_label_values(&["get"]);

        Self {
            pages_ingested,
            commit_seconds,
            resolve_seconds,
            flatten_depth,
            meta_txn_seconds,
            nodes,
            integrity_errors,
            page_channel_bytes,
            page_channel_batches,
            page_channel_clients,
            page_channel_crosscheck_mismatch,
        }
    }
}
