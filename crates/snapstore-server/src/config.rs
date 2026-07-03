// #![deny(unsafe_code)] -- inherited from crate root
//! `config.toml` loader for `snapstore-server`.
//!
//! Unknown keys are rejected loudly via `serde(deny_unknown_fields)`.

use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;

// ── Sub-sections ────────────────────────────────────────────────────────────

/// Configuration for the SEQPACKET page channel (M5 / WI2-WI3).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PageChannelConfig {
    /// Maximum number of pages that can be in-flight through the ingest
    /// backpressure gate at any one time.  When the gate is full an incoming
    /// PUT_BATCH is rejected with `ERROR OVERLOAD`; the client backs off and
    /// retries (the operation is content-idempotent).
    ///
    /// Default: 65536 (= 256 MiB of 4 KiB pages).
    pub ingest_queue_pages: Option<u32>,

    /// Test-only: when set to `true`, the server deliberately flips one byte
    /// of the `batch_blake3` it returns in `PUT_BATCH_OK` responses.  This is
    /// used by the client cross-check test to verify that
    /// `ChannelError::CrossCheckMismatch` is surfaced and never retried.
    ///
    /// **MUST NOT be set in production.**  Documented here so the field does
    /// not trigger `deny_unknown_fields` in tests.
    #[doc(hidden)]
    pub corrupt_cross_check_for_test: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PagestoreConfig {
    /// Write-buffer flush threshold in bytes (default 4 MiB).
    pub write_buf_size: Option<usize>,
    /// Pack-file rotation threshold in bytes (default 1 GiB).
    pub max_pack_bytes: Option<u64>,
    /// LRU read-handle cache capacity (number of open file descriptors).
    pub read_handle_cap: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MetaConfig {
    /// Maximum inline input-log container size in bytes (default 16 MiB).
    pub input_log_max_bytes: Option<usize>,
}

/// M7 GC tuning (`[gc]` section).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcConfig {
    /// Watermark auto-trigger. Default OFF: flipping it on for an already
    /// deployed instance is an operator decision at upgrade time.
    #[serde(default = "default_gc_auto")]
    pub auto: bool,
    /// Disk-used percentage at which the auto-trigger fires a cycle.
    #[serde(default = "default_gc_trigger_disk_pct")]
    pub trigger_disk_pct: u8,
    /// Auto-trigger poll interval.
    #[serde(default = "default_gc_check_interval_secs")]
    pub check_interval_secs: u64,
    /// Pack liveness fraction below which a pack is compacted.
    #[serde(default = "default_gc_compact_threshold")]
    pub compact_threshold: f64,
    /// Cycles a tombstoned subtree survives before its rows are reaped.
    #[serde(default = "default_gc_tombstone_grace_cycles")]
    pub tombstone_grace_cycles: u32,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            auto: default_gc_auto(),
            trigger_disk_pct: default_gc_trigger_disk_pct(),
            check_interval_secs: default_gc_check_interval_secs(),
            compact_threshold: default_gc_compact_threshold(),
            tombstone_grace_cycles: default_gc_tombstone_grace_cycles(),
        }
    }
}

fn default_gc_auto() -> bool {
    false
}

fn default_gc_trigger_disk_pct() -> u8 {
    80
}

fn default_gc_check_interval_secs() -> u64 {
    60
}

fn default_gc_compact_threshold() -> f64 {
    0.5
}

fn default_gc_tombstone_grace_cycles() -> u32 {
    1
}

// ── Top-level config ─────────────────────────────────────────────────────────

/// Parsed and resolved server configuration.
///
/// # UNKNOWN KEYS
/// `serde(deny_unknown_fields)` is set on every struct — an unknown key in
/// `config.toml` produces a hard error that names the offending key.  This is
/// intentional: typos in the config file should never silently produce a
/// differently-behaving server.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Root of the data directory.  All sub-paths are relative to this.
    pub data_root: PathBuf,

    /// TCP address for the gRPC service.  Default: 127.0.0.1:7410.
    #[serde(default = "default_grpc_tcp_addr")]
    pub grpc_tcp_addr: SocketAddr,

    /// Path to the Unix domain socket.  Default: `<data_root>/snapstore.sock`.
    ///
    /// `None` only when the user sets an explicit non-relative path via config.
    /// In practice this is always `Some` because the file-load logic fills in
    /// the default if the field is absent.
    pub grpc_uds_path: Option<PathBuf>,

    /// Path for the page channel FIFO / socket (used in M5 / WI3).
    pub page_channel_path: Option<PathBuf>,

    /// TCP address for the HTTP `/healthz` + `/metrics` service.  Default: 127.0.0.1:7411.
    #[serde(default = "default_http_addr")]
    pub http_addr: SocketAddr,

    /// Pagestore tuning.
    #[serde(default)]
    pub pagestore: PagestoreConfig,

    /// Meta tuning.
    #[serde(default)]
    pub meta: MetaConfig,

    /// Page channel tuning (optional; ignored when `page_channel_path` is
    /// absent).
    #[serde(default)]
    pub page_channel: PageChannelConfig,

    /// M7 GC tuning. Absent section = all defaults (auto-trigger OFF).
    #[serde(default)]
    pub gc: GcConfig,
}

fn default_grpc_tcp_addr() -> SocketAddr {
    "127.0.0.1:7410".parse().unwrap()
}

fn default_http_addr() -> SocketAddr {
    "127.0.0.1:7411".parse().unwrap()
}

impl ServerConfig {
    /// Resolve the UDS path: use explicit `grpc_uds_path` or default to
    /// `<data_root>/snapstore.sock`.
    pub fn resolved_uds_path(&self) -> PathBuf {
        match &self.grpc_uds_path {
            Some(p) => p.clone(),
            None => self.data_root.join("snapstore.sock"),
        }
    }
}

// ── Loader ───────────────────────────────────────────────────────────────────

/// Load a `ServerConfig` from `path`.
///
/// If the file is absent, all defaults are used (missing file = fine).
/// If the file is present but malformed, an error is returned.
pub fn load_config(path: &std::path::Path) -> Result<ServerConfig, ConfigError> {
    if !path.exists() {
        // Build a minimal config with only the required `data_root` set to CWD.
        // Callers that need a specific data_root must pass a config file.
        let default_str = r#"data_root = "./data""#;
        return parse_toml(default_str);
    }
    let raw = std::fs::read_to_string(path)
        .map_err(|e| ConfigError::Io(format!("read {}: {e}", path.display())))?;
    parse_toml(&raw)
}

fn parse_toml(s: &str) -> Result<ServerConfig, ConfigError> {
    toml::from_str(s).map_err(|e| ConfigError::Parse(e.to_string()))
}

// ── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("I/O error: {0}")]
    Io(String),
    #[error("config parse error: {0}")]
    Parse(String),
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn parse(s: &str) -> Result<ServerConfig, ConfigError> {
        parse_toml(s)
    }

    #[test]
    fn defaults_compiles() {
        let cfg = parse(r#"data_root = "/tmp/test""#).unwrap();
        assert_eq!(cfg.grpc_tcp_addr.port(), 7410);
        assert_eq!(cfg.http_addr.port(), 7411);
        assert!(cfg.grpc_uds_path.is_none());
        assert!(cfg.page_channel_path.is_none());
    }

    #[test]
    fn override_ports() {
        let cfg = parse(
            r#"
data_root = "/data"
grpc_tcp_addr = "0.0.0.0:9000"
http_addr = "0.0.0.0:9001"
"#,
        )
        .unwrap();
        assert_eq!(cfg.grpc_tcp_addr.port(), 9000);
        assert_eq!(cfg.http_addr.port(), 9001);
    }

    #[test]
    fn override_pagestore_meta() {
        let cfg = parse(
            r#"
data_root = "/data"
[pagestore]
write_buf_size = 8388608
max_pack_bytes = 2147483648
read_handle_cap = 512
[meta]
input_log_max_bytes = 33554432
"#,
        )
        .unwrap();
        assert_eq!(cfg.pagestore.write_buf_size, Some(8 * 1024 * 1024));
        assert_eq!(cfg.meta.input_log_max_bytes, Some(32 * 1024 * 1024));
    }

    #[test]
    fn unknown_key_rejected() {
        let err = parse(
            r#"
data_root = "/data"
mystery_field = "oops"
"#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("mystery_field") || msg.contains("unknown field"),
            "expected unknown-key error, got: {msg}"
        );
    }

    #[test]
    fn unknown_sub_key_rejected() {
        let err = parse(
            r#"
data_root = "/data"
[pagestore]
mystery_subkey = 42
"#,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("mystery_subkey") || msg.contains("unknown field"),
            "expected unknown-subkey error, got: {msg}"
        );
    }

    #[test]
    fn file_load_absent_uses_defaults() {
        let cfg = load_config(std::path::Path::new("/nonexistent/config.toml")).unwrap();
        assert_eq!(cfg.grpc_tcp_addr.port(), 7410);
    }

    #[test]
    fn file_load_present() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "data_root = \"/tmp/x\"").unwrap();
        let cfg = load_config(tmp.path()).unwrap();
        assert_eq!(cfg.data_root, std::path::PathBuf::from("/tmp/x"));
    }

    #[test]
    fn resolved_uds_path_default() {
        let cfg = parse(r#"data_root = "/data""#).unwrap();
        assert_eq!(
            cfg.resolved_uds_path(),
            std::path::PathBuf::from("/data/snapstore.sock")
        );
    }

    #[test]
    fn resolved_uds_path_explicit() {
        let cfg = parse(
            r#"
data_root = "/data"
grpc_uds_path = "/var/run/snapstore.sock"
"#,
        )
        .unwrap();
        assert_eq!(
            cfg.resolved_uds_path(),
            std::path::PathBuf::from("/var/run/snapstore.sock")
        );
    }
}
