// ── snapstore-crash library API ───────────────────────────────────────────────
//!
//! Exposes the harness, fsck, child-workload, and full-stack modules for
//! integration tests that drive the harness directly (without shell-out to
//! the binary).

#![forbid(unsafe_code)]

pub mod child;
pub mod fsck;
pub mod fullstack;
pub mod gc_fixture;
pub mod harness;

pub use child::Scenario;
pub use fsck::{FsckCounts, FsckReport, Violation};
pub use fullstack::find_server_binary;
pub use gc_fixture::{populate_gc_fixture, GcFixtureOpts, GcFixtureSummary};
pub use harness::{run_cycles, RunOptions, Summary};

/// Re-export PAGE_SIZE so tests/fsck can use it.
pub use snapstore_types::PAGE_SIZE;
