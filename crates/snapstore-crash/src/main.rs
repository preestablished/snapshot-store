// ── snapstore-crash — crash-injection harness (M6) ──────────────────────────
//!
//! Subcommands:
//! - `child` — seeded workload process (spawned by the parent, killed with SIGKILL)
//! - `run`   — parent/test-runner: spawns children, kills them, recovers,
//!   runs fsck, and checks invariants
//! - `fsck`  — offline integrity check, prints JSON report

#![forbid(unsafe_code)]

mod child;
mod fsck;
mod harness;

// Re-export for integration tests.
pub use child::Scenario;
pub use fsck::{FsckReport, Violation};
pub use harness::{run_cycles, RunOptions, Summary};

use snapstore_types::PAGE_SIZE;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "snapstore-crash", about = "M6 crash-injection harness")]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Seeded child workload (spawned by `run`; not for direct use).
    Child {
        /// Scratch directory for the store + oracle journal.
        #[arg(long)]
        dir: PathBuf,
        /// PRNG seed for deterministic workload.
        #[arg(long)]
        seed: u64,
        /// Number of operations to run before exiting normally.
        #[arg(long, default_value_t = 128)]
        ops: u64,
        /// Workload scenario.
        #[arg(long, default_value = "default")]
        scenario: String,
    },
    /// Parent: spawn + kill children, recover, check invariants.
    Run {
        /// Number of randomized kill cycles.
        #[arg(long, default_value_t = 5)]
        cycles: u64,
        /// Master PRNG seed.
        #[arg(long, default_value_t = 42)]
        seed: u64,
        /// Failpoint matrix passes (0 = skip; requires --features failpoints).
        #[arg(long, default_value_t = 1)]
        matrix_passes: u64,
        /// Operations per child cycle.
        #[arg(long, default_value_t = 64)]
        ops_per_cycle: u64,
        /// Workload scenario.
        #[arg(long, default_value = "default")]
        scenario: String,
        /// Arm one named failpoint for every randomized cycle (matrix-failure
        /// repro; requires --features failpoints).
        #[arg(long)]
        failpoint: Option<String>,
    },
    /// Offline integrity check — prints a JSON report and exits nonzero on violations.
    Fsck {
        /// Store root directory.
        #[arg(long)]
        store_root: PathBuf,
        /// Path to the SQLite meta database.
        #[arg(long)]
        meta_db: PathBuf,
        /// Re-hash every page payload and sealed pack body.
        #[arg(long, default_value_t = false)]
        deep: bool,
    },
}

// ── main ──────────────────────────────────────────────────────────────────────

fn main() {
    let cli = Cli::parse();

    match cli.command {
        // ── child ────────────────────────────────────────────────────────────
        Cmd::Child {
            dir,
            seed,
            ops,
            scenario,
        } => {
            let sc: child::Scenario = scenario.parse().unwrap_or_else(|e| {
                eprintln!("bad scenario: {e}");
                std::process::exit(1);
            });
            child::run_child(&dir, seed, ops, sc);
        }

        // ── run ──────────────────────────────────────────────────────────────
        Cmd::Run {
            cycles,
            seed,
            matrix_passes,
            ops_per_cycle,
            scenario,
            failpoint,
        } => {
            let sc: child::Scenario = scenario.parse().unwrap_or_else(|e| {
                eprintln!("bad scenario: {e}");
                std::process::exit(1);
            });

            let opts = RunOptions {
                cycles,
                seed,
                matrix_passes,
                ops_per_cycle,
                scenario: sc,
                failpoint,
            };

            println!(
                "snapstore-crash: {} cycles, seed={}, matrix_passes={}, ops_per_cycle={}",
                cycles, seed, matrix_passes, ops_per_cycle
            );

            let summary = run_cycles(&opts);

            println!(
                "DONE  cycles={} inv_failures={} fsck_violations={} \
                 matrix_cycles={} matrix_failures={} \
                 elapsed={:.2}s cycles/s={:.1}",
                summary.total_cycles,
                summary.invariant_failures,
                summary.fsck_violations,
                summary.matrix_cycles,
                summary.matrix_failures,
                summary.elapsed_secs,
                summary.cycles_per_sec,
            );

            let failed =
                summary.invariant_failures + summary.fsck_violations + summary.matrix_failures;
            if failed > 0 {
                std::process::exit(1);
            }
        }

        // ── fsck ─────────────────────────────────────────────────────────────
        Cmd::Fsck {
            store_root,
            meta_db,
            deep,
        } => {
            let report = fsck::fsck(&store_root, &meta_db, deep);
            let json = serde_json::to_string_pretty(&report)
                .unwrap_or_else(|e| format!(r#"{{"error":"{e}"}}"#));
            println!("{json}");
            if !report.ok() {
                std::process::exit(1);
            }
        }
    }
}
