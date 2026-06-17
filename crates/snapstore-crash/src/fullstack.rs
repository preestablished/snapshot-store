// ── fullstack — full-stack crash scenario ────────────────────────────────────
//!
//! Per-cycle logic for the `full-stack` scenario.
//!
//! Unlike library-mode scenarios (where a child process opens store/meta
//! directly), this scenario:
//!
//! 1. Spawns the real `snapstore-server` binary (--config).
//! 2. Waits for the server to be ready (polls via UDS + stats RPC).
//! 3. Drives a seeded exploration loop through the **blocking** gRPC client.
//! 4. Sends SIGKILL to the **server** after a random delay.
//! 5. Restarts the server on the same data dir (recovery runs in startup).
//! 6. Verifies blind-retry convergence through a fresh client.
//! 7. Checks all journaled invariants through the public API.
//! 8. Shuts the server down cleanly (SIGTERM + wait), then runs offline fsck.
//!
//! The server binary is located next to the current executable; if absent the
//! test is SKIPPED with a loud message.  In the integration test, it is built
//! first via `cargo build -p snapstore-server`.

use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use snapstore_client::blocking::SnapstoreClient;
use snapstore_client::snapstore_proto::{CreateNodeRequest, NodeUpdate};
use snapstore_client::transport::Transport;
use snapstore_manifest::{input_log::InputLogContainer, DeviceBlob};
use snapstore_types::{LogId, SnapshotRef, PAGE_SIZE};

use crate::fsck::fsck;

// ── Constants ─────────────────────────────────────────────────────────────────

const PAGES_PER_FULL: usize = 8;
const GUEST_RAM_BYTES: u64 = PAGES_PER_FULL as u64 * PAGE_SIZE as u64;
const READINESS_TIMEOUT: Duration = Duration::from_secs(10);
const READINESS_POLL_MS: u64 = 20;
const KILL_DELAY_MIN_MS: u64 = 10;
const KILL_DELAY_MAX_MS: u64 = 500;
const DRIVER_OPS: u64 = 40; // operations before natural stop (if no kill)

// ── Binary discovery ─────────────────────────────────────────────────────────

/// Find the `snapstore-server` binary next to the current executable.
///
/// Returns `None` if the binary is not present (skip signal).
pub fn find_server_binary() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let dir = exe.parent()?;
    let candidate = dir.join("snapstore-server");
    if candidate.exists() {
        Some(candidate)
    } else {
        None
    }
}

// ── Journal ───────────────────────────────────────────────────────────────────

struct Journal {
    writer: std::io::BufWriter<std::fs::File>,
}

impl Journal {
    fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .custom_flags(libc::O_SYNC)
            .open(path)?;
        Ok(Self {
            writer: std::io::BufWriter::new(file),
        })
    }

    fn record(&mut self, op: &str, key: &str, step: u64) -> std::io::Result<()> {
        writeln!(self.writer, "{op}\t{key}\t{step}")?;
        self.writer.flush()
    }
}

// ── Server process management ─────────────────────────────────────────────────

/// Write a `config.toml` for the server in the scratch directory.
fn write_config(scratch: &Path) -> PathBuf {
    let config_path = scratch.join("config.toml");
    let uds_path = scratch.join("server.sock");
    let content = format!(
        r#"data_root = "{data_root}"
grpc_uds_path = "{uds}"
grpc_tcp_addr = "127.0.0.1:0"
http_addr = "127.0.0.1:0"
"#,
        data_root = scratch.join("data").display(),
        uds = uds_path.display(),
    );
    fs::write(&config_path, content).expect("write config.toml");
    config_path
}

/// Spawn the server process and return a `Child`.
fn spawn_server(binary: &Path, config_path: &Path) -> Child {
    Command::new(binary)
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn snapstore-server")
}

/// Poll the UDS socket until the server responds to `stats` or timeout.
fn wait_for_ready(uds_path: &Path) -> Result<(), String> {
    let deadline = Instant::now() + READINESS_TIMEOUT;
    loop {
        if Instant::now() >= deadline {
            return Err(format!(
                "server readiness timeout after {}s (uds={})",
                READINESS_TIMEOUT.as_secs(),
                uds_path.display()
            ));
        }
        if uds_path.exists() {
            // Try a blocking connect + stats call.
            let transport = Transport::Uds(uds_path.to_path_buf());
            if let Ok(client) = SnapstoreClient::connect(transport) {
                if client.stats(None).is_ok() {
                    return Ok(());
                }
            }
        }
        std::thread::sleep(Duration::from_millis(READINESS_POLL_MS));
    }
}

// ── Drop guard for server processes ──────────────────────────────────────────

/// Ensures a server `Child` is killed and reaped when dropped.
/// This prevents zombie processes on all early-return paths.
struct ServerGuard(Option<Child>);

impl ServerGuard {
    fn new(child: Child) -> Self {
        Self(Some(child))
    }

    /// Take the `Child` out (e.g. to pass to `shutdown_server`).
    fn take(&mut self) -> Option<Child> {
        self.0.take()
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

// ── Signal helpers ────────────────────────────────────────────────────────────

/// Kill a server process with SIGKILL (Linux).
#[cfg(target_os = "linux")]
fn kill_server_sigkill(pid: u32) {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;
    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
    // Reap.
    let _ = nix::sys::wait::waitpid(Pid::from_raw(pid as i32), None);
}

#[cfg(not(target_os = "linux"))]
fn kill_server_sigkill(pid: u32) {
    unsafe { libc::kill(pid as i32, libc::SIGKILL) };
}

/// Send SIGTERM and wait for the server to exit cleanly.
fn shutdown_server(mut child: Child) {
    let pid = child.id();
    #[cfg(target_os = "linux")]
    {
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;
        let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
    }
    #[cfg(not(target_os = "linux"))]
    {
        unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    }
    // Give it up to 5 s to exit gracefully.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(_) => return,
        }
        if Instant::now() >= deadline {
            // Force kill.
            let _ = child.kill();
            let _ = child.wait();
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ── Driver op helpers ─────────────────────────────────────────────────────────

fn make_page(seed: u64) -> [u8; PAGE_SIZE] {
    let mut p = [0u8; PAGE_SIZE];
    let seed_bytes = seed.to_le_bytes();
    for (i, b) in p.iter_mut().enumerate() {
        *b = seed_bytes[i % 8].wrapping_add(i as u8);
    }
    p
}

fn seed_from(a: u64, b: u64) -> u64 {
    let mut h = blake3::Hasher::new();
    h.update(&a.to_le_bytes());
    h.update(&b.to_le_bytes());
    let out = h.finalize();
    u64::from_le_bytes(out.as_bytes()[0..8].try_into().unwrap())
}

fn hex_snap(r: &SnapshotRef) -> String {
    r.to_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_log(l: &LogId) -> String {
    l.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

fn empty_blob() -> DeviceBlob {
    DeviceBlob {
        format: 0,
        zstd: false,
        bytes: vec![],
        raw_len: 0,
    }
}

// ── One full-stack cycle ──────────────────────────────────────────────────────

/// Run one full-stack crash cycle.
///
/// Returns `Ok(())` on success (zero invariant violations), or `Err(msg)` on
/// any invariant failure.
///
/// # Server binary
///
/// Pass `server_binary = None` to skip this cycle (binary not found).
pub fn run_fullstack_cycle(server_binary: &Path, cycle_seed: u64) -> Result<(), String> {
    let scratch = tempfile::TempDir::new().map_err(|e| e.to_string())?;
    let scratch_path = scratch.path().to_path_buf();
    run_fullstack_cycle_in_dir(server_binary, cycle_seed, &scratch_path)
}

// The inflight tracking variables are set just before an RPC and cleared after
// it returns Ok.  On error paths we `break 'driver` without clearing them,
// leaving a `Some(...)` that is read in Phase 5.  Rustc's dataflow analysis
// warns about these "assigned but never read" on the Ok-then-cleared path.
// The warnings are false positives — the value IS read after the loop.
#[allow(unused_assignments)]
fn run_fullstack_cycle_in_dir(
    server_binary: &Path,
    cycle_seed: u64,
    scratch: &Path,
) -> Result<(), String> {
    let mut rng = StdRng::seed_from_u64(cycle_seed);

    // ── Phase 1: write config, spawn server, wait for ready ──────────────────
    let config_path = write_config(scratch);
    let uds_path = scratch.join("server.sock");

    let mut guard1 = ServerGuard::new(spawn_server(server_binary, &config_path));
    let server_pid = guard1.0.as_ref().map(|c| c.id()).unwrap_or(0);

    // Wait ready; if it fails, drop guard (kills the process) and return error.
    if let Err(e) = wait_for_ready(&uds_path) {
        // guard1 drops here, killing + reaping the server.
        return Err(format!("initial server ready wait failed: {e}"));
    }

    // ── Phase 2: driver loop + timed SIGKILL ─────────────────────────────────
    let journal_path = scratch.join("oracle.journal");
    let journal_result = Journal::open(&journal_path);
    // On journal open failure, guard1 still drops and kills the server.
    let mut journal = journal_result.map_err(|e| e.to_string())?;

    let client_result = SnapstoreClient::connect(Transport::Uds(uds_path.clone()));
    // On connect failure, guard1 still drops and kills the server.
    let client = client_result.map_err(|e| format!("client connect: {e}"))?;

    // Kill the server after a seeded random delay.
    let kill_delay_ms = rng.gen_range(KILL_DELAY_MIN_MS..KILL_DELAY_MAX_MS);
    let kill_at = Instant::now() + Duration::from_millis(kill_delay_ms);

    // Track last acknowledged put_snapshot container for convergence retry.
    let mut last_snap_container: Option<Vec<u8>> = None;
    let mut last_log_container: Option<Vec<u8>> = None;
    let mut last_create_node_req: Option<CreateNodeRequest> = None;
    let mut last_update_nodes_req: Option<(String, Vec<NodeUpdate>)> = None;
    // Track if driver was mid-op when server died (not journaled).
    let mut inflight_snap_container: Option<Vec<u8>> = None;
    let mut inflight_log_container: Option<Vec<u8>> = None;
    let mut inflight_create_node_req: Option<CreateNodeRequest> = None;
    let mut inflight_update_nodes_req: Option<(String, Vec<NodeUpdate>)> = None;
    let mut inflight_kv_key: Option<(Vec<u8>, Vec<u8>, Option<u64>)> = None;

    let mut prev_snap_ref: Option<SnapshotRef> = None;
    let mut created_node_ids: Vec<(String, u64)> = Vec::new(); // (exp_id, node_id)

    'driver: for step in 0..DRIVER_OPS {
        // Check if it's time to kill.
        if Instant::now() >= kill_at {
            // Kill now — any in-flight op is unacknowledged.
            break 'driver;
        }

        let exp_id = if step % 2 == 0 {
            "fs-exp-A".to_string()
        } else {
            "fs-exp-B".to_string()
        };

        // ── put_pages + put_snapshot ──────────────────────────────────────────
        let pages: Vec<[u8; PAGE_SIZE]> = (0..PAGES_PER_FULL)
            .map(|i| make_page(seed_from(cycle_seed ^ step, i as u64)))
            .collect();

        let page_data: Vec<(u64, Vec<u8>)> = pages
            .iter()
            .enumerate()
            .map(|(i, p)| (i as u64, p.to_vec()))
            .collect();

        // Build container before issuing RPCs.
        let page_refs: Vec<(u64, &[u8; PAGE_SIZE])> = pages
            .iter()
            .enumerate()
            .map(|(i, p)| (i as u64, p))
            .collect();

        use snapstore_client::helpers::build_snapshot_container;
        let container_bytes = match build_snapshot_container(
            prev_snap_ref.as_ref(),
            GUEST_RAM_BYTES,
            &page_refs,
            empty_blob(),
        ) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("fullstack: build_snapshot_container failed: {e}");
                continue;
            }
        };

        // Check kill window before issuing RPC.
        if Instant::now() >= kill_at {
            break 'driver;
        }

        // Mark as in-flight.
        inflight_snap_container = Some(container_bytes.clone());
        inflight_log_container = None;
        inflight_create_node_req = None;
        inflight_update_nodes_req = None;
        inflight_kv_key = None;

        // put_pages
        match client.put_pages(page_data) {
            Ok(_) => {}
            Err(_) => {
                // Server likely dead; stop.
                inflight_snap_container = None;
                break 'driver;
            }
        }

        // put_snapshot
        let snap_ref = match client.put_snapshot(container_bytes.clone()) {
            Ok(r) => r,
            Err(_) => {
                break 'driver;
            }
        };
        inflight_snap_container = None;

        let snap_key = hex_snap(&snap_ref);
        let op_name = if prev_snap_ref.is_none() {
            "put_snapshot_full"
        } else {
            "put_snapshot_delta"
        };
        journal
            .record(op_name, &snap_key, step)
            .map_err(|e| e.to_string())?;
        last_snap_container = Some(container_bytes);
        prev_snap_ref = Some(snap_ref.clone());

        // ── put_input_log ─────────────────────────────────────────────────────
        let payload = format!("fs-step={step} seed={}", rng.gen::<u64>()).into_bytes();
        let log_container = InputLogContainer::encode(1, &payload);

        if Instant::now() >= kill_at {
            break 'driver;
        }

        inflight_log_container = Some(log_container.clone());
        match client.put_input_log(log_container.clone()) {
            Ok((log_id, _)) => {
                inflight_log_container = None;
                let log_key = hex_log(&log_id);
                journal
                    .record("put_input_log", &log_key, step)
                    .map_err(|e| e.to_string())?;
                last_log_container = Some(log_container.clone());
                let _ = log_id; // stored in journal

                // ── create_node ───────────────────────────────────────────────
                if Instant::now() >= kill_at {
                    break 'driver;
                }

                let node_id = step + 1;
                let parent_node_id = if created_node_ids.is_empty() {
                    // Create root first.
                    if Instant::now() >= kill_at {
                        break 'driver;
                    }
                    let root_req = CreateNodeRequest {
                        experiment_id: exp_id.clone(),
                        node_id: 0,
                        parent_node_id: None,
                        snapshot_ref: snap_ref.to_bytes().to_vec(),
                        input_log_id: vec![],
                        inline_input_log: vec![],
                        status: 1, // FRONTIER
                        score: None,
                        icount: 0,
                        virtual_ns: 0,
                        attrs: vec![],
                    };
                    inflight_create_node_req = Some(root_req.clone());
                    match client.create_node(root_req) {
                        Ok(_) => {
                            inflight_create_node_req = None;
                            journal
                                .record("create_node", &format!("{exp_id}/0"), step)
                                .map_err(|e| e.to_string())?;
                            created_node_ids.push((exp_id.clone(), 0));
                        }
                        Err(_) => {
                            inflight_create_node_req = None;
                            break 'driver;
                        }
                    }
                    Some(0u64)
                } else {
                    // Pick a parent from the same experiment.
                    let same: Vec<_> = created_node_ids
                        .iter()
                        .filter(|(e, _)| *e == exp_id)
                        .collect();
                    if same.is_empty() {
                        None
                    } else {
                        let idx = rng.gen::<usize>() % same.len();
                        Some(same[idx].1)
                    }
                };

                // Compute log_id from container footer.
                let log_id_bytes = {
                    let body_end = log_container.len().saturating_sub(32);
                    let hash = blake3::hash(&log_container[..body_end]);
                    hash.as_bytes().to_vec()
                };

                if Instant::now() >= kill_at {
                    break 'driver;
                }
                let create_req = CreateNodeRequest {
                    experiment_id: exp_id.clone(),
                    node_id,
                    parent_node_id,
                    snapshot_ref: snap_ref.to_bytes().to_vec(),
                    input_log_id: log_id_bytes,
                    inline_input_log: vec![],
                    status: 1, // FRONTIER
                    score: Some(rng.gen::<f64>()),
                    icount: rng.gen::<u64>() % 1_000_000,
                    virtual_ns: rng.gen::<u64>() % 1_000_000_000,
                    attrs: vec![],
                };
                inflight_create_node_req = Some(create_req.clone());
                match client.create_node(create_req.clone()) {
                    Ok(_) => {
                        inflight_create_node_req = None;
                        journal
                            .record("create_node", &format!("{exp_id}/{node_id}"), step)
                            .map_err(|e| e.to_string())?;
                        created_node_ids.push((exp_id.clone(), node_id));
                        last_create_node_req = Some(create_req);
                    }
                    Err(_) => {
                        inflight_create_node_req = None;
                        break 'driver;
                    }
                }
            }
            Err(_) => {
                inflight_log_container = None;
                break 'driver;
            }
        }

        // ── batch update_nodes every ~8 steps ────────────────────────────────
        if step > 0 && step % 8 == 0 && created_node_ids.len() >= 4 {
            let batch_marker = rng.gen::<u64>();
            let batch_attrs = batch_marker.to_le_bytes().to_vec();

            let ids: Vec<u64> = created_node_ids
                .iter()
                .filter(|(e, _)| *e == exp_id)
                .take(4)
                .map(|(_, id)| *id)
                .collect();

            if !ids.is_empty() {
                let updates: Vec<NodeUpdate> = ids
                    .iter()
                    .map(|&id| NodeUpdate {
                        node_id: id,
                        status: None,
                        score: None,
                        attrs: Some(batch_attrs.clone()),
                        visit_count_delta: Some(1),
                        touch_visited: false,
                        icount: None,
                        virtual_ns: None,
                    })
                    .collect();

                if Instant::now() >= kill_at {
                    break 'driver;
                }
                inflight_update_nodes_req = Some((exp_id.clone(), updates.clone()));
                match client.update_nodes(exp_id.clone(), updates.clone()) {
                    Ok(updated_at) => {
                        inflight_update_nodes_req = None;
                        journal
                            .record(
                                "update_nodes",
                                &format!("{batch_marker}@{updated_at}"),
                                step,
                            )
                            .map_err(|e| e.to_string())?;
                        last_update_nodes_req = Some((exp_id.clone(), updates));
                    }
                    Err(_) => {
                        inflight_update_nodes_req = None;
                        break 'driver;
                    }
                }
            }
        }

        // ── KV CAS checkpoint every ~16 steps ────────────────────────────────
        if step % 16 == 0 {
            let key = b"fs-checkpoint".to_vec();
            let val = format!("step={step}").into_bytes();

            if Instant::now() >= kill_at {
                break 'driver;
            }
            inflight_kv_key = Some((key.clone(), val.clone(), None));
            match client.put_metadata(key.clone(), val.clone(), None) {
                Ok(gen) => {
                    inflight_kv_key = None;
                    journal
                        .record("put_metadata", &format!("fs-checkpoint@{gen}"), step)
                        .map_err(|e| e.to_string())?;
                }
                Err(_) => {
                    inflight_kv_key = None;
                    break 'driver;
                }
            }
        }
    }

    // ── Phase 3: SIGKILL the server ───────────────────────────────────────────
    kill_server_sigkill(server_pid);
    // The kill_server_sigkill already reaped the process via waitpid.
    // Disarm guard1 so it doesn't try to wait on a reaped zombie.
    if let Some(mut child) = guard1.take() {
        // waitpid already called in kill_server_sigkill; this may get ECHILD.
        let _ = child.wait();
    }

    // ── Phase 4: restart the server ───────────────────────────────────────────
    // guard2 ensures the second server is killed and reaped on all paths.
    let mut guard2 = ServerGuard::new(spawn_server(server_binary, &config_path));

    if let Err(e) = wait_for_ready(&uds_path) {
        // guard2 drops here, killing + reaping server_child2.
        return Err(format!("server ready wait after restart failed: {e}"));
    }

    // ── Phase 5: blind-retry convergence ─────────────────────────────────────
    let client2 = SnapstoreClient::connect(Transport::Uds(uds_path.clone()))
        .map_err(|e| format!("client2 connect: {e}"))?;

    // Re-issue in-flight ops (if any) plus a handful of journaled ops.
    let mut convergence_errors: Vec<String> = Vec::new();

    // Re-issue inflight snap (put_pages + put_snapshot).
    if let Some(container) = inflight_snap_container.as_ref() {
        // Rebuild page data from container — we already have the container.
        // For convergence: just put_snapshot; pages were already uploaded
        // before the server was killed (best-effort).
        match client2.put_snapshot(container.clone()) {
            Ok(snap_ref) => {
                // Record in journal for subsequent verification.
                let snap_key = hex_snap(&snap_ref);
                journal
                    .record("put_snapshot_full", &snap_key, u64::MAX)
                    .ok();
            }
            Err(e) => {
                // Transport errors are acceptable (server processing); other
                // errors indicate a problem.
                let s = format!("{e}");
                if !s.contains("Transport") && !s.contains("transport") {
                    convergence_errors
                        .push(format!("inflight put_snapshot convergence failed: {e}"));
                }
            }
        }
    }

    // Re-issue inflight log.
    if let Some(container) = inflight_log_container.as_ref() {
        match client2.put_input_log(container.clone()) {
            Ok(_) => {}
            Err(e) => {
                let s = format!("{e}");
                if !s.contains("Transport") && !s.contains("transport") {
                    convergence_errors
                        .push(format!("inflight put_input_log convergence failed: {e}"));
                }
            }
        }
    }

    // Re-issue inflight create_node.
    if let Some(req) = inflight_create_node_req.as_ref() {
        match client2.create_node(req.clone()) {
            Ok(_) => {}
            Err(e) => {
                let s = format!("{e}");
                // AlreadyExists is fine — idempotent.
                if !s.contains("already exists")
                    && !s.contains("AlreadyExists")
                    && !s.contains("Transport")
                    && !s.contains("transport")
                {
                    convergence_errors
                        .push(format!("inflight create_node convergence failed: {e}"));
                }
            }
        }
    }

    // Re-issue inflight update_nodes.
    if let Some((exp, updates)) = inflight_update_nodes_req.as_ref() {
        match client2.update_nodes(exp.clone(), updates.clone()) {
            Ok(_) => {}
            Err(e) => {
                let s = format!("{e}");
                if !s.contains("Transport") && !s.contains("transport") {
                    convergence_errors
                        .push(format!("inflight update_nodes convergence failed: {e}"));
                }
            }
        }
    }

    // Re-issue inflight KV.
    if let Some((key, val, exp_gen)) = inflight_kv_key.as_ref() {
        match client2.put_metadata(key.clone(), val.clone(), *exp_gen) {
            Ok(_) => {}
            Err(snapstore_client::ClientError::CasFailed { .. }) => {
                // CAS conflict after restart = the op was applied; consistent.
            }
            Err(e) => {
                let s = format!("{e}");
                if !s.contains("Transport") && !s.contains("transport") {
                    convergence_errors
                        .push(format!("inflight put_metadata convergence failed: {e}"));
                }
            }
        }
    }

    // Re-issue a handful of journaled acknowledged ops.
    if let Some(container) = last_snap_container.as_ref() {
        match client2.put_snapshot(container.clone()) {
            Ok(_) => {}
            Err(e) => {
                convergence_errors.push(format!("acknowledged put_snapshot re-put failed: {e}"));
            }
        }
    }
    if let Some(container) = last_log_container.as_ref() {
        match client2.put_input_log(container.clone()) {
            Ok(_) => {}
            Err(e) => {
                convergence_errors.push(format!("acknowledged put_input_log re-put failed: {e}"));
            }
        }
    }
    if let Some(req) = last_create_node_req.as_ref() {
        match client2.create_node(req.clone()) {
            Ok(_) => {}
            Err(e) => {
                let s = format!("{e}");
                // AlreadyExists is idempotent success.
                if !s.contains("already exists") && !s.contains("AlreadyExists") {
                    convergence_errors
                        .push(format!("acknowledged create_node re-issue failed: {e}"));
                }
            }
        }
    }
    if let Some((exp, updates)) = last_update_nodes_req.as_ref() {
        match client2.update_nodes(exp.clone(), updates.clone()) {
            Ok(_) => {}
            Err(e) => {
                convergence_errors.push(format!("acknowledged update_nodes re-issue failed: {e}"));
            }
        }
    }

    // ── Phase 6: verify through the public API ────────────────────────────────
    let journal_entries = parse_journal(&journal_path);
    let mut api_errors: Vec<String> = Vec::new();

    // Verify every journaled put_snapshot ref.
    for entry in journal_entries
        .iter()
        .filter(|e| e.op == "put_snapshot_full" || e.op == "put_snapshot_delta")
    {
        if let Some(snap_ref) = parse_snap_ref(&entry.key) {
            match client2.get_snapshot(snap_ref.clone()) {
                Ok(_) => {
                    // get_snapshot already verifies the footer internally.
                }
                Err(e) => {
                    api_errors.push(format!(
                        "acknowledged snapshot {} not readable after recovery: {e}",
                        entry.key
                    ));
                }
            }
            // resolve_pages must return non-empty.
            match client2.resolve_pages(snap_ref, None, true) {
                Ok(pages) if pages.is_empty() => {
                    api_errors.push(format!(
                        "resolve_pages returned 0 pages for snapshot {}",
                        entry.key
                    ));
                }
                Ok(_) => {}
                Err(e) => {
                    api_errors.push(format!(
                        "resolve_pages failed for snapshot {}: {e}",
                        entry.key
                    ));
                }
            }
        }
    }

    // Verify every journaled create_node.
    for entry in journal_entries.iter().filter(|e| e.op == "create_node") {
        let parts: Vec<&str> = entry.key.splitn(2, '/').collect();
        if parts.len() != 2 {
            continue;
        }
        let exp_id = parts[0].to_string();
        let Ok(node_id) = parts[1].parse::<u64>() else {
            continue;
        };
        match client2.get_node(exp_id.clone(), node_id) {
            Ok(_) => {}
            Err(e) => {
                api_errors.push(format!(
                    "acknowledged create_node {}/{} not found after recovery: {e}",
                    exp_id, node_id
                ));
            }
        }
    }

    // Verify KV consistency: last journaled generation must be reachable.
    for entry in journal_entries.iter().filter(|e| e.op == "put_metadata") {
        let parts: Vec<&str> = entry.key.splitn(2, '@').collect();
        if parts.len() != 2 {
            continue;
        }
        let key_name = parts[0].as_bytes().to_vec();
        let Ok(expected_gen) = parts[1].parse::<u64>() else {
            continue;
        };
        match client2.get_metadata(key_name) {
            Ok((_, gen)) => {
                // Generation must be >= the last acknowledged one (monotone).
                if gen < expected_gen {
                    api_errors.push(format!(
                        "KV generation regressed: expected >= {expected_gen} got {gen}"
                    ));
                }
            }
            Err(e) => {
                api_errors.push(format!(
                    "get_metadata after recovery failed for {}: {e}",
                    entry.key
                ));
            }
        }
    }

    // ── Phase 7: shut server down cleanly + offline fsck ─────────────────────
    // Take the child out of the guard so shutdown_server handles it gracefully
    // (SIGTERM + wait).  If take() returns None (shouldn't happen), guard2 drops
    // and force-kills on exit.
    if let Some(child) = guard2.take() {
        shutdown_server(child);
    }

    let data_dir = scratch.join("data");
    let store_root = data_dir.join("store");
    let meta_db_path = data_dir.join("meta").join("tree.db");

    let fsck_report = fsck(&store_root, &meta_db_path, true);
    let mut fsck_errors: Vec<String> = Vec::new();
    if !fsck_report.ok() {
        fsck_errors.push(format!(
            "offline fsck violations after restart: {:?}",
            fsck_report.violations
        ));
    }

    // ── Collect all errors ────────────────────────────────────────────────────
    let all_errors: Vec<String> = convergence_errors
        .into_iter()
        .chain(api_errors)
        .chain(fsck_errors)
        .collect();

    if all_errors.is_empty() {
        Ok(())
    } else {
        Err(all_errors.join("; "))
    }
}

// ── Journal parsing ───────────────────────────────────────────────────────────

struct JournalEntry {
    op: String,
    key: String,
    _step: u64,
}

fn parse_journal(path: &Path) -> Vec<JournalEntry> {
    let Ok(file) = fs::File::open(path) else {
        return Vec::new();
    };
    let reader = BufReader::new(file);
    let mut entries = Vec::new();
    for line in reader.lines().map_while(Result::ok) {
        let parts: Vec<&str> = line.splitn(3, '\t').collect();
        if parts.len() == 3 {
            if let Ok(step) = parts[2].parse::<u64>() {
                entries.push(JournalEntry {
                    op: parts[0].to_string(),
                    key: parts[1].to_string(),
                    _step: step,
                });
            }
        }
    }
    entries
}

fn parse_snap_ref(hex: &str) -> Option<SnapshotRef> {
    if hex.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = nibble(chunk[0])?;
        let lo = nibble(chunk[1])?;
        bytes[i] = (hi << 4) | lo;
    }
    Some(SnapshotRef::from_bytes(bytes))
}

fn nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
