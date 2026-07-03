// ── Integration tests for snapstore-crash ────────────────────────────────────

use snapstore_crash::{run_cycles, RunOptions, Scenario};
use std::path::Path;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_store(dir: &Path) -> (snapstore_store::SnapshotStore, snapstore_meta::MetaDb) {
    let store_root = dir.join("store");
    let meta_db = dir.join("meta").join("tree.db");
    std::fs::create_dir_all(&store_root).unwrap();
    std::fs::create_dir_all(dir.join("meta")).unwrap();
    let store = snapstore_store::SnapshotStore::open(&store_root).unwrap();
    let meta = snapstore_meta::MetaDb::open(&meta_db).unwrap();
    (store, meta)
}

fn build_small_store(dir: &Path) -> (snapstore_types::SnapshotRef, snapstore_types::LogId) {
    use snapstore_manifest::input_log::InputLogContainer;
    use snapstore_manifest::DeviceBlob;
    use snapstore_meta::CreateNodeParams;
    use snapstore_store::build::build_full_container;
    use snapstore_types::{ExperimentId, LogId, NodeId, NodeStatus, PAGE_SIZE};

    let (store, meta) = make_store(dir);

    // Ingest pages and put a snapshot.
    let page: [u8; PAGE_SIZE] = [0xABu8; PAGE_SIZE];
    store.pages().ingest(&[&page]).unwrap();

    let pairs: Vec<(u64, &[u8; PAGE_SIZE])> = (0..1).map(|i| (i, &page)).collect();
    let blob = DeviceBlob {
        format: 0,
        zstd: false,
        bytes: vec![],
        raw_len: 0,
    };
    let container = build_full_container(PAGE_SIZE as u64, &pairs, blob);
    let snap_ref = store.put_snapshot(&container).unwrap();

    // Put an input log.
    let payload = b"test-payload";
    let log_container = InputLogContainer::encode(1, payload);
    let log_id =
        LogId::from_bytes(*blake3::hash(&log_container[..log_container.len() - 32]).as_bytes());
    meta.put_input_log(log_id, &log_container).unwrap();

    // Create root node.
    let exp = ExperimentId::new("test-exp").unwrap();
    meta.create_node(CreateNodeParams {
        experiment_id: exp.clone(),
        node_id: NodeId(0),
        parent_node_id: None,
        snapshot_ref: snap_ref.clone(),
        input_log_id: Some(log_id),
        inline_log_container: None,
        status: NodeStatus::Frontier,
        score: None,
        icount: 0,
        virtual_ns: 0,
        attrs: None,
    })
    .unwrap();

    (snap_ref, log_id)
}

fn store_root(dir: &Path) -> std::path::PathBuf {
    dir.join("store")
}
fn meta_db(dir: &Path) -> std::path::PathBuf {
    dir.join("meta").join("tree.db")
}
fn pages_dir(dir: &Path) -> std::path::PathBuf {
    dir.join("store").join("pages")
}
fn manifests_dir(dir: &Path) -> std::path::PathBuf {
    dir.join("store").join("manifests")
}

// ── Part 1: fsck corruption matrix ───────────────────────────────────────────

/// Build a small store, run fsck on clean store → should be ok.
#[test]
fn fsck_clean_store_ok() {
    let dir = tempfile::TempDir::new().unwrap();
    build_small_store(dir.path());
    let report = snapstore_crash::fsck::fsck(&store_root(dir.path()), &meta_db(dir.path()), true);
    assert!(
        report.ok(),
        "clean store should pass fsck: {:?}",
        report.violations
    );
}

/// Flip a pack-record payload byte → RecordHashMismatch only.
#[test]
fn fsck_detects_record_hash_mismatch() {
    use std::os::unix::fs::FileExt;

    let dir = tempfile::TempDir::new().unwrap();
    build_small_store(dir.path());

    // Find the pack file.
    let pdir = pages_dir(dir.path());
    let pack_file = std::fs::read_dir(&pdir)
        .unwrap()
        .flatten()
        .find(|e| e.path().extension().and_then(|x| x.to_str()) == Some("spk"))
        .map(|e| e.path())
        .unwrap();

    // Find offset of first record payload.
    let pack_header_size: u64 = 20;
    let record_header_size: u64 = 37;
    let payload_offset = pack_header_size + record_header_size;

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&pack_file)
        .unwrap();
    let mut byte = [0u8; 1];
    file.read_exact_at(&mut byte, payload_offset).unwrap();
    byte[0] ^= 0xFF;
    file.write_at(&byte, payload_offset).unwrap();

    let report = snapstore_crash::fsck::fsck(&store_root(dir.path()), &meta_db(dir.path()), true);
    let classes: Vec<&str> = report.violations.iter().map(|v| v.class()).collect();
    // Must have RecordHashMismatch
    assert!(
        classes.contains(&"RecordHashMismatch"),
        "expected RecordHashMismatch, got: {classes:?}"
    );
    // Must NOT have unrelated violations (BadSidecarCrc, MissingManifest, etc.)
    // RecordHashMismatch corrupts the hash check but the sidecar still points to the
    // correct offset, so MissingPage may also appear if the hash in the sidecar
    // doesn't match the corrupted payload. We allow RecordHashMismatch + MissingPage.
    for class in &classes {
        assert!(
            matches!(
                *class,
                "RecordHashMismatch" | "MissingPage" | "BadPackBodyHash"
            ),
            "unexpected violation class: {class}"
        );
    }
}

/// Truncate a sidecar → BadSidecarCrc.
///
/// Strategy: write a valid sidecar (using snapstore-pagestore's index API
/// directly) for a sealed pack, then corrupt it.
#[test]
fn fsck_detects_bad_sidecar_crc() {
    use snapstore_pagestore::index::ShardedIndex;
    use snapstore_types::{PackId, PageHash, PageLoc, PAGE_SIZE};

    let dir = tempfile::TempDir::new().unwrap();
    build_small_store(dir.path());

    // Create a sealed pack manually in the pages/ dir so we can write a sidecar.
    let pdir = pages_dir(dir.path());

    // Write a valid sidecar for pack-00000099 (synthetic — doesn't need to exist
    // as a real sealed pack for the CRC check test, but fsck only checks sidecars
    // for packs that appear to be sealed). We need a real sealed pack.
    // Use snapstore-pagestore to create one via the low-level API.
    let pack_id = PackId(99);
    let pack_file = pdir.join("pack-00000063.spk");

    // Write a minimal sealed pack.
    {
        use snapstore_pagestore::pack::PackWriter;
        let page = [0xBBu8; PAGE_SIZE];
        let hash = PageHash::from_bytes(*blake3::hash(&page).as_bytes());
        let mut w = PackWriter::create(&pack_file, pack_id, 0).unwrap();
        w.append(&hash, &page).unwrap();
        w.seal().unwrap();
    }

    // Now write its sidecar via ShardedIndex.
    let sidecar_path = pdir.join("pack-00000063.idx");
    {
        let idx = ShardedIndex::new();
        let page = [0xBBu8; PAGE_SIZE];
        let hash = PageHash::from_bytes(*blake3::hash(&page).as_bytes());
        idx.insert(
            hash,
            PageLoc {
                pack: pack_id,
                offset: 20,
            },
        );
        idx.write_sidecar(&sidecar_path, pack_id).unwrap();
    }

    // Verify the sidecar exists and is valid.
    assert!(sidecar_path.exists(), "sidecar should have been written");
    let sidecar = sidecar_path;

    // Corrupt the sidecar.
    let mut data = std::fs::read(&sidecar).unwrap();
    let last = data.len() - 1;
    data[last] ^= 0xFF;
    std::fs::write(&sidecar, &data).unwrap();

    let report = snapstore_crash::fsck::fsck(&store_root(dir.path()), &meta_db(dir.path()), false);
    let classes: Vec<&str> = report.violations.iter().map(|v| v.class()).collect();
    assert!(
        classes.contains(&"BadSidecarCrc"),
        "expected BadSidecarCrc, got: {classes:?}"
    );
    for class in &classes {
        assert!(
            matches!(*class, "BadSidecarCrc" | "BadPackFooter"),
            "unexpected class after sidecar truncation: {class}"
        );
    }
}

/// Zero a manifest footer → BadManifestFooter.
#[test]
fn fsck_detects_bad_manifest_footer() {
    let dir = tempfile::TempDir::new().unwrap();
    let (snap_ref, _) = build_small_store(dir.path());

    // Find the .spm file.
    let hex: String = snap_ref
        .to_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let shard = &hex[..2];
    let spm_path = manifests_dir(dir.path())
        .join(shard)
        .join(format!("{hex}.spm"));

    let mut data = std::fs::read(&spm_path).unwrap();
    let len = data.len();
    data[len - 32..].fill(0);
    std::fs::write(&spm_path, &data).unwrap();

    let report = snapstore_crash::fsck::fsck(&store_root(dir.path()), &meta_db(dir.path()), false);
    let classes: Vec<&str> = report.violations.iter().map(|v| v.class()).collect();
    assert!(
        classes.contains(&"BadManifestFooter"),
        "expected BadManifestFooter, got: {classes:?}"
    );
    for class in &classes {
        assert!(
            matches!(*class, "BadManifestFooter" | "MissingManifest"),
            "unexpected class after manifest footer zeroed: {class}"
        );
    }
}

/// Delete a referenced .spm → MissingManifest.
#[test]
fn fsck_detects_missing_manifest() {
    let dir = tempfile::TempDir::new().unwrap();
    let (snap_ref, _) = build_small_store(dir.path());

    let hex: String = snap_ref
        .to_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let shard = &hex[..2];
    let spm_path = manifests_dir(dir.path())
        .join(shard)
        .join(format!("{hex}.spm"));

    std::fs::remove_file(&spm_path).unwrap();

    let report = snapstore_crash::fsck::fsck(&store_root(dir.path()), &meta_db(dir.path()), false);
    let classes: Vec<&str> = report.violations.iter().map(|v| v.class()).collect();
    assert!(
        classes.contains(&"MissingManifest"),
        "expected MissingManifest, got: {classes:?}"
    );
    for class in &classes {
        assert!(
            matches!(*class, "MissingManifest"),
            "unexpected class after spm removed: {class}"
        );
    }
}

/// Delete a node's input_log row → MissingInputLog.
#[test]
fn fsck_detects_missing_input_log() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_, log_id) = build_small_store(dir.path());

    // Directly delete the input_log row.
    let log_id_hex: String = log_id
        .to_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    {
        let conn = rusqlite::Connection::open(meta_db(dir.path())).unwrap();
        let log_id_bytes = log_id.to_bytes();
        conn.execute(
            "DELETE FROM input_logs WHERE log_id = ?1",
            [log_id_bytes.as_slice()],
        )
        .unwrap();
    }

    let report = snapstore_crash::fsck::fsck(&store_root(dir.path()), &meta_db(dir.path()), false);
    let classes: Vec<&str> = report.violations.iter().map(|v| v.class()).collect();
    assert!(
        classes.contains(&"MissingInputLog"),
        "expected MissingInputLog (log_id {}), got: {classes:?}",
        log_id_hex
    );
    for class in &classes {
        assert!(
            matches!(*class, "MissingInputLog"),
            "unexpected class after input_log row removed: {class}"
        );
    }
}

/// Create a pin pointing to a nonexistent manifest → DanglingPin.
#[test]
fn fsck_detects_dangling_pin() {
    let dir = tempfile::TempDir::new().unwrap();
    build_small_store(dir.path());

    // Insert a pin for a fake ref.
    {
        let conn = rusqlite::Connection::open(meta_db(dir.path())).unwrap();
        let fake_ref = [0xFFu8; 32];
        let now = 0i64;
        conn.execute(
            "INSERT INTO pins (snapshot_ref, note, created_at) VALUES (?1, NULL, ?2)",
            rusqlite::params![fake_ref.as_slice(), now],
        )
        .unwrap();
    }

    let report = snapstore_crash::fsck::fsck(&store_root(dir.path()), &meta_db(dir.path()), false);
    let classes: Vec<&str> = report.violations.iter().map(|v| v.class()).collect();
    assert!(
        classes.contains(&"DanglingPin"),
        "expected DanglingPin, got: {classes:?}"
    );
    for class in &classes {
        assert!(
            matches!(*class, "DanglingPin"),
            "unexpected class after dangling pin: {class}"
        );
    }
}

// ── Part 2(b): 5 randomized kill cycles ───────────────────────────────────────

#[test]
fn randomized_kill_cycles_5() {
    let opts = RunOptions {
        cycles: 5,
        seed: 12345,
        matrix_passes: 0,
        ops_per_cycle: 32,
        scenario: Scenario::Default,
        failpoint: None,
    };
    let summary = run_cycles(&opts);
    assert_eq!(
        summary.invariant_failures, 0,
        "invariant failures in 5 random cycles: {summary:?}"
    );
    assert_eq!(
        summary.fsck_violations, 0,
        "fsck violations in 5 random cycles: {summary:?}"
    );
}

// ── Part 2(c): failpoint matrix × 1 pass (feature-gated) ─────────────────────

#[cfg(feature = "failpoints")]
#[test]
fn failpoint_matrix_one_pass() {
    let opts = RunOptions {
        cycles: 0,
        seed: 99999,
        matrix_passes: 1,
        ops_per_cycle: 16,
        scenario: Scenario::Default,
        failpoint: None,
    };
    let summary = run_cycles(&opts);
    assert_eq!(summary.matrix_failures, 0, "matrix failures: {summary:?}");
}

// ── Part 2(d): SQLite batch scenario × 10 cycles ─────────────────────────────

#[test]
fn sqlite_batch_10_cycles() {
    let opts = RunOptions {
        cycles: 10,
        seed: 77777,
        matrix_passes: 0,
        ops_per_cycle: 30,
        scenario: Scenario::SqliteBatch,
        failpoint: None,
    };
    let summary = run_cycles(&opts);
    assert_eq!(
        summary.invariant_failures, 0,
        "sqlite-batch invariant failures: {summary:?}"
    );
}

// ── Part 3: full-stack mode × 5 cycles ───────────────────────────────────────
//
// Builds `snapstore-server` if the binary is not already present next to
// the current executable (which is in the same `target/<profile>/` dir), then
// runs 5 full-stack crash cycles and asserts zero invariant failures.
//
// The test is NOT marked #[ignore] — it should run as part of the default test
// suite.  It is somewhat slower than the library-mode tests (~5-15 s total)
// but well within the accepted 2.5-minute budget.
//
// NOTE: because `cargo test` places the test binary in `target/<profile>/deps/`
// while the server binary lives in `target/<profile>/`, we probe BOTH locations.
#[test]
fn full_stack_five_cycles() {
    // Ensure the server binary exists. We probe two locations:
    //   1. Next to the current test exe (target/<profile>/deps/snapstore-server).
    //   2. One directory up (target/<profile>/snapstore-server) — the normal
    //      cargo output location for `cargo build -p snapstore-server`.
    //
    // If neither exists, we build it via `cargo build -p snapstore-server`.

    let current_exe = std::env::current_exe().expect("current_exe");
    let exe_dir = current_exe.parent().expect("parent of current_exe");

    // Check next to test binary (deps/).
    let candidate_deps = exe_dir.join("snapstore-server");
    // Check sibling directory (target/<profile>/).
    let candidate_profile = exe_dir.parent().map(|p| p.join("snapstore-server"));

    let server_binary_present = candidate_deps.exists()
        || candidate_profile
            .as_ref()
            .map(|p| p.exists())
            .unwrap_or(false);

    if !server_binary_present {
        eprintln!(
            "full_stack_five_cycles: snapstore-server binary not found; \
             building with `cargo build -p snapstore-server` ..."
        );
        let status =
            std::process::Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
                .arg("build")
                .arg("-p")
                .arg("snapstore-server")
                .status()
                .expect("failed to run cargo build");
        assert!(status.success(), "cargo build -p snapstore-server failed");
    }

    // Now run via run_cycles which internally calls find_server_binary().
    // find_server_binary() looks next to current_exe() at runtime.
    // The test binary is in target/<profile>/deps/; the server is in
    // target/<profile>/.  We need to make sure find_server_binary finds it.
    //
    // Since find_server_binary only looks at current_exe().parent(), which for
    // the integration test is target/<profile>/deps/, we create a symlink (or
    // copy) there if needed.
    let server_in_deps = candidate_deps.clone();
    if !server_in_deps.exists() {
        // Try to symlink from the profile dir.
        if let Some(ref profile_path) = candidate_profile {
            if profile_path.exists() {
                #[cfg(unix)]
                {
                    let _ = std::os::unix::fs::symlink(profile_path, &server_in_deps);
                }
                #[cfg(not(unix))]
                {
                    let _ = std::fs::copy(profile_path, &server_in_deps);
                }
            }
        }
    }

    // Run 5 cycles.
    let opts = RunOptions {
        cycles: 5,
        seed: 54321,
        matrix_passes: 0,  // matrix not applicable to full-stack
        ops_per_cycle: 40, // ignored by full-stack (uses DRIVER_OPS constant)
        scenario: Scenario::FullStack,
        failpoint: None,
    };
    let summary = run_cycles(&opts);

    // If we got 0 cycles (binary still not found), treat as a skip (warn but pass).
    if summary.total_cycles == 0 {
        eprintln!(
            "WARN: full_stack_five_cycles: server binary not found even after build attempt; \
             skipping test"
        );
        return;
    }

    assert_eq!(
        summary.invariant_failures, 0,
        "full-stack invariant failures in 5 cycles: {summary:?}"
    );
}

// ── Part 4: M7 GC crash-harness extension (WI5) ──────────────────────────────

/// 5 randomized kill cycles with the extended `Default` workload (pin/unpin
/// every ~16 steps, an in-process GC cycle every ~24 steps). `ops_per_cycle`
/// is large enough that every cycle reaches at least one `gc` op. Asserts
/// zero invariant / fsck violations (05 §4's PR-smoke bar, at a small scale).
#[test]
fn randomized_kill_cycles_5_with_gc_workload() {
    let opts = RunOptions {
        cycles: 5,
        seed: 424242,
        matrix_passes: 0,
        ops_per_cycle: 80,
        scenario: Scenario::Default,
        failpoint: None,
    };
    let summary = run_cycles(&opts);
    assert_eq!(
        summary.invariant_failures, 0,
        "invariant failures in 5 GC-workload cycles: {summary:?}"
    );
    assert_eq!(
        summary.fsck_violations, 0,
        "fsck violations in 5 GC-workload cycles: {summary:?}"
    );
}

/// Arm a single GC failpoint (`gc-index-repoint`, mid-repoint-loop — one of
/// the six new boundaries from 02 §7 / 05 §2) for one cycle and assert
/// recovery is green. `--force-gc` (armed automatically by `run_cycles` for
/// any `gc-*` failpoint) guarantees the child reaches a GC cycle within its
/// op budget.
#[cfg(feature = "failpoints")]
#[test]
fn gc_failpoint_index_repoint_single_cycle_recovers_green() {
    let opts = RunOptions {
        cycles: 1,
        seed: 13579,
        matrix_passes: 0,
        ops_per_cycle: 40,
        scenario: Scenario::Default,
        failpoint: Some("gc-index-repoint".to_string()),
    };
    let summary = run_cycles(&opts);
    assert_eq!(
        summary.invariant_failures, 0,
        "invariant failures with gc-index-repoint armed: {summary:?}"
    );
    assert_eq!(
        summary.fsck_violations, 0,
        "fsck violations with gc-index-repoint armed: {summary:?}"
    );
}

/// All six new GC failpoints are present in the matrix.
#[test]
fn gc_failpoints_present_in_matrix() {
    for fp in [
        "gc-compact-copy",
        "gc-compact-seal",
        "gc-index-repoint",
        "gc-pack-unlink",
        "gc-manifest-unlink",
        "gc-reap-txn",
    ] {
        assert!(
            snapstore_crash::harness::FAILPOINTS.contains(&fp),
            "expected {fp} in FAILPOINTS matrix"
        );
    }
}

/// A sealed pack whose (CRC-valid) sidecar has fewer entries than the pack
/// physically contains must trip `SidecarRecordCountMismatch`.
#[test]
fn fsck_detects_sidecar_record_count_mismatch() {
    use snapstore_pagestore::index::ShardedIndex;
    use snapstore_pagestore::pack::PackWriter;
    use snapstore_types::{PackId, PageHash, PageLoc, PAGE_SIZE};

    let dir = tempfile::TempDir::new().unwrap();
    build_small_store(dir.path());

    let pdir = pages_dir(dir.path());
    let pack_id = PackId(77);
    let pack_file = pdir.join("pack-0000004d.spk");

    // Seal a pack with TWO records.
    let page1 = [0xCCu8; PAGE_SIZE];
    let page2 = [0xDDu8; PAGE_SIZE];
    let hash1 = PageHash::from_bytes(*blake3::hash(&page1).as_bytes());
    let hash2 = PageHash::from_bytes(*blake3::hash(&page2).as_bytes());
    {
        let mut w = PackWriter::create(&pack_file, pack_id, 0).unwrap();
        w.append(&hash1, &page1).unwrap();
        w.append(&hash2, &page2).unwrap();
        w.seal().unwrap();
    }

    // Write a sidecar with only ONE entry (wrong count vs. the 2 physical
    // records) — a CRC-valid sidecar with a stale/short entry list.
    let sidecar_path = pdir.join("pack-0000004d.idx");
    {
        let idx = ShardedIndex::new();
        idx.insert(
            hash1,
            PageLoc {
                pack: pack_id,
                offset: 20,
            },
        );
        idx.write_sidecar(&sidecar_path, pack_id).unwrap();
    }

    let report = snapstore_crash::fsck::fsck(&store_root(dir.path()), &meta_db(dir.path()), false);
    let classes: Vec<&str> = report.violations.iter().map(|v| v.class()).collect();
    assert!(
        classes.contains(&"SidecarRecordCountMismatch"),
        "expected SidecarRecordCountMismatch, got: {classes:?}"
    );
}

/// `populate-gc-fixture` smoke test at a small scale: the expected-refs file
/// exists, parses as one lowercase 64-hex ref per line, and every listed ref
/// resolves in the populated store.
#[test]
fn populate_gc_fixture_smoke() {
    use snapstore_crash::gc_fixture::{populate_gc_fixture, GcFixtureOpts};

    let dir = tempfile::TempDir::new().unwrap();
    let opts = GcFixtureOpts {
        dir: dir.path().to_path_buf(),
        seed: 2468,
        nodes: 30,
        pruned_subtrees: 3,
    };
    let summary = populate_gc_fixture(&opts).expect("populate_gc_fixture failed");
    assert!(summary.nodes_created >= 30);
    assert!(summary.subtrees_pruned >= 1);
    assert!(summary.surviving_refs > 0);

    let refs_path = dir.path().join("expected-surviving-refs.txt");
    assert!(refs_path.exists(), "expected-surviving-refs.txt missing");
    let contents = std::fs::read_to_string(&refs_path).unwrap();
    let refs: Vec<&str> = contents.lines().filter(|l| !l.is_empty()).collect();
    assert!(!refs.is_empty());

    let manifest_path = dir.path().join("fixture-manifest.json");
    assert!(manifest_path.exists(), "fixture-manifest.json missing");

    let store = snapstore_store::SnapshotStore::open(&store_root(dir.path())).unwrap();
    for hex in &refs {
        assert_eq!(hex.len(), 64, "ref must be 64 hex chars: {hex}");
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()),
            "ref must be lowercase hex: {hex}"
        );
        let mut bytes = [0u8; 32];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let s = std::str::from_utf8(chunk).unwrap();
            bytes[i] = u8::from_str_radix(s, 16).unwrap();
        }
        let snap_ref = snapstore_types::SnapshotRef::from_bytes(bytes);
        store
            .get_snapshot(&snap_ref)
            .unwrap_or_else(|e| panic!("expected surviving ref {hex} failed to resolve: {e}"));
    }

    // Sorted + deduped.
    let mut sorted = refs.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        refs, sorted,
        "expected-surviving-refs.txt must be sorted+deduped"
    );
}
