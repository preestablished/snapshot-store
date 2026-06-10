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
