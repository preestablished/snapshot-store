// ── fsck — offline deep store integrity checker ──────────────────────────────
//!
//! Walks the store directory and the meta SQLite DB entirely outside the normal
//! API layer so it works on a stopped or crashed store.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};

use crc32fast::Hasher as Crc32Hasher;
use serde::{Deserialize, Serialize};

use crate::PAGE_SIZE;

// ── Pack / sidecar format constants ──────────────────────────────────────────

const PACK_MAGIC: &[u8; 4] = b"SPK1";
const FOOTER_MAGIC: &[u8; 4] = b"SPKF";
const PACK_HEADER_SIZE: u64 = 20;
const RECORD_HEADER_SIZE: u64 = 37; // hash(32) + flags(1) + len(4)
const PACK_FOOTER_SIZE: u64 = 44; // magic(4) + record_count(8) + body_blake3(32)

const SIDECAR_ENTRY_SIZE: usize = 44; // hash(32) + pack_id(4) + offset(8)

// ── Manifest format constants ─────────────────────────────────────────────────

const SPM_MAGIC: &[u8; 8] = b"SPSMAN01";
const SPM_FOOTER_LEN: usize = 32;
const SPM_HEADER_LEN: usize = 96;
const SPM_ENTRY_SIZE: usize = 40; // page_index(8) + hash(32)

// ── Violation types ───────────────────────────────────────────────────────────

/// A single integrity violation found by [`fsck`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "class", content = "detail")]
pub enum Violation {
    /// A node row references a snapshot_ref whose `.spm` file does not exist.
    MissingManifest {
        node_key: String,
        snap_ref_hex: String,
    },
    /// A pinned snapshot_ref does not exist as a `.spm` file.
    DanglingPin { snap_ref_hex: String },
    /// A manifest entry references a page_hash not found in any pack/index.
    MissingPage {
        snap_ref_hex: String,
        page_index: u64,
        page_hash_hex: String,
    },
    /// A sidecar `.idx` file has a bad CRC32 trailer.
    BadSidecarCrc { path: String },
    /// A `.spm` manifest file has a BLAKE3 footer that does not match its body.
    BadManifestFooter { path: String },
    /// A sealed pack has an SPKF footer whose `record_count` does not match the
    /// number of records in the sidecar for that pack.
    BadPackFooter {
        pack_path: String,
        footer_count: u64,
        sidecar_count: u64,
    },
    /// A sealed pack has a (CRC-valid) sidecar present, but the sidecar's
    /// entry count does not match the number of records physically scanned
    /// from the pack body. Catches the empty-sidecar failure mode where
    /// `load_sidecar` succeeds with 0 entries and the rebuild-from-scan
    /// fallback never fires because the sidecar "loaded fine" (01 §2
    /// `GcPackWriter` caution). A sealed pack with a MISSING sidecar is fine
    /// — `open()` rebuilds it — so this only fires when a sidecar exists.
    SidecarRecordCountMismatch {
        pack_path: String,
        sidecar_count: u64,
        pack_record_count: u64,
    },
    /// A node row references an `input_log_id` that has no row in `input_logs`.
    MissingInputLog {
        node_key: String,
        log_id_hex: String,
    },
    /// A node row references a snapshot_ref whose manifest cannot be decoded
    /// or is otherwise structurally invalid.
    BadManifestDecode { snap_ref_hex: String, error: String },
    /// (deep) BLAKE3(payload) != the page_hash stored in the pack record.
    RecordHashMismatch {
        pack_path: String,
        offset: u64,
        stored_hex: String,
        computed_hex: String,
    },
    /// (deep) Recomputed body BLAKE3 of a sealed pack != its SPKF footer.
    BadPackBodyHash { pack_path: String },
    /// (deep) An input-log container's BLAKE3 footer does not match.
    BadInputLogFooter { log_id_hex: String },
}

impl Violation {
    /// Short class name.
    pub fn class(&self) -> &'static str {
        match self {
            Violation::MissingManifest { .. } => "MissingManifest",
            Violation::DanglingPin { .. } => "DanglingPin",
            Violation::MissingPage { .. } => "MissingPage",
            Violation::BadSidecarCrc { .. } => "BadSidecarCrc",
            Violation::BadManifestFooter { .. } => "BadManifestFooter",
            Violation::BadPackFooter { .. } => "BadPackFooter",
            Violation::SidecarRecordCountMismatch { .. } => "SidecarRecordCountMismatch",
            Violation::MissingInputLog { .. } => "MissingInputLog",
            Violation::BadManifestDecode { .. } => "BadManifestDecode",
            Violation::RecordHashMismatch { .. } => "RecordHashMismatch",
            Violation::BadPackBodyHash { .. } => "BadPackBodyHash",
            Violation::BadInputLogFooter { .. } => "BadInputLogFooter",
        }
    }
}

// ── FsckCounts ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FsckCounts {
    pub packs_scanned: u64,
    pub pack_records_deep_checked: u64,
    pub sidecars_scanned: u64,
    pub manifests_checked: u64,
    pub nodes_checked: u64,
    pub pins_checked: u64,
    pub input_logs_deep_checked: u64,
}

// ── FsckReport ────────────────────────────────────────────────────────────────

/// Machine-readable report produced by [`fsck`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsckReport {
    pub violations: Vec<Violation>,
    pub counts: FsckCounts,
    pub deep: bool,
}

impl FsckReport {
    pub fn ok(&self) -> bool {
        self.violations.is_empty()
    }
}

// ── Internal types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
struct PageLoc {
    _pack_id: u32,
    _offset: u64,
}

// ── fsck entry point ─────────────────────────────────────────────────────────

/// Run an offline integrity check on the store rooted at `store_root`.
///
/// Directory layout expected:
/// - `<store_root>/pages/` — PageStore root (pack files + sidecars).
/// - `<store_root>/manifests/<2-hex>/<64-hex>.spm`
/// - `<store_root>/tmp/` (ignored)
///
/// `meta_db_path` — path to the SQLite database (e.g. `<store_root>/../meta/tree.db`).
///
/// Pass `deep = true` to re-hash every page payload and every sealed pack body.
pub fn fsck(store_root: &Path, meta_db_path: &Path, deep: bool) -> FsckReport {
    let mut v: Vec<Violation> = Vec::new();
    let mut counts = FsckCounts::default();

    // ── Step 1: build page index + sidecar entry counts ──────────────────────

    let pages_dir = store_root.join("pages");
    // page_hash → location
    let mut page_index: HashMap<[u8; 32], PageLoc> = HashMap::new();
    // pack_id → number of entries known from sidecar (for footer vs sidecar check)
    let mut sidecar_counts: HashMap<u32, u64> = HashMap::new();

    let pack_files = collect_pack_files(&pages_dir);

    for (pack_id, pack_path) in &pack_files {
        counts.packs_scanned += 1;
        let sealed = is_pack_sealed(pack_path);
        let sidecar = sidecar_path_for(&pages_dir, *pack_id);

        if sealed {
            match load_sidecar_raw(&sidecar) {
                Ok(entries) => {
                    let sidecar_count = entries.len() as u64;
                    sidecar_counts.insert(*pack_id, sidecar_count);
                    for (hash, loc) in entries {
                        page_index.entry(hash).or_insert(loc);
                    }
                    counts.sidecars_scanned += 1;

                    // Sidecar-integrity check: a sealed pack WITH a sidecar
                    // present must have sidecar entry count == pack record
                    // count (scanned from the pack body, independent of the
                    // SPKF footer's own claimed count).
                    if let Ok(records) = scan_pack_headers(pack_path) {
                        let pack_record_count = records.len() as u64;
                        if pack_record_count != sidecar_count {
                            v.push(Violation::SidecarRecordCountMismatch {
                                pack_path: pack_path.to_string_lossy().into_owned(),
                                sidecar_count,
                                pack_record_count,
                            });
                        }
                    }
                }
                Err(bad_crc) => {
                    if bad_crc && sidecar.exists() {
                        v.push(Violation::BadSidecarCrc {
                            path: sidecar.to_string_lossy().into_owned(),
                        });
                    }
                    // Rebuild from pack so page presence checks still work.
                    if let Ok(records) = scan_pack_headers(pack_path) {
                        let n = records.len() as u64;
                        sidecar_counts.insert(*pack_id, n);
                        for (hash, offset) in records {
                            page_index.entry(hash).or_insert(PageLoc {
                                _pack_id: *pack_id,
                                _offset: offset,
                            });
                        }
                    }
                }
            }
        } else {
            // Unsealed pack: scan without footer.
            if let Ok(records) = scan_pack_headers(pack_path) {
                for (hash, offset) in records {
                    page_index.entry(hash).or_insert(PageLoc {
                        _pack_id: *pack_id,
                        _offset: offset,
                    });
                }
            }
        }
    }

    // ── Step 2: verify SPKF footer record counts match sidecar counts ─────────

    for (pack_id, pack_path) in &pack_files {
        if !is_pack_sealed(pack_path) {
            continue;
        }
        if let Ok(footer_count) = read_pack_footer_count(pack_path) {
            let sidecar_count = sidecar_counts.get(pack_id).copied().unwrap_or(0);
            if footer_count != sidecar_count {
                v.push(Violation::BadPackFooter {
                    pack_path: pack_path.to_string_lossy().into_owned(),
                    footer_count,
                    sidecar_count,
                });
            }
        }
    }

    // ── Step 3: scan manifests directory + verify .spm footers ───────────────

    let manifests_dir = store_root.join("manifests");
    // snap_ref bytes → (path, page entries vec)
    let mut manifest_entries: HashMap<[u8; 32], Vec<([u8; 32], u64)>> = HashMap::new();
    // snap_ref bytes → path (for DB cross-reference)
    let mut manifest_paths: HashMap<[u8; 32], PathBuf> = HashMap::new();

    if let Ok(shard_iter) = fs::read_dir(&manifests_dir) {
        for shard_e in shard_iter.flatten() {
            if !shard_e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            if let Ok(file_iter) = fs::read_dir(shard_e.path()) {
                for fe in file_iter.flatten() {
                    let p = fe.path();
                    let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    if let Some(hex) = name.strip_suffix(".spm") {
                        if hex.len() == 64 {
                            if let Ok(bytes) = hex_to_32(hex) {
                                counts.manifests_checked += 1;
                                match verify_spm_and_extract(&p, &bytes) {
                                    Ok(entries) => {
                                        manifest_entries.insert(bytes, entries);
                                        manifest_paths.insert(bytes, p);
                                    }
                                    Err(e) => {
                                        v.push(e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // ── Step 4: verify every manifest entry's page_hash is in the index ──────

    for (&snap_ref_bytes, entries) in &manifest_entries {
        let snap_ref_hex = hex_from_32(&snap_ref_bytes);
        for &(page_hash, page_index_val) in entries {
            if !page_index.contains_key(&page_hash) {
                v.push(Violation::MissingPage {
                    snap_ref_hex: snap_ref_hex.clone(),
                    page_index: page_index_val,
                    page_hash_hex: hex_from_32(&page_hash),
                });
            }
        }
    }

    // ── Step 5: meta DB shallow checks ───────────────────────────────────────

    if let Ok(db) = open_meta_db_readonly(meta_db_path) {
        // Collect known log_ids.
        let known_log_ids = collect_log_ids(&db);

        // Check node rows.
        check_nodes(&db, &manifest_paths, &known_log_ids, &mut v, &mut counts);

        // Check pinned refs.
        check_pins(&db, &manifest_paths, &mut v, &mut counts);

        // ── Step 6 (deep): re-hash payloads, pack bodies, input logs ─────────

        if deep {
            for (_pack_id, pack_path) in &pack_files {
                deep_check_pack_records(pack_path, &mut v, &mut counts);
                if is_pack_sealed(pack_path) {
                    deep_check_pack_body_hash(pack_path, &mut v);
                }
            }
            deep_check_input_logs(&db, &mut v, &mut counts);
        }
    }

    FsckReport {
        violations: v,
        counts,
        deep,
    }
}

// ── Manifest verification ─────────────────────────────────────────────────────

/// Read a `.spm` file, verify its BLAKE3 footer matches its filename, and
/// extract page entries.  Returns `Err(Violation)` on any problem.
fn verify_spm_and_extract(
    path: &Path,
    expected_ref: &[u8; 32],
) -> Result<Vec<([u8; 32], u64)>, Violation> {
    let data = fs::read(path).map_err(|e| Violation::BadManifestDecode {
        snap_ref_hex: hex_from_32(expected_ref),
        error: e.to_string(),
    })?;

    if data.len() < SPM_FOOTER_LEN {
        return Err(Violation::BadManifestFooter {
            path: path.to_string_lossy().into_owned(),
        });
    }

    let body = &data[..data.len() - SPM_FOOTER_LEN];
    let stored_footer: [u8; 32] = data[data.len() - SPM_FOOTER_LEN..].try_into().unwrap();
    let computed = *blake3::hash(body).as_bytes();

    if computed != stored_footer || computed != *expected_ref {
        return Err(Violation::BadManifestFooter {
            path: path.to_string_lossy().into_owned(),
        });
    }

    // Quick magic check.
    if data.len() < 8 || &data[0..8] != SPM_MAGIC {
        return Err(Violation::BadManifestDecode {
            snap_ref_hex: hex_from_32(expected_ref),
            error: "bad magic".to_string(),
        });
    }

    // Extract entry table.
    if data.len() < SPM_HEADER_LEN {
        return Err(Violation::BadManifestDecode {
            snap_ref_hex: hex_from_32(expected_ref),
            error: "header truncated".to_string(),
        });
    }
    // Header layout (offsets):
    // 0..8=magic, 8..10=version, 10..12=flags, 12..16=header_len(u32),
    // 16..48=parent_hash, 48..56=guest_ram_bytes, 56..64=page_size,
    // 64..72=entry_count, 72..80=device_blob_len, ...
    let header_len = u32::from_le_bytes(data[12..16].try_into().unwrap()) as usize;
    if header_len != SPM_HEADER_LEN {
        return Err(Violation::BadManifestDecode {
            snap_ref_hex: hex_from_32(expected_ref),
            error: format!("unexpected header_len {header_len}"),
        });
    }
    let entry_count = u64::from_le_bytes(data[64..72].try_into().unwrap()) as usize;
    let table_end = SPM_HEADER_LEN + entry_count * SPM_ENTRY_SIZE;
    if table_end > data.len() - SPM_FOOTER_LEN {
        return Err(Violation::BadManifestDecode {
            snap_ref_hex: hex_from_32(expected_ref),
            error: "entry table truncated".to_string(),
        });
    }

    let mut entries = Vec::with_capacity(entry_count);
    for i in 0..entry_count {
        let base = SPM_HEADER_LEN + i * SPM_ENTRY_SIZE;
        let page_index = u64::from_le_bytes(data[base..base + 8].try_into().unwrap());
        let hash: [u8; 32] = data[base + 8..base + 40].try_into().unwrap();
        entries.push((hash, page_index));
    }

    Ok(entries)
}

// ── Meta DB helpers ───────────────────────────────────────────────────────────

fn open_meta_db_readonly(path: &Path) -> Result<rusqlite::Connection, rusqlite::Error> {
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = rusqlite::Connection::open_with_flags(path, flags)?;
    conn.execute_batch("PRAGMA query_only=ON; PRAGMA busy_timeout=3000;")?;
    Ok(conn)
}

fn collect_log_ids(db: &rusqlite::Connection) -> HashSet<[u8; 32]> {
    let mut out = HashSet::new();
    let Ok(mut stmt) = db.prepare("SELECT log_id FROM input_logs") else {
        return out;
    };
    let _ = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map(|rows| {
            for r in rows.flatten() {
                if r.len() == 32 {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&r);
                    out.insert(arr);
                }
            }
        });
    out
}

fn check_nodes(
    db: &rusqlite::Connection,
    manifests: &HashMap<[u8; 32], PathBuf>,
    known_log_ids: &HashSet<[u8; 32]>,
    v: &mut Vec<Violation>,
    counts: &mut FsckCounts,
) {
    let Ok(mut stmt) =
        db.prepare("SELECT experiment_id, node_id, snapshot_ref, input_log_id FROM nodes")
    else {
        return;
    };
    let _ = stmt
        .query_map([], |row| {
            let exp: String = row.get(0)?;
            let nid: i64 = row.get(1)?;
            let snap: Vec<u8> = row.get(2)?;
            let log: Option<Vec<u8>> = row.get(3)?;
            Ok((exp, nid, snap, log))
        })
        .map(|rows| {
            for r in rows.flatten() {
                let (exp, nid, snap, log) = r;
                counts.nodes_checked += 1;
                let node_key = format!("{exp}/{nid}");

                if snap.len() == 32 {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&snap);
                    if !manifests.contains_key(&arr) {
                        v.push(Violation::MissingManifest {
                            node_key: node_key.clone(),
                            snap_ref_hex: hex_from_32(&arr),
                        });
                    }
                }

                if let Some(lb) = log {
                    if lb.len() == 32 {
                        let mut arr = [0u8; 32];
                        arr.copy_from_slice(&lb);
                        if !known_log_ids.contains(&arr) {
                            v.push(Violation::MissingInputLog {
                                node_key,
                                log_id_hex: hex_from_32(&arr),
                            });
                        }
                    }
                }
            }
        });
}

fn check_pins(
    db: &rusqlite::Connection,
    manifests: &HashMap<[u8; 32], PathBuf>,
    v: &mut Vec<Violation>,
    counts: &mut FsckCounts,
) {
    let Ok(mut stmt) = db.prepare("SELECT snapshot_ref FROM pins") else {
        return;
    };
    let _ = stmt
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map(|rows| {
            for r in rows.flatten() {
                counts.pins_checked += 1;
                if r.len() == 32 {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&r);
                    if !manifests.contains_key(&arr) {
                        v.push(Violation::DanglingPin {
                            snap_ref_hex: hex_from_32(&arr),
                        });
                    }
                }
            }
        });
}

// ── Deep checks ───────────────────────────────────────────────────────────────

fn deep_check_pack_records(pack_path: &Path, v: &mut Vec<Violation>, counts: &mut FsckCounts) {
    use std::os::unix::fs::FileExt;

    let Ok(file) = fs::File::open(pack_path) else {
        return;
    };
    let Ok(meta) = file.metadata() else { return };
    let file_len = meta.len();

    let sealed = is_pack_sealed(pack_path);
    let scan_end = if sealed && file_len >= PACK_FOOTER_SIZE {
        file_len - PACK_FOOTER_SIZE
    } else {
        file_len
    };

    let mut offset = PACK_HEADER_SIZE;
    while offset < scan_end {
        let remaining = scan_end - offset;
        if remaining < RECORD_HEADER_SIZE {
            break;
        }

        let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
        if file.read_exact_at(&mut rec_header, offset).is_err() {
            break;
        }

        let stored_hash: [u8; 32] = rec_header[0..32].try_into().unwrap();
        let len = u32::from_le_bytes(rec_header[33..37].try_into().unwrap());

        if len as usize > PAGE_SIZE * 2 {
            break;
        }
        if offset + RECORD_HEADER_SIZE + len as u64 > scan_end {
            break;
        }

        let mut payload = vec![0u8; len as usize];
        if file
            .read_exact_at(&mut payload, offset + RECORD_HEADER_SIZE)
            .is_err()
        {
            break;
        }

        counts.pack_records_deep_checked += 1;

        let computed = *blake3::hash(&payload).as_bytes();
        if computed != stored_hash {
            v.push(Violation::RecordHashMismatch {
                pack_path: pack_path.to_string_lossy().into_owned(),
                offset,
                stored_hex: hex_from_32(&stored_hash),
                computed_hex: hex_from_32(&computed),
            });
        }

        offset += RECORD_HEADER_SIZE + len as u64;
    }
}

fn deep_check_pack_body_hash(pack_path: &Path, v: &mut Vec<Violation>) {
    let Ok(data) = fs::read(pack_path) else {
        return;
    };
    if data.len() < (PACK_HEADER_SIZE + PACK_FOOTER_SIZE) as usize {
        return;
    }
    let body_end = data.len() - PACK_FOOTER_SIZE as usize;
    let footer = &data[body_end..];

    if &footer[0..4] != FOOTER_MAGIC {
        return;
    }
    let stored_body_hash: [u8; 32] = footer[12..44].try_into().unwrap();

    // The pack writer feeds individual record bytes (header+payload) into the
    // body hasher — which is equivalent to hashing the entire body bytes since
    // records are laid out contiguously.  So hash data[PACK_HEADER_SIZE..body_end].
    let body_slice = &data[PACK_HEADER_SIZE as usize..body_end];
    let computed = *blake3::hash(body_slice).as_bytes();

    if computed != stored_body_hash {
        v.push(Violation::BadPackBodyHash {
            pack_path: pack_path.to_string_lossy().into_owned(),
        });
    }
}

fn deep_check_input_logs(
    db: &rusqlite::Connection,
    v: &mut Vec<Violation>,
    counts: &mut FsckCounts,
) {
    let Ok(mut stmt) = db.prepare("SELECT log_id, content FROM input_logs") else {
        return;
    };
    let _ = stmt
        .query_map([], |row| {
            let log_id: Vec<u8> = row.get(0)?;
            let content: Vec<u8> = row.get(1)?;
            Ok((log_id, content))
        })
        .map(|rows| {
            for r in rows.flatten() {
                let (log_id_blob, content) = r;
                counts.input_logs_deep_checked += 1;
                if log_id_blob.len() != 32 || content.len() < 32 {
                    continue;
                }
                // log_id = blake3(content[..len-32])
                let body = &content[..content.len() - 32];
                let computed = *blake3::hash(body).as_bytes();
                let mut stored = [0u8; 32];
                stored.copy_from_slice(&log_id_blob);
                if computed != stored {
                    v.push(Violation::BadInputLogFooter {
                        log_id_hex: hex_from_32(&stored),
                    });
                }
            }
        });
}

// ── Filesystem helpers ────────────────────────────────────────────────────────

fn collect_pack_files(dir: &Path) -> Vec<(u32, PathBuf)> {
    let mut out = Vec::new();
    let Ok(rd) = fs::read_dir(dir) else {
        return out;
    };
    for e in rd.flatten() {
        let p = e.path();
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if let Some(hex) = name
            .strip_prefix("pack-")
            .and_then(|s| s.strip_suffix(".spk"))
        {
            if hex.len() == 8 {
                if let Ok(id) = u32::from_str_radix(hex, 16) {
                    out.push((id, p));
                }
            }
        }
    }
    out.sort_by_key(|(id, _)| *id);
    out
}

fn sidecar_path_for(dir: &Path, pack_id: u32) -> PathBuf {
    dir.join(format!("pack-{pack_id:08x}.idx"))
}

fn is_pack_sealed(path: &Path) -> bool {
    let Ok(mut f) = fs::File::open(path) else {
        return false;
    };
    let len = f.seek(std::io::SeekFrom::End(0)).unwrap_or(0);
    if len < PACK_HEADER_SIZE + PACK_FOOTER_SIZE {
        return false;
    }
    f.seek(std::io::SeekFrom::End(-(PACK_FOOTER_SIZE as i64)))
        .ok();
    let mut magic = [0u8; 4];
    f.read_exact(&mut magic).ok();
    &magic == FOOTER_MAGIC
}

fn read_pack_footer_count(path: &Path) -> Result<u64, ()> {
    let mut f = fs::File::open(path).map_err(|_| ())?;
    f.seek(std::io::SeekFrom::End(-(PACK_FOOTER_SIZE as i64)))
        .map_err(|_| ())?;
    let mut footer = [0u8; PACK_FOOTER_SIZE as usize];
    f.read_exact(&mut footer).map_err(|_| ())?;
    if &footer[0..4] != FOOTER_MAGIC {
        return Err(());
    }
    Ok(u64::from_le_bytes(footer[4..12].try_into().unwrap()))
}

/// Load a sidecar. Returns Err(true) on bad CRC, Err(false) on missing/IO.
fn load_sidecar_raw(path: &Path) -> Result<Vec<([u8; 32], PageLoc)>, bool> {
    let data = fs::read(path).map_err(|_| false)?;
    if data.len() < 8 {
        return Err(true);
    }
    let (payload, crc_bytes) = data.split_at(data.len() - 4);
    let stored_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
    let mut h = Crc32Hasher::new();
    h.update(payload);
    if h.finalize() != stored_crc {
        return Err(true);
    }
    if payload.len() < 4 {
        return Err(true);
    }
    let entry_count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    let entries_bytes = &payload[4..];
    if entries_bytes.len() < entry_count * SIDECAR_ENTRY_SIZE {
        return Err(true);
    }
    let mut out = Vec::with_capacity(entry_count);
    for i in 0..entry_count {
        let base = i * SIDECAR_ENTRY_SIZE;
        let hash: [u8; 32] = entries_bytes[base..base + 32].try_into().unwrap();
        let pack_id = u32::from_le_bytes(entries_bytes[base + 32..base + 36].try_into().unwrap());
        let offset = u64::from_le_bytes(entries_bytes[base + 36..base + 44].try_into().unwrap());
        out.push((
            hash,
            PageLoc {
                _pack_id: pack_id,
                _offset: offset,
            },
        ));
    }
    Ok(out)
}

/// Scan a pack file and return (hash, offset) for every complete record header
/// (without verifying payloads).
fn scan_pack_headers(path: &Path) -> Result<Vec<([u8; 32], u64)>, ()> {
    use std::os::unix::fs::FileExt;

    let file = fs::File::open(path).map_err(|_| ())?;
    let file_len = file.metadata().map_err(|_| ())?.len();

    let mut magic = [0u8; 4];
    file.read_exact_at(&mut magic, 0).map_err(|_| ())?;
    if &magic != PACK_MAGIC {
        return Err(());
    }

    let sealed = is_pack_sealed(path);
    let scan_end = if sealed && file_len >= PACK_FOOTER_SIZE {
        file_len - PACK_FOOTER_SIZE
    } else {
        file_len
    };

    let mut offset = PACK_HEADER_SIZE;
    let mut out = Vec::new();

    while offset < scan_end {
        if scan_end - offset < RECORD_HEADER_SIZE {
            break;
        }
        let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
        if file.read_exact_at(&mut rec_header, offset).is_err() {
            break;
        }
        let hash: [u8; 32] = rec_header[0..32].try_into().unwrap();
        let len = u32::from_le_bytes(rec_header[33..37].try_into().unwrap());
        if len as usize > PAGE_SIZE * 2 {
            break;
        }
        if offset + RECORD_HEADER_SIZE + len as u64 > scan_end {
            break;
        }
        out.push((hash, offset));
        offset += RECORD_HEADER_SIZE + len as u64;
    }

    Ok(out)
}

// ── Hex helpers ───────────────────────────────────────────────────────────────

pub fn hex_from_32(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn hex_to_32(hex: &str) -> Result<[u8; 32], ()> {
    if hex.len() != 64 {
        return Err(());
    }
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = nibble(chunk[0]).ok_or(())?;
        let lo = nibble(chunk[1]).ok_or(())?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
