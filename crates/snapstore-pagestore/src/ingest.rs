use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use rayon::prelude::*;
use snapstore_types::{PageHash, PackId, PageLoc, PAGE_SIZE};

use crate::index::{rebuild_from_pack, IndexError, ShardedIndex};
use crate::pack::{PackError, PackReader, PackWriter, PACK_FOOTER_SIZE, PACK_HEADER_SIZE, RECORD_HEADER_SIZE};

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("pack error: {0}")]
    Pack(#[from] PackError),
    #[error("index error: {0}")]
    Index(#[from] IndexError),
}

// ── StoreOptions ──────────────────────────────────────────────────────────────

pub struct StoreOptions {
    /// Flush write buffer when it reaches this size (bytes). Default: 4 MiB.
    pub write_buf_size: usize,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            write_buf_size: 4 * 1024 * 1024,
        }
    }
}

// ── IngestOutcome ─────────────────────────────────────────────────────────────

pub struct IngestOutcome {
    pub hash: PageHash,
    pub loc: PageLoc,
    /// false = dedup hit (page already existed in the store)
    pub newly_written: bool,
}

// ── ActiveState ───────────────────────────────────────────────────────────────

struct ActiveState {
    writer: PackWriter,
    pack_id: PackId,
    /// Pack IDs that received writes since the last sync() call.
    dirty_since_sync: HashSet<PackId>,
    /// True if new pack files were created since the last sync() call.
    new_files_since_sync: bool,
}

// ── PageStore ─────────────────────────────────────────────────────────────────

pub struct PageStore {
    dir: PathBuf,
    index: Arc<ShardedIndex>,
    active: Mutex<ActiveState>,
    opts: StoreOptions,
}

impl PageStore {
    /// Open (or create) a store at `dir`.
    ///
    /// Recovery logic:
    /// 1. Discover all pack files (`pack-{:08x}.spk`).
    /// 2. For each pack that lacks a valid footer ("unsealed"), except the
    ///    highest-numbered: seal it (verify records, truncate bad tail, write footer).
    /// 3. The highest-numbered unsealed pack (if any) becomes the active pack for
    ///    continued appending.
    /// 4. For every sealed pack, load its sidecar index (.idx).  If missing or
    ///    corrupt, rebuild from the pack and regenerate the sidecar.
    /// 5. If there are no unsealed packs, create a fresh pack.
    pub fn open(dir: &Path, opts: StoreOptions) -> Result<Self, StoreError> {
        std::fs::create_dir_all(dir)?;

        // 1. Find all pack files, sorted ascending by PackId.
        let mut pack_ids = discover_packs(dir)?;

        let index = Arc::new(ShardedIndex::new());

        // Separate sealed from unsealed packs.
        let mut sealed_packs: Vec<PackId> = Vec::new();
        let mut active_pack_id: Option<PackId> = None;

        // We need to process unsealed packs carefully:
        // - The highest-numbered unsealed pack becomes the active pack.
        // - All others are sealed immediately.
        // First pass: identify which packs are sealed vs unsealed.
        let mut unsealed_packs: Vec<PackId> = Vec::new();
        for &pack_id in &pack_ids {
            let path = pack_path(dir, pack_id);
            if is_pack_sealed(&path) {
                sealed_packs.push(pack_id);
            } else {
                unsealed_packs.push(pack_id);
            }
        }

        // Sort so we can identify the highest-numbered unsealed pack.
        unsealed_packs.sort();

        if !unsealed_packs.is_empty() {
            // All unsealed packs except the last one need to be sealed.
            let last_idx = unsealed_packs.len() - 1;
            for (i, &pack_id) in unsealed_packs.iter().enumerate() {
                let path = pack_path(dir, pack_id);
                if i < last_idx {
                    // Seal this old unseal pack and populate the index.
                    let entries = seal_existing_pack(&path, pack_id)?;
                    index.insert_batch(
                        entries
                            .into_iter()
                            .map(|(h, o)| (h, PageLoc { pack: pack_id, offset: o })),
                    );
                    let sidecar = sidecar_path(dir, pack_id);
                    index.write_sidecar(&sidecar, pack_id)?;
                    sealed_packs.push(pack_id);
                } else {
                    // Highest-numbered unsealed: this will be the active pack.
                    active_pack_id = Some(pack_id);
                }
            }
        }

        // Load sealed packs from sidecars (or rebuild if missing/corrupt).
        for &pack_id in &sealed_packs {
            let p = pack_path(dir, pack_id);
            let sidecar = sidecar_path(dir, pack_id);
            let loaded = index.load_sidecar(&sidecar);
            if loaded.is_err() {
                // Missing or corrupt sidecar: rebuild from the pack.
                let entries = rebuild_from_pack(&p, pack_id, true)?;
                index.insert_batch(entries.into_iter());
                index.write_sidecar(&sidecar, pack_id)?;
            }
        }

        // Create or reopen the active pack.
        let (writer, pack_id, new_file) = if let Some(existing_id) = active_pack_id {
            let path = pack_path(dir, existing_id);
            let writer = reopen_pack_for_append(&path, existing_id, &index)?;
            (writer, existing_id, false)
        } else {
            // No active pack; create a new one.
            let new_id = pack_ids.last().map(|id| PackId(id.0 + 1)).unwrap_or(PackId(0));
            let path = pack_path(dir, new_id);
            let writer = PackWriter::create(&path, new_id, unix_now())?;
            pack_ids.push(new_id); // keep sorted list consistent (not used further)
            (writer, new_id, true)
        };

        Ok(PageStore {
            dir: dir.to_path_buf(),
            index,
            active: Mutex::new(ActiveState {
                writer,
                pack_id,
                dirty_since_sync: HashSet::new(),
                new_files_since_sync: new_file,
            }),
            opts,
        })
    }

    /// Ingest a batch of pages.  Returns one `IngestOutcome` per input page.
    ///
    /// Deduplication is applied both within the batch (second occurrence of a
    /// hash reuses the first's location) and globally via the shared index.
    pub fn ingest(&self, pages: &[&[u8; PAGE_SIZE]]) -> Result<Vec<IngestOutcome>, StoreError> {
        if pages.is_empty() {
            return Ok(vec![]);
        }

        // 1. Hash the entire batch in parallel.
        let hashes: Vec<PageHash> = pages
            .par_iter()
            .map(|p| PageHash::from_bytes(*blake3::hash(*p).as_bytes()))
            .collect();

        // 2. Batch-local dedup: for each unique hash, record the first occurrence index.
        let mut batch_dedup: HashMap<PageHash, usize> = HashMap::new();
        for (i, &hash) in hashes.iter().enumerate() {
            batch_dedup.entry(hash).or_insert(i);
        }

        // 3. Probe global index for cache misses.
        let mut to_write: Vec<(PageHash, usize)> = Vec::new();
        // outcomes[i] is None until filled.
        let mut outcomes: Vec<Option<IngestOutcome>> = (0..pages.len()).map(|_| None).collect();

        for (&hash, &first_idx) in &batch_dedup {
            if let Some(loc) = self.index.get(&hash) {
                // Global dedup hit.
                outcomes[first_idx] = Some(IngestOutcome {
                    hash,
                    loc,
                    newly_written: false,
                });
            } else {
                to_write.push((hash, first_idx));
            }
        }

        // 4. Write misses to the active pack (under the mutex).
        if !to_write.is_empty() {
            let mut active = self.active.lock();

            for &(hash, page_idx) in &to_write {
                // Re-check: another thread may have written this between our probe
                // and the lock acquisition.
                if let Some(loc) = self.index.get(&hash) {
                    outcomes[page_idx] = Some(IngestOutcome {
                        hash,
                        loc,
                        newly_written: false,
                    });
                    continue;
                }

                // Rotate pack if the next record would exceed the 1 GiB cap.
                if active.writer.would_exceed_cap() {
                    let old_pack_id = active.pack_id;
                    active.dirty_since_sync.insert(old_pack_id);
                    active.writer.seal()?;

                    // Write sidecar for the now-sealed pack.
                    let sidecar = sidecar_path(&self.dir, old_pack_id);
                    self.index.write_sidecar(&sidecar, old_pack_id)?;

                    // Create the next pack.
                    let new_id = PackId(old_pack_id.0 + 1);
                    let new_path = pack_path(&self.dir, new_id);
                    active.writer = PackWriter::create(&new_path, new_id, unix_now())?;
                    active.pack_id = new_id;
                    active.new_files_since_sync = true;
                }

                let offset = active.writer.append(&hash, pages[page_idx])?;
                let current_pack_id = active.pack_id;
                let loc = PageLoc {
                    pack: current_pack_id,
                    offset,
                };
                active.dirty_since_sync.insert(current_pack_id);

                // Publish to the index so concurrent readers can find it.
                self.index.insert(hash, loc);

                outcomes[page_idx] = Some(IngestOutcome {
                    hash,
                    loc,
                    newly_written: true,
                });
            }
        }

        // 5. Fill outcomes for duplicate pages within this batch (non-first occurrences).
        for (i, &hash) in hashes.iter().enumerate() {
            if outcomes[i].is_none() {
                let loc = self
                    .index
                    .get(&hash)
                    .expect("hash must be in index after ingest");
                outcomes[i] = Some(IngestOutcome {
                    hash,
                    loc,
                    newly_written: false,
                });
            }
        }

        Ok(outcomes.into_iter().map(|o| o.unwrap()).collect())
    }

    /// Read a page by its hash.  Returns `None` if not found.
    pub fn get(&self, hash: &PageHash) -> Result<Option<bytes::Bytes>, StoreError> {
        let loc = match self.index.get(hash) {
            Some(l) => l,
            None => return Ok(None),
        };

        // If the page lives in the active pack, flush the write buffer first so
        // the data is visible on disk before we open a reader.
        {
            let mut active = self.active.lock();
            if active.pack_id == loc.pack {
                active.writer.flush_buf()?;
            }
        }

        let path = pack_path(&self.dir, loc.pack);

        // Try sealed reader first; fall back to unsealed if no valid footer.
        let reader = PackReader::open(&path, loc.pack)
            .or_else(|_| PackReader::open_unsealed(&path, loc.pack).map(|(r, _)| r))?;

        let (_, data) = reader.read_at(loc.offset)?;
        Ok(Some(data))
    }

    /// Flush buffered writes and fdatasync all dirty packs.
    pub fn sync(&self) -> Result<(), StoreError> {
        let mut active = self.active.lock();

        // Flush the in-memory write buffer to the OS.
        active.writer.flush_buf()?;

        // fdatasync every pack that received writes since the last sync.
        let mut dirty = std::mem::take(&mut active.dirty_since_sync);
        dirty.insert(active.pack_id); // always sync the currently-active pack

        for &pack_id in &dirty {
            let path = pack_path(&self.dir, pack_id);
            let file = std::fs::OpenOptions::new().write(true).open(&path)?;
            file.sync_data()?;
        }

        // If any new pack files were created, fsync the store directory so the
        // directory entries are durable.
        if active.new_files_since_sync {
            let dir_file = std::fs::File::open(&self.dir)?;
            dir_file.sync_all()?;
            active.new_files_since_sync = false;
        }

        Ok(())
    }
}

// ── Helper functions ──────────────────────────────────────────────────────────

/// Construct the path for a pack file given its ID.
fn pack_path(dir: &Path, pack_id: PackId) -> PathBuf {
    dir.join(format!("pack-{:08x}.spk", pack_id.0))
}

/// Construct the path for a pack's sidecar index file.
fn sidecar_path(dir: &Path, pack_id: PackId) -> PathBuf {
    dir.join(format!("pack-{:08x}.idx", pack_id.0))
}

/// Scan `dir` for `pack-{:08x}.spk` files and return their IDs sorted ascending.
fn discover_packs(dir: &Path) -> Result<Vec<PackId>, StoreError> {
    let mut ids = Vec::new();

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if let Some(hex) = name_str
            .strip_prefix("pack-")
            .and_then(|s| s.strip_suffix(".spk"))
        {
            if hex.len() == 8 {
                if let Ok(n) = u32::from_str_radix(hex, 16) {
                    ids.push(PackId(n));
                }
            }
        }
    }

    ids.sort();
    Ok(ids)
}

/// Return true if the pack at `path` has a valid footer (is sealed).
fn is_pack_sealed(path: &Path) -> bool {
    PackReader::open(path, PackId(0)).is_ok()
        || {
            // Quick size check: if the file is too small it cannot be sealed.
            match std::fs::metadata(path) {
                Ok(m) => {
                    if m.len() < PACK_HEADER_SIZE + PACK_FOOTER_SIZE {
                        return false;
                    }
                    // Re-try with a dummy pack_id; open() validates footer magic so
                    // any PackId works for the sealed check.
                    false
                }
                Err(_) => false,
            }
        }
}

/// Seal an existing unsealed pack file.
///
/// 1. Opens the file with `PackReader::open_unsealed` (truncates bad tail).
/// 2. Scans all valid records to rebuild the body hash.
/// 3. Writes the 44-byte footer and calls fdatasync.
/// 4. Returns `(hash, offset)` pairs for index population.
fn seal_existing_pack(
    path: &Path,
    pack_id: PackId,
) -> Result<Vec<(PageHash, u64)>, StoreError> {
    use std::io::Write;
    use crate::pack::FOOTER_MAGIC;

    // open_unsealed truncates corrupt tail; returns a reader over valid records.
    let (reader, _) = PackReader::open_unsealed(path, pack_id)?;
    let records = reader.scan()?;

    // Rebuild the body hash by re-reading each record.
    // The body hash covers (record_header || payload) for every record in order.
    let mut body_hasher = blake3::Hasher::new();
    let mut entries: Vec<(PageHash, u64)> = Vec::with_capacity(records.len());

    for (offset, hash) in &records {
        // Read the full record bytes (header + payload).
        let (_, payload) = reader.read_at(*offset)?;

        // Reconstruct the on-disk record header.
        let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
        rec_header[0..32].copy_from_slice(hash.as_bytes());
        rec_header[32] = 0x01; // flags: raw page
        rec_header[33..37].copy_from_slice(&(payload.len() as u32).to_le_bytes());

        body_hasher.update(&rec_header);
        body_hasher.update(&payload);

        entries.push((*hash, *offset));
    }

    let record_count = records.len() as u64;
    let body_hash = body_hasher.finalize();

    // Open file for appending and write the footer.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .append(true)
        .open(path)?;

    let mut footer = [0u8; PACK_FOOTER_SIZE as usize];
    footer[0..4].copy_from_slice(FOOTER_MAGIC);
    footer[4..12].copy_from_slice(&record_count.to_le_bytes());
    footer[12..44].copy_from_slice(body_hash.as_bytes());

    file.write_all(&footer)?;
    file.flush()?;
    file.sync_data()?;

    Ok(entries)
}

/// Open an unsealed pack for continued appending, also populating the index
/// with any existing records.
fn reopen_pack_for_append(
    path: &Path,
    pack_id: PackId,
    index: &Arc<ShardedIndex>,
) -> Result<PackWriter, StoreError> {
    // open_unsealed truncates any corrupt tail.
    let (reader, _) = PackReader::open_unsealed(path, pack_id)?;
    let records = reader.scan()?;

    // Populate the index with all existing records.
    index.insert_batch(
        records
            .into_iter()
            .map(|(offset, hash)| (hash, PageLoc { pack: pack_id, offset })),
    );

    // Use PackWriter::reopen to reconstruct writer state from the (now clean) file.
    let writer = PackWriter::reopen(path, pack_id)?;
    Ok(writer)
}

/// Current Unix time in seconds.
fn unix_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_unique_pages(count: usize) -> Vec<Box<[u8; PAGE_SIZE]>> {
        (0..count)
            .map(|i| {
                let mut p = Box::new([0u8; PAGE_SIZE]);
                // Use the index bytes to make each page unique.
                p[0] = (i & 0xFF) as u8;
                p[1] = ((i >> 8) & 0xFF) as u8;
                p[2] = ((i >> 16) & 0xFF) as u8;
                p[3] = ((i >> 24) & 0xFF) as u8;
                // Fill rest with a pattern derived from i to avoid all-zero dedup.
                for j in 4..PAGE_SIZE {
                    p[j] = ((i ^ j).wrapping_add(0xA5)) as u8;
                }
                p
            })
            .collect()
    }

    // ── Test 1: Ingest + get round-trip ──────────────────────────────────────

    #[test]
    fn ingest_get_round_trip() {
        let dir = TempDir::new().unwrap();
        let store = PageStore::open(dir.path(), StoreOptions::default()).unwrap();

        let pages = make_unique_pages(10);
        let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();

        let outcomes = store.ingest(&page_refs).unwrap();
        assert_eq!(outcomes.len(), 10);

        for (i, outcome) in outcomes.iter().enumerate() {
            assert!(outcome.newly_written, "page {i} should be newly written");
            let got = store.get(&outcome.hash).unwrap().expect("should find page");
            assert_eq!(
                got.as_ref(),
                pages[i].as_ref(),
                "page {i} bytes should match"
            );
        }
    }

    // ── Test 2: Dedup same page appearing twice in one batch ─────────────────

    #[test]
    fn dedup_same_page_twice() {
        let dir = TempDir::new().unwrap();
        let store = PageStore::open(dir.path(), StoreOptions::default()).unwrap();

        let pages = make_unique_pages(5);
        // Duplicate page at index 2.
        let page_refs: Vec<&[u8; PAGE_SIZE]> = vec![
            pages[0].as_ref(),
            pages[1].as_ref(),
            pages[2].as_ref(),
            pages[2].as_ref(), // duplicate
            pages[3].as_ref(),
        ];

        let outcomes = store.ingest(&page_refs).unwrap();
        assert_eq!(outcomes.len(), 5);

        // Exactly one of the two identical entries should be newly_written.
        let new_count = outcomes
            .iter()
            .filter(|o| o.newly_written && o.hash == outcomes[2].hash)
            .count();
        assert_eq!(new_count, 1, "exactly one write for the duplicated page");

        // Both occurrences should have the same location.
        assert_eq!(
            outcomes[2].loc, outcomes[3].loc,
            "duplicate pages must map to the same location"
        );

        // get() returns the correct bytes.
        let got = store.get(&outcomes[2].hash).unwrap().expect("must be in store");
        assert_eq!(got.as_ref(), pages[2].as_ref());
    }

    // ── Test 3: Dedup across two separate ingest batches ─────────────────────

    #[test]
    fn dedup_across_batches() {
        let dir = TempDir::new().unwrap();
        let store = PageStore::open(dir.path(), StoreOptions::default()).unwrap();

        let pages = make_unique_pages(5);
        let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();

        // First batch: all newly written.
        let outcomes_a = store.ingest(&page_refs).unwrap();
        for (i, o) in outcomes_a.iter().enumerate() {
            assert!(o.newly_written, "batch A page {i} should be new");
        }

        // Second batch: all dedup hits.
        let outcomes_b = store.ingest(&page_refs).unwrap();
        for (i, o) in outcomes_b.iter().enumerate() {
            assert!(
                !o.newly_written,
                "batch B page {i} should be a dedup hit"
            );
            assert_eq!(
                outcomes_a[i].loc, outcomes_b[i].loc,
                "location must match across batches"
            );
        }
    }

    // ── Test 4: sync() does not error after a normal ingest ──────────────────

    #[test]
    fn sync_after_ingest() {
        let dir = TempDir::new().unwrap();
        let store = PageStore::open(dir.path(), StoreOptions::default()).unwrap();

        let pages = make_unique_pages(20);
        let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();

        store.ingest(&page_refs).unwrap();
        store.sync().unwrap(); // must not error

        // Pages are still readable after sync.
        for p in &pages {
            let hash = PageHash::from_bytes(*blake3::hash(p.as_ref()).as_bytes());
            let got = store.get(&hash).unwrap().expect("page must still be readable after sync");
            assert_eq!(got.as_ref(), p.as_ref());
        }
    }

    // ── Test 5: Concurrent ingest from multiple threads ───────────────────────

    #[test]
    fn concurrent_ingest() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(PageStore::open(dir.path(), StoreOptions::default()).unwrap());

        const THREADS: usize = 4;
        const PAGES_PER_THREAD: usize = 100;

        // Generate non-overlapping pages for each thread.
        let all_pages: Vec<Vec<Box<[u8; PAGE_SIZE]>>> = (0..THREADS)
            .map(|t| make_unique_pages_with_offset(PAGES_PER_THREAD, t * PAGES_PER_THREAD))
            .collect();

        // Pre-compute expected hashes.
        let expected_hashes: Vec<Vec<PageHash>> = all_pages
            .iter()
            .map(|thread_pages| {
                thread_pages
                    .iter()
                    .map(|p| PageHash::from_bytes(*blake3::hash(p.as_ref()).as_bytes()))
                    .collect()
            })
            .collect();

        let mut handles = Vec::new();
        for t in 0..THREADS {
            let store_clone = Arc::clone(&store);
            let pages = all_pages[t].clone();
            let handle = std::thread::spawn(move || {
                let page_refs: Vec<&[u8; PAGE_SIZE]> =
                    pages.iter().map(|p| p.as_ref()).collect();
                store_clone.ingest(&page_refs).unwrap();
            });
            handles.push(handle);
        }
        for h in handles {
            h.join().unwrap();
        }

        // Verify all pages are readable.
        for (t, hashes) in expected_hashes.iter().enumerate() {
            for (i, hash) in hashes.iter().enumerate() {
                assert!(
                    store.get(hash).unwrap().is_some(),
                    "thread {t} page {i} should be readable"
                );
            }
        }
    }

    // ── Test 6: Store reopen persists data ───────────────────────────────────

    #[test]
    fn store_reopen() {
        let dir = TempDir::new().unwrap();

        let pages = make_unique_pages(10);
        let hashes: Vec<PageHash> = pages
            .iter()
            .map(|p| PageHash::from_bytes(*blake3::hash(p.as_ref()).as_bytes()))
            .collect();

        // Ingest and sync, then drop the store.
        {
            let store = PageStore::open(dir.path(), StoreOptions::default()).unwrap();
            let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
            store.ingest(&page_refs).unwrap();
            store.sync().unwrap();
        }

        // Reopen the same directory.
        let store2 = PageStore::open(dir.path(), StoreOptions::default()).unwrap();

        // All previously-ingested pages must still be readable.
        for (i, hash) in hashes.iter().enumerate() {
            let got = store2
                .get(hash)
                .unwrap()
                .unwrap_or_else(|| panic!("page {i} should be readable after reopen"));
            assert_eq!(got.as_ref(), pages[i].as_ref(), "page {i} data must match");
        }

        // Ingesting the same pages again should be all dedup hits.
        let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
        let outcomes = store2.ingest(&page_refs).unwrap();
        for (i, o) in outcomes.iter().enumerate() {
            assert!(
                !o.newly_written,
                "page {i} should be a dedup hit after reopen"
            );
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn make_unique_pages_with_offset(count: usize, offset: usize) -> Vec<Box<[u8; PAGE_SIZE]>> {
        (0..count)
            .map(|i| {
                let idx = i + offset;
                let mut p = Box::new([0u8; PAGE_SIZE]);
                p[0] = (idx & 0xFF) as u8;
                p[1] = ((idx >> 8) & 0xFF) as u8;
                p[2] = ((idx >> 16) & 0xFF) as u8;
                p[3] = ((idx >> 24) & 0xFF) as u8;
                for j in 4..PAGE_SIZE {
                    p[j] = ((idx ^ j).wrapping_add(0xA5)) as u8;
                }
                p
            })
            .collect()
    }
}
