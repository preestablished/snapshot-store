use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use rayon::prelude::*;
use snapstore_types::{PackId, PageHash, PageLoc, PAGE_SIZE};

use crate::index::{rebuild_from_pack, IndexError, ShardedIndex};
use crate::pack::{
    PackError, PackReader, PackWriter, PACK_FOOTER_SIZE, PACK_HEADER_SIZE, RECORD_HEADER_SIZE,
};
use crate::read_cache::ReadHandleCache;

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
    /// Rotation threshold for pack files. Default: PACK_MAX_BYTES (1 GiB).
    /// Set smaller in tests to force rotation cheaply.
    pub max_pack_bytes: u64,
    /// Capacity of the LRU read-handle cache (number of open file descriptors
    /// to keep cached for sealed packs).  Default: 256.
    pub read_handle_cap: usize,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            write_buf_size: 4 * 1024 * 1024,
            max_pack_bytes: crate::pack::PACK_MAX_BYTES,
            read_handle_cap: 256,
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
    /// LRU cache of open read handles for sealed packs.
    ///
    /// Sealed packs are immutable: once a footer is appended during rotation,
    /// records at their fixed offsets are never rewritten.  An open fd cached
    /// here stays correct even after `seal_no_sync` runs, because:
    ///
    /// - Records live at offsets `[PACK_HEADER_SIZE .. body_end)`.
    /// - The footer is appended *after* body_end; it does not overwrite records.
    /// - pread calls target record offsets, never the footer region.
    ///
    /// Therefore, there is no need to evict or replace cached fds on rotation —
    /// the sealed pack's fd remains valid for the lifetime of the store.
    ///
    /// The active pack's handle is NOT cached here; its identity changes on
    /// rotation.  Active-pack reads are performed under the `active` lock using
    /// the writer's file handle (or a fresh open under the lock).
    read_cache: ReadHandleCache,
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
                    index.insert_batch(entries.into_iter().map(|(h, o)| {
                        (
                            h,
                            PageLoc {
                                pack: pack_id,
                                offset: o,
                            },
                        )
                    }));
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
                index.insert_batch(entries);
                index.write_sidecar(&sidecar, pack_id)?;
            }
        }

        // Create or reopen the active pack.
        let max_pack_bytes = opts.max_pack_bytes;
        let (writer, pack_id, new_file) = if let Some(existing_id) = active_pack_id {
            let path = pack_path(dir, existing_id);
            let writer = reopen_pack_for_append(&path, existing_id, &index)?;
            (writer, existing_id, false)
        } else {
            // No active pack; create a new one.
            let new_id = pack_ids
                .last()
                .map(|id| PackId(id.0 + 1))
                .unwrap_or(PackId(0));
            let path = pack_path(dir, new_id);
            let writer =
                PackWriter::create_with_max_bytes(&path, new_id, unix_now(), max_pack_bytes)?;
            pack_ids.push(new_id); // keep sorted list consistent (not used further)
            (writer, new_id, true)
        };

        let read_handle_cap = opts.read_handle_cap;
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
            read_cache: ReadHandleCache::new(dir.to_path_buf(), read_handle_cap),
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

                // Rotate pack if the next record would exceed the cap.
                // Use seal_no_sync() to avoid blocking the hot path on fdatasync;
                // dirty packs (including sealed ones) are fsynced lazily by sync().
                if active.writer.would_exceed_cap() {
                    let old_pack_id = active.pack_id;
                    active.dirty_since_sync.insert(old_pack_id);
                    active.writer.seal_no_sync()?;

                    // Write sidecar for the now-sealed pack.
                    let sidecar = sidecar_path(&self.dir, old_pack_id);
                    self.index.write_sidecar(&sidecar, old_pack_id)?;

                    // Create the next pack with the configured cap.
                    let new_id = PackId(old_pack_id.0 + 1);
                    let new_path = pack_path(&self.dir, new_id);
                    active.writer = PackWriter::create_with_max_bytes(
                        &new_path,
                        new_id,
                        unix_now(),
                        self.opts.max_pack_bytes,
                    )?;
                    active.pack_id = new_id;
                    active.new_files_since_sync = true;

                    // Note: we do NOT insert the sealed pack's fd into read_cache here.
                    // The fd in the writer was opened for read+write; we don't cache it.
                    // The read_cache will open a read-only fd on the first cache-miss
                    // read of this newly-sealed pack.  The cached fd will stay valid
                    // because sealing only appended a footer — record offsets are unchanged.
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
    ///
    /// This is equivalent to `get_batch(&[hash])[0]`.  Delegates to the
    /// batched implementation for a consistent cached read path.
    pub fn get(&self, hash: &PageHash) -> Result<Option<bytes::Bytes>, StoreError> {
        let mut results = self.get_batch(std::slice::from_ref(hash))?;
        Ok(results.remove(0))
    }

    /// Read multiple pages by their hashes.  Output order matches input order.
    ///
    /// Internally the lookups are sorted by `(pack_id, offset)` so reads are
    /// sequential per pack, maximising page-cache locality and minimising seek
    /// overhead.  Each pack's cached `Arc<File>` handle is reused for all reads
    /// in that pack's run.
    ///
    /// Pages not found in the index are returned as `None` at their input position.
    pub fn get_batch(&self, hashes: &[PageHash]) -> Result<Vec<Option<bytes::Bytes>>, StoreError> {
        if hashes.is_empty() {
            return Ok(vec![]);
        }

        // 1. Resolve hashes to locations.  Collect (input_index, loc) for found entries.
        let mut results: Vec<Option<bytes::Bytes>> = vec![None; hashes.len()];
        let mut lookups: Vec<(usize, PageHash, PageLoc)> = Vec::with_capacity(hashes.len());

        for (i, hash) in hashes.iter().enumerate() {
            if let Some(loc) = self.index.get(hash) {
                lookups.push((i, *hash, loc));
            }
            // Not found entries stay as None.
        }

        if lookups.is_empty() {
            return Ok(results);
        }

        // 2. Separate active-pack reads from sealed-pack reads.
        //    We need the active lock to know which pack is currently active.
        let active_pack_id = {
            let active = self.active.lock();
            active.pack_id
        };

        let (mut active_lookups, mut sealed_lookups): (Vec<_>, Vec<_>) = lookups
            .into_iter()
            .partition(|(_, _, loc)| loc.pack == active_pack_id);

        // 3. Sort sealed lookups by (pack_id, offset) for sequential access per pack.
        sealed_lookups.sort_unstable_by_key(|(_, _, loc)| (loc.pack.0, loc.offset));

        // 4. Process sealed-pack reads using the LRU handle cache.
        //    Retry-on-ENOENT: if opening a pack fails with NotFound, drop the cached
        //    handle, re-consult the index (M7 GC may have repointed the hash to a
        //    new pack), and retry once before erroring.
        //    M7 GC relies on this probe: it repoints index entries then unlinks the
        //    old pack; one retry is sufficient to follow the repoint.
        for (input_idx, hash, loc) in &sealed_lookups {
            let data = self.read_sealed_with_retry(*input_idx, hash, *loc)?;
            results[*input_idx] = data;
        }

        // 5. Process active-pack reads under the active lock.
        //    Flush the write buffer first so all buffered records are visible on disk,
        //    then pread directly from the active pack's file.
        //    Active-pack handles are NOT cached in the LRU — the active pack's
        //    identity changes on every rotation.
        if !active_lookups.is_empty() {
            // Sort by offset for sequential reads within the active pack.
            active_lookups.sort_unstable_by_key(|(_, _, loc)| loc.offset);

            let mut active = self.active.lock();

            // Re-check: some lookups may have been in the old active pack if rotation
            // happened between the partition step and taking the lock.  Handle the case
            // where pack identity changed by re-routing mismatched entries to sealed reads.
            let current_active_id = active.pack_id;
            let mut rerouted: Vec<(usize, PageHash, PageLoc)> = Vec::new();
            active_lookups.retain(|(i, h, loc)| {
                if loc.pack == current_active_id {
                    true
                } else {
                    rerouted.push((*i, *h, *loc));
                    false
                }
            });

            if !active_lookups.is_empty() {
                active.writer.flush_buf()?;
            }

            for (input_idx, hash, loc) in &active_lookups {
                use crate::pack::RECORD_HEADER_SIZE;
                use std::os::unix::fs::FileExt;

                // pread the record header.
                let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
                active
                    .writer
                    .file_for_pread()
                    .read_exact_at(&mut rec_header, loc.offset)
                    .map_err(|_| PackError::TruncatedRecord { offset: loc.offset })?;

                let stored_hash_bytes: [u8; 32] = rec_header[0..32].try_into().unwrap();
                let len = u32::from_le_bytes(rec_header[33..37].try_into().unwrap());

                if len as usize != PAGE_SIZE {
                    return Err(StoreError::Pack(PackError::TruncatedRecord {
                        offset: loc.offset,
                    }));
                }

                let mut payload = vec![0u8; len as usize];
                active
                    .writer
                    .file_for_pread()
                    .read_exact_at(&mut payload, loc.offset + RECORD_HEADER_SIZE)
                    .map_err(|_| PackError::TruncatedRecord { offset: loc.offset })?;

                // Verify stored hash == requested hash.
                if &stored_hash_bytes != hash.as_bytes() {
                    return Err(StoreError::Pack(PackError::HashMismatch {
                        offset: loc.offset,
                    }));
                }

                // Re-verify blake3(payload) == stored hash for per-record integrity.
                let computed = *blake3::hash(&payload).as_bytes();
                if computed != stored_hash_bytes {
                    return Err(StoreError::Pack(PackError::HashMismatch {
                        offset: loc.offset,
                    }));
                }

                results[*input_idx] = Some(bytes::Bytes::from(payload));
            }

            // Release the lock before handling rerouted entries.
            drop(active);

            // Handle rerouted entries (were in the old active pack, now sealed).
            for (input_idx, hash, loc) in &rerouted {
                let data = self.read_sealed_with_retry(*input_idx, hash, *loc)?;
                results[*input_idx] = data;
            }
        }

        Ok(results)
    }

    /// Internal helper: read a page from a sealed pack via the LRU handle cache,
    /// with one retry if the pack file is not found (for M7 GC repoint-then-unlink).
    fn read_sealed_with_retry(
        &self,
        _input_idx: usize,
        hash: &PageHash,
        loc: PageLoc,
    ) -> Result<Option<bytes::Bytes>, StoreError> {
        match self.read_from_cached_handle(hash, loc) {
            Ok(data) => Ok(Some(data)),
            Err(StoreError::Pack(PackError::Io(ref e)))
                if e.kind() == std::io::ErrorKind::NotFound =>
            {
                // M7 GC relies on this retry: GC repoints the index then unlinks the
                // old pack.  Drop the stale cached handle, re-consult the index for a
                // (possibly new) location, and retry once.
                self.read_cache.invalidate(loc.pack);

                // Re-consult the index.
                let new_loc = match self.index.get(hash) {
                    Some(l) => l,
                    None => return Ok(None), // GC pruned it entirely
                };

                if new_loc == loc {
                    // Index still points to the missing pack — genuine error.
                    Err(StoreError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("pack-{:08x}.spk not found", loc.pack.0),
                    )))
                } else {
                    // Retry with new location.
                    match self.read_from_cached_handle(hash, new_loc) {
                        Ok(data) => Ok(Some(data)),
                        Err(e) => Err(e),
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Read one page from the LRU handle cache (no retry).
    fn read_from_cached_handle(
        &self,
        hash: &PageHash,
        loc: PageLoc,
    ) -> Result<bytes::Bytes, StoreError> {
        let handle = self.read_cache.get_or_open(loc.pack).map_err(|e| {
            // Wrap Io errors so NotFound propagates correctly.
            match e {
                PackError::Io(io) => StoreError::Io(io),
                other => StoreError::Pack(other),
            }
        })?;

        PackReader::read_at_from_file(&handle, loc.offset, hash).map_err(StoreError::Pack)
    }

    /// Evict a cached read handle for `pack`.
    ///
    /// Called by M7 GC before unlinking a pack file so the next reader opens a
    /// fresh fd to the repointed location.  Also available for testing.
    pub fn invalidate_pack_handle(&self, pack: PackId) {
        self.read_cache.invalidate(pack);
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
    PackReader::open(path, PackId(0)).is_ok() || {
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
fn seal_existing_pack(path: &Path, pack_id: PackId) -> Result<Vec<(PageHash, u64)>, StoreError> {
    use crate::pack::FOOTER_MAGIC;
    use std::io::Write;

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
    let mut file = std::fs::OpenOptions::new().append(true).open(path)?;

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
    index.insert_batch(records.into_iter().map(|(offset, hash)| {
        (
            hash,
            PageLoc {
                pack: pack_id,
                offset,
            },
        )
    }));

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
        let got = store
            .get(&outcomes[2].hash)
            .unwrap()
            .expect("must be in store");
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
            assert!(!o.newly_written, "batch B page {i} should be a dedup hit");
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
            let got = store
                .get(&hash)
                .unwrap()
                .expect("page must still be readable after sync");
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
        for thread_pages in all_pages.iter().take(THREADS) {
            let store_clone = Arc::clone(&store);
            let pages = thread_pages.clone();
            let handle = std::thread::spawn(move || {
                let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
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

    // ── Test 7: crash_during_rotation_sealed_no_sidecar (M1 WI3 exit gate) ───

    #[test]
    fn crash_during_rotation_sealed_no_sidecar() {
        use crate::pack::PackWriter;

        let dir = TempDir::new().unwrap();

        // Phase 1: ingest 20 pages, drop store (pack 0 remains UNSEALED, no sidecar)
        let pages = make_unique_pages(20);
        let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
        let hashes: Vec<PageHash> = pages
            .iter()
            .map(|p| PageHash::from_bytes(*blake3::hash(p.as_ref()).as_bytes()))
            .collect();

        {
            let store = PageStore::open(dir.path(), StoreOptions::default()).unwrap();
            store.ingest(&page_refs).unwrap();
            // Flush write buffer to OS so records are on disk, then drop without sealing.
            // pack 0 is unsealed on disk (no footer), and no sidecar has been written.
            store.sync().unwrap();
        }

        // Phase 2: simulate "rotation completed but crash before sidecar":
        //   - seal pack 0 manually (write footer) — this is what rotation does
        //   - ensure no .idx sidecar exists for pack 0
        let pack0_path = dir.path().join("pack-00000000.spk");
        let sidecar0_path = dir.path().join("pack-00000000.idx");

        {
            let mut writer = PackWriter::reopen(&pack0_path, PackId(0)).unwrap();
            writer.seal().unwrap(); // footer written — pack 0 is now sealed
        }
        // Remove sidecar if it somehow exists (it shouldn't, but be explicit)
        let _ = std::fs::remove_file(&sidecar0_path);
        // No pack-00000001.spk exists — this is the key scenario

        // Phase 3: reopen the store — must find pack 0 (sealed, no sidecar),
        //   rebuild index from pack body scan, zero lost entries
        let store2 = PageStore::open(dir.path(), StoreOptions::default()).unwrap();

        for (i, hash) in hashes.iter().enumerate() {
            assert!(
                store2.get(hash).unwrap().is_some(),
                "page {i} must be findable after crash-during-rotation recovery"
            );
        }

        // Sidecar should have been regenerated by open()
        assert!(
            sidecar0_path.exists(),
            "open() must regenerate missing sidecar for sealed pack"
        );
    }

    // ── Test 8: sync_spans_rotation (M1 WI4 exit gate) ───────────────────────

    #[test]
    fn sync_spans_rotation() {
        let dir = TempDir::new().unwrap();

        // Use a small pack cap so rotation happens cheaply.
        // Each record = RECORD_HEADER_SIZE (37) + PAGE_SIZE (4096) = 4133 bytes.
        // Pack header = 20 bytes. Set cap so exactly 5 pages fit (20 + 5*4133 = 20685 bytes).
        // 6th page would exceed cap.
        use crate::pack::{PACK_HEADER_SIZE, RECORD_HEADER_SIZE};
        let record_size = RECORD_HEADER_SIZE + PAGE_SIZE as u64;
        let pages_per_pack: u64 = 5;
        let max_pack_bytes = PACK_HEADER_SIZE + pages_per_pack * record_size;

        let opts = StoreOptions {
            max_pack_bytes,
            ..StoreOptions::default()
        };

        let store = PageStore::open(dir.path(), opts).unwrap();

        // Ingest 8 pages in a SINGLE batch — this must span the rotation:
        // pages 0-4 land in pack 0, pack 0 rotates, pages 5-7 land in pack 1.
        let pages = make_unique_pages(8);
        let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
        let hashes: Vec<PageHash> = pages
            .iter()
            .map(|p| PageHash::from_bytes(*blake3::hash(p.as_ref()).as_bytes()))
            .collect();

        let outcomes = store.ingest(&page_refs).unwrap();
        assert_eq!(outcomes.len(), 8);

        // Verify pack 0 and pack 1 both exist — confirming rotation happened
        let pack0_exists = dir.path().join("pack-00000000.spk").exists();
        let pack1_exists = dir.path().join("pack-00000001.spk").exists();
        assert!(pack0_exists, "pack 0 must exist after rotation");
        assert!(pack1_exists, "pack 1 must exist after rotation");

        // Call sync() — must cover BOTH packs (pack 0 was sealed mid-ingest, pack 1 is active)
        store.sync().unwrap();

        // Drop the store and reopen — all 8 pages must still be readable
        // (this is the observable durability invariant: sync + reopen = no data loss)
        drop(store);

        let store2 = PageStore::open(dir.path(), StoreOptions::default()).unwrap();
        for (i, hash) in hashes.iter().enumerate() {
            assert!(
                store2.get(hash).unwrap().is_some(),
                "page {i} must be readable after sync-spans-rotation + reopen"
            );
        }
    }

    // ── Test 9: get_batch order preservation and missing-hash → None ─────────

    #[test]
    fn get_batch_order_and_missing() {
        let dir = TempDir::new().unwrap();
        let store = PageStore::open(dir.path(), StoreOptions::default()).unwrap();

        let pages = make_unique_pages(5);
        let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
        let outcomes = store.ingest(&page_refs).unwrap();

        // Build a query that interleaves known hashes with an unknown one.
        let unknown_hash = PageHash::from_bytes([0xFF; 32]);
        let query: Vec<PageHash> = vec![
            outcomes[3].hash,
            unknown_hash,
            outcomes[0].hash,
            outcomes[4].hash,
            unknown_hash,
            outcomes[2].hash,
        ];

        let results = store.get_batch(&query).unwrap();
        assert_eq!(results.len(), 6);

        // Check order: result[i] corresponds to query[i].
        assert_eq!(results[0].as_deref(), Some(pages[3].as_ref() as &[u8]));
        assert!(results[1].is_none(), "unknown hash should be None");
        assert_eq!(results[2].as_deref(), Some(pages[0].as_ref() as &[u8]));
        assert_eq!(results[3].as_deref(), Some(pages[4].as_ref() as &[u8]));
        assert!(results[4].is_none(), "unknown hash should be None");
        assert_eq!(results[5].as_deref(), Some(pages[2].as_ref() as &[u8]));
    }

    // ── Test 10: reads spanning sealed + active packs in one batch ────────────

    #[test]
    fn get_batch_spans_sealed_and_active() {
        let dir = TempDir::new().unwrap();

        // Use a tiny pack so 3 pages fill it, forcing rotation.
        use crate::pack::{PACK_HEADER_SIZE, RECORD_HEADER_SIZE};
        let record_size = RECORD_HEADER_SIZE + PAGE_SIZE as u64;
        let max_pack_bytes = PACK_HEADER_SIZE + 3 * record_size;

        let opts = StoreOptions {
            max_pack_bytes,
            ..StoreOptions::default()
        };
        let store = PageStore::open(dir.path(), opts).unwrap();

        // Ingest 5 pages: 3 → pack 0 (sealed), 2 → pack 1 (active).
        let pages = make_unique_pages(5);
        let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
        let outcomes = store.ingest(&page_refs).unwrap();

        // Verify rotation happened: pages should span at least 2 different packs.
        let pack_ids: std::collections::HashSet<u32> =
            outcomes.iter().map(|o| o.loc.pack.0).collect();
        assert!(
            pack_ids.len() >= 2,
            "rotation must have occurred, got pack_ids: {:?}",
            pack_ids
        );

        // Read all 5 in a single batch — must return all 5 correctly.
        let hashes: Vec<PageHash> = outcomes.iter().map(|o| o.hash).collect();
        let results = store.get_batch(&hashes).unwrap();
        assert_eq!(results.len(), 5);
        for (i, result) in results.iter().enumerate() {
            assert_eq!(
                result.as_deref(),
                Some(pages[i].as_ref() as &[u8]),
                "page {i} must match in cross-pack batch"
            );
        }
    }

    // ── Test 11: read-after-rotation ──────────────────────────────────────────

    #[test]
    fn read_after_rotation() {
        let dir = TempDir::new().unwrap();

        use crate::pack::{PACK_HEADER_SIZE, RECORD_HEADER_SIZE};
        let record_size = RECORD_HEADER_SIZE + PAGE_SIZE as u64;
        let max_pack_bytes = PACK_HEADER_SIZE + 3 * record_size;

        let opts = StoreOptions {
            max_pack_bytes,
            ..StoreOptions::default()
        };
        let store = PageStore::open(dir.path(), opts).unwrap();

        // Ingest 6 pages across two packs (3 per pack).
        let pages = make_unique_pages(6);
        let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
        let outcomes = store.ingest(&page_refs).unwrap();

        // Flush so pack 0 is on disk.
        store.sync().unwrap();

        // Now read all pages — the first 3 are in the sealed pack, last 3 in active.
        for (i, outcome) in outcomes.iter().enumerate() {
            let got = store
                .get(&outcome.hash)
                .unwrap()
                .unwrap_or_else(|| panic!("page {i} not found after rotation"));
            assert_eq!(
                got.as_ref(),
                pages[i].as_ref(),
                "page {i} mismatch after rotation"
            );
        }
    }

    // ── Test 12: LRU eviction with read_handle_cap=2, >2 packs ───────────────

    #[test]
    fn lru_eviction_all_reads_correct() {
        let dir = TempDir::new().unwrap();

        // 3 pages per pack, 3 packs total = 9 pages across 3 packs.
        use crate::pack::{PACK_HEADER_SIZE, RECORD_HEADER_SIZE};
        let record_size = RECORD_HEADER_SIZE + PAGE_SIZE as u64;
        let max_pack_bytes = PACK_HEADER_SIZE + 3 * record_size;

        let opts = StoreOptions {
            max_pack_bytes,
            read_handle_cap: 2, // only 2 handles cached at a time
            ..StoreOptions::default()
        };
        let store = PageStore::open(dir.path(), opts).unwrap();

        // Ingest 9 pages to create 3 packs (3rd pack is active with 3 pages).
        let pages = make_unique_pages(9);
        let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
        let outcomes = store.ingest(&page_refs).unwrap();
        store.sync().unwrap();

        // Read all 9 pages — with cap=2, the third pack forces eviction of the first.
        // All reads must still return correct data.
        for (i, outcome) in outcomes.iter().enumerate() {
            let got = store
                .get(&outcome.hash)
                .unwrap()
                .unwrap_or_else(|| panic!("page {i} not found with LRU cap=2"));
            assert_eq!(
                got.as_ref(),
                pages[i].as_ref(),
                "page {i} mismatch with LRU eviction"
            );
        }
    }

    // ── Test 13: invalidate + re-read ─────────────────────────────────────────

    #[test]
    fn invalidate_and_reread() {
        let dir = TempDir::new().unwrap();
        let store = PageStore::open(dir.path(), StoreOptions::default()).unwrap();

        let pages = make_unique_pages(5);
        let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
        let outcomes = store.ingest(&page_refs).unwrap();
        store.sync().unwrap();

        // Read pages to populate the cache.
        for o in &outcomes {
            store.get(&o.hash).unwrap().expect("page must be readable");
        }

        // Invalidate all pack handles.
        for o in &outcomes {
            store.invalidate_pack_handle(o.loc.pack);
        }

        // Re-read — must still return correct data (re-opens the files).
        for (i, outcome) in outcomes.iter().enumerate() {
            let got = store
                .get(&outcome.hash)
                .unwrap()
                .unwrap_or_else(|| panic!("page {i} not found after invalidation"));
            assert_eq!(
                got.as_ref(),
                pages[i].as_ref(),
                "page {i} mismatch after invalidation"
            );
        }
    }

    // ── Test 14: hash-mismatch (corruption) returns error, not wrong bytes ────

    #[test]
    fn corruption_detected_on_read() {
        use std::os::unix::fs::FileExt;

        let dir = TempDir::new().unwrap();
        let store = PageStore::open(dir.path(), StoreOptions::default()).unwrap();

        let pages = make_unique_pages(3);
        let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();
        let outcomes = store.ingest(&page_refs).unwrap();
        store.sync().unwrap();

        // Pick a page that ended up in a sealed pack.
        // After sync the active pack was flushed; pack 0 is sealed (no rotation here,
        // but all data is flushed to disk).  We'll corrupt the first page's payload.
        let target = &outcomes[1];
        let pack_path = dir
            .path()
            .join(format!("pack-{:08x}.spk", target.loc.pack.0));

        // Flip a byte in the payload of target record.
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&pack_path)
                .unwrap();
            let payload_start = target.loc.offset + RECORD_HEADER_SIZE;
            let mut byte = [0u8; 1];
            file.read_exact_at(&mut byte, payload_start).unwrap();
            byte[0] ^= 0xFF;
            file.write_at(&byte, payload_start).unwrap();
        }

        // Invalidate any cached handle so we re-open the (now corrupt) file.
        store.invalidate_pack_handle(target.loc.pack);

        // Reading the corrupted page must return an error, not wrong bytes.
        let result = store.get(&target.hash);
        assert!(
            result.is_err(),
            "reading a corrupted page must return an error, got: {:?}",
            result
        );
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
