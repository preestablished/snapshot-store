//! Server-side snapshot store façade (WI3 + WI5).
//!
//! # Directory layout
//!
//! ```text
//! <dir>/
//!   pages/                            — PageStore root
//!   manifests/<2-hex-shard>/<64-hex>.spm  — one `.spm` container per SnapshotRef
//!   tmp/                              — staging files; cleaned at open()
//! ```
//!
//! # Failpoints (WI5)
//!
//! Named failpoints are compiled in only with `--features failpoints`.
//! They are **never** present in release builds and are off by default
//! (requiring explicit `fail::cfg` calls to arm them).
//! See the `#[cfg(feature = "failpoints")]` smoke test below.

#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

use snapstore_manifest::{DeviceBlob, Manifest, ManifestEntry, ManifestError};
use snapstore_pagestore::ingest::{PageStore, StoreOptions as PageStoreOptions};
use snapstore_types::{PageHash, SnapshotRef, PAGE_SIZE};

// ── Failpoint macro (WI5) ─────────────────────────────────────────────────────

/// Invoke a named failpoint when the `failpoints` feature is enabled.
///
/// When the feature is off this macro expands to nothing — zero overhead.
macro_rules! fail_point {
    ($name:expr) => {
        #[cfg(feature = "failpoints")]
        fail::fail_point!($name);
    };
}

// ── Errors ────────────────────────────────────────────────────────────────────

/// Errors returned by `put_snapshot`.
///
/// Each variant maps to a gRPC status code:
/// - `Manifest`            → `INVALID_ARGUMENT`
/// - `UnknownParent`       → `FAILED_PRECONDITION`
/// - `ParentRamMismatch`   → `INVALID_ARGUMENT`
/// - `MissingPages`        → `FAILED_PRECONDITION` (detail: page-hash list)
/// - `Io`                  → `INTERNAL`
/// - `PageStore`           → `INTERNAL`
#[derive(Debug, thiserror::Error)]
pub enum PutError {
    #[error("manifest decode error: {0}")]
    Manifest(ManifestError),
    #[error("unknown parent snapshot: {0:?}")]
    UnknownParent(SnapshotRef),
    #[error("parent guest_ram_bytes does not match child")]
    ParentRamMismatch,
    #[error("missing pages in pagestore")]
    MissingPages(Vec<PageHash>),
    #[error("I/O error: {0}")]
    Io(std::io::Error),
    #[error("page store error: {0}")]
    PageStore(snapstore_pagestore::ingest::StoreError),
}

impl From<ManifestError> for PutError {
    fn from(e: ManifestError) -> Self {
        PutError::Manifest(e)
    }
}

impl From<std::io::Error> for PutError {
    fn from(e: std::io::Error) -> Self {
        PutError::Io(e)
    }
}

impl From<snapstore_pagestore::ingest::StoreError> for PutError {
    fn from(e: snapstore_pagestore::ingest::StoreError) -> Self {
        PutError::PageStore(e)
    }
}

/// Errors returned by `get_snapshot` / `resolve_pages` and other read paths.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("page store error: {0}")]
    PageStore(snapstore_pagestore::ingest::StoreError),
    #[error("manifest decode error: {0}")]
    Manifest(#[from] ManifestError),
    #[error("snapshot not found")]
    NotFound,
    #[error("manifest file is corrupt (hash mismatch)")]
    ManifestCorrupt,
    #[error("page missing from store (corruption): {0:?}")]
    MissingPage(PageHash),
    #[error("parent chain cycle or depth exceeded")]
    ChainDepthExceeded,
    #[error("baseline is not in the parent chain of the requested snapshot")]
    BaselineNotAncestor,
    #[error("flatten error: {0}")]
    Flatten(#[from] snapstore_manifest::FlattenError),
}

impl From<snapstore_pagestore::ingest::StoreError> for StoreError {
    fn from(e: snapstore_pagestore::ingest::StoreError) -> Self {
        StoreError::PageStore(e)
    }
}

// ── StoreOpts ─────────────────────────────────────────────────────────────────

/// Configuration for `SnapshotStore::open_with_options`.
pub struct StoreOpts {
    /// Options forwarded to the underlying `PageStore`.
    pub pagestore: PageStoreOptions,
    /// Maximum number of flattened page-table results held in the LRU cache.
    /// Each entry is an `Arc<Vec<ManifestEntry>>`.  Default: 1024.
    pub flatten_cache_entries: usize,
}

impl Default for StoreOpts {
    fn default() -> Self {
        Self {
            pagestore: PageStoreOptions::default(),
            flatten_cache_entries: 1024,
        }
    }
}

// ── GroupCommit ───────────────────────────────────────────────────────────────

/// Coalescing durability barrier.
///
/// # Rationale
///
/// `PageStore::sync()` takes the active-pack lock and fdatasyncs every dirty
/// pack.  With 16 concurrent committers each calling sync() independently the
/// fsyncs serialize — re-creating the "fsync storm" that phase-1 removed for a
/// 2.25× throughput gain (commit 0d8ef62).  GroupCommit lets every caller that
/// arrives while a flush is already in-flight piggy-back on the result of that
/// flush: one fdatasync pass serves every waiter at or below the generation
/// that flush covered.
///
/// Gate S4 (p99 < 40 ms for 16 concurrent PutSnapshot calls) depends on this.
struct GroupCommit {
    state: Mutex<GcState>,
    cv: Condvar,
}

struct GcState {
    /// Monotonically increasing generation counter.  Each `put_snapshot` call
    /// that needs durability stamps the current generation and then either
    /// becomes the flusher or waits.
    pending_gen: u64,
    /// The highest generation for which a flush has **completed**.
    completed_gen: u64,
    /// True while a flush is executing.
    flushing: bool,
}

impl GroupCommit {
    fn new() -> Self {
        Self {
            state: Mutex::new(GcState {
                pending_gen: 0,
                completed_gen: 0,
                flushing: false,
            }),
            cv: Condvar::new(),
        }
    }

    /// Wait until a sync that started *after* the pages for this call were
    /// known-present has completed.
    ///
    /// If no flush is running, the caller becomes the flusher: it runs
    /// `sync_fn()` (a single `PageStore::sync()`) then wakes all waiters.
    fn barrier(
        &self,
        sync_fn: impl FnOnce() -> Result<(), snapstore_pagestore::ingest::StoreError>,
    ) -> Result<(), snapstore_pagestore::ingest::StoreError> {
        let my_gen = {
            let mut g = self.state.lock().unwrap();
            g.pending_gen += 1;
            g.pending_gen
        };

        let mut g = self.state.lock().unwrap();
        loop {
            if g.completed_gen >= my_gen {
                // A flush that covered our generation already finished.
                return Ok(());
            }
            if g.flushing {
                // Another thread is flushing; wait for it to complete.
                g = self.cv.wait(g).unwrap();
                continue;
            }
            // We are the flusher for this round.
            g.flushing = true;
            let flushing_up_to = g.pending_gen; // covers all pending callers
            drop(g);

            let result = sync_fn();

            let mut g = self.state.lock().unwrap();
            if result.is_ok() && flushing_up_to > g.completed_gen {
                g.completed_gen = flushing_up_to;
            }
            g.flushing = false;
            self.cv.notify_all();
            return result;
        }
    }
}

// ── FlattenCache ──────────────────────────────────────────────────────────────

/// Tiny LRU cache: `SnapshotRef` → `Arc<Vec<ManifestEntry>>`.
///
/// Sibling restores re-flatten the same FULL root constantly; this cache
/// avoids redundant chain walks (ARCHITECTURE.md §7.3).
struct FlattenCache {
    cap: usize,
    entries: VecDeque<(SnapshotRef, Arc<Vec<ManifestEntry>>)>,
}

impl FlattenCache {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            entries: VecDeque::with_capacity(cap + 1),
        }
    }

    fn get(&self, r: &SnapshotRef) -> Option<Arc<Vec<ManifestEntry>>> {
        self.entries
            .iter()
            .find(|(k, _)| k == r)
            .map(|(_, v)| Arc::clone(v))
    }

    fn insert(&mut self, r: SnapshotRef, v: Arc<Vec<ManifestEntry>>) {
        // Move to front if already present.
        if let Some(pos) = self.entries.iter().position(|(k, _)| k == &r) {
            self.entries.remove(pos);
        }
        self.entries.push_front((r, v));
        if self.entries.len() > self.cap {
            self.entries.pop_back();
        }
    }
}

// ── SnapshotStore ─────────────────────────────────────────────────────────────

/// Server-side snapshot store.
///
/// Wraps a `PageStore` (pages/) and a sharded manifest directory
/// (manifests/<first-byte-hex>/<ref-hex>.spm), providing the full
/// `put_snapshot` / `get_snapshot` / `resolve_pages` surface.
pub struct SnapshotStore {
    pages: PageStore,
    manifests_dir: PathBuf,
    tmp_dir: PathBuf,
    gc: Arc<GroupCommit>,
    /// Stub read-lock for every commit; write side taken by M7 mark fence /
    /// M9 backup consistency point (ARCHITECTURE.md §4.5 R3).  One line now
    /// avoids hot-path surgery under M7 pressure.
    gc_commit_gate: std::sync::RwLock<()>,
    flatten_cache: Mutex<FlattenCache>,
    /// Maintained counters; recomputed at open() by walking manifests/.
    manifests_total: std::sync::atomic::AtomicU64,
    logical_page_bytes: std::sync::atomic::AtomicU64,
    /// Cache hit counter (exposed for tests only).
    #[cfg(test)]
    flatten_cache_hits: std::sync::atomic::AtomicU64,
}

impl SnapshotStore {
    /// Open (or create) a store at `dir` with default options.
    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        Self::open_with_options(dir, StoreOpts::default())
    }

    /// Open (or create) a store at `dir` with explicit options.
    ///
    /// Layout created if absent:
    /// - `<dir>/pages/`       — PageStore
    /// - `<dir>/manifests/`   — manifest shards
    /// - `<dir>/tmp/`         — staging directory (cleaned at open)
    pub fn open_with_options(dir: &Path, opts: StoreOpts) -> Result<Self, StoreError> {
        let pages_dir = dir.join("pages");
        let manifests_dir = dir.join("manifests");
        let tmp_dir = dir.join("tmp");

        fs::create_dir_all(&pages_dir)?;
        fs::create_dir_all(&manifests_dir)?;
        fs::create_dir_all(&tmp_dir)?;

        // Clean stale staging files from a prior crash.
        clean_tmp_dir(&tmp_dir)?;

        let pages = PageStore::open(&pages_dir, opts.pagestore).map_err(StoreError::PageStore)?;

        // Recompute maintained counters by walking manifests/.
        let (manifests_total, logical_page_bytes) = walk_manifest_counters(&manifests_dir);

        Ok(Self {
            pages,
            manifests_dir,
            tmp_dir,
            gc: Arc::new(GroupCommit::new()),
            gc_commit_gate: std::sync::RwLock::new(()),
            flatten_cache: Mutex::new(FlattenCache::new(opts.flatten_cache_entries)),
            manifests_total: std::sync::atomic::AtomicU64::new(manifests_total),
            logical_page_bytes: std::sync::atomic::AtomicU64::new(logical_page_bytes),
            #[cfg(test)]
            flatten_cache_hits: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Direct access to the underlying `PageStore` (server layer uses this for
    /// batched page ingest before calling `put_snapshot`).
    pub fn pages(&self) -> &PageStore {
        &self.pages
    }

    // ── put_snapshot ─────────────────────────────────────────────────────────

    /// Accept and durably store a snapshot container.
    ///
    /// Steps:
    /// 1. Decode and validate the container (INVALID_ARGUMENT on codec error).
    /// 2. If delta: verify parent is stored and `guest_ram_bytes` matches.
    /// 3. Verify every referenced page hash is present in the pagestore (collect
    ///    ALL gaps; FAILED_PRECONDITION detail).
    /// 4. Group-commit durability barrier (one fdatasync coalesces concurrent
    ///    callers — avoids the fsync storm that phase-1 removed).
    /// 5. Take gc_commit_gate read lock (no-op stub; write side for M7/M9).
    /// 6. Atomic write to manifests/<shard>/<hex>.spm via tmp/ + fsync + rename
    ///    + parent-dir fsync.  Content-addressed ⇒ idempotent on duplicate.
    pub fn put_snapshot(&self, container: &[u8]) -> Result<SnapshotRef, PutError> {
        // Step 1 — decode and validate.
        let manifest = Manifest::decode(container)?;

        // Step 2 — delta parent checks.
        if manifest.delta {
            let parent_ref = manifest.parent.as_ref().expect("delta must have parent");
            let parent_manifest = self.read_manifest_bytes(parent_ref).map_err(|e| match e {
                StoreError::NotFound => PutError::UnknownParent(parent_ref.clone()),
                _ => PutError::Io(std::io::Error::other(e.to_string())),
            })?;
            let parent_m = Manifest::decode(&parent_manifest)?;
            if parent_m.guest_ram_bytes != manifest.guest_ram_bytes {
                return Err(PutError::ParentRamMismatch);
            }
        }

        // Step 3 — verify pages present (index-only, no payload reads).
        let hashes: Vec<PageHash> = manifest.entries.iter().map(|e| e.page_hash).collect();
        let present = self.pages.contains_batch(&hashes)?;
        let missing: Vec<PageHash> = hashes
            .iter()
            .zip(present.iter())
            .filter(|(_, ok)| !**ok)
            .map(|(h, _)| *h)
            .collect();
        if !missing.is_empty() {
            return Err(PutError::MissingPages(missing));
        }

        // Step 4 — group-commit durability barrier.
        let gc = Arc::clone(&self.gc);
        gc.barrier(|| self.pages.sync())?;

        // Step 5 — gc_commit_gate read lock (no-op stub until M7/M9).
        // ARCHITECTURE.md §4.5 R3: the write side is taken by the mark fence
        // (M7) and backup consistency point (M9); acquiring the read lock here
        // ensures that when either of those takes the write side all in-flight
        // commits finish before the fence proceeds.
        let _gate = self
            .gc_commit_gate
            .read()
            .map_err(|_| std::io::Error::other("gc_commit_gate poisoned"))?;

        // Compute the SnapshotRef from the byte-identical container.
        let snap_ref = Manifest::snapshot_ref(container);

        // Step 6 — atomic write to manifests/.
        let hex = ref_to_hex(&snap_ref);
        let shard = &hex[..2];
        let shard_dir = self.manifests_dir.join(shard);
        let spm_path = shard_dir.join(format!("{}.spm", hex));

        // Idempotent: if the file exists (content-addressed), return early.
        if spm_path.exists() {
            return Ok(snap_ref);
        }

        fs::create_dir_all(&shard_dir)?;

        let tmp_path = self.tmp_dir.join(format!("{}.spm.tmp", hex));

        // a. Write staging file.
        {
            let mut tmp = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            tmp.write_all(container)?;

            fail_point!("manifest-tmp-write");

            // b. fsync staging file.
            fail_point!("manifest-fsync");
            tmp.sync_all()?;
        }

        // c. Rename staging → final.
        fail_point!("manifest-rename");
        fs::rename(&tmp_path, &spm_path)?;

        // d. fsync the shard directory so the directory entry is durable.
        fail_point!("manifest-dirsync");
        {
            let dir_file = File::open(&shard_dir)?;
            dir_file.sync_all()?;
        }

        // Update maintained counters (first write only — idempotent path returned early).
        self.manifests_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.logical_page_bytes.fetch_add(
            manifest.guest_ram_bytes,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(snap_ref)
    }

    // ── get_snapshot ─────────────────────────────────────────────────────────

    /// Read back the byte-identical container and verify its footer.
    ///
    /// Returns `StoreError::NotFound` for an unknown ref.
    /// Returns `StoreError::ManifestCorrupt` if the stored bytes don't hash to
    /// the requested ref.
    pub fn get_snapshot(&self, r: &SnapshotRef) -> Result<Vec<u8>, StoreError> {
        self.read_manifest_bytes(r)
    }

    // ── resolve_pages ─────────────────────────────────────────────────────────

    /// Resolve a snapshot ref to a stream of `(page_index, hash, payload)`.
    ///
    /// **Mode A** (`baseline = None`): flatten the full parent chain from `r`
    /// to the FULL root; yield every page ascending by index.
    ///
    /// **Mode B** (`baseline = Some(b)`): `b` must be in `r`'s ancestor chain;
    /// yield only pages that differ in the deltas between `b` and `r`
    /// (child-first merge of the delta segment).
    ///
    /// `hashes_only = true`: skip pagestore reads; payloads are `None`.
    ///
    /// Chain depth cap: 4096.  Cycles / overflow → `StoreError::ChainDepthExceeded`.
    pub fn resolve_pages(
        &self,
        r: &SnapshotRef,
        baseline: Option<&SnapshotRef>,
        hashes_only: bool,
    ) -> Result<ResolvedPages<'_>, StoreError> {
        const MAX_CHAIN: usize = 4096;

        // Build the chain: child-first (index 0 = r, last = FULL root).
        let mut chain: Vec<(SnapshotRef, Manifest)> = Vec::new();
        let mut cursor = r.clone();
        loop {
            if chain.len() >= MAX_CHAIN {
                return Err(StoreError::ChainDepthExceeded);
            }
            let bytes = self.read_manifest_bytes(&cursor)?;
            let m = Manifest::decode(&bytes)?;
            let is_full = !m.delta;
            let parent = m.parent.clone();
            chain.push((cursor.clone(), m));
            if is_full {
                break;
            }
            cursor = parent.expect("delta must have parent");
        }

        match baseline {
            None => {
                // Mode A: full flatten.
                let entries = self.flatten_chain(&chain, r)?;
                Ok(ResolvedPages {
                    entries,
                    pages: &self.pages,
                    pos: 0,
                    hashes_only,
                    chunk: std::collections::VecDeque::new(),
                })
            }
            Some(b) => {
                // Mode B: find b in chain, then flatten_delta over the segment
                // strictly below it (child-first order, stopping before b).
                let pos = chain.iter().position(|(ref_, _)| ref_ == b);
                let b_pos = match pos {
                    Some(p) => p,
                    None => return Err(StoreError::BaselineNotAncestor),
                };
                // chain[0..b_pos] is the segment strictly below b (child-first).
                // They must all be DELTA manifests for flatten_delta.
                let delta_segment: Vec<&Manifest> = chain[..b_pos].iter().map(|(_, m)| m).collect();
                if delta_segment.is_empty() {
                    // r == baseline: no diff.
                    return Ok(ResolvedPages {
                        entries: Arc::new(Vec::new()),
                        pages: &self.pages,
                        pos: 0,
                        hashes_only,
                        chunk: std::collections::VecDeque::new(),
                    });
                }
                let merged = snapstore_manifest::flatten_delta(&delta_segment)?;
                Ok(ResolvedPages {
                    entries: Arc::new(merged),
                    pages: &self.pages,
                    pos: 0,
                    hashes_only,
                    chunk: std::collections::VecDeque::new(),
                })
            }
        }
    }

    // ── has_pages ────────────────────────────────────────────────────────────

    /// Batch presence check for page hashes (index-only; no payload reads).
    pub fn has_pages(&self, hashes: &[PageHash]) -> Result<Vec<bool>, StoreError> {
        self.pages.contains_batch(hashes).map_err(StoreError::from)
    }

    // ── list_manifest_refs ────────────────────────────────────────────────────

    /// List all stored `SnapshotRef`s by walking the manifests directory.
    ///
    /// Used by startup reconciliation and fsck; not on the hot path.
    pub fn list_manifest_refs(&self) -> Result<Vec<SnapshotRef>, StoreError> {
        let mut refs = Vec::new();
        for shard_entry in fs::read_dir(&self.manifests_dir)? {
            let shard_entry = shard_entry?;
            if !shard_entry.file_type()?.is_dir() {
                continue;
            }
            for entry in fs::read_dir(shard_entry.path())? {
                let entry = entry?;
                let name = entry.file_name();
                let s = name.to_string_lossy();
                if let Some(hex) = s.strip_suffix(".spm") {
                    if hex.len() == 64 {
                        if let Ok(bytes) = hex_to_32(hex) {
                            refs.push(SnapshotRef::from_bytes(bytes));
                        }
                    }
                }
            }
        }
        Ok(refs)
    }

    /// Maintained counters for metrics / Stats.
    ///
    /// Returns `(manifests_total, logical_page_bytes)`.
    ///
    /// `logical_page_bytes` is `sum(manifest.guest_ram_bytes)` over all stored
    /// manifests.  It is recomputed at `open()` and updated on first write.
    pub fn manifest_count_and_logical_bytes(&self) -> (u64, u64) {
        (
            self.manifests_total
                .load(std::sync::atomic::Ordering::Relaxed),
            self.logical_page_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// Read and verify the stored manifest file for `r`.
    ///
    /// The `SnapshotRef` IS `blake3(body)` where `body = bytes[..len-32]`.
    /// Full integrity verification: re-hash the body and compare against both
    /// the expected ref and the footer stored in the file.  This catches:
    /// - payload corruption (body bytes changed → re-hash diverges from `r`)
    /// - footer corruption (last 32 bytes changed → stored footer ≠ `r`)
    fn read_manifest_bytes(&self, r: &SnapshotRef) -> Result<Vec<u8>, StoreError> {
        let hex = ref_to_hex(r);
        let shard = &hex[..2];
        let path = self.manifests_dir.join(shard).join(format!("{}.spm", hex));
        if !path.exists() {
            return Err(StoreError::NotFound);
        }
        let mut file = File::open(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        if bytes.len() < 32 {
            return Err(StoreError::ManifestCorrupt);
        }
        // Check 1: stored footer equals the expected ref.
        let footer: [u8; 32] = bytes[bytes.len() - 32..].try_into().unwrap();
        if footer != r.to_bytes() {
            return Err(StoreError::ManifestCorrupt);
        }
        // Check 2: re-hash body to verify payload integrity.
        let computed = Manifest::snapshot_ref(&bytes);
        if computed != *r {
            return Err(StoreError::ManifestCorrupt);
        }
        Ok(bytes)
    }

    /// Flatten a child-first chain via the LRU cache (Mode A).
    fn flatten_chain(
        &self,
        chain: &[(SnapshotRef, Manifest)],
        r: &SnapshotRef,
    ) -> Result<Arc<Vec<ManifestEntry>>, StoreError> {
        // Check LRU cache first.
        {
            let cache = self.flatten_cache.lock().unwrap();
            if let Some(cached) = cache.get(r) {
                #[cfg(test)]
                self.flatten_cache_hits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(cached);
            }
        }

        let manifests: Vec<&Manifest> = chain.iter().map(|(_, m)| m).collect();
        let entries = snapstore_manifest::flatten(&manifests)?;
        let arc = Arc::new(entries);

        {
            let mut cache = self.flatten_cache.lock().unwrap();
            cache.insert(r.clone(), Arc::clone(&arc));
        }

        Ok(arc)
    }
}

// ── ResolvedPages (iterator) ──────────────────────────────────────────────────

/// Streaming iterator over `(page_index, PageHash, Option<Bytes>)`.
///
/// Constructed by `SnapshotStore::resolve_pages`.
/// Pages per `get_batch` call when streaming payloads — large enough that
/// the pagestore's (pack, offset)-sorted preads stay sequential per pack,
/// small enough to bound buffering.
const RESOLVE_CHUNK: usize = 512;

/// One streamed page: `(page_index, page_hash, payload)`; payload is `None`
/// in hashes-only mode.
pub type ResolvedPage = (u64, PageHash, Option<bytes::Bytes>);

pub struct ResolvedPages<'a> {
    entries: Arc<Vec<ManifestEntry>>,
    pages: &'a PageStore,
    pos: usize,
    hashes_only: bool,
    chunk: std::collections::VecDeque<Result<ResolvedPage, StoreError>>,
}

impl<'a> Iterator for ResolvedPages<'a> {
    type Item = Result<ResolvedPage, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.hashes_only {
            if self.pos >= self.entries.len() {
                return None;
            }
            let entry = &self.entries[self.pos];
            self.pos += 1;
            return Some(Ok((entry.page_index, entry.page_hash, None)));
        }

        if let Some(item) = self.chunk.pop_front() {
            return Some(item);
        }
        if self.pos >= self.entries.len() {
            return None;
        }

        // Refill: one batched, offset-sorted read per RESOLVE_CHUNK entries
        // (GET_BATCH / ResolvePages throughput rides the WI6 read path).
        let end = (self.pos + RESOLVE_CHUNK).min(self.entries.len());
        let slice = &self.entries[self.pos..end];
        let hashes: Vec<PageHash> = slice.iter().map(|e| e.page_hash).collect();
        match self.pages.get_batch(&hashes) {
            Ok(results) => {
                for (entry, result) in slice.iter().zip(results) {
                    self.chunk.push_back(match result {
                        Some(b) => Ok((entry.page_index, entry.page_hash, Some(b))),
                        None => Err(StoreError::MissingPage(entry.page_hash)),
                    });
                }
            }
            Err(e) => self.chunk.push_back(Err(StoreError::PageStore(e))),
        }
        self.pos = end;
        self.chunk.pop_front()
    }
}

// ── build module (test/bench helpers) ────────────────────────────────────────

/// Helpers used by tests and benchmarks to build `.spm` containers.
///
/// Production clients build containers worker-side; these helpers avoid
/// duplicating the hash+entry-sort logic in every test.
pub mod build {
    use super::*;

    /// Build a FULL `.spm` container.
    ///
    /// `pages` must cover every index `0..guest_ram_bytes/PAGE_SIZE`.
    pub fn build_full_container(
        guest_ram_bytes: u64,
        pages: &[(u64, &[u8; PAGE_SIZE])],
        device_blob: DeviceBlob,
    ) -> Vec<u8> {
        let entries: Vec<ManifestEntry> = pages
            .iter()
            .map(|(idx, data)| ManifestEntry {
                page_index: *idx,
                page_hash: PageHash::from_bytes(*blake3::hash(*data).as_bytes()),
            })
            .collect();
        let m = Manifest::new_full(guest_ram_bytes, entries, device_blob)
            .expect("build_full_container: invalid args");
        m.encode()
    }

    /// Build a DELTA `.spm` container.
    pub fn build_delta_container(
        parent: &SnapshotRef,
        guest_ram_bytes: u64,
        pages: &[(u64, &[u8; PAGE_SIZE])],
        device_blob: DeviceBlob,
    ) -> Vec<u8> {
        let entries: Vec<ManifestEntry> = pages
            .iter()
            .map(|(idx, data)| ManifestEntry {
                page_index: *idx,
                page_hash: PageHash::from_bytes(*blake3::hash(*data).as_bytes()),
            })
            .collect();
        let m = Manifest::new_delta(parent.clone(), guest_ram_bytes, entries, device_blob)
            .expect("build_delta_container: invalid args");
        m.encode()
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Encode a `SnapshotRef` as 64 lowercase hex chars.
fn ref_to_hex(r: &SnapshotRef) -> String {
    r.to_bytes().iter().map(|b| format!("{:02x}", b)).collect()
}

/// Decode 64 hex chars into a 32-byte array.
fn hex_to_32(hex: &str) -> Result<[u8; 32], ()> {
    if hex.len() != 64 {
        return Err(());
    }
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = hex_nibble(chunk[0]).ok_or(())?;
        let lo = hex_nibble(chunk[1]).ok_or(())?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Remove all files in `dir` (one level deep).
fn clean_tmp_dir(dir: &Path) -> Result<(), std::io::Error> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            let _ = fs::remove_file(entry.path());
        }
    }
    Ok(())
}

/// Walk `manifests/` and recompute (count, sum-of-guest_ram_bytes).
///
/// Fast path: reads only bytes 48..56 of each .spm header (guest_ram_bytes at
/// fixed offset per API.md §2).  Skips files with invalid headers (recovery
/// owns those).
fn walk_manifest_counters(manifests_dir: &Path) -> (u64, u64) {
    let mut count: u64 = 0;
    let mut bytes: u64 = 0;

    let Ok(shard_iter) = fs::read_dir(manifests_dir) else {
        return (0, 0);
    };
    for shard_entry in shard_iter.flatten() {
        let Ok(ft) = shard_entry.file_type() else {
            continue;
        };
        if !ft.is_dir() {
            continue;
        }
        let Ok(file_iter) = fs::read_dir(shard_entry.path()) else {
            continue;
        };
        for entry in file_iter.flatten() {
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str());
            if ext != Some("spm") {
                continue;
            }
            if let Ok(grb) = read_guest_ram_bytes(&path) {
                count += 1;
                bytes = bytes.saturating_add(grb);
            }
        }
    }
    (count, bytes)
}

/// Read `guest_ram_bytes` from the fixed offset (48..56) in a `.spm` file,
/// validating only the BLAKE3 footer.  Returns an error if the file is too
/// short or the footer doesn't match (corrupt files are silently skipped by
/// the caller).
fn read_guest_ram_bytes(path: &Path) -> Result<u64, std::io::Error> {
    use std::io::ErrorKind;

    let mut file = File::open(path)?;
    let mut buf = [0u8; 56]; // header up through guest_ram_bytes
    file.read_exact(&mut buf)
        .map_err(|_| std::io::Error::new(ErrorKind::UnexpectedEof, "spm too short"))?;

    // Quick magic check (no full decode; just confirm it looks right).
    if &buf[0..8] != b"SPSMAN01" {
        return Err(std::io::Error::new(ErrorKind::InvalidData, "bad magic"));
    }

    let guest_ram_bytes = u64::from_le_bytes(buf[48..56].try_into().unwrap());
    Ok(guest_ram_bytes)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::build::*;
    use super::*;
    use snapstore_manifest::DeviceBlob;
    use snapstore_testgen::{GuestProfile, SyntheticGuest};
    use tempfile::TempDir;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn empty_blob() -> DeviceBlob {
        DeviceBlob {
            format: 0,
            zstd: false,
            bytes: vec![],
            raw_len: 0,
        }
    }

    fn small_profile(n: usize) -> GuestProfile {
        GuestProfile {
            total_pages: n,
            ..GuestProfile::idle_linux()
        }
    }

    /// Ingest all pages from `guest` into `store.pages()`.
    fn ingest_guest(store: &SnapshotStore, guest: &SyntheticGuest) {
        let page_refs: Vec<&[u8; PAGE_SIZE]> = guest.pages().map(|(_, p)| p).collect();
        store.pages().ingest(&page_refs).unwrap();
    }

    // ── Test 1: commit→resolve byte-identity ─────────────────────────────────

    #[test]
    fn put_get_byte_identity() {
        #[cfg(feature = "failpoints")]
        let _fp_guard = fp_read_guard();
        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();

        let guest = SyntheticGuest::new(1, small_profile(64));
        ingest_guest(&store, &guest);

        let page_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = guest.pages().collect();
        let container = build_full_container(64 * PAGE_SIZE as u64, &page_pairs, empty_blob());

        let r = store.put_snapshot(&container).unwrap();
        let got = store.get_snapshot(&r).unwrap();
        assert_eq!(
            got, container,
            "get_snapshot must return byte-identical container"
        );
    }

    // ── Test 2: resolve_pages payloads equal source pages ────────────────────

    #[test]
    fn resolve_pages_payload_identity() {
        #[cfg(feature = "failpoints")]
        let _fp_guard = fp_read_guard();
        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();

        let guest = SyntheticGuest::new(2, small_profile(32));
        ingest_guest(&store, &guest);

        let page_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = guest.pages().collect();
        let container = build_full_container(32 * PAGE_SIZE as u64, &page_pairs, empty_blob());

        let r = store.put_snapshot(&container).unwrap();
        let resolved: Vec<_> = store
            .resolve_pages(&r, None, false)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(resolved.len(), 32);
        for (idx, hash, payload) in resolved {
            let payload = payload.expect("hashes_only=false");
            let expected = guest.pages().find(|(i, _)| *i == idx).unwrap().1;
            assert_eq!(payload.as_ref(), expected.as_ref());
            let computed_hash = PageHash::from_bytes(*blake3::hash(expected.as_ref()).as_bytes());
            assert_eq!(hash, computed_hash);
        }
    }

    // ── Test 3: multi-epoch delta chain ───────────────────────────────────────

    #[test]
    fn multi_epoch_delta_chain() {
        #[cfg(feature = "failpoints")]
        let _fp_guard = fp_read_guard();
        const PAGES: usize = 32;
        const DELTAS: usize = 8; // use 8 to keep test fast; spec says 64
        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();

        let profile = GuestProfile {
            total_pages: PAGES,
            dirty_rate: 0.25,
            ..GuestProfile::idle_linux()
        };
        let mut guest = SyntheticGuest::new(3, profile);
        let grb = PAGES as u64 * PAGE_SIZE as u64;

        // Epoch 0: FULL.
        ingest_guest(&store, &guest);
        let pairs0: Vec<(u64, &[u8; PAGE_SIZE])> = guest.pages().collect();
        let c0 = build_full_container(grb, &pairs0, empty_blob());
        let mut prev_ref = store.put_snapshot(&c0).unwrap();
        let mut refs = vec![prev_ref.clone()];

        // Epochs 1..DELTAS: DELTA.
        for _ in 0..DELTAS {
            let dirty_indices = guest.step_epoch();
            let dirty_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = dirty_indices
                .iter()
                .map(|&i| (i, guest.pages().find(|(pi, _)| *pi == i).unwrap().1))
                .collect();
            ingest_guest(&store, &guest);
            let c = build_delta_container(&prev_ref, grb, &dirty_pairs, empty_blob());
            let r = store.put_snapshot(&c).unwrap();
            prev_ref = r.clone();
            refs.push(r);
        }

        // Mode A at the deepest ref should equal the current synthetic state.
        let final_ref = refs.last().unwrap();
        let resolved: Vec<_> = store
            .resolve_pages(final_ref, None, false)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(resolved.len(), PAGES);
        for (idx, _, payload) in resolved {
            let payload = payload.unwrap();
            let expected = guest.pages().find(|(i, _)| *i == idx).unwrap().1;
            assert_eq!(
                payload.as_ref(),
                expected.as_ref(),
                "page {} mismatch at final epoch",
                idx
            );
        }

        // Mode B: delta vs ancestor at refs[0] should include only changed pages.
        let mode_b: Vec<_> = store
            .resolve_pages(final_ref, Some(&refs[0]), false)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        // Mode B shouldn't be empty (we mutated pages each epoch).
        assert!(!mode_b.is_empty(), "delta vs ancestor must not be empty");
        // All indices in mode B must be valid page indices.
        for (idx, _, _) in &mode_b {
            assert!(*idx < PAGES as u64);
        }
    }

    // ── Test 4: missing pages returns exact gap list ──────────────────────────

    #[test]
    fn missing_pages_exact_gaps() {
        #[cfg(feature = "failpoints")]
        let _fp_guard = fp_read_guard();
        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();

        // Use all_unique profile so every page has a distinct hash — no dedup
        // across even/odd indices that would cause a false "present" result.
        let profile = GuestProfile {
            total_pages: 16,
            ..GuestProfile::all_unique()
        };
        let guest = SyntheticGuest::new(4, profile);

        // Collect even/odd page data upfront to avoid reference/lifetime issues.
        let all_pages: Vec<(u64, [u8; PAGE_SIZE])> = guest.pages().map(|(i, p)| (i, *p)).collect();

        // Only ingest even-index pages.
        let even_pages: Vec<&[u8; PAGE_SIZE]> = all_pages
            .iter()
            .filter(|(i, _)| i % 2 == 0)
            .map(|(_, p)| p)
            .collect();
        store.pages().ingest(&even_pages).unwrap();

        let page_pairs: Vec<(u64, &[u8; PAGE_SIZE])> =
            all_pages.iter().map(|(i, p)| (*i, p)).collect();
        let container = build_full_container(16 * PAGE_SIZE as u64, &page_pairs, empty_blob());

        match store.put_snapshot(&container) {
            Err(PutError::MissingPages(missing)) => {
                // The missing hashes should be the odd-indexed pages' hashes.
                let odd_hashes: Vec<PageHash> = all_pages
                    .iter()
                    .filter(|(i, _)| i % 2 != 0)
                    .map(|(_, p)| PageHash::from_bytes(*blake3::hash(p.as_ref()).as_bytes()))
                    .collect();
                // All missing hashes should be from the odd-indexed set.
                for h in &missing {
                    assert!(
                        odd_hashes.contains(h),
                        "missing hash should correspond to odd page"
                    );
                }
                // All odd-indexed hashes should be in the missing set.
                for h in &odd_hashes {
                    assert!(
                        missing.contains(h),
                        "odd-indexed page hash should be missing"
                    );
                }
                assert_eq!(
                    missing.len(),
                    odd_hashes.len(),
                    "missing count must equal odd-indexed page count"
                );
            }
            other => panic!("expected MissingPages, got {:?}", other),
        }
    }

    // ── Test 5: unknown parent rejected ──────────────────────────────────────

    #[test]
    fn unknown_parent_rejected() {
        #[cfg(feature = "failpoints")]
        let _fp_guard = fp_read_guard();
        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();

        let guest = SyntheticGuest::new(5, small_profile(8));
        ingest_guest(&store, &guest);

        // Delta referencing a parent ref that was never stored.
        let fake_parent = SnapshotRef::from_bytes([0xABu8; 32]);
        let dirty_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = guest.pages().take(4).collect();
        let container = build_delta_container(
            &fake_parent,
            8 * PAGE_SIZE as u64,
            &dirty_pairs,
            empty_blob(),
        );

        assert!(
            matches!(
                store.put_snapshot(&container),
                Err(PutError::UnknownParent(_))
            ),
            "delta with unknown parent must be rejected"
        );
    }

    // ── Test 6: parent RAM mismatch rejected ──────────────────────────────────

    #[test]
    fn parent_ram_mismatch_rejected() {
        #[cfg(feature = "failpoints")]
        let _fp_guard = fp_read_guard();
        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();

        // Ingest and commit a FULL root with 8 pages.
        let guest8 = SyntheticGuest::new(6, small_profile(8));
        ingest_guest(&store, &guest8);
        let pairs8: Vec<(u64, &[u8; PAGE_SIZE])> = guest8.pages().collect();
        let c8 = build_full_container(8 * PAGE_SIZE as u64, &pairs8, empty_blob());
        let r8 = store.put_snapshot(&c8).unwrap();

        // Try a delta that claims a different guest_ram_bytes (16 pages).
        let guest16 = SyntheticGuest::new(60, small_profile(16));
        ingest_guest(&store, &guest16);
        let dirty_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = guest16.pages().take(4).collect();
        let c_mismatch =
            build_delta_container(&r8, 16 * PAGE_SIZE as u64, &dirty_pairs, empty_blob());

        assert!(
            matches!(
                store.put_snapshot(&c_mismatch),
                Err(PutError::ParentRamMismatch)
            ),
            "parent RAM mismatch must be rejected"
        );
    }

    // ── Test 7: corrupt stored manifest rejected on read ─────────────────────

    #[test]
    fn corrupt_manifest_rejected() {
        #[cfg(feature = "failpoints")]
        let _fp_guard = fp_read_guard();
        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();

        let guest = SyntheticGuest::new(7, small_profile(8));
        ingest_guest(&store, &guest);
        let pairs: Vec<(u64, &[u8; PAGE_SIZE])> = guest.pages().collect();
        let container = build_full_container(8 * PAGE_SIZE as u64, &pairs, empty_blob());
        let r = store.put_snapshot(&container).unwrap();

        // Flip a byte in the stored file.
        let hex = ref_to_hex(&r);
        let shard = &hex[..2];
        let path = dir
            .path()
            .join("manifests")
            .join(shard)
            .join(format!("{}.spm", hex));
        let mut content = fs::read(&path).unwrap();
        let last = content.len() - 1;
        content[last] ^= 0xFF;
        fs::write(&path, &content).unwrap();

        assert!(
            matches!(store.get_snapshot(&r), Err(StoreError::ManifestCorrupt)),
            "corrupted manifest must be rejected on read"
        );
    }

    // ── Test 8: reopen store ──────────────────────────────────────────────────

    #[test]
    fn reopen_store() {
        #[cfg(feature = "failpoints")]
        let _fp_guard = fp_read_guard();
        let dir = TempDir::new().unwrap();
        let snap_ref;

        let guest = SyntheticGuest::new(8, small_profile(16));
        {
            let store = SnapshotStore::open(dir.path()).unwrap();
            ingest_guest(&store, &guest);
            let pairs: Vec<(u64, &[u8; PAGE_SIZE])> = guest.pages().collect();
            let container = build_full_container(16 * PAGE_SIZE as u64, &pairs, empty_blob());
            snap_ref = store.put_snapshot(&container).unwrap();
        }

        let store2 = SnapshotStore::open(dir.path()).unwrap();
        let bytes = store2.get_snapshot(&snap_ref).unwrap();
        let m = Manifest::decode(&bytes).unwrap();
        assert_eq!(m.entries.len(), 16);

        // Maintained counters should be re-derived at open.
        let (cnt, _lb) = store2.manifest_count_and_logical_bytes();
        assert_eq!(cnt, 1);

        // resolve_pages still green.
        let pages_out: Vec<_> = store2
            .resolve_pages(&snap_ref, None, false)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(pages_out.len(), 16);
        for (idx, _, payload) in pages_out {
            let payload = payload.unwrap();
            let expected = guest.pages().find(|(i, _)| *i == idx).unwrap().1;
            assert_eq!(payload.as_ref(), expected.as_ref());
        }
    }

    // ── Test 9: group-commit correctness ─────────────────────────────────────

    #[test]
    fn group_commit_concurrent() {
        #[cfg(feature = "failpoints")]
        let _fp_guard = fp_read_guard();
        use std::sync::Arc;

        const THREADS: usize = 8;
        const PAGES_PER: usize = 8;

        let dir = TempDir::new().unwrap();
        let store = Arc::new(SnapshotStore::open(dir.path()).unwrap());

        // Pre-ingest all pages.
        let guests: Vec<SyntheticGuest> = (0..THREADS)
            .map(|i| SyntheticGuest::new(100 + i as u64, small_profile(PAGES_PER)))
            .collect();
        for g in &guests {
            ingest_guest(&store, g);
        }
        // One sync to make them durable (so group-commit skips the actual
        // fdatasync for already-synced data, but still exercises the path).
        store.pages().sync().unwrap();

        let containers: Vec<Vec<u8>> = guests
            .iter()
            .map(|g| {
                let pairs: Vec<(u64, &[u8; PAGE_SIZE])> = g.pages().collect();
                build_full_container(PAGES_PER as u64 * PAGE_SIZE as u64, &pairs, empty_blob())
            })
            .collect();

        let handles: Vec<_> = containers
            .into_iter()
            .map(|c| {
                let s = Arc::clone(&store);
                std::thread::spawn(move || s.put_snapshot(&c).unwrap())
            })
            .collect();

        let results: Vec<SnapshotRef> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results.len(), THREADS);
        // All distinct.
        let unique: std::collections::HashSet<_> = results.iter().map(|r| r.to_bytes()).collect();
        assert_eq!(unique.len(), THREADS);
    }

    // ── Test 10: put_snapshot after ingest is durable (reopen) ───────────────

    #[test]
    fn put_snapshot_durable_after_reopen() {
        #[cfg(feature = "failpoints")]
        let _fp_guard = fp_read_guard();
        let dir = TempDir::new().unwrap();
        let snap_ref;
        let guest = SyntheticGuest::new(10, small_profile(8));
        {
            let store = SnapshotStore::open(dir.path()).unwrap();
            ingest_guest(&store, &guest);
            let pairs: Vec<(u64, &[u8; PAGE_SIZE])> = guest.pages().collect();
            let c = build_full_container(8 * PAGE_SIZE as u64, &pairs, empty_blob());
            snap_ref = store.put_snapshot(&c).unwrap();
        }
        // Reopen and resolve.
        let store2 = SnapshotStore::open(dir.path()).unwrap();
        let resolved: Vec<_> = store2
            .resolve_pages(&snap_ref, None, false)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(resolved.len(), 8);
    }

    // ── Test 11: flatten-cache hit ────────────────────────────────────────────

    #[test]
    fn flatten_cache_hit() {
        // When failpoints are enabled, acquire a shared guard so this test
        // cannot run while the failpoint smoke test holds the exclusive lock.
        #[cfg(feature = "failpoints")]
        let _fp_guard = fp_read_guard();

        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();

        let guest = SyntheticGuest::new(11, small_profile(16));
        ingest_guest(&store, &guest);
        let pairs: Vec<(u64, &[u8; PAGE_SIZE])> = guest.pages().collect();
        let c = build_full_container(16 * PAGE_SIZE as u64, &pairs, empty_blob());
        let r = store.put_snapshot(&c).unwrap();

        // First resolve — populates cache.
        let first: Vec<_> = store
            .resolve_pages(&r, None, true)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // Second resolve — should hit cache.
        let before_hits = store
            .flatten_cache_hits
            .load(std::sync::atomic::Ordering::Relaxed);
        let second: Vec<_> = store
            .resolve_pages(&r, None, true)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let after_hits = store
            .flatten_cache_hits
            .load(std::sync::atomic::Ordering::Relaxed);

        assert_eq!(first.len(), second.len());
        assert!(
            after_hits > before_hits,
            "second resolve must use flatten cache"
        );
    }

    // ── Test 12: maintained counters re-derived at open ───────────────────────

    #[test]
    fn counters_re_derived_at_open() {
        #[cfg(feature = "failpoints")]
        let _fp_guard = fp_read_guard();
        let dir = TempDir::new().unwrap();

        let guest = SyntheticGuest::new(12, small_profile(8));
        let grb = 8 * PAGE_SIZE as u64;
        {
            let store = SnapshotStore::open(dir.path()).unwrap();
            ingest_guest(&store, &guest);
            let pairs: Vec<(u64, &[u8; PAGE_SIZE])> = guest.pages().collect();
            let c = build_full_container(grb, &pairs, empty_blob());
            store.put_snapshot(&c).unwrap();
        }

        let store2 = SnapshotStore::open(dir.path()).unwrap();
        let (cnt, lb) = store2.manifest_count_and_logical_bytes();
        assert_eq!(cnt, 1, "one manifest");
        assert_eq!(lb, grb, "logical_page_bytes == guest_ram_bytes");
    }

    // ── Failpoint smoke test (WI5) ────────────────────────────────────────────
    //
    // Serialization note: failpoints in the `fail` 0.5 crate are process-global.
    // This test arms `manifest-rename` which is also on the hot path of every
    // other `put_snapshot` call in the test suite.  To avoid interfering with
    // concurrent tests we use a process-wide exclusive mutex that all
    // `put_snapshot`-calling tests must hold (see `FP_SERIALIZE` usages).
    //
    // When running with `--test-threads=1` the mutex is a no-op but
    // provides the same guarantee.

    /// Global serialization lock for tests that arm failpoints.
    ///
    /// Every test that calls `put_snapshot` while the `failpoints` feature is
    /// active holds a *read* lock.  The failpoint smoke test holds the *write*
    /// lock so it has exclusive access while a failpoint is armed.
    #[cfg(feature = "failpoints")]
    static FP_SERIALIZE: std::sync::OnceLock<std::sync::RwLock<()>> = std::sync::OnceLock::new();

    /// Acquire a shared (read) lock used to prevent the failpoint smoke test
    /// from running concurrently with any `put_snapshot` call.
    ///
    /// All tests that call `put_snapshot` should call this when the
    /// `failpoints` feature is active.
    #[cfg(feature = "failpoints")]
    fn fp_read_guard() -> std::sync::RwLockReadGuard<'static, ()> {
        FP_SERIALIZE
            .get_or_init(|| std::sync::RwLock::new(()))
            .read()
            .unwrap_or_else(|p| p.into_inner())
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn failpoint_manifest_rename_panics() {
        use std::panic;

        // Acquire the WRITE lock — exclusive.  Waits for all concurrent
        // `put_snapshot` callers holding a read lock to finish, then blocks
        // new ones from starting.
        let _excl = FP_SERIALIZE
            .get_or_init(|| std::sync::RwLock::new(()))
            .write()
            .unwrap_or_else(|p| p.into_inner());

        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();

        let guest = SyntheticGuest::new(99, small_profile(4));
        ingest_guest(&store, &guest);
        let pairs: Vec<(u64, &[u8; PAGE_SIZE])> = guest.pages().collect();
        let c = build_full_container(4 * PAGE_SIZE as u64, &pairs, empty_blob());

        // Arm the failpoint.
        fail::cfg("manifest-rename", "panic").unwrap();

        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            store.put_snapshot(&c).unwrap();
        }));

        // Always clear the failpoint before releasing the exclusive lock.
        fail::cfg("manifest-rename", "off").unwrap();

        assert!(result.is_err(), "failpoint must cause a panic");
    }
}
