// ── Read-handle LRU cache ─────────────────────────────────────────────────────
//
// Sealed packs are immutable: once written and footed, records are never
// rewritten.  An open file descriptor to a sealed pack remains valid even after
// `seal_no_sync` appends a footer — because records live at fixed offsets below
// the body_end boundary, and the footer is only *appended* past them.  pread
// into record offsets therefore stays correct across rotation.
//
// The cache holds `Arc<std::fs::File>` handles so callers can clone a handle out
// of the map cheaply (Arc::clone is just a ref-count bump) and then perform
// lock-free pread calls on their own clone without holding the cache lock.
//
// Concurrency model: a single `parking_lot::Mutex<LruHandleMap>` guards the
// map.  Reads proceed lock-free on the cloned `Arc<File>` — the lock is only
// held during the brief cache lookup/insert, not during I/O.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use snapstore_types::PackId;

use crate::pack::PackError;

// ── LruHandleMap ──────────────────────────────────────────────────────────────

/// Internal state of the LRU.  Uses a HashMap + a generation counter to track
/// recency without a doubly-linked list.  Eviction walks the map to find the
/// minimum generation — O(capacity) but capacity is small (≤ 256 by default)
/// and eviction is rare.
struct LruHandleMap {
    entries: HashMap<PackId, (Arc<std::fs::File>, u64)>,
    generation: u64,
    capacity: usize,
}

impl LruHandleMap {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity + 1),
            generation: 0,
            capacity,
        }
    }

    /// Return a clone of the cached handle (updating its recency), or None.
    fn get(&mut self, pack: PackId) -> Option<Arc<std::fs::File>> {
        self.generation = self.generation.saturating_add(1);
        let gen = self.generation;
        self.entries.get_mut(&pack).map(|(file, g)| {
            *g = gen;
            Arc::clone(file)
        })
    }

    /// Insert a handle, evicting the LRU entry if at capacity.
    fn insert(&mut self, pack: PackId, file: Arc<std::fs::File>) {
        if self.entries.contains_key(&pack) {
            // Already present; update generation.
            self.generation = self.generation.saturating_add(1);
            let gen = self.generation;
            self.entries.entry(pack).and_modify(|(_, g)| *g = gen);
            return;
        }

        if self.entries.len() >= self.capacity {
            // Evict the entry with the smallest (oldest) generation.
            if let Some(&victim) = self
                .entries
                .iter()
                .min_by_key(|(_, (_, g))| *g)
                .map(|(k, _)| k)
            {
                self.entries.remove(&victim);
            }
        }

        self.generation = self.generation.saturating_add(1);
        let gen = self.generation;
        self.entries.insert(pack, (file, gen));
    }

    /// Remove a handle (e.g. for M7 GC invalidation or stale-entry cleanup).
    fn remove(&mut self, pack: PackId) {
        self.entries.remove(&pack);
    }
}

// ── ReadHandleCache ───────────────────────────────────────────────────────────

pub struct ReadHandleCache {
    map: Mutex<LruHandleMap>,
    /// Base directory of the page store (for constructing pack file paths).
    dir: PathBuf,
}

impl ReadHandleCache {
    pub fn new(dir: PathBuf, capacity: usize) -> Self {
        Self {
            map: Mutex::new(LruHandleMap::new(capacity.max(1))),
            dir,
        }
    }

    /// Get or open a cached read handle for a *sealed* pack.
    ///
    /// Returns an `Arc<File>` that the caller can pread without holding any lock.
    ///
    /// Uses the fast open path (header-only validation) because sealed packs'
    /// sidecars were verified at startup — a full body scan on every cache-miss
    /// would be prohibitively slow.
    pub fn get_or_open(&self, pack: PackId) -> Result<Arc<std::fs::File>, PackError> {
        // Fast path: already cached.
        {
            let mut map = self.map.lock();
            if let Some(handle) = map.get(pack) {
                return Ok(handle);
            }
        }

        // Slow path: open the file and insert into cache.
        let path = pack_path(&self.dir, pack);
        let file = crate::pack::PackReader::open_sealed_fast(&path, pack)?;
        let arc = Arc::new(file);

        {
            let mut map = self.map.lock();
            // Another thread may have beaten us; that's fine — insert wins
            // (generation bump keeps the fresh handle most-recent).
            map.insert(pack, Arc::clone(&arc));
        }

        Ok(arc)
    }

    /// Evict a cached handle for `pack`.
    ///
    /// M7 GC calls this before unlinking a pack file so the next reader opens a
    /// fresh fd to the repointed location.
    pub fn invalidate(&self, pack: PackId) {
        self.map.lock().remove(pack);
    }

    /// Insert or update a handle explicitly (e.g. immediately after rotation so
    /// the freshly-sealed pack is available without a re-open).
    pub fn insert(&self, pack: PackId, file: Arc<std::fs::File>) {
        self.map.lock().insert(pack, file);
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn pack_path(dir: &Path, pack_id: PackId) -> PathBuf {
    dir.join(format!("pack-{:08x}.spk", pack_id.0))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pack::PackWriter;
    use snapstore_types::{PackId, PAGE_SIZE};
    use tempfile::TempDir;

    fn make_and_seal_pack(dir: &Path, pack_id: PackId, n_pages: usize) -> Vec<u64> {
        let path = dir.join(format!("pack-{:08x}.spk", pack_id.0));
        let mut offsets = Vec::new();
        let mut w = PackWriter::create(&path, pack_id, 0).unwrap();
        for i in 0..n_pages {
            let mut page = [0u8; PAGE_SIZE];
            page[0] = i as u8;
            page[1] = (pack_id.0 & 0xFF) as u8;
            let hash = snapstore_types::PageHash::from_bytes(*blake3::hash(&page).as_bytes());
            let off = w.append(&hash, &page).unwrap();
            offsets.push(off);
        }
        w.seal().unwrap();
        offsets
    }

    // ── Test: cache hit avoids extra open ────────────────────────────────────
    #[test]
    fn cache_get_or_open() {
        let dir = TempDir::new().unwrap();
        make_and_seal_pack(dir.path(), PackId(0), 3);

        let cache = ReadHandleCache::new(dir.path().to_path_buf(), 4);
        let h1 = cache.get_or_open(PackId(0)).unwrap();
        let h2 = cache.get_or_open(PackId(0)).unwrap();
        // Both should be the same Arc (same underlying pointer).
        assert!(Arc::ptr_eq(&h1, &h2), "second lookup should be a cache hit");
    }

    // ── Test: LRU eviction with cap=2, 3 packs ───────────────────────────────
    #[test]
    fn lru_eviction_correctness() {
        let dir = TempDir::new().unwrap();
        // Create 3 sealed packs with one unique page each.
        let mut all_pages: Vec<([u8; PAGE_SIZE], u64)> = Vec::new();
        for pid in 0u32..3 {
            let path = dir.path().join(format!("pack-{:08x}.spk", pid));
            let mut page = [0u8; PAGE_SIZE];
            page[0] = pid as u8;
            page[1] = 0xAB;
            let hash = snapstore_types::PageHash::from_bytes(*blake3::hash(&page).as_bytes());
            let mut w = PackWriter::create(&path, PackId(pid), 0).unwrap();
            let off = w.append(&hash, &page).unwrap();
            w.seal().unwrap();
            all_pages.push((page, off));
        }

        // Cache capacity = 2: only 2 handles fit at once.
        let cache = ReadHandleCache::new(dir.path().to_path_buf(), 2);

        // Open packs 0 and 1 — fills cache.
        cache.get_or_open(PackId(0)).unwrap();
        cache.get_or_open(PackId(1)).unwrap();

        // Touch pack 1 again to make it more recent than pack 0.
        cache.get_or_open(PackId(1)).unwrap();

        // Open pack 2 — should evict pack 0 (LRU).
        cache.get_or_open(PackId(2)).unwrap();

        // All reads must still return correct data regardless of eviction.
        use crate::pack::PackReader;
        for (pid, (page, off)) in all_pages.iter().enumerate() {
            let expected_hash =
                snapstore_types::PageHash::from_bytes(*blake3::hash(page).as_bytes());
            let handle = cache.get_or_open(PackId(pid as u32)).unwrap();
            let data = PackReader::read_at_from_file(&handle, *off, &expected_hash).unwrap();
            assert_eq!(data.as_ref(), page.as_ref());
        }
    }

    // ── Test: invalidate clears the entry ────────────────────────────────────
    #[test]
    fn invalidate_reopens() {
        let dir = TempDir::new().unwrap();
        let mut page = [0u8; PAGE_SIZE];
        page[0] = 0x42;
        let hash = snapstore_types::PageHash::from_bytes(*blake3::hash(&page).as_bytes());
        {
            let path = dir.path().join("pack-00000000.spk");
            let mut w = PackWriter::create(&path, PackId(0), 0).unwrap();
            w.append(&hash, &page).unwrap();
            w.seal().unwrap();
        }

        let cache = ReadHandleCache::new(dir.path().to_path_buf(), 4);
        let h1 = cache.get_or_open(PackId(0)).unwrap();
        cache.invalidate(PackId(0));
        let h2 = cache.get_or_open(PackId(0)).unwrap();
        // After invalidation, a new handle must be opened (different Arc allocation).
        assert!(
            !Arc::ptr_eq(&h1, &h2),
            "after invalidation, a fresh handle should be returned"
        );
    }
}
