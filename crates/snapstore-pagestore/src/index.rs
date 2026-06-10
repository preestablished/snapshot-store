use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use parking_lot::RwLock;
use snapstore_types::{PageHash, PackId, PageLoc};

use crate::pack::PackReader;

// ── Constants ────────────────────────────────────────────────────────────────

const SHARD_COUNT: usize = 256;

// ── ShardedIndex ─────────────────────────────────────────────────────────────

pub struct ShardedIndex {
    shards: Box<[RwLock<HashMap<PageHash, PageLoc>>; SHARD_COUNT]>,
}

impl ShardedIndex {
    /// Create an empty in-memory index.
    pub fn new() -> Self {
        // Can't use array::from_fn directly with a const generics Box<[...; 256]>
        // pattern without unsafe, so build a Vec and convert.
        let shards: Vec<RwLock<HashMap<PageHash, PageLoc>>> = (0..SHARD_COUNT)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();
        // SAFETY: we just built exactly SHARD_COUNT elements.
        let boxed = shards
            .into_boxed_slice()
            .try_into()
            .unwrap_or_else(|_| panic!("shard count mismatch"));
        Self { shards: boxed }
    }

    /// Look up a page hash. Returns None if not found.
    pub fn get(&self, hash: &PageHash) -> Option<PageLoc> {
        let shard = shard_for(hash);
        self.shards[shard].read().get(hash).copied()
    }

    /// Insert a mapping. If the hash already exists, keeps the existing entry.
    /// (First writer wins.)
    pub fn insert(&self, hash: PageHash, loc: PageLoc) {
        let shard = shard_for(&hash);
        let mut guard = self.shards[shard].write();
        guard.entry(hash).or_insert(loc);
    }

    /// Batch insert many entries.
    pub fn insert_batch(&self, entries: impl IntoIterator<Item = (PageHash, PageLoc)>) {
        // Group by shard to minimise lock acquisitions.
        let mut buckets: Vec<Vec<(PageHash, PageLoc)>> = (0..SHARD_COUNT)
            .map(|_| Vec::new())
            .collect();
        for (hash, loc) in entries {
            buckets[shard_for(&hash)].push((hash, loc));
        }
        for (i, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            let mut guard = self.shards[i].write();
            for (hash, loc) in bucket {
                guard.entry(hash).or_insert(loc);
            }
        }
    }

    /// Load entries from a sidecar `.idx` file into this index.
    /// Returns the number of entries loaded, or an error if the file is malformed.
    pub fn load_sidecar(&self, path: &Path) -> Result<usize, IndexError> {
        let data = std::fs::read(path)?;

        // Minimum size: u32 entry_count (4) + u32 crc (4) = 8 bytes.
        if data.len() < 8 {
            return Err(IndexError::Truncated);
        }

        // Verify CRC over all bytes before the last 4.
        let (payload, crc_bytes) = data.split_at(data.len() - 4);
        let stored_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        let computed_crc = crc32fast::hash(payload);
        if stored_crc != computed_crc {
            return Err(IndexError::BadCrc);
        }

        // Parse entry_count.
        if payload.len() < 4 {
            return Err(IndexError::Truncated);
        }
        let entry_count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;

        // Each entry: hash(32) + pack_id(4) + offset(8) = 44 bytes.
        const ENTRY_SIZE: usize = 44;
        let entries_bytes = &payload[4..];
        if entries_bytes.len() < entry_count * ENTRY_SIZE {
            return Err(IndexError::Truncated);
        }

        let mut batch = Vec::with_capacity(entry_count);
        for i in 0..entry_count {
            let base = i * ENTRY_SIZE;
            let hash_bytes: [u8; 32] = entries_bytes[base..base + 32].try_into().unwrap();
            let pack_id = u32::from_le_bytes(
                entries_bytes[base + 32..base + 36].try_into().unwrap(),
            );
            let offset = u64::from_le_bytes(
                entries_bytes[base + 36..base + 44].try_into().unwrap(),
            );
            batch.push((
                PageHash::from_bytes(hash_bytes),
                PageLoc { pack: PackId(pack_id), offset },
            ));
        }

        self.insert_batch(batch);
        Ok(entry_count)
    }

    /// Write a sidecar `.idx` file for the given pack's entries.
    pub fn write_sidecar(&self, path: &Path, pack_id: PackId) -> Result<(), IndexError> {
        // Collect all entries belonging to this pack.
        let mut entries: Vec<(PageHash, PageLoc)> = Vec::new();
        for shard in self.shards.iter() {
            let guard = shard.read();
            for (&hash, &loc) in guard.iter() {
                if loc.pack == pack_id {
                    entries.push((hash, loc));
                }
            }
        }

        // Sort by hash (lexicographic on the 32 bytes).
        entries.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));

        // Encode.
        const ENTRY_SIZE: usize = 44;
        let entry_count = entries.len() as u32;
        let mut buf = Vec::with_capacity(4 + entries.len() * ENTRY_SIZE + 4);

        buf.write_all(&entry_count.to_le_bytes()).unwrap();
        for (hash, loc) in &entries {
            buf.write_all(hash.as_bytes()).unwrap();
            buf.write_all(&loc.pack.0.to_le_bytes()).unwrap();
            buf.write_all(&loc.offset.to_le_bytes()).unwrap();
        }

        // CRC over all preceding bytes.
        let crc = crc32fast::hash(&buf);
        buf.write_all(&crc.to_le_bytes()).unwrap();

        // Atomic write: write to a temp file then rename.
        let tmp_path = path.with_extension("idx.tmp");
        {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            file.write_all(&buf)?;
            file.flush()?;
            file.sync_data()?;
        }
        std::fs::rename(&tmp_path, path)?;

        Ok(())
    }

    /// Total number of entries across all shards.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for ShardedIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

#[inline]
fn shard_for(hash: &PageHash) -> usize {
    hash.as_bytes()[0] as usize
}

// ── rebuild_from_pack ─────────────────────────────────────────────────────────

/// Rebuild the index for a single pack by scanning all its records.
/// Used when a sidecar is missing or corrupt.
pub fn rebuild_from_pack(
    pack_path: &Path,
    pack_id: PackId,
    is_sealed: bool,
) -> Result<Vec<(PageHash, PageLoc)>, IndexError> {
    let records = if is_sealed {
        let reader = PackReader::open(pack_path, pack_id)?;
        reader.scan()?
    } else {
        let (reader, _) = PackReader::open_unsealed(pack_path, pack_id)?;
        reader.scan()?
    };

    let entries = records
        .into_iter()
        .map(|(offset, hash)| (hash, PageLoc { pack: pack_id, offset }))
        .collect();

    Ok(entries)
}

// ── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("sidecar CRC mismatch")]
    BadCrc,
    #[error("sidecar truncated")]
    Truncated,
    #[error("pack error: {0}")]
    Pack(#[from] crate::pack::PackError),
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use snapstore_types::{PageHash, PackId, PageLoc, PAGE_SIZE};
    use crate::pack::PackWriter;

    fn make_hash(seed: u8) -> PageHash {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        bytes[1] = seed.wrapping_mul(7);
        bytes[2] = seed.wrapping_add(3);
        PageHash::from_bytes(bytes)
    }

    fn make_loc(pack: u32, offset: u64) -> PageLoc {
        PageLoc { pack: PackId(pack), offset }
    }

    fn make_page(seed: u8) -> [u8; PAGE_SIZE] {
        let mut p = [0u8; PAGE_SIZE];
        for (i, b) in p.iter_mut().enumerate() {
            *b = seed.wrapping_add(i as u8);
        }
        p
    }

    fn hash_page(page: &[u8; PAGE_SIZE]) -> PageHash {
        PageHash::from_bytes(*blake3::hash(page).as_bytes())
    }

    // ── Test 1: Basic get/insert ──────────────────────────────────────────────
    #[test]
    fn basic_get_insert() {
        let idx = ShardedIndex::new();

        let h1 = make_hash(1);
        let h2 = make_hash(2);
        let h3 = make_hash(255);

        let loc1 = make_loc(0, 20);
        let loc2 = make_loc(1, 4153);
        let loc3 = make_loc(0, 8286);

        idx.insert(h1, loc1);
        idx.insert(h2, loc2);
        idx.insert(h3, loc3);

        assert_eq!(idx.get(&h1), Some(loc1));
        assert_eq!(idx.get(&h2), Some(loc2));
        assert_eq!(idx.get(&h3), Some(loc3));

        // Unknown hash returns None.
        let unknown = make_hash(42);
        assert_eq!(idx.get(&unknown), None);

        assert_eq!(idx.len(), 3);

        // First-writer-wins: re-inserting with different loc should not overwrite.
        let loc1_alt = make_loc(5, 9999);
        idx.insert(h1, loc1_alt);
        assert_eq!(idx.get(&h1), Some(loc1), "first writer should win");
    }

    // ── Test 2: Concurrent insert ─────────────────────────────────────────────
    #[test]
    fn concurrent_insert() {
        let idx = Arc::new(ShardedIndex::new());
        let num_threads = 8usize;
        let entries_per_thread = 1000usize;

        // Pre-generate all hashes deterministically so we can verify them later.
        // Thread t inserts entries with "thread-unique" hashes.
        let all_hashes: Vec<Vec<PageHash>> = (0..num_threads)
            .map(|t| {
                (0..entries_per_thread)
                    .map(|i| {
                        let mut bytes = [0u8; 32];
                        bytes[0] = t as u8;
                        bytes[1] = (i & 0xFF) as u8;
                        bytes[2] = ((i >> 8) & 0xFF) as u8;
                        bytes[3] = t.wrapping_mul(37) as u8;
                        PageHash::from_bytes(bytes)
                    })
                    .collect()
            })
            .collect();

        let mut handles = Vec::new();
        for t in 0..num_threads {
            let idx_clone = Arc::clone(&idx);
            let hashes = all_hashes[t].clone();
            let handle = std::thread::spawn(move || {
                for (i, hash) in hashes.into_iter().enumerate() {
                    let loc = PageLoc { pack: PackId(t as u32), offset: i as u64 * 4133 };
                    idx_clone.insert(hash, loc);
                }
            });
            handles.push(handle);
        }
        for h in handles {
            h.join().unwrap();
        }

        // Verify all entries are findable.
        for t in 0..num_threads {
            for (i, hash) in all_hashes[t].iter().enumerate() {
                let loc = idx.get(hash).expect("entry should be present");
                assert_eq!(loc.pack, PackId(t as u32));
                assert_eq!(loc.offset, i as u64 * 4133);
            }
        }

        assert_eq!(idx.len(), num_threads * entries_per_thread);
    }

    // ── Test 3: Sidecar round-trip ────────────────────────────────────────────
    #[test]
    fn sidecar_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let sidecar_path = dir.path().join("pack-00000000.idx");

        let idx = ShardedIndex::new();
        let pack_id = PackId(0);

        // Insert entries for pack 0 (and one for pack 1 to ensure filtering works).
        for i in 0u8..20 {
            let h = make_hash(i);
            idx.insert(h, make_loc(0, i as u64 * 4133));
        }
        idx.insert(make_hash(200), make_loc(1, 42)); // different pack — should not appear

        idx.write_sidecar(&sidecar_path, pack_id).unwrap();

        // Load into a fresh index.
        let idx2 = ShardedIndex::new();
        let count = idx2.load_sidecar(&sidecar_path).unwrap();
        assert_eq!(count, 20, "should load exactly 20 entries for pack 0");

        // Verify all pack-0 entries are present.
        for i in 0u8..20 {
            let h = make_hash(i);
            let loc = idx2.get(&h).expect("entry should be in loaded index");
            assert_eq!(loc.pack, PackId(0));
            assert_eq!(loc.offset, i as u64 * 4133);
        }

        // Pack-1 entry should not be in the new index.
        assert_eq!(idx2.get(&make_hash(200)), None);
    }

    // ── Test 4: Sidecar CRC mismatch ──────────────────────────────────────────
    #[test]
    fn sidecar_crc_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let sidecar_path = dir.path().join("pack-00000001.idx");

        let idx = ShardedIndex::new();
        for i in 0u8..5 {
            idx.insert(make_hash(i), make_loc(1, i as u64 * 100));
        }
        idx.write_sidecar(&sidecar_path, PackId(1)).unwrap();

        // Read the file and flip a byte in the entry data (after the 4-byte entry_count).
        let mut data = std::fs::read(&sidecar_path).unwrap();
        // Flip a byte in the first entry (byte 4 is the start of entry data).
        let flip_pos = 4;
        data[flip_pos] ^= 0xFF;
        std::fs::write(&sidecar_path, &data).unwrap();

        let idx2 = ShardedIndex::new();
        let result = idx2.load_sidecar(&sidecar_path);
        assert!(
            matches!(result, Err(IndexError::BadCrc)),
            "expected BadCrc, got {:?}",
            result
        );
    }

    // ── Test 5: Missing sidecar falls back to rebuild ─────────────────────────
    #[test]
    fn rebuild_from_pack_test() {
        let dir = tempfile::tempdir().unwrap();
        let pack_path = dir.path().join("pack-00000005.spk");
        let pack_id = PackId(5);

        // Write 6 pages to a pack and seal it.
        let mut expected: Vec<(PageHash, u64)> = Vec::new();
        {
            let mut w = PackWriter::create(&pack_path, pack_id, 0).unwrap();
            for seed in 0u8..6 {
                let page = make_page(seed);
                let hash = hash_page(&page);
                let off = w.append(&hash, &page).unwrap();
                expected.push((hash, off));
            }
            w.seal().unwrap();
        }

        // Rebuild index from the pack (no sidecar).
        let entries = rebuild_from_pack(&pack_path, pack_id, true).unwrap();

        assert_eq!(entries.len(), 6, "should find 6 records");

        // Build a lookup map from what rebuild returned.
        let rebuilt: HashMap<PageHash, PageLoc> = entries.into_iter().collect();

        for (hash, expected_off) in &expected {
            let loc = rebuilt.get(hash).expect("hash should be present after rebuild");
            assert_eq!(loc.pack, pack_id);
            assert_eq!(loc.offset, *expected_off);
        }
    }
}
