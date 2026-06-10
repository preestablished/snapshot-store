#![forbid(unsafe_code)]

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use snapstore_manifest::{DeviceState, Manifest, MemoryMap, MemoryRegion};
use snapstore_pagestore::ingest::{PageStore, StoreError as PageStoreError, StoreOptions};
use snapstore_types::{PageHash, SnapshotRef, PAGE_SIZE};

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("page store error: {0}")]
    PageStore(#[from] PageStoreError),
    #[error("manifest decode error: {0}")]
    Decode(#[from] snapstore_manifest::DecodeError),
    #[error("manifest build error: {0}")]
    ManifestBuild(#[from] snapstore_manifest::ManifestError),
    #[error("snapshot not found")]
    NotFound,
    #[error("manifest file is corrupt (hash mismatch)")]
    ManifestCorrupt,
    #[error("page missing from store")]
    MissingPage(PageHash),
    #[error("metadata DB error: {0}")]
    Meta(snapstore_meta::MetaError),
}

// ── GuestImage types ──────────────────────────────────────────────────────────

/// A borrowed view of one contiguous guest-physical memory region.
pub struct MemoryRegionView<'a> {
    /// Guest-physical base address of this region.
    pub gpa: u64,
    /// One entry per page (each page is PAGE_SIZE bytes).
    pub pages: Vec<&'a [u8; PAGE_SIZE]>,
}

/// A borrow-view of a guest's complete state at one point in time.
pub struct GuestImage<'a> {
    pub icount: u64,
    pub virtual_ns: u64,
    pub parent: Option<SnapshotRef>,
    pub regions: Vec<MemoryRegionView<'a>>,
    pub devices: Vec<DeviceStateOwned>,
}

/// Owned device state (mirrors `snapstore_manifest::DeviceState` but owned).
pub struct DeviceStateOwned {
    pub kind: String,
    pub blob: Vec<u8>,
}

// ── SnapshotStore ─────────────────────────────────────────────────────────────

pub struct SnapshotStore {
    pages: PageStore,
    manifests_dir: PathBuf,
}

impl SnapshotStore {
    /// Open (or create) a snapshot store rooted at `dir`.
    ///
    /// Creates two subdirectories:
    ///   `dir/pages/`     — PageStore root
    ///   `dir/manifests/` — one `.smf` file per committed SnapshotRef
    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        let pages_dir = dir.join("pages");
        let manifests_dir = dir.join("manifests");

        fs::create_dir_all(&pages_dir)?;
        fs::create_dir_all(&manifests_dir)?;

        let pages = PageStore::open(&pages_dir, StoreOptions::default())?;

        Ok(Self {
            pages,
            manifests_dir,
        })
    }

    /// Commit a guest snapshot, returning its content-addressed `SnapshotRef`.
    ///
    /// Idempotent: committing the same state twice returns the same ref.
    ///
    /// If `meta_db` is `Some`, the committed snapshot will be registered in the
    /// metadata database after the manifest is durably written.
    pub fn commit(
        &self,
        guest: &GuestImage<'_>,
        meta_db: Option<&snapstore_meta::MetaDb>,
    ) -> Result<SnapshotRef, StoreError> {
        // 1. Ingest all pages region-by-region; collect outcomes for manifest building.
        let mut region_hashes: Vec<Vec<PageHash>> = Vec::with_capacity(guest.regions.len());
        let mut total_new_pages: u64 = 0;
        let mut total_page_count: u64 = 0;

        for region in &guest.regions {
            if region.pages.is_empty() {
                region_hashes.push(Vec::new());
                continue;
            }
            let outcomes = self.pages.ingest(&region.pages)?;
            total_new_pages += outcomes.iter().filter(|o| o.newly_written).count() as u64;
            total_page_count += outcomes.len() as u64;
            let hashes: Vec<PageHash> = outcomes.into_iter().map(|o| o.hash).collect();
            region_hashes.push(hashes);
        }

        // 2. Durability barrier — pages must be durable before we record the ref.
        self.pages.sync()?;

        // 3. Build the Manifest.
        let memory_regions: Vec<MemoryRegion> = guest
            .regions
            .iter()
            .zip(region_hashes.into_iter())
            .map(|(region, hashes)| MemoryRegion {
                gpa: region.gpa,
                pages: hashes,
            })
            .collect();

        let memory = MemoryMap {
            page_size: PAGE_SIZE as u32,
            regions: memory_regions,
        };

        let devices: Vec<DeviceState> = guest
            .devices
            .iter()
            .map(|d| DeviceState {
                kind: d.kind.clone(),
                blob: d.blob.clone(),
            })
            .collect();

        let manifest = Manifest::new(
            guest.parent.clone(),
            guest.icount,
            guest.virtual_ns,
            memory,
            devices,
        )?;

        // 4. Encode and compute the SnapshotRef.
        let encoded = manifest.encode();
        let snap_ref = manifest.compute_ref();

        // 5. Store manifest durably using atomic write.
        let hex = hex_ref(&snap_ref);
        let smf_path = self.manifests_dir.join(format!("{}.smf", hex));

        // If the file already exists (same state committed twice), it's idempotent.
        // The MetaDb record was already registered on the first commit; skip re-registration
        // to avoid a created_at timestamp mismatch triggering ConflictingRegister.
        if smf_path.exists() {
            return Ok(snap_ref);
        }

        let tmp_path = self.manifests_dir.join(format!("{}.smf.tmp", hex));

        // a. Write to temp file.
        {
            let mut tmp_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            tmp_file.write_all(&encoded)?;
            tmp_file.flush()?;
            // b. fsync the temp file.
            tmp_file.sync_all()?;
        }

        // c. Atomic rename temp → final.
        fs::rename(&tmp_path, &smf_path)?;

        // d. fsync the manifests/ directory so the directory entry is durable.
        {
            let dir_file = File::open(&self.manifests_dir)?;
            dir_file.sync_all()?;
        }

        // 6. Register with MetaDb after manifest is durable.
        if let Some(db) = meta_db {
            let record = snapstore_meta::SnapshotRecord {
                r: snap_ref.clone(),
                parent: guest.parent.clone(),
                icount: guest.icount,
                virtual_ns: guest.virtual_ns,
                created_at: unix_now_nanos(),
                label: None,
                page_count: total_page_count,
                new_pages: total_new_pages,
            };
            db.register(&record).map_err(StoreError::Meta)?;
        }

        Ok(snap_ref)
    }

    /// Resolve a `SnapshotRef` to its `Manifest`.
    ///
    /// Returns `StoreError::NotFound` if the ref is unknown.
    /// Returns `StoreError::ManifestCorrupt` if the file hash does not match the ref.
    pub fn resolve(&self, r: &SnapshotRef) -> Result<Manifest, StoreError> {
        let hex = hex_ref(r);
        let smf_path = self.manifests_dir.join(format!("{}.smf", hex));

        if !smf_path.exists() {
            return Err(StoreError::NotFound);
        }

        let mut file = File::open(&smf_path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        // Verify the file content matches the claimed ref.
        let actual_hash = blake3::hash(&bytes);
        let expected = r.to_bytes();
        if actual_hash.as_bytes() != &expected {
            return Err(StoreError::ManifestCorrupt);
        }

        let manifest = Manifest::decode(&bytes)?;
        Ok(manifest)
    }

    /// Iterate over all (gpa, page_bytes) pairs described by `manifest`.
    ///
    /// Pages are yielded in region order, then page order within each region.
    pub fn read_memory<'a>(
        &'a self,
        m: &'a Manifest,
    ) -> impl Iterator<Item = Result<(u64, bytes::Bytes), StoreError>> + 'a {
        let page_size = m.memory.page_size as u64;

        m.memory.regions.iter().flat_map(move |region| {
            let base_gpa = region.gpa;
            region.pages.iter().enumerate().map(move |(page_idx, hash)| {
                let gpa = base_gpa + (page_idx as u64 * page_size);
                match self.pages.get(hash)? {
                    Some(data) => Ok((gpa, data)),
                    None => Err(StoreError::MissingPage(*hash)),
                }
            })
        })
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn hex_ref(r: &SnapshotRef) -> String {
    r.to_bytes()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>()
}

fn unix_now_nanos() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use snapstore_testgen::{GuestProfile, SyntheticGuest};
    use tempfile::TempDir;

    /// Build a `GuestImage` from a `SyntheticGuest`, with an optional parent ref.
    fn guest_image_from_synthetic<'a>(
        guest: &'a SyntheticGuest,
        icount: u64,
        virtual_ns: u64,
        parent: Option<SnapshotRef>,
    ) -> GuestImage<'a> {
        let raw_regions = guest.as_regions();
        let regions = raw_regions
            .into_iter()
            .map(|(gpa, pages)| MemoryRegionView { gpa, pages })
            .collect();

        GuestImage {
            icount,
            virtual_ns,
            parent,
            regions,
            devices: vec![],
        }
    }

    fn small_profile(total_pages: usize) -> GuestProfile {
        GuestProfile {
            total_pages,
            ..GuestProfile::idle_linux()
        }
    }

    // ── Test 1: commit + resolve + read_memory byte identity ─────────────────

    #[test]
    fn commit_resolve_byte_identity() {
        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();

        let profile = small_profile(128);
        let guest = SyntheticGuest::new(42, profile);
        let image = guest_image_from_synthetic(&guest, 42, 100, None);

        let snap_ref = store.commit(&image, None).unwrap();
        let manifest = store.resolve(&snap_ref).unwrap();

        // Build expected map: gpa -> page bytes from the guest.
        // as_regions returns a single region at gpa=0.
        let raw_regions = guest.as_regions();
        let mut expected: std::collections::HashMap<u64, &[u8; PAGE_SIZE]> =
            std::collections::HashMap::new();
        for (base_gpa, pages) in &raw_regions {
            for (idx, page) in pages.iter().enumerate() {
                let gpa = base_gpa + (idx as u64 * PAGE_SIZE as u64);
                expected.insert(gpa, page);
            }
        }

        // Read back and verify every page.
        for result in store.read_memory(&manifest) {
            let (gpa, bytes) = result.unwrap();
            let expected_page = expected
                .get(&gpa)
                .unwrap_or_else(|| panic!("unexpected gpa {:#x}", gpa));
            assert_eq!(
                bytes.as_ref(),
                expected_page.as_ref(),
                "page mismatch at gpa {:#x}",
                gpa
            );
        }
    }

    // ── Test 2: multi-epoch dedup ─────────────────────────────────────────────

    #[test]
    fn multi_epoch_dedup() {
        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();

        let profile = small_profile(128);
        let mut guest = SyntheticGuest::new(99, profile);

        // Capture epoch-0 page data before mutation.
        let epoch0_data: Vec<[u8; PAGE_SIZE]> = guest
            .pages()
            .map(|(_, p)| *p)
            .collect();

        // Commit epoch 0.
        let image0 = guest_image_from_synthetic(&guest, 0, 0, None);
        let ref0 = store.commit(&image0, None).unwrap();

        // Count newly-written pages in epoch 0.
        // (We can't easily intercept outcomes here; just verify correctness of dedup
        //  by checking that the second commit of the same image writes 0 new pages
        //  indirectly via the same-state-twice test below.)

        // Advance one epoch (~5% dirty).
        guest.step_epoch();

        // Commit epoch 1 with parent = ref0.
        let image1 = guest_image_from_synthetic(&guest, 1, 1000, Some(ref0.clone()));
        let ref1 = store.commit(&image1, None).unwrap();

        // Both refs must be distinct.
        assert_ne!(ref0, ref1, "epoch 0 and epoch 1 must have different refs");

        // Both refs must resolve.
        let manifest0 = store.resolve(&ref0).unwrap();
        let manifest1 = store.resolve(&ref1).unwrap();

        // Epoch 1 manifest must record ref0 as parent.
        assert_eq!(manifest1.parent, Some(ref0.clone()));

        // Re-read epoch 0 and verify it still matches original data.
        let epoch0_gpa_map: std::collections::HashMap<u64, [u8; PAGE_SIZE]> = (0..128)
            .map(|i| (i as u64 * PAGE_SIZE as u64, epoch0_data[i]))
            .collect();

        for result in store.read_memory(&manifest0) {
            let (gpa, bytes) = result.unwrap();
            let expected = epoch0_gpa_map
                .get(&gpa)
                .unwrap_or_else(|| panic!("unexpected gpa {:#x} in epoch 0", gpa));
            assert_eq!(
                bytes.as_ref(),
                expected.as_ref(),
                "epoch 0 data changed at gpa {:#x}",
                gpa
            );
        }

        // Epoch 1 manifest should be accessible and have 128 pages total.
        let total_pages1: usize = manifest1.memory.regions.iter().map(|r| r.pages.len()).sum();
        assert_eq!(total_pages1, 128);
    }

    // ── Test 3: reopen store ──────────────────────────────────────────────────

    #[test]
    fn reopen_store() {
        let dir = TempDir::new().unwrap();

        let profile = small_profile(64);
        let guest = SyntheticGuest::new(7, profile);

        let snap_ref;
        let epoch_data: Vec<[u8; PAGE_SIZE]> = guest.pages().map(|(_, p)| *p).collect();

        {
            let store = SnapshotStore::open(dir.path()).unwrap();
            let image = guest_image_from_synthetic(&guest, 10, 500, None);
            snap_ref = store.commit(&image, None).unwrap();
        }
        // Store is dropped here.

        // Reopen the same directory.
        let store2 = SnapshotStore::open(dir.path()).unwrap();

        // resolve must still work.
        let manifest = store2.resolve(&snap_ref).unwrap();

        // read_memory must return correct bytes.
        for result in store2.read_memory(&manifest) {
            let (gpa, bytes) = result.unwrap();
            let page_idx = (gpa / PAGE_SIZE as u64) as usize;
            assert_eq!(
                bytes.as_ref(),
                epoch_data[page_idx].as_ref(),
                "byte mismatch after reopen at gpa {:#x}",
                gpa
            );
        }
    }

    // ── Test 4: same state committed twice returns same ref ───────────────────

    #[test]
    fn same_state_twice() {
        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();

        let profile = small_profile(64);
        let guest = SyntheticGuest::new(55, profile);

        let image1 = guest_image_from_synthetic(&guest, 1, 1, None);
        let image2 = guest_image_from_synthetic(&guest, 1, 1, None);

        let ref1 = store.commit(&image1, None).unwrap();
        let ref2 = store.commit(&image2, None).unwrap();

        assert_eq!(ref1, ref2, "same state must produce the same SnapshotRef");

        // resolve must work on the returned ref.
        let manifest = store.resolve(&ref1).unwrap();
        assert_eq!(manifest.icount, 1);
    }

    // ── Test 5: manifest corruption rejection ─────────────────────────────────

    #[test]
    fn manifest_corruption_rejection() {
        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();

        let profile = small_profile(32);
        let guest = SyntheticGuest::new(13, profile);
        let image = guest_image_from_synthetic(&guest, 5, 50, None);

        let snap_ref = store.commit(&image, None).unwrap();

        // Find and corrupt the .smf file by flipping one byte.
        let hex = snap_ref
            .to_bytes()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();
        let smf_path = dir.path().join("manifests").join(format!("{}.smf", hex));

        let mut content = fs::read(&smf_path).unwrap();
        // Flip the last byte.
        let last = content.len() - 1;
        content[last] ^= 0xFF;
        fs::write(&smf_path, &content).unwrap();

        // resolve must now return ManifestCorrupt.
        let result = store.resolve(&snap_ref);
        assert!(
            matches!(result, Err(StoreError::ManifestCorrupt)),
            "expected ManifestCorrupt, got: {:?}",
            result
        );
    }

    // ── Test 6: commit with MetaDb registers snapshot ─────────────────────────

    #[test]
    fn commit_with_metadb_registers() {
        use snapstore_meta::MetaDb;

        let dir = TempDir::new().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();

        // Create MetaDb
        let meta_path = dir.path().join("meta.db");
        let db = MetaDb::open(&meta_path).unwrap();

        let profile = GuestProfile {
            total_pages: 64,
            ..GuestProfile::all_unique()
        };
        let guest = SyntheticGuest::new(42, profile);

        // Commit with MetaDb
        let image = guest_image_from_synthetic(&guest, 100, 200, None);
        let snap_ref = store.commit(&image, Some(&db)).unwrap();

        // Verify record was registered
        let record = db.get(&snap_ref).unwrap().expect("should be in MetaDb");
        assert_eq!(record.r, snap_ref);
        assert_eq!(record.icount, 100);
        assert_eq!(record.virtual_ns, 200);
        assert!(record.parent.is_none());
        assert_eq!(record.page_count, 64);
        // all_unique profile: all pages should be newly written
        assert_eq!(record.new_pages, 64);

        // Re-commit identical state: must return same ref without error
        let image2 = guest_image_from_synthetic(&guest, 100, 200, None);
        let snap_ref2 = store.commit(&image2, Some(&db)).unwrap();
        assert_eq!(snap_ref, snap_ref2, "same state must give same ref");

        // Second commit was idempotent (no error, same record still in DB)
        let record2 = db.get(&snap_ref).unwrap().expect("should still be in MetaDb");
        assert_eq!(record.page_count, record2.page_count);
    }
}
