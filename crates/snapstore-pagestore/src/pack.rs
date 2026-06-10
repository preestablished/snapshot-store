use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use bytes::Bytes;
use snapstore_types::{PageHash, PackId, PAGE_SIZE};

// ── Constants ────────────────────────────────────────────────────────────────

pub const PACK_MAGIC: &[u8; 4] = b"SPK1";
pub const FOOTER_MAGIC: &[u8; 4] = b"SPKF";
pub const PACK_FORMAT_VERSION: u32 = 1;
/// Rotation threshold: 1 GiB
pub const PACK_MAX_BYTES: u64 = 1 << 30;
/// On-disk size of one record header: hash(32) + flags(1) + len(4)
pub const RECORD_HEADER_SIZE: u64 = 32 + 1 + 4;
/// Pack file header: magic(4) + version(4) + pack_id(4) + created_unix(8) = 20 bytes
pub const PACK_HEADER_SIZE: u64 = 20;
/// Pack footer: magic(4) + record_count(8) + body_blake3(32) = 44 bytes
pub const PACK_FOOTER_SIZE: u64 = 44;

/// Flush write buffer when it reaches this size (4 MiB).
const WRITE_BUF_FLUSH_THRESHOLD: usize = 4 * 1024 * 1024;

// ── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum PackError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("pack is already sealed")]
    Sealed,
    #[error("pack is full (would exceed 1 GiB cap)")]
    Full,
    #[error("invalid pack magic")]
    BadMagic,
    #[error("invalid footer magic")]
    BadFooterMagic,
    #[error("footer record count mismatch: expected {expected}, got {found}")]
    FooterMismatch { expected: u64, found: u64 },
    #[error("record hash mismatch at offset {offset}")]
    HashMismatch { offset: u64 },
    #[error("truncated record at offset {offset}")]
    TruncatedRecord { offset: u64 },
}

// ── PackWriter ───────────────────────────────────────────────────────────────

pub struct PackWriter {
    file: std::fs::File,
    pack_id: PackId,
    body_hasher: blake3::Hasher,
    write_buf: Vec<u8>,
    /// Current logical end of written + buffered data (bytes from file start).
    write_offset: u64,
    record_count: u64,
    sealed: bool,
    /// Rotation threshold; PACK_MAX_BYTES by default.
    max_bytes: u64,
}

impl PackWriter {
    /// Create a new pack file. `created_unix` is seconds since epoch.
    pub fn create(path: &Path, pack_id: PackId, created_unix: u64) -> Result<Self, PackError> {
        Self::create_with_max_bytes(path, pack_id, created_unix, PACK_MAX_BYTES)
    }

    /// Create a new pack file with a custom rotation cap.
    /// Identical to `create` but accepts an explicit `max_bytes` threshold.
    /// Set smaller than `PACK_MAX_BYTES` in tests to force rotation cheaply.
    pub fn create_with_max_bytes(
        path: &Path,
        pack_id: PackId,
        created_unix: u64,
        max_bytes: u64,
    ) -> Result<Self, PackError> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        // Write 20-byte header immediately so the file is identifiable.
        let mut header = [0u8; PACK_HEADER_SIZE as usize];
        header[0..4].copy_from_slice(PACK_MAGIC);
        header[4..8].copy_from_slice(&PACK_FORMAT_VERSION.to_le_bytes());
        header[8..12].copy_from_slice(&pack_id.0.to_le_bytes());
        header[12..20].copy_from_slice(&created_unix.to_le_bytes());
        file.write_all(&header)?;
        file.flush()?;

        Ok(Self {
            file,
            pack_id,
            body_hasher: blake3::Hasher::new(),
            write_buf: Vec::with_capacity(WRITE_BUF_FLUSH_THRESHOLD + PAGE_SIZE + 37),
            write_offset: PACK_HEADER_SIZE,
            record_count: 0,
            sealed: false,
            max_bytes,
        })
    }

    /// Reopen an existing unsealed pack file for continued appending.
    ///
    /// The file must already exist and must NOT have a valid footer (unsealed).
    /// Scans existing records to reconstruct `write_offset`, `record_count`, and
    /// `body_hasher`.  Returns a `PackWriter` ready to append more records.
    pub fn reopen(path: &Path, pack_id: PackId) -> Result<Self, PackError> {
        use std::os::unix::fs::FileExt;

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        // Validate header magic.
        let mut magic = [0u8; 4];
        file.read_exact_at(&mut magic, 0)?;
        if &magic != PACK_MAGIC {
            return Err(PackError::BadMagic);
        }

        // Determine file length.
        let file_len = file.seek(SeekFrom::End(0))?;

        // Scan forward through records to reconstruct hasher and counts.
        let mut offset = PACK_HEADER_SIZE;
        let mut record_count: u64 = 0;
        let mut body_hasher = blake3::Hasher::new();

        while offset < file_len {
            let remaining = file_len - offset;
            if remaining < RECORD_HEADER_SIZE {
                break;
            }

            // Read record header.
            let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
            file.read_exact_at(&mut rec_header, offset)?;
            let len = u32::from_le_bytes(rec_header[33..37].try_into().unwrap());

            // Sanity-check length.
            if len as usize > PAGE_SIZE * 2 {
                break;
            }

            // Check that the payload is present.
            if offset + RECORD_HEADER_SIZE + len as u64 > file_len {
                break;
            }

            // Read payload.
            let mut payload = vec![0u8; len as usize];
            file.read_exact_at(&mut payload, offset + RECORD_HEADER_SIZE)?;

            // Feed the complete record (header + payload) into the body hasher.
            body_hasher.update(&rec_header);
            body_hasher.update(&payload);

            record_count += 1;
            offset += RECORD_HEADER_SIZE + len as u64;
        }

        // Re-open and seek to the write position so subsequent flush_buf calls
        // land at the right offset.
        drop(file);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        file.seek(SeekFrom::Start(offset))?;

        Ok(Self {
            file,
            pack_id,
            body_hasher,
            write_buf: Vec::with_capacity(WRITE_BUF_FLUSH_THRESHOLD + PAGE_SIZE + 37),
            write_offset: offset,
            record_count,
            sealed: false,
            max_bytes: PACK_MAX_BYTES,
        })
    }

    pub fn pack_id(&self) -> PackId {
        self.pack_id
    }

    /// True if appending one more record would exceed the rotation cap.
    pub fn would_exceed_cap(&self) -> bool {
        self.write_offset + RECORD_HEADER_SIZE + PAGE_SIZE as u64 > self.max_bytes
    }

    /// Append one raw page. Returns the byte offset where the record starts
    /// (i.e. the offset of the page_hash field).
    pub fn append(&mut self, hash: &PageHash, payload: &[u8; PAGE_SIZE]) -> Result<u64, PackError> {
        if self.sealed {
            return Err(PackError::Sealed);
        }
        if self.would_exceed_cap() {
            return Err(PackError::Full);
        }

        let record_offset = self.write_offset;

        // Encode into write_buf: hash(32) | flags(1) | len(4) | payload(4096)
        let record_start_in_buf = self.write_buf.len();
        self.write_buf.extend_from_slice(hash.as_bytes());
        self.write_buf.push(0x01); // flags: raw page
        self.write_buf.extend_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        self.write_buf.extend_from_slice(payload);

        // Update body hasher with the entire encoded record.
        self.body_hasher.update(&self.write_buf[record_start_in_buf..]);

        self.write_offset += RECORD_HEADER_SIZE + PAGE_SIZE as u64;
        self.record_count += 1;

        // Flush when buffer is large enough.
        if self.write_buf.len() >= WRITE_BUF_FLUSH_THRESHOLD {
            self.flush_buf()?;
        }

        Ok(record_offset)
    }

    /// Flush the in-memory write buffer to the OS.
    pub fn flush_buf(&mut self) -> Result<(), PackError> {
        if !self.write_buf.is_empty() {
            self.file.write_all(&self.write_buf)?;
            self.write_buf.clear();
        }
        Ok(())
    }

    /// Flush buffered records and write footer without fdatasync.
    ///
    /// Use during pack rotation so the hot ingest path never blocks on disk I/O.
    /// The caller is responsible for ensuring `sync_data()` is eventually called
    /// (e.g. via `PageStore::sync()`).  The pack is correctly treated as sealed
    /// by `PackReader::open()` once the footer reaches the filesystem.
    pub fn seal_no_sync(&mut self) -> Result<(), PackError> {
        if self.sealed {
            return Err(PackError::Sealed);
        }

        // Flush buffered records to OS (page cache only, no fdatasync).
        self.flush_buf()?;

        // Finalise body hash and write footer.
        let body_hash = self.body_hasher.finalize();
        let mut footer = [0u8; PACK_FOOTER_SIZE as usize];
        footer[0..4].copy_from_slice(FOOTER_MAGIC);
        footer[4..12].copy_from_slice(&self.record_count.to_le_bytes());
        footer[12..44].copy_from_slice(body_hash.as_bytes());
        self.file.write_all(&footer)?;

        self.sealed = true;
        Ok(())
    }

    /// Flush, fdatasync record data, write footer, fdatasync again.
    ///
    /// Use this for explicit, durable sealing (e.g. in tests or when called
    /// directly outside the hot ingest path).  The ingest rotation path should
    /// use `seal_no_sync()` instead to avoid blocking on disk I/O.
    pub fn seal(&mut self) -> Result<(), PackError> {
        if self.sealed {
            return Err(PackError::Sealed);
        }

        // 1. Flush buffered records to OS.
        self.flush_buf()?;
        self.file.flush()?;

        // 2. fdatasync — ensure record data is durable before footer.
        self.file.sync_data()?;

        // 3. Finalise body hash.
        let body_hash = self.body_hasher.finalize();

        // 4. Write 44-byte footer.
        let mut footer = [0u8; PACK_FOOTER_SIZE as usize];
        footer[0..4].copy_from_slice(FOOTER_MAGIC);
        footer[4..12].copy_from_slice(&self.record_count.to_le_bytes());
        footer[12..44].copy_from_slice(body_hash.as_bytes());
        self.file.write_all(&footer)?;
        self.file.flush()?;

        // 5. fdatasync footer.
        self.file.sync_data()?;

        self.sealed = true;
        Ok(())
    }
}

// ── PackReader ───────────────────────────────────────────────────────────────

pub struct PackReader {
    file: std::fs::File,
    pack_id: PackId,
    /// If Some, the file has a valid footer and body ends at this offset.
    body_end: Option<u64>,
}

impl PackReader {
    /// Open a sealed pack for reading.  Validates header and footer.
    pub fn open(path: &Path, pack_id: PackId) -> Result<Self, PackError> {
        let mut file = std::fs::OpenOptions::new().read(true).open(path)?;

        // Validate header magic.
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != PACK_MAGIC {
            return Err(PackError::BadMagic);
        }

        // Seek to footer position (EOF - PACK_FOOTER_SIZE).
        let file_len = file.seek(SeekFrom::End(0))?;
        if file_len < PACK_HEADER_SIZE + PACK_FOOTER_SIZE {
            return Err(PackError::BadFooterMagic);
        }
        file.seek(SeekFrom::End(-(PACK_FOOTER_SIZE as i64)))?;

        let mut footer = [0u8; PACK_FOOTER_SIZE as usize];
        file.read_exact(&mut footer)?;

        if &footer[0..4] != FOOTER_MAGIC {
            return Err(PackError::BadFooterMagic);
        }

        let body_end = file_len - PACK_FOOTER_SIZE;
        let footer_record_count = u64::from_le_bytes(footer[4..12].try_into().unwrap());

        // Validate record count by scanning up to body_end.
        let scanned = count_records_in_file(&mut file, body_end)?;
        if scanned != footer_record_count {
            return Err(PackError::FooterMismatch {
                expected: footer_record_count,
                found: scanned,
            });
        }

        Ok(Self {
            file,
            pack_id,
            body_end: Some(body_end),
        })
    }

    /// Open an unsealed (active or crashed) pack.  Scans records forward,
    /// verifying blake3(payload) == page_hash.  Truncates at first bad record.
    /// Returns (reader, good_record_count).
    pub fn open_unsealed(path: &Path, pack_id: PackId) -> Result<(Self, u64), PackError> {
        // Open read-write so we can truncate if needed.
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        // Validate header magic.
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if &magic != PACK_MAGIC {
            return Err(PackError::BadMagic);
        }

        let file_len = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(PACK_HEADER_SIZE))?;

        let mut offset = PACK_HEADER_SIZE;
        let mut good_count: u64 = 0;
        let mut truncate_at: Option<u64> = None;

        loop {
            // Check whether there's even room for a record header.
            let remaining = file_len.saturating_sub(offset);
            if remaining == 0 {
                break;
            }
            if remaining < RECORD_HEADER_SIZE {
                // Partial header — truncate here.
                truncate_at = Some(offset);
                break;
            }

            // Read hash + flags + len.
            let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
            file.read_exact(&mut rec_header)?;
            let hash_bytes: [u8; 32] = rec_header[0..32].try_into().unwrap();
            let flags = rec_header[32];
            let len = u32::from_le_bytes(rec_header[33..37].try_into().unwrap());

            // For M1 we only write raw pages (flags & 1 == 1, len == PAGE_SIZE).
            let expected_len = if flags & 0x01 != 0 { PAGE_SIZE as u32 } else { len };
            if len != expected_len || len as usize > PAGE_SIZE * 2 {
                // Implausible length — treat as truncation.
                truncate_at = Some(offset);
                break;
            }

            // Check whether payload bytes exist.
            let payload_remaining = file_len.saturating_sub(offset + RECORD_HEADER_SIZE);
            if payload_remaining < len as u64 {
                truncate_at = Some(offset);
                break;
            }

            // Read payload and verify hash.
            let mut payload = vec![0u8; len as usize];
            file.read_exact(&mut payload)?;

            let computed = *blake3::hash(&payload).as_bytes();
            if computed != hash_bytes {
                truncate_at = Some(offset);
                break;
            }

            good_count += 1;
            offset += RECORD_HEADER_SIZE + len as u64;
        }

        let final_body_end = truncate_at.unwrap_or(offset);
        if let Some(trunc) = truncate_at {
            file.set_len(trunc)?;
            file.flush()?;
        }

        // Re-open read-only for the returned reader.
        drop(file);
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        Ok((
            Self {
                file,
                pack_id,
                body_end: Some(final_body_end),
            },
            good_count,
        ))
    }

    /// Read the record at `offset` (the byte offset of its page_hash field).
    pub fn read_at(&self, offset: u64) -> Result<(PageHash, Bytes), PackError> {
        use std::os::unix::fs::FileExt;

        // Read record header.
        let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
        self.file
            .read_exact_at(&mut rec_header, offset)
            .map_err(|_| PackError::TruncatedRecord { offset })?;

        let hash_bytes: [u8; 32] = rec_header[0..32].try_into().unwrap();
        let len = u32::from_le_bytes(rec_header[33..37].try_into().unwrap());

        // Read payload.
        let mut payload = vec![0u8; len as usize];
        self.file
            .read_exact_at(&mut payload, offset + RECORD_HEADER_SIZE)
            .map_err(|_| PackError::TruncatedRecord { offset })?;

        Ok((PageHash::from_bytes(hash_bytes), Bytes::from(payload)))
    }

    /// Iterate all records; yields (offset, PageHash) pairs.
    /// Stops at `body_end` (before the footer in sealed packs).
    pub fn scan(&self) -> Result<Vec<(u64, PageHash)>, PackError> {
        use std::os::unix::fs::FileExt;

        // Determine scan boundary.
        let file_len = {
            let mut f = &self.file;
            f.seek(SeekFrom::End(0))?
        };
        let scan_end = self.body_end.unwrap_or(file_len);

        let mut results = Vec::new();
        let mut offset = PACK_HEADER_SIZE;

        loop {
            if offset >= scan_end {
                break;
            }
            let remaining = scan_end - offset;
            if remaining < RECORD_HEADER_SIZE {
                break;
            }

            let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
            self.file
                .read_exact_at(&mut rec_header, offset)
                .map_err(|_| PackError::TruncatedRecord { offset })?;

            let hash_bytes: [u8; 32] = rec_header[0..32].try_into().unwrap();
            let len = u32::from_le_bytes(rec_header[33..37].try_into().unwrap());

            results.push((offset, PageHash::from_bytes(hash_bytes)));
            offset += RECORD_HEADER_SIZE + len as u64;
        }

        Ok(results)
    }

    pub fn pack_id(&self) -> PackId {
        self.pack_id
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Count how many complete records exist between PACK_HEADER_SIZE and body_end,
/// without verifying hashes.
fn count_records_in_file(
    file: &mut std::fs::File,
    body_end: u64,
) -> Result<u64, PackError> {
    file.seek(SeekFrom::Start(PACK_HEADER_SIZE))?;

    let mut offset = PACK_HEADER_SIZE;
    let mut count = 0u64;

    while offset < body_end {
        let remaining = body_end - offset;
        if remaining < RECORD_HEADER_SIZE {
            break;
        }

        let mut rec_header = [0u8; RECORD_HEADER_SIZE as usize];
        file.read_exact(&mut rec_header)?;
        let len = u32::from_le_bytes(rec_header[33..37].try_into().unwrap());

        let record_size = RECORD_HEADER_SIZE + len as u64;
        if offset + record_size > body_end {
            break;
        }

        // Skip over payload.
        file.seek(SeekFrom::Current(len as i64))?;
        offset += record_size;
        count += 1;
    }

    Ok(count)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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

    // ── Test 1 ───────────────────────────────────────────────────────────────
    #[test]
    fn write_seal_reopen_read() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pack-0001.spk");
        let pack_id = PackId(1);

        // Write 10 pages and record their offsets.
        let mut offsets = Vec::new();
        let mut pages: Vec<[u8; PAGE_SIZE]> = Vec::new();
        {
            let mut w = PackWriter::create(&path, pack_id, 0).unwrap();
            for seed in 0u8..10 {
                let page = make_page(seed);
                let hash = hash_page(&page);
                let off = w.append(&hash, &page).unwrap();
                offsets.push(off);
                pages.push(page);
            }
            w.seal().unwrap();
        }

        // Reopen as sealed reader.
        let r = PackReader::open(&path, pack_id).unwrap();

        // read_at returns original bytes.
        for (i, &off) in offsets.iter().enumerate() {
            let (hash, data) = r.read_at(off).unwrap();
            assert_eq!(hash, hash_page(&pages[i]));
            assert_eq!(data.as_ref(), pages[i].as_ref());
        }

        // scan returns 10 records.
        let scanned = r.scan().unwrap();
        assert_eq!(scanned.len(), 10);
        for (i, (off, hash)) in scanned.iter().enumerate() {
            assert_eq!(*off, offsets[i]);
            assert_eq!(*hash, hash_page(&pages[i]));
        }
    }

    // ── Test 2 ───────────────────────────────────────────────────────────────
    #[test]
    fn rotation_boundary() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pack-0002.spk");
        let pack_id = PackId(2);

        let mut w = PackWriter::create(&path, pack_id, 0).unwrap();
        // Simulate nearly-full pack by manipulating write_offset directly.
        // One record = RECORD_HEADER_SIZE + PAGE_SIZE = 37 + 4096 = 4133 bytes.
        // Set write_offset so adding one more would exceed cap (one byte short).
        w.write_offset = PACK_MAX_BYTES - (RECORD_HEADER_SIZE + PAGE_SIZE as u64 - 1);
        assert!(w.would_exceed_cap());

        // Exactly at capacity boundary — should NOT exceed cap.
        w.write_offset = PACK_MAX_BYTES - (RECORD_HEADER_SIZE + PAGE_SIZE as u64);
        assert!(!w.would_exceed_cap());
    }

    // ── Test 3 ───────────────────────────────────────────────────────────────
    #[test]
    fn torn_write_truncated_record() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pack-0003.spk");
        let pack_id = PackId(3);

        // Write 5 pages and seal.
        let mut offsets = Vec::new();
        {
            let mut w = PackWriter::create(&path, pack_id, 0).unwrap();
            for seed in 0u8..5 {
                let page = make_page(seed);
                let hash = hash_page(&page);
                let off = w.append(&hash, &page).unwrap();
                offsets.push(off);
            }
            w.seal().unwrap();
        }

        // Re-open file and truncate mid-way through last record.
        {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap();
            // last record starts at offsets[4]; cut halfway through payload
            let trunc_at = offsets[4] + RECORD_HEADER_SIZE + (PAGE_SIZE as u64 / 2);
            file.set_len(trunc_at).unwrap();
        }

        // open_unsealed should recover 4 good records.
        let (_, good) = PackReader::open_unsealed(&path, pack_id).unwrap();
        assert_eq!(good, 4, "expected 4 good records after torn write");
    }

    // ── Test 4 ───────────────────────────────────────────────────────────────
    #[test]
    fn torn_write_corrupt_payload() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pack-0004.spk");
        let pack_id = PackId(4);

        // Write 5 pages and seal.
        let mut offsets = Vec::new();
        {
            let mut w = PackWriter::create(&path, pack_id, 0).unwrap();
            for seed in 0u8..5 {
                let page = make_page(seed);
                let hash = hash_page(&page);
                let off = w.append(&hash, &page).unwrap();
                offsets.push(off);
            }
            w.seal().unwrap();
        }

        // Corrupt one byte of the payload in record 4 (0-indexed → the 5th record).
        {
            use std::os::unix::fs::FileExt;
            let file = std::fs::OpenOptions::new()
                .write(true)
                .read(true)
                .open(&path)
                .unwrap();
            // Flip the first payload byte of record index 4.
            let payload_start = offsets[4] + RECORD_HEADER_SIZE;
            let mut byte = [0u8; 1];
            file.read_exact_at(&mut byte, payload_start).unwrap();
            byte[0] ^= 0xFF;
            file.write_at(&byte, payload_start).unwrap();
        }

        // open_unsealed should return 4 good records (record 4 is dropped).
        let (_, good) = PackReader::open_unsealed(&path, pack_id).unwrap();
        assert_eq!(good, 4, "expected 4 good records after payload corruption");
    }
}
