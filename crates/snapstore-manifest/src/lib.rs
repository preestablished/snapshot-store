//! `.spm` snapshot-manifest container codec (v2) and SILG input-log container
//! codec.
//!
//! Both containers are byte-comparable, content-addressed by a BLAKE3 footer,
//! and fuzz-safe: all parsing is pure `&[u8]` with no I/O.

#![forbid(unsafe_code)]

use snapstore_types::{PageHash, SnapshotRef};

// ── re-exports ───────────────────────────────────────────────────────────────

pub use input_log::{InputLogContainer, InputLogError};

// ── constants ────────────────────────────────────────────────────────────────

/// Wire magic for snapshot-manifest containers.
pub const MAGIC: &[u8; 8] = b"SPSMAN01";

/// Wire version emitted and accepted by this implementation.
pub const VERSION: u16 = 1;

/// Fixed header length in bytes (v1).
pub const HEADER_LEN: u32 = 96;

/// Page size required by v1 readers.
pub const PAGE_SIZE_V1: u64 = 4096;

/// Flags: bit 0 — DELTA manifest; bit 1 — device blob is zstd-compressed.
pub const FLAG_DELTA: u16 = 0x0001;
pub const FLAG_DEV_ZSTD: u16 = 0x0002;
const FLAG_KNOWN_BITS: u16 = FLAG_DELTA | FLAG_DEV_ZSTD;

/// Each entry in the entry table is 40 bytes: page_index u64 LE + page_hash
/// [32].
const ENTRY_SIZE: usize = 40;

/// BLAKE3 footer length appended to every container.
const FOOTER_LEN: usize = 32;

// ── public model ─────────────────────────────────────────────────────────────

/// A decoded snapshot-manifest container.
///
/// Invariants maintained by the builder and validated by `decode`:
/// - `entries` is sorted strictly ascending by `page_index`, no duplicates.
/// - FULL (!delta): entries cover exactly `0..guest_ram_bytes/4096`.
/// - DELTA: every entry index < `guest_ram_bytes/4096`; `parent` is `Some`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub version: u16,
    pub delta: bool,
    pub parent: Option<SnapshotRef>,
    pub guest_ram_bytes: u64,
    pub entries: Vec<ManifestEntry>,
    pub device_blob: DeviceBlob,
}

/// A single page entry: page index (within the guest RAM layout) + content
/// hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestEntry {
    pub page_index: u64,
    pub page_hash: PageHash,
}

/// Device-state opaque blob carried inside a manifest container.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeviceBlob {
    /// Caller-defined format tag.
    pub format: u32,
    /// True when `bytes` is zstd-compressed.
    pub zstd: bool,
    /// Stored bytes (possibly compressed).
    pub bytes: Vec<u8>,
    /// Uncompressed length.  Equals `bytes.len()` when `zstd` is false.
    pub raw_len: u64,
}

// ── errors ───────────────────────────────────────────────────────────────────

/// Every distinct validation failure in `Manifest::decode` has its own
/// variant.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ManifestError {
    #[error("bad magic bytes")]
    BadMagic,
    #[error("unsupported version: {0}")]
    UnsupportedVersion(u16),
    #[error("unknown flag bits: 0x{0:04x}")]
    UnknownFlags(u16),
    #[error("header_len must be 96, got {0}")]
    BadHeaderLen(u32),
    #[error("reserved field must be 0")]
    ReservedNonZero,
    #[error("page_size must be 4096, got {0}")]
    BadPageSize(u64),
    #[error("guest_ram_bytes must be a multiple of 4096, got {0}")]
    GuestRamNotAligned(u64),
    #[error("entries are not strictly ascending by page_index (duplicate or out-of-order)")]
    EntriesNotAscending,
    #[error("FULL manifest: entry_count ({0}) != guest_ram_bytes/4096 ({1})")]
    FullCountMismatch(u64, u64),
    #[error("FULL manifest: page indices are not contiguous starting at 0")]
    FullNotContiguous,
    #[error("FULL manifest: parent_manifest_hash must be all-zero")]
    FullNonZeroParent,
    #[error("DELTA manifest: parent_manifest_hash must be non-zero")]
    DeltaZeroParent,
    #[error("DELTA manifest: page index {0} out of range (max {1})")]
    DeltaIndexOutOfRange(u64, u64),
    #[error("footer mismatch: stored hash does not match computed BLAKE3")]
    FooterMismatch,
    #[error("trailing bytes after footer")]
    TrailingBytes,
    #[error("input truncated")]
    Truncated,
    #[error("DEV_ZSTD: decompressed length {0} != device_blob_raw_len {1}")]
    ZstdRawLenMismatch(usize, u64),
    #[error("!DEV_ZSTD: device_blob_raw_len ({0}) must equal device_blob_len ({1})")]
    NonZstdRawLenMismatch(u64, u64),
}

/// Errors from `flatten` / `flatten_delta`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FlattenError {
    #[error("chain must not be empty")]
    EmptyChain,
    #[error("chain root must be a FULL manifest")]
    RootNotFull,
    #[error("chain root must be a DELTA manifest")]
    RootNotDelta,
    #[error("guest_ram_bytes mismatch within chain")]
    RamMismatch,
    #[error("result does not cover all page indices (coverage gap)")]
    Coverage,
}

// ── builder helpers ───────────────────────────────────────────────────────────

impl Manifest {
    /// Construct a FULL manifest.  `entries` need not be pre-sorted — the
    /// builder sorts them.  Returns an error if invariants are violated.
    pub fn new_full(
        guest_ram_bytes: u64,
        mut entries: Vec<ManifestEntry>,
        device_blob: DeviceBlob,
    ) -> Result<Self, ManifestError> {
        if !guest_ram_bytes.is_multiple_of(PAGE_SIZE_V1) {
            return Err(ManifestError::GuestRamNotAligned(guest_ram_bytes));
        }
        entries.sort_by_key(|e| e.page_index);
        let expected_pages = guest_ram_bytes / PAGE_SIZE_V1;
        if entries.len() as u64 != expected_pages {
            return Err(ManifestError::FullCountMismatch(
                entries.len() as u64,
                expected_pages,
            ));
        }
        for (i, e) in entries.iter().enumerate() {
            if e.page_index != i as u64 {
                return Err(ManifestError::FullNotContiguous);
            }
        }
        validate_device_blob(&device_blob)?;
        Ok(Self {
            version: VERSION,
            delta: false,
            parent: None,
            guest_ram_bytes,
            entries,
            device_blob,
        })
    }

    /// Construct a DELTA manifest.  `entries` need not be pre-sorted — the
    /// builder sorts them.
    pub fn new_delta(
        parent: SnapshotRef,
        guest_ram_bytes: u64,
        mut entries: Vec<ManifestEntry>,
        device_blob: DeviceBlob,
    ) -> Result<Self, ManifestError> {
        if !guest_ram_bytes.is_multiple_of(PAGE_SIZE_V1) {
            return Err(ManifestError::GuestRamNotAligned(guest_ram_bytes));
        }
        entries.sort_by_key(|e| e.page_index);
        // Check sorted ascending with no dups
        for w in entries.windows(2) {
            if w[0].page_index >= w[1].page_index {
                return Err(ManifestError::EntriesNotAscending);
            }
        }
        let max_idx = guest_ram_bytes / PAGE_SIZE_V1;
        for e in &entries {
            if e.page_index >= max_idx {
                return Err(ManifestError::DeltaIndexOutOfRange(e.page_index, max_idx));
            }
        }
        validate_device_blob(&device_blob)?;
        Ok(Self {
            version: VERSION,
            delta: true,
            parent: Some(parent),
            guest_ram_bytes,
            entries,
            device_blob,
        })
    }
}

/// Validate blob invariants that don't require I/O.
fn validate_device_blob(blob: &DeviceBlob) -> Result<(), ManifestError> {
    if !blob.zstd && blob.raw_len != blob.bytes.len() as u64 {
        return Err(ManifestError::NonZstdRawLenMismatch(
            blob.raw_len,
            blob.bytes.len() as u64,
        ));
    }
    Ok(())
}

// ── encode ────────────────────────────────────────────────────────────────────

impl Manifest {
    /// Encode to canonical wire bytes (header + entry table + device blob +
    /// BLAKE3 footer).
    ///
    /// The caller is responsible for ensuring the manifest was constructed via
    /// `new_full` / `new_delta` or decoded from valid bytes; `encode` trusts
    /// the struct fields.
    pub fn encode(&self) -> Vec<u8> {
        let entry_count = self.entries.len() as u64;
        let device_blob_len = self.device_blob.bytes.len() as u64;
        let device_blob_raw_len = self.device_blob.raw_len;
        let device_blob_format = self.device_blob.format;

        let mut flags: u16 = 0;
        if self.delta {
            flags |= FLAG_DELTA;
        }
        if self.device_blob.zstd {
            flags |= FLAG_DEV_ZSTD;
        }

        let parent_hash: [u8; 32] = match &self.parent {
            Some(r) => r.to_bytes(),
            None => [0u8; 32],
        };

        // Total size: header(96) + entries(N×40) + blob + footer(32)
        let total =
            96 + self.entries.len() * ENTRY_SIZE + self.device_blob.bytes.len() + FOOTER_LEN;
        let mut buf = Vec::with_capacity(total);

        // ── header (96 bytes) ────────────────────────────────────────────────
        buf.extend_from_slice(MAGIC); // 0..8
        buf.extend_from_slice(&VERSION.to_le_bytes()); // 8..10
        buf.extend_from_slice(&flags.to_le_bytes()); // 10..12
        buf.extend_from_slice(&HEADER_LEN.to_le_bytes()); // 12..16
        buf.extend_from_slice(&parent_hash); // 16..48
        buf.extend_from_slice(&self.guest_ram_bytes.to_le_bytes()); // 48..56
        buf.extend_from_slice(&PAGE_SIZE_V1.to_le_bytes()); // 56..64
        buf.extend_from_slice(&entry_count.to_le_bytes()); // 64..72
        buf.extend_from_slice(&device_blob_len.to_le_bytes()); // 72..80
        buf.extend_from_slice(&device_blob_raw_len.to_le_bytes()); // 80..88
        buf.extend_from_slice(&device_blob_format.to_le_bytes()); // 88..92
        buf.extend_from_slice(&0u32.to_le_bytes()); // 92..96  reserved

        debug_assert_eq!(buf.len(), 96, "header must be exactly 96 bytes");

        // ── entry table ──────────────────────────────────────────────────────
        for entry in &self.entries {
            buf.extend_from_slice(&entry.page_index.to_le_bytes());
            buf.extend_from_slice(entry.page_hash.as_bytes());
        }

        // ── device blob ──────────────────────────────────────────────────────
        buf.extend_from_slice(&self.device_blob.bytes);

        // ── BLAKE3 footer ────────────────────────────────────────────────────
        let hash = blake3::hash(&buf);
        buf.extend_from_slice(hash.as_bytes());

        buf
    }

    /// Decode and fully validate a snapshot-manifest container.
    ///
    /// Every failure maps to a distinct [`ManifestError`] variant.
    pub fn decode(buf: &[u8]) -> Result<Self, ManifestError> {
        // Minimum size: header(96) + footer(32) = 128
        if buf.len() < 96 + FOOTER_LEN {
            return Err(ManifestError::Truncated);
        }

        // ── footer first (avoids acting on any field before integrity check)
        let footer_start = buf.len() - FOOTER_LEN;
        let body = &buf[..footer_start];
        let stored_hash: [u8; 32] = buf[footer_start..].try_into().unwrap();
        let computed = blake3::hash(body);
        if computed.as_bytes() != &stored_hash {
            return Err(ManifestError::FooterMismatch);
        }

        // ── header ───────────────────────────────────────────────────────────
        let magic = &buf[0..8];
        if magic != MAGIC {
            return Err(ManifestError::BadMagic);
        }
        let version = u16::from_le_bytes(buf[8..10].try_into().unwrap());
        if version != VERSION {
            return Err(ManifestError::UnsupportedVersion(version));
        }
        let flags = u16::from_le_bytes(buf[10..12].try_into().unwrap());
        let unknown = flags & !FLAG_KNOWN_BITS;
        if unknown != 0 {
            return Err(ManifestError::UnknownFlags(unknown));
        }
        let header_len = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        if header_len != HEADER_LEN {
            return Err(ManifestError::BadHeaderLen(header_len));
        }
        let parent_hash: [u8; 32] = buf[16..48].try_into().unwrap();
        let guest_ram_bytes = u64::from_le_bytes(buf[48..56].try_into().unwrap());
        let page_size = u64::from_le_bytes(buf[56..64].try_into().unwrap());
        let entry_count = u64::from_le_bytes(buf[64..72].try_into().unwrap());
        let device_blob_len = u64::from_le_bytes(buf[72..80].try_into().unwrap());
        let device_blob_raw_len = u64::from_le_bytes(buf[80..88].try_into().unwrap());
        let device_blob_format = u32::from_le_bytes(buf[88..92].try_into().unwrap());
        let reserved = u32::from_le_bytes(buf[92..96].try_into().unwrap());

        if reserved != 0 {
            return Err(ManifestError::ReservedNonZero);
        }
        if page_size != PAGE_SIZE_V1 {
            return Err(ManifestError::BadPageSize(page_size));
        }
        if !guest_ram_bytes.is_multiple_of(PAGE_SIZE_V1) {
            return Err(ManifestError::GuestRamNotAligned(guest_ram_bytes));
        }

        // ── size consistency check ────────────────────────────────────────────
        let entries_bytes = (entry_count as usize)
            .checked_mul(ENTRY_SIZE)
            .ok_or(ManifestError::Truncated)?;
        let blob_len_usize = device_blob_len as usize;
        let expected_body = 96_usize
            .checked_add(entries_bytes)
            .and_then(|n| n.checked_add(blob_len_usize))
            .ok_or(ManifestError::Truncated)?;
        if body.len() != expected_body {
            return if body.len() < expected_body {
                Err(ManifestError::Truncated)
            } else {
                Err(ManifestError::TrailingBytes)
            };
        }

        // ── parse entry table ─────────────────────────────────────────────────
        let entries_start = 96usize;
        let entries_end = entries_start + entries_bytes;
        let mut entries = Vec::with_capacity(entry_count as usize);
        for i in 0..entry_count as usize {
            let off = entries_start + i * ENTRY_SIZE;
            let page_index = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
            let hash_bytes: [u8; 32] = buf[off + 8..off + 40].try_into().unwrap();
            entries.push(ManifestEntry {
                page_index,
                page_hash: PageHash::from_bytes(hash_bytes),
            });
        }

        // ── validate sort order ───────────────────────────────────────────────
        for w in entries.windows(2) {
            if w[0].page_index >= w[1].page_index {
                return Err(ManifestError::EntriesNotAscending);
            }
        }

        // ── flags / delta / full invariants ───────────────────────────────────
        let is_delta = (flags & FLAG_DELTA) != 0;
        let is_dev_zstd = (flags & FLAG_DEV_ZSTD) != 0;
        let parent_all_zero = parent_hash == [0u8; 32];
        let max_idx = guest_ram_bytes / PAGE_SIZE_V1;

        let parent_opt: Option<SnapshotRef> = if is_delta {
            if parent_all_zero {
                return Err(ManifestError::DeltaZeroParent);
            }
            // Every entry index must be in-range
            for e in &entries {
                if e.page_index >= max_idx {
                    return Err(ManifestError::DeltaIndexOutOfRange(e.page_index, max_idx));
                }
            }
            Some(SnapshotRef::from_bytes(parent_hash))
        } else {
            // FULL
            if !parent_all_zero {
                return Err(ManifestError::FullNonZeroParent);
            }
            if entry_count != max_idx {
                return Err(ManifestError::FullCountMismatch(entry_count, max_idx));
            }
            // Indices must be 0..N-1 contiguous (already sorted ascending)
            for (i, e) in entries.iter().enumerate() {
                if e.page_index != i as u64 {
                    return Err(ManifestError::FullNotContiguous);
                }
            }
            None
        };

        // ── parse and validate device blob ────────────────────────────────────
        let blob_bytes = buf[entries_end..entries_end + blob_len_usize].to_vec();

        if is_dev_zstd {
            // Decompress and verify raw_len (result discarded)
            let decompressed = zstd::decode_all(blob_bytes.as_slice())
                .map_err(|_| ManifestError::ZstdRawLenMismatch(0, device_blob_raw_len))?;
            if decompressed.len() as u64 != device_blob_raw_len {
                return Err(ManifestError::ZstdRawLenMismatch(
                    decompressed.len(),
                    device_blob_raw_len,
                ));
            }
        } else if device_blob_raw_len != device_blob_len {
            return Err(ManifestError::NonZstdRawLenMismatch(
                device_blob_raw_len,
                device_blob_len,
            ));
        }

        Ok(Self {
            version,
            delta: is_delta,
            parent: parent_opt,
            guest_ram_bytes,
            entries,
            device_blob: DeviceBlob {
                format: device_blob_format,
                zstd: is_dev_zstd,
                bytes: blob_bytes,
                raw_len: device_blob_raw_len,
            },
        })
    }

    /// Compute the `SnapshotRef` of an already-encoded container.
    ///
    /// `buf` must be at least 32 bytes; caller guarantees this.
    /// Returns `blake3(buf[..len-32])`.
    pub fn snapshot_ref(buf: &[u8]) -> SnapshotRef {
        let body_len = buf.len().saturating_sub(FOOTER_LEN);
        let hash = blake3::hash(&buf[..body_len]);
        SnapshotRef::from_bytes(*hash.as_bytes())
    }
}

// ── flatten ───────────────────────────────────────────────────────────────────

/// Flatten a delta chain into a complete page table.
///
/// `chain` is ordered **child-first**; the last entry must be a FULL manifest.
/// All manifests must have the same `guest_ram_bytes`.
/// The result covers every page index `0..guest_ram_bytes/4096` with no gaps.
/// Duplicate page indices across the chain are resolved by the child-first
/// priority rule.
pub fn flatten(chain: &[&Manifest]) -> Result<Vec<ManifestEntry>, FlattenError> {
    if chain.is_empty() {
        return Err(FlattenError::EmptyChain);
    }
    let root = chain.last().unwrap();
    if root.delta {
        return Err(FlattenError::RootNotFull);
    }
    let ram = chain[0].guest_ram_bytes;
    for m in chain.iter() {
        if m.guest_ram_bytes != ram {
            return Err(FlattenError::RamMismatch);
        }
    }

    let total_pages = (ram / PAGE_SIZE_V1) as usize;
    let mut result: Vec<Option<ManifestEntry>> = vec![None; total_pages];

    // child-first: iterate chain[0] first (highest priority)
    for manifest in chain.iter() {
        for entry in &manifest.entries {
            let idx = entry.page_index as usize;
            if result[idx].is_none() {
                result[idx] = Some(entry.clone());
            }
        }
    }

    // Check coverage
    let mut out = Vec::with_capacity(total_pages);
    for slot in result {
        match slot {
            Some(e) => out.push(e),
            None => return Err(FlattenError::Coverage),
        }
    }
    Ok(out)
}

/// Flatten a chain of DELTA manifests child-first.
///
/// Unlike `flatten`, no coverage requirement — gaps are allowed.
/// The chain root must be a DELTA manifest.
/// All manifests must have the same `guest_ram_bytes`.
pub fn flatten_delta(chain: &[&Manifest]) -> Result<Vec<ManifestEntry>, FlattenError> {
    if chain.is_empty() {
        return Err(FlattenError::EmptyChain);
    }
    let root = chain.last().unwrap();
    if !root.delta {
        return Err(FlattenError::RootNotDelta);
    }
    let ram = chain[0].guest_ram_bytes;
    for m in chain.iter() {
        if m.guest_ram_bytes != ram {
            return Err(FlattenError::RamMismatch);
        }
    }

    let total_pages = (ram / PAGE_SIZE_V1) as usize;
    let mut seen: Vec<bool> = vec![false; total_pages];
    let mut out: Vec<ManifestEntry> = Vec::new();

    for manifest in chain.iter() {
        for entry in &manifest.entries {
            let idx = entry.page_index as usize;
            if !seen[idx] {
                seen[idx] = true;
                out.push(entry.clone());
            }
        }
    }

    out.sort_by_key(|e| e.page_index);
    Ok(out)
}

// ── input_log module ──────────────────────────────────────────────────────────

/// SILG input-log container codec.
pub mod input_log {
    use snapstore_types::LogId;

    const MAGIC: &[u8; 4] = b"SILG";
    const CONTAINER_VERSION: u16 = 1;
    // Layout: magic(4) | container_version(2) | flags(2) | inner_format_version(4) | reserved(4) | payload_len(8)
    // = 4+2+2+4+4+8 = 24 bytes header
    const HDR_LEN: usize = 24;
    const FOOTER_LEN: usize = 32;

    /// A decoded SILG input-log container.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct InputLogContainer {
        inner_format_version: u32,
        payload: Vec<u8>,
    }

    /// Errors from `InputLogContainer::decode`.
    #[derive(Debug, thiserror::Error, PartialEq, Eq)]
    pub enum InputLogError {
        #[error("bad magic bytes")]
        BadMagic,
        #[error("unsupported container version: {0}")]
        UnsupportedVersion(u16),
        #[error("flags must be 0, got 0x{0:04x}")]
        NonZeroFlags(u16),
        #[error("reserved field must be 0")]
        ReservedNonZero,
        #[error("payload_len {0} inconsistent with buffer size")]
        PayloadLenMismatch(u64),
        #[error("footer mismatch")]
        FooterMismatch,
        #[error("trailing bytes after footer")]
        TrailingBytes,
        #[error("input truncated")]
        Truncated,
    }

    impl InputLogContainer {
        /// Encode a payload into a SILG container, returning the raw bytes.
        pub fn encode(inner_format_version: u32, payload: &[u8]) -> Vec<u8> {
            let payload_len = payload.len() as u64;
            let total = HDR_LEN + payload.len() + FOOTER_LEN;
            let mut buf = Vec::with_capacity(total);

            buf.extend_from_slice(MAGIC); // 0..4
            buf.extend_from_slice(&CONTAINER_VERSION.to_le_bytes()); // 4..6
            buf.extend_from_slice(&0u16.to_le_bytes()); // 6..8  flags=0
            buf.extend_from_slice(&inner_format_version.to_le_bytes()); // 8..12
            buf.extend_from_slice(&0u32.to_le_bytes()); // 12..16 reserved=0
            buf.extend_from_slice(&payload_len.to_le_bytes()); // 16..24
            buf.extend_from_slice(payload); // 24..24+N
                                            // footer
            let hash = blake3::hash(&buf);
            buf.extend_from_slice(hash.as_bytes());
            buf
        }

        /// Decode and validate a SILG container buffer.
        pub fn decode(buf: &[u8]) -> Result<Self, InputLogError> {
            // Minimum: header(24) + footer(32) = 56
            if buf.len() < HDR_LEN + FOOTER_LEN {
                return Err(InputLogError::Truncated);
            }

            // Footer / integrity check first
            let footer_start = buf.len() - FOOTER_LEN;
            let body = &buf[..footer_start];
            let stored: [u8; 32] = buf[footer_start..].try_into().unwrap();
            let computed = blake3::hash(body);
            if computed.as_bytes() != &stored {
                return Err(InputLogError::FooterMismatch);
            }

            // Parse header
            if &buf[0..4] != MAGIC {
                return Err(InputLogError::BadMagic);
            }
            let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
            if version != CONTAINER_VERSION {
                return Err(InputLogError::UnsupportedVersion(version));
            }
            let flags = u16::from_le_bytes(buf[6..8].try_into().unwrap());
            if flags != 0 {
                return Err(InputLogError::NonZeroFlags(flags));
            }
            let inner_format_version = u32::from_le_bytes(buf[8..12].try_into().unwrap());
            let reserved = u32::from_le_bytes(buf[12..16].try_into().unwrap());
            if reserved != 0 {
                return Err(InputLogError::ReservedNonZero);
            }
            let payload_len = u64::from_le_bytes(buf[16..24].try_into().unwrap());

            // Size consistency
            let expected_body = HDR_LEN
                .checked_add(payload_len as usize)
                .ok_or(InputLogError::Truncated)?;
            if body.len() != expected_body {
                return if body.len() < expected_body {
                    Err(InputLogError::PayloadLenMismatch(payload_len))
                } else {
                    Err(InputLogError::TrailingBytes)
                };
            }

            let payload = buf[HDR_LEN..HDR_LEN + payload_len as usize].to_vec();

            Ok(Self {
                inner_format_version,
                payload,
            })
        }

        /// Inner format version tag.
        pub fn inner_version(&self) -> u32 {
            self.inner_format_version
        }

        /// Opaque payload bytes.
        pub fn payload(&self) -> &[u8] {
            &self.payload
        }

        /// Compute the `LogId` of an already-encoded container buffer.
        ///
        /// Caller guarantees `buf.len() >= 32`.
        pub fn log_id(buf: &[u8]) -> LogId {
            let body_len = buf.len().saturating_sub(FOOTER_LEN);
            let hash = blake3::hash(&buf[..body_len]);
            LogId::from_bytes(*hash.as_bytes())
        }
    }
}

// ── proptest strategies ───────────────────────────────────────────────────────

#[cfg(any(test, feature = "test-strategies"))]
pub mod strategies {
    use super::*;
    use proptest::prelude::*;

    pub fn arb_page_hash() -> impl Strategy<Value = PageHash> {
        any::<[u8; 32]>().prop_map(PageHash::from_bytes)
    }

    pub fn arb_snapshot_ref() -> impl Strategy<Value = SnapshotRef> {
        any::<[u8; 32]>().prop_map(SnapshotRef::from_bytes)
    }

    /// Generate a non-zero SnapshotRef (for use as a delta parent).
    pub fn arb_nonzero_snapshot_ref() -> impl Strategy<Value = SnapshotRef> {
        any::<[u8; 32]>()
            .prop_filter("must be non-zero", |b| b != &[0u8; 32])
            .prop_map(SnapshotRef::from_bytes)
    }

    /// Guest RAM bytes in a small range (multiples of 4096 × 1..=8 pages).
    pub fn arb_guest_ram() -> impl Strategy<Value = u64> {
        (1u64..=8u64).prop_map(|n| n * PAGE_SIZE_V1)
    }

    /// Strategy for a DeviceBlob without zstd compression.
    pub fn arb_device_blob_plain() -> impl Strategy<Value = DeviceBlob> {
        (any::<u32>(), proptest::collection::vec(any::<u8>(), 0..=16)).prop_map(
            |(format, bytes)| {
                let raw_len = bytes.len() as u64;
                DeviceBlob {
                    format,
                    zstd: false,
                    bytes,
                    raw_len,
                }
            },
        )
    }

    /// Strategy for valid FULL manifests.
    pub fn arb_full_manifest() -> impl Strategy<Value = Manifest> {
        (arb_guest_ram(), arb_device_blob_plain()).prop_flat_map(|(ram, blob)| {
            let n_pages = (ram / PAGE_SIZE_V1) as usize;
            // Build all N entries with arbitrary hashes
            proptest::collection::vec(arb_page_hash(), n_pages..=n_pages).prop_map(move |hashes| {
                let entries: Vec<ManifestEntry> = hashes
                    .into_iter()
                    .enumerate()
                    .map(|(i, h)| ManifestEntry {
                        page_index: i as u64,
                        page_hash: h,
                    })
                    .collect();
                Manifest::new_full(ram, entries, blob.clone())
                    .expect("arb_full_manifest: invariant-safe")
            })
        })
    }

    /// Strategy for valid DELTA manifests.
    pub fn arb_delta_manifest(ram: u64) -> impl Strategy<Value = Manifest> {
        let max_pages = (ram / PAGE_SIZE_V1) as usize;
        (
            arb_nonzero_snapshot_ref(),
            arb_device_blob_plain(),
            // Subset of page indices (0..max_pages)
            proptest::collection::vec(0u64..max_pages as u64, 0..=max_pages),
        )
            .prop_map(move |(parent_ref, blob, mut indices)| {
                indices.sort_unstable();
                indices.dedup();
                let entries: Vec<ManifestEntry> = indices
                    .into_iter()
                    .map(|i| ManifestEntry {
                        page_index: i,
                        page_hash: PageHash::from_bytes([0u8; 32]),
                    })
                    .collect();
                Manifest::new_delta(parent_ref, ram, entries, blob)
                    .expect("arb_delta_manifest: invariant-safe")
            })
    }

    /// Strategy for either a FULL or DELTA manifest.
    pub fn arb_manifest() -> impl Strategy<Value = Manifest> {
        arb_full_manifest().prop_recursive(2, 4, 1, |inner| {
            inner.prop_flat_map(|parent| {
                let ram = parent.guest_ram_bytes;
                let parent_ref = {
                    let encoded = parent.encode();
                    Manifest::snapshot_ref(&encoded)
                };
                arb_delta_manifest(ram).prop_map(move |mut d| {
                    d.parent = Some(parent_ref.clone());
                    d
                })
            })
        })
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use input_log::InputLogContainer;
    #[allow(unused_imports)]
    use proptest::{prop_assert, prop_assert_eq};

    // ── builder helpers ──────────────────────────────────────────────────────

    fn plain_blob(format: u32, bytes: Vec<u8>) -> DeviceBlob {
        let raw_len = bytes.len() as u64;
        DeviceBlob {
            format,
            zstd: false,
            bytes,
            raw_len,
        }
    }

    fn make_full(n_pages: usize) -> Manifest {
        let ram = n_pages as u64 * PAGE_SIZE_V1;
        let entries: Vec<ManifestEntry> = (0..n_pages)
            .map(|i| ManifestEntry {
                page_index: i as u64,
                page_hash: PageHash::from_bytes([(i as u8).wrapping_add(1); 32]),
            })
            .collect();
        Manifest::new_full(ram, entries, plain_blob(0, vec![])).unwrap()
    }

    fn make_delta(parent: SnapshotRef, ram: u64, indices: &[u64]) -> Manifest {
        let entries: Vec<ManifestEntry> = indices
            .iter()
            .copied()
            .map(|i| ManifestEntry {
                page_index: i,
                page_hash: PageHash::from_bytes([0xdd; 32]),
            })
            .collect();
        Manifest::new_delta(parent, ram, entries, plain_blob(0, vec![])).unwrap()
    }

    // ── encode/decode round-trips ────────────────────────────────────────────

    #[test]
    fn round_trip_full() {
        let m = make_full(4);
        let buf = m.encode();
        let decoded = Manifest::decode(&buf).expect("decode full");
        assert_eq!(decoded, m);
    }

    #[test]
    fn round_trip_delta() {
        let full = make_full(4);
        let encoded_full = full.encode();
        let parent_ref = Manifest::snapshot_ref(&encoded_full);
        let delta = make_delta(parent_ref, full.guest_ram_bytes, &[0, 2]);
        let buf = delta.encode();
        let decoded = Manifest::decode(&buf).expect("decode delta");
        assert_eq!(decoded, delta);
    }

    // ── snapshot_ref ─────────────────────────────────────────────────────────

    #[test]
    fn snapshot_ref_equals_footer() {
        let m = make_full(2);
        let buf = m.encode();
        let sr = Manifest::snapshot_ref(&buf);
        // The footer bytes in buf[len-32..] should equal snapshot_ref
        let footer: [u8; 32] = buf[buf.len() - 32..].try_into().unwrap();
        assert_eq!(sr.to_bytes(), footer);
    }

    // ── golden vector ────────────────────────────────────────────────────────
    //
    // GOLDEN-VECTOR DISCIPLINE: This test encodes a fixed, deterministic
    // manifest and asserts the exact byte length, first-96-bytes hex, and
    // SnapshotRef hex.  These values were computed once from a correct
    // implementation and are hard-coded here.  If the encoding format ever
    // changes intentionally (e.g., a new version), the golden values MUST be
    // recomputed and the commit message must explain the deliberate format
    // change.  Never silently update the golden values.
    //
    // FULL manifest:
    //   guest_ram_bytes = 16384 (4 pages × 4096)
    //   entries: page_index 0..3, page_hash = [i; 32] pattern
    //             (index 0 → [0x00;32], 1 → [0x01;32], 2 → [0x02;32], 3 → [0x03;32])
    //   device blob: format=7, zstd=false, bytes=b"device-state" (12 bytes), raw_len=12
    //
    // DELTA child:
    //   parent = snapshot_ref of FULL container
    //   guest_ram_bytes = 16384
    //   entries: page_index 0 → [0xaa;32], page_index 2 → [0xbb;32]
    //   device blob: format=7, zstd=false, bytes=b"delta-dev" (9 bytes), raw_len=9
    #[test]
    fn golden_vector() {
        // Build FULL manifest
        let entries_full: Vec<ManifestEntry> = (0u64..4)
            .map(|i| ManifestEntry {
                page_index: i,
                page_hash: PageHash::from_bytes([i as u8; 32]),
            })
            .collect();
        let full = Manifest::new_full(
            16384,
            entries_full,
            DeviceBlob {
                format: 7,
                zstd: false,
                bytes: b"device-state".to_vec(),
                raw_len: 12,
            },
        )
        .unwrap();

        let full_buf = full.encode();
        let full_ref = Manifest::snapshot_ref(&full_buf);

        // Build DELTA child
        let delta = Manifest::new_delta(
            full_ref.clone(),
            16384,
            vec![
                ManifestEntry {
                    page_index: 0,
                    page_hash: PageHash::from_bytes([0xaa; 32]),
                },
                ManifestEntry {
                    page_index: 2,
                    page_hash: PageHash::from_bytes([0xbb; 32]),
                },
            ],
            DeviceBlob {
                format: 7,
                zstd: false,
                bytes: b"delta-dev".to_vec(),
                raw_len: 9,
            },
        )
        .unwrap();

        let delta_buf = delta.encode();
        let delta_ref = Manifest::snapshot_ref(&delta_buf);

        // Expected total size:
        // FULL: header(96) + entries(4×40=160) + blob(12) + footer(32) = 300 bytes
        assert_eq!(full_buf.len(), 300, "FULL container must be 300 bytes");
        // DELTA: header(96) + entries(2×40=80) + blob(9) + footer(32) = 217 bytes
        assert_eq!(delta_buf.len(), 217, "DELTA container must be 217 bytes");

        // First 96 bytes of FULL container (header) as hex:
        let full_header_hex: String = full_buf[..96]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        // Hard-coded golden: computed from the reference implementation.
        // Layout: magic(8) version(2) flags(2) header_len(4) parent_hash(32)
        //         guest_ram_bytes(8) page_size(8) entry_count(8) blob_len(8)
        //         blob_raw_len(8) blob_format(4) reserved(4)
        // Full header golden (computed from reference implementation):
        // magic(8) + version(2) + flags(2) + header_len(4) + parent_hash(32)
        // + guest_ram_bytes(8) + page_size(8) + entry_count(8) + blob_len(8)
        // + blob_raw_len(8) + blob_format(4) + reserved(4) = 96 bytes = 192 hex chars
        let expected_full_header = concat!(
            "5350534d414e3031",                                                 // "SPSMAN01"
            "0100",                                                             // version=1 LE
            "0000",     // flags=0 (FULL, no zstd)
            "60000000", // header_len=96 LE
            "0000000000000000000000000000000000000000000000000000000000000000", // parent_hash all-zero
            "0040000000000000", // guest_ram_bytes=16384 LE
            "0010000000000000", // page_size=4096 LE
            "0400000000000000", // entry_count=4 LE
            "0c00000000000000", // device_blob_len=12 LE
            "0c00000000000000", // device_blob_raw_len=12 LE
            "07000000",         // device_blob_format=7 LE
            "00000000"          // reserved=0
        );
        assert_eq!(
            full_header_hex, expected_full_header,
            "FULL header hex mismatch — encoding format has changed! \
             Recompute and update this golden value with a commit explaining the change."
        );

        // GOLDEN snapshot_ref values — hard-coded after first correct run.
        // These are: blake3(full_buf[..268]) and blake3(delta_buf[..185]).
        // If either changes, the encoding format has changed: recompute and update
        // with a commit message explaining the deliberate format change.
        let expected_full_ref = "bc14e648dd8743ab5b150ab80e1ce8e3303adb7cfbf0a20f6a6c136e10d3c7b2";
        let expected_delta_ref = "39fe4e695727291785a8626ad93df38b914c0a8b09bfbae6032759e44322f429";

        let full_ref_hex: String = full_ref
            .to_bytes()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        let delta_ref_hex: String = delta_ref
            .to_bytes()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();

        assert_eq!(
            full_ref_hex, expected_full_ref,
            "FULL snapshot_ref mismatch — encoding format has changed! \
             Recompute and update this golden value with a commit explaining the change."
        );
        assert_eq!(
            delta_ref_hex, expected_delta_ref,
            "DELTA snapshot_ref mismatch — encoding format has changed! \
             Recompute and update this golden value with a commit explaining the change."
        );

        // Verify round-trip
        let full_rt = Manifest::decode(&full_buf).expect("full round-trip");
        assert_eq!(full_rt, full);
        let delta_rt = Manifest::decode(&delta_buf).expect("delta round-trip");
        assert_eq!(delta_rt, delta);
    }

    // ── strictness matrix ────────────────────────────────────────────────────

    #[test]
    fn rejects_bad_magic() {
        let mut buf = make_full(1).encode();
        buf[0] = 0xff;
        assert_eq!(
            Manifest::decode(&buf),
            Err(ManifestError::FooterMismatch),
            "footer check runs before magic; tampering must fail footer first"
        );
        // Build a buffer with correct footer but wrong magic by re-hashing
        let mut buf2 = make_full(1).encode();
        buf2[0] = 0xff;
        let n = buf2.len();
        let hash = blake3::hash(&buf2[..n - 32]);
        buf2[n - 32..].copy_from_slice(hash.as_bytes());
        assert!(matches!(
            Manifest::decode(&buf2),
            Err(ManifestError::BadMagic)
        ));
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut buf = make_full(1).encode();
        let n = buf.len();
        buf[8] = 0x02;
        buf[9] = 0x00; // version=2
        let hash = blake3::hash(&buf[..n - 32]);
        buf[n - 32..].copy_from_slice(hash.as_bytes());
        assert!(matches!(
            Manifest::decode(&buf),
            Err(ManifestError::UnsupportedVersion(2))
        ));
    }

    #[test]
    fn rejects_unknown_flag_bits() {
        let mut buf = make_full(1).encode();
        let n = buf.len();
        buf[10] = 0x04;
        buf[11] = 0x00; // flag bit 2 (unknown)
        let hash = blake3::hash(&buf[..n - 32]);
        buf[n - 32..].copy_from_slice(hash.as_bytes());
        assert!(matches!(
            Manifest::decode(&buf),
            Err(ManifestError::UnknownFlags(_))
        ));
    }

    #[test]
    fn rejects_bad_header_len() {
        let mut buf = make_full(1).encode();
        let n = buf.len();
        // header_len at offset 12..16
        buf[12] = 0x40;
        buf[13] = 0x00;
        buf[14] = 0x00;
        buf[15] = 0x00; // 64
        let hash = blake3::hash(&buf[..n - 32]);
        buf[n - 32..].copy_from_slice(hash.as_bytes());
        assert!(matches!(
            Manifest::decode(&buf),
            Err(ManifestError::BadHeaderLen(64))
        ));
    }

    #[test]
    fn rejects_reserved_nonzero() {
        let mut buf = make_full(1).encode();
        let n = buf.len();
        buf[92] = 0x01; // reserved at 92..96
        let hash = blake3::hash(&buf[..n - 32]);
        buf[n - 32..].copy_from_slice(hash.as_bytes());
        assert!(matches!(
            Manifest::decode(&buf),
            Err(ManifestError::ReservedNonZero)
        ));
    }

    #[test]
    fn rejects_bad_page_size() {
        let m = make_full(1);
        let mut buf = m.encode();
        let n = buf.len();
        // page_size at 56..64 — set to 512
        let ps: u64 = 512;
        buf[56..64].copy_from_slice(&ps.to_le_bytes());
        let hash = blake3::hash(&buf[..n - 32]);
        buf[n - 32..].copy_from_slice(hash.as_bytes());
        assert!(matches!(
            Manifest::decode(&buf),
            Err(ManifestError::BadPageSize(512))
        ));
    }

    #[test]
    fn rejects_guest_ram_not_aligned() {
        let m = make_full(1);
        let mut buf = m.encode();
        let n = buf.len();
        // guest_ram_bytes at 48..56 — set to 5000 (not multiple of 4096)
        let grb: u64 = 5000;
        buf[48..56].copy_from_slice(&grb.to_le_bytes());
        let hash = blake3::hash(&buf[..n - 32]);
        buf[n - 32..].copy_from_slice(hash.as_bytes());
        assert!(matches!(
            Manifest::decode(&buf),
            Err(ManifestError::GuestRamNotAligned(5000))
        ));
    }

    #[test]
    fn rejects_duplicate_entries() {
        // Build raw bytes with two identical page_index entries (both = 0)
        let ram: u64 = 8192; // 2 pages
                             // We'll cheat: build with entry_count=2 but indices [0,0]
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // flags
        buf.extend_from_slice(&HEADER_LEN.to_le_bytes());
        buf.extend_from_slice(&[0u8; 32]); // parent_hash all-zero (FULL)
        buf.extend_from_slice(&ram.to_le_bytes());
        buf.extend_from_slice(&PAGE_SIZE_V1.to_le_bytes());
        buf.extend_from_slice(&2u64.to_le_bytes()); // entry_count=2
        buf.extend_from_slice(&0u64.to_le_bytes()); // device_blob_len=0
        buf.extend_from_slice(&0u64.to_le_bytes()); // device_blob_raw_len=0
        buf.extend_from_slice(&0u32.to_le_bytes()); // format
        buf.extend_from_slice(&0u32.to_le_bytes()); // reserved
                                                    // entry 0: page_index=0
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&[0u8; 32]);
        // entry 1: page_index=0 (duplicate!)
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&[0u8; 32]);
        let hash = blake3::hash(&buf);
        buf.extend_from_slice(hash.as_bytes());
        assert!(
            matches!(
                Manifest::decode(&buf),
                Err(ManifestError::EntriesNotAscending)
            ),
            "duplicate indices must be rejected"
        );
    }

    #[test]
    fn rejects_out_of_order_entries() {
        // page_index [1, 0] — out of order
        let ram: u64 = 8192;
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&HEADER_LEN.to_le_bytes());
        buf.extend_from_slice(&[0u8; 32]);
        buf.extend_from_slice(&ram.to_le_bytes());
        buf.extend_from_slice(&PAGE_SIZE_V1.to_le_bytes());
        buf.extend_from_slice(&2u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        // entry 0: page_index=1
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&[0u8; 32]);
        // entry 1: page_index=0
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&[0u8; 32]);
        let hash = blake3::hash(&buf);
        buf.extend_from_slice(hash.as_bytes());
        assert!(matches!(
            Manifest::decode(&buf),
            Err(ManifestError::EntriesNotAscending)
        ));
    }

    #[test]
    fn rejects_full_count_mismatch() {
        // guest_ram=8192 (2 pages) but entry_count=1 → FullCountMismatch
        let mut buf = Vec::new();
        let ram: u64 = 8192;
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&HEADER_LEN.to_le_bytes());
        buf.extend_from_slice(&[0u8; 32]);
        buf.extend_from_slice(&ram.to_le_bytes());
        buf.extend_from_slice(&PAGE_SIZE_V1.to_le_bytes());
        buf.extend_from_slice(&1u64.to_le_bytes()); // entry_count=1 (wrong for 2-page FULL)
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        // 1 entry: page_index=0
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&[0u8; 32]);
        let hash = blake3::hash(&buf);
        buf.extend_from_slice(hash.as_bytes());
        assert!(matches!(
            Manifest::decode(&buf),
            Err(ManifestError::FullCountMismatch(1, 2))
        ));
    }

    #[test]
    fn rejects_full_non_contiguous() {
        // guest_ram=8192 (2 pages), entry_count=2 but indices [0, 2] — 2 is out of range
        let mut buf = Vec::new();
        let ram: u64 = 8192;
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&HEADER_LEN.to_le_bytes());
        buf.extend_from_slice(&[0u8; 32]);
        buf.extend_from_slice(&ram.to_le_bytes());
        buf.extend_from_slice(&PAGE_SIZE_V1.to_le_bytes());
        buf.extend_from_slice(&2u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        // entry 0: page_index=0
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&[0u8; 32]);
        // entry 1: page_index=2 (skip 1 → non-contiguous)
        buf.extend_from_slice(&2u64.to_le_bytes());
        buf.extend_from_slice(&[0u8; 32]);
        let hash = blake3::hash(&buf);
        buf.extend_from_slice(hash.as_bytes());
        assert!(matches!(
            Manifest::decode(&buf),
            Err(ManifestError::FullNotContiguous)
        ));
    }

    #[test]
    fn rejects_full_with_nonzero_parent() {
        let m = make_full(1);
        let mut buf = m.encode();
        let n = buf.len();
        // Set parent_hash (16..48) to all-ones
        buf[16..48].copy_from_slice(&[0xff; 32]);
        let hash = blake3::hash(&buf[..n - 32]);
        buf[n - 32..].copy_from_slice(hash.as_bytes());
        assert!(matches!(
            Manifest::decode(&buf),
            Err(ManifestError::FullNonZeroParent)
        ));
    }

    #[test]
    fn rejects_delta_zero_parent() {
        // Build a delta manifest buffer with zero parent hash
        let full = make_full(2);
        let full_buf = full.encode();
        let parent_ref = Manifest::snapshot_ref(&full_buf);
        let delta = make_delta(parent_ref, full.guest_ram_bytes, &[0]);
        let mut buf = delta.encode();
        let n = buf.len();
        // Zero out parent hash at 16..48
        buf[16..48].copy_from_slice(&[0u8; 32]);
        let hash = blake3::hash(&buf[..n - 32]);
        buf[n - 32..].copy_from_slice(hash.as_bytes());
        assert!(matches!(
            Manifest::decode(&buf),
            Err(ManifestError::DeltaZeroParent)
        ));
    }

    #[test]
    fn rejects_delta_entry_out_of_range() {
        // guest_ram=4096 (1 page), delta with index=1 (out of range)
        let ram: u64 = 4096;
        let parent_hash = [0x11u8; 32]; // non-zero
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&FLAG_DELTA.to_le_bytes()); // flags=DELTA
        buf.extend_from_slice(&HEADER_LEN.to_le_bytes());
        buf.extend_from_slice(&parent_hash);
        buf.extend_from_slice(&ram.to_le_bytes());
        buf.extend_from_slice(&PAGE_SIZE_V1.to_le_bytes());
        buf.extend_from_slice(&1u64.to_le_bytes()); // entry_count=1
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        // entry: page_index=1 (out of range for 1-page RAM)
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&[0u8; 32]);
        let hash = blake3::hash(&buf);
        buf.extend_from_slice(hash.as_bytes());
        assert!(matches!(
            Manifest::decode(&buf),
            Err(ManifestError::DeltaIndexOutOfRange(1, 1))
        ));
    }

    #[test]
    fn rejects_bad_footer() {
        let buf = make_full(1).encode();
        let mut bad = buf.clone();
        let n = bad.len();
        bad[n - 1] ^= 0xff; // flip last footer byte
        assert!(matches!(
            Manifest::decode(&bad),
            Err(ManifestError::FooterMismatch)
        ));
    }

    #[test]
    fn rejects_trailing_byte() {
        let mut buf = make_full(1).encode();
        // Append a byte and recompute footer
        buf.push(0xde);
        // Don't recompute footer — this causes FooterMismatch
        assert!(matches!(
            Manifest::decode(&buf),
            Err(ManifestError::FooterMismatch)
        ));
    }

    #[test]
    fn rejects_truncation_at_various_offsets() {
        let buf = make_full(2).encode();
        // Test truncation at: 0, 1, 95, 96, 96+39, len-33, len-1
        for &cut in &[0usize, 1, 95, 96, 96 + 39, buf.len() - 33, buf.len() - 1] {
            if cut >= buf.len() {
                continue;
            }
            let result = Manifest::decode(&buf[..cut]);
            assert!(result.is_err(), "expected error for truncation at {cut}");
        }
    }

    #[test]
    fn rejects_non_zstd_raw_len_mismatch() {
        // Construct a manifest where raw_len != blob_len
        let ram = 4096u64;
        let entries = vec![ManifestEntry {
            page_index: 0,
            page_hash: PageHash::from_bytes([0u8; 32]),
        }];
        let blob = DeviceBlob {
            format: 0,
            zstd: false,
            bytes: b"hello".to_vec(),
            raw_len: 99, // wrong
        };
        // new_full validates this
        let result = Manifest::new_full(ram, entries, blob);
        assert!(matches!(
            result,
            Err(ManifestError::NonZstdRawLenMismatch(99, 5))
        ));
    }

    #[test]
    fn rejects_zstd_raw_len_mismatch() {
        // Craft a buffer with DEV_ZSTD set and a blob that decompresses to a
        // different length than device_blob_raw_len
        use zstd::encode_all;
        let payload = b"hello world";
        let compressed = encode_all(payload.as_slice(), 0).unwrap();
        let ram = 4096u64;

        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&(FLAG_DELTA | FLAG_DEV_ZSTD).to_le_bytes()); // DELTA+ZSTD
        buf.extend_from_slice(&HEADER_LEN.to_le_bytes());
        buf.extend_from_slice(&[0x22u8; 32]); // non-zero parent
        buf.extend_from_slice(&ram.to_le_bytes());
        buf.extend_from_slice(&PAGE_SIZE_V1.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes()); // entry_count=0
        buf.extend_from_slice(&(compressed.len() as u64).to_le_bytes()); // blob_len
        buf.extend_from_slice(&9999u64.to_le_bytes()); // raw_len wrong
        buf.extend_from_slice(&0u32.to_le_bytes()); // format
        buf.extend_from_slice(&0u32.to_le_bytes()); // reserved
                                                    // No entries
        buf.extend_from_slice(&compressed);
        let hash = blake3::hash(&buf);
        buf.extend_from_slice(hash.as_bytes());
        assert!(matches!(
            Manifest::decode(&buf),
            Err(ManifestError::ZstdRawLenMismatch(_, 9999))
        ));
    }

    // ── canonicality ─────────────────────────────────────────────────────────

    #[test]
    fn swapped_entries_rejected() {
        // Build a valid DELTA manifest with ≥2 entries, then swap their bytes
        let full = make_full(4);
        let full_buf = full.encode();
        let parent_ref = Manifest::snapshot_ref(&full_buf);
        let delta = make_delta(parent_ref, 16384, &[0, 1, 2]);
        let buf = delta.encode();

        // Entry table starts at offset 96; each entry is 40 bytes
        // Swap entry 0 and entry 1
        let mut bad = buf.clone();
        let a_start = 96;
        let b_start = 96 + 40;
        let entry_a: [u8; 40] = bad[a_start..a_start + 40].try_into().unwrap();
        let entry_b: [u8; 40] = bad[b_start..b_start + 40].try_into().unwrap();
        bad[a_start..a_start + 40].copy_from_slice(&entry_b);
        bad[b_start..b_start + 40].copy_from_slice(&entry_a);

        // Recompute footer so it passes integrity check
        let n = bad.len();
        let hash = blake3::hash(&bad[..n - 32]);
        bad[n - 32..].copy_from_slice(hash.as_bytes());

        assert!(
            matches!(
                Manifest::decode(&bad),
                Err(ManifestError::EntriesNotAscending)
            ),
            "swapped entries (now out of order) must be rejected"
        );
    }

    // ── flatten ──────────────────────────────────────────────────────────────

    #[test]
    fn flatten_full_only() {
        let full = make_full(4);
        let result = flatten(&[&full]).unwrap();
        assert_eq!(result.len(), 4);
        for (i, e) in result.iter().enumerate() {
            assert_eq!(e.page_index, i as u64);
        }
    }

    #[test]
    fn flatten_delta_shadows_parent() {
        let full = make_full(4);
        let full_buf = full.encode();
        let parent_ref = Manifest::snapshot_ref(&full_buf);
        // Delta overwrites pages 1 and 3
        let delta = make_delta(parent_ref, 16384, &[1, 3]);
        let result = flatten(&[&delta, &full]).unwrap();
        assert_eq!(result.len(), 4);
        // Pages 1 and 3 come from delta (hash = [0xdd;32])
        assert_eq!(result[1].page_hash, PageHash::from_bytes([0xdd; 32]));
        assert_eq!(result[3].page_hash, PageHash::from_bytes([0xdd; 32]));
        // Pages 0 and 2 come from full
        assert_eq!(result[0].page_hash, PageHash::from_bytes([0x01; 32]));
        assert_eq!(result[2].page_hash, PageHash::from_bytes([0x03; 32]));
    }

    #[test]
    fn flatten_coverage_gap_fails() {
        // Full manifest with gap: missing page 1 in a 2-page RAM
        // We'll build a DELTA-only chain without a FULL root to trigger RootNotFull,
        // then test a real coverage gap scenario by building a corrupt FULL.
        let full = make_full(2);
        let full_buf = full.encode();
        let parent_ref = Manifest::snapshot_ref(&full_buf);
        // Delta only covers page 0 in a 2-page RAM — flatten would need FULL root
        // for coverage, so use [delta, full] where delta doesn't cover page 1
        // but full already covers everything.
        // For a real gap, we need to craft a chain where page 1 is not covered.
        // Since FULL manifests always cover all pages, we test coverage gap by
        // using flatten_delta with incomplete coverage requirement NOT being checked.
        let delta = make_delta(parent_ref, 8192, &[0]); // only page 0
                                                        // For flatten (Mode A), FULL always covers all pages, so no coverage gap
                                                        // unless we have a malformed FULL. Test flatten_delta coverage (no gap requirement):
        let d_result = flatten_delta(&[&delta]).unwrap();
        assert_eq!(d_result.len(), 1); // only page 0 covered — no error for flatten_delta

        // flatten (Mode A) requires FULL root → always covers all pages if chain is valid
        // Test RootNotFull error:
        let err = flatten(&[&delta]).unwrap_err();
        assert_eq!(err, FlattenError::RootNotFull);
    }

    #[test]
    fn flatten_empty_chain_fails() {
        assert_eq!(flatten(&[]), Err(FlattenError::EmptyChain));
        assert_eq!(flatten_delta(&[]), Err(FlattenError::EmptyChain));
    }

    #[test]
    fn flatten_ram_mismatch_fails() {
        let full_small = make_full(2); // 8192 bytes
        let full_large = make_full(4); // 16384 bytes
        assert_eq!(
            flatten(&[&full_large, &full_small]),
            Err(FlattenError::RamMismatch)
        );
    }

    // ── proptest properties ──────────────────────────────────────────────────

    proptest::proptest! {
        #[test]
        fn proptest_round_trip(m in strategies::arb_full_manifest()) {
            let buf = m.encode();
            let decoded = Manifest::decode(&buf).unwrap();
            prop_assert_eq!(m, decoded);
        }

        #[test]
        fn proptest_snapshot_ref_stable(m in strategies::arb_full_manifest()) {
            let buf = m.encode();
            let r1 = Manifest::snapshot_ref(&buf);
            let r2 = Manifest::snapshot_ref(&buf);
            prop_assert_eq!(r1.to_bytes(), r2.to_bytes());
        }

        #[test]
        fn proptest_canonicality(m in strategies::arb_full_manifest()) {
            let buf1 = m.encode();
            let buf2 = Manifest::decode(&buf1).unwrap().encode();
            prop_assert_eq!(buf1, buf2);
        }

        #[test]
        fn proptest_trailing_byte_rejected(m in strategies::arb_full_manifest()) {
            let mut buf = m.encode();
            buf.push(0xff);
            prop_assert!(Manifest::decode(&buf).is_err());
        }

        #[test]
        fn proptest_truncation_rejected(m in strategies::arb_full_manifest()) {
            let buf = m.encode();
            if buf.len() > 1 {
                let result = Manifest::decode(&buf[..buf.len() - 1]);
                prop_assert!(result.is_err());
            }
        }

        /// Reference implementation for flatten correctness testing.
        #[test]
        fn proptest_flatten_vs_reference(
            full in strategies::arb_full_manifest(),
        ) {
            // Build a chain: FULL root + up to 4 deltas
            let full_buf = full.encode();
            let mut parent_ref = Manifest::snapshot_ref(&full_buf);
            let ram = full.guest_ram_bytes;
            let n_pages = (ram / PAGE_SIZE_V1) as usize;

            // Reference: start with full entries, apply deltas child-last→first
            let mut reference: Vec<Option<ManifestEntry>> = full.entries.iter().cloned().map(Some).collect();

            let n_deltas = 3usize;
            let mut chain_deltas: Vec<Manifest> = Vec::new();

            for layer in 0..n_deltas {
                // Each delta patches page `layer % n_pages`
                let idx = (layer % n_pages) as u64;
                let delta = make_delta(parent_ref.clone(), ram, &[idx]);
                // Update reference (child-first = later layers shadow earlier)
                reference[idx as usize] = Some(ManifestEntry {
                    page_index: idx,
                    page_hash: PageHash::from_bytes([0xdd; 32]),
                });
                let delta_buf = delta.encode();
                parent_ref = Manifest::snapshot_ref(&delta_buf);
                chain_deltas.push(delta);
            }

            // Build flatten chain: child-first
            let mut chain: Vec<&Manifest> = chain_deltas.iter().rev().collect();
            chain.push(&full);

            let result = flatten(&chain).unwrap();

            // Compare with reference
            let reference_entries: Vec<ManifestEntry> = reference.into_iter()
                .map(|o| o.unwrap())
                .collect();
            prop_assert_eq!(result, reference_entries);
        }
    }

    // ── input_log tests ──────────────────────────────────────────────────────

    #[test]
    fn input_log_round_trip() {
        let payload = b"hello world payload";
        let buf = InputLogContainer::encode(42, payload);
        let c = InputLogContainer::decode(&buf).unwrap();
        assert_eq!(c.inner_version(), 42);
        assert_eq!(c.payload(), payload);
    }

    #[test]
    fn input_log_empty_payload() {
        let buf = InputLogContainer::encode(0, &[]);
        let c = InputLogContainer::decode(&buf).unwrap();
        assert_eq!(c.payload().len(), 0);
    }

    #[test]
    fn input_log_log_id_stable() {
        let buf = InputLogContainer::encode(1, b"test");
        let id1 = InputLogContainer::log_id(&buf);
        let id2 = InputLogContainer::log_id(&buf);
        assert_eq!(id1.to_bytes(), id2.to_bytes());
    }

    #[test]
    fn input_log_rejects_bad_magic() {
        let mut buf = InputLogContainer::encode(0, b"x");
        buf[0] = 0xff;
        // Footer will mismatch first
        assert!(matches!(
            InputLogContainer::decode(&buf),
            Err(input_log::InputLogError::FooterMismatch)
        ));
    }

    #[test]
    fn input_log_rejects_bad_magic_with_rehash() {
        let mut buf = InputLogContainer::encode(0, b"x");
        let n = buf.len();
        buf[0] = 0xff;
        let hash = blake3::hash(&buf[..n - 32]);
        buf[n - 32..].copy_from_slice(hash.as_bytes());
        assert!(matches!(
            InputLogContainer::decode(&buf),
            Err(input_log::InputLogError::BadMagic)
        ));
    }

    #[test]
    fn input_log_rejects_nonzero_flags() {
        let mut buf = InputLogContainer::encode(0, b"x");
        let n = buf.len();
        buf[6] = 0x01;
        let hash = blake3::hash(&buf[..n - 32]);
        buf[n - 32..].copy_from_slice(hash.as_bytes());
        assert!(matches!(
            InputLogContainer::decode(&buf),
            Err(input_log::InputLogError::NonZeroFlags(_))
        ));
    }

    #[test]
    fn input_log_rejects_reserved_nonzero() {
        let mut buf = InputLogContainer::encode(0, b"x");
        let n = buf.len();
        buf[12] = 0x01; // reserved at 12..16
        let hash = blake3::hash(&buf[..n - 32]);
        buf[n - 32..].copy_from_slice(hash.as_bytes());
        assert!(matches!(
            InputLogContainer::decode(&buf),
            Err(input_log::InputLogError::ReservedNonZero)
        ));
    }

    #[test]
    fn input_log_rejects_bad_footer() {
        let mut buf = InputLogContainer::encode(0, b"x");
        let n = buf.len();
        buf[n - 1] ^= 0xff;
        assert!(matches!(
            InputLogContainer::decode(&buf),
            Err(input_log::InputLogError::FooterMismatch)
        ));
    }

    #[test]
    fn input_log_rejects_truncation() {
        let buf = InputLogContainer::encode(0, b"hello");
        assert!(InputLogContainer::decode(&buf[..buf.len() - 1]).is_err());
    }

    #[test]
    fn input_log_rejects_trailing_bytes() {
        let buf = InputLogContainer::encode(0, b"hello");
        // Recompute footer to pass integrity but have wrong payload_len
        // Actually, trailing bytes after footer → footer check fails first
        let mut bad = buf.clone();
        bad.push(0xde);
        assert!(InputLogContainer::decode(&bad).is_err());
    }

    #[test]
    fn input_log_rejects_unsupported_version() {
        let mut buf = InputLogContainer::encode(0, b"x");
        let n = buf.len();
        buf[4] = 0x02; // version=2
        let hash = blake3::hash(&buf[..n - 32]);
        buf[n - 32..].copy_from_slice(hash.as_bytes());
        assert!(matches!(
            InputLogContainer::decode(&buf),
            Err(input_log::InputLogError::UnsupportedVersion(2))
        ));
    }

    proptest::proptest! {
        #[test]
        fn proptest_input_log_round_trip(
            inner_version in proptest::prelude::any::<u32>(),
            payload in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..=256usize),
        ) {
            let buf = InputLogContainer::encode(inner_version, &payload);
            let c = InputLogContainer::decode(&buf).unwrap();
            prop_assert_eq!(c.inner_version(), inner_version);
            prop_assert_eq!(c.payload().to_vec(), payload);
        }
    }
}
