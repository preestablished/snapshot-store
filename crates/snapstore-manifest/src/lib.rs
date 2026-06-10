#![forbid(unsafe_code)]

use snapstore_types::{PageHash, SnapshotRef};

pub const SNAPSHOT_MANIFEST_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub version: u32,
    pub parent: Option<SnapshotRef>,
    pub icount: u64,
    pub virtual_ns: u64,
    pub memory: MemoryMap,
    pub devices: Vec<DeviceState>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryMap {
    pub page_size: u32,
    pub regions: Vec<MemoryRegion>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryRegion {
    pub gpa: u64,
    pub pages: Vec<PageHash>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeviceState {
    pub kind: String,
    pub blob: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("page_size must be a non-zero power of two")]
    InvalidPageSize,
    #[error("memory regions overlap")]
    OverlappingRegions,
    #[error("duplicate device kind: {0}")]
    DuplicateDeviceKind(String),
}

/// Error returned by [`Manifest::decode`].
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("trailing bytes after manifest")]
    TrailingBytes,
    #[error("invalid UTF-8 string")]
    InvalidUtf8,
    #[error("manifest validation failed: {0}")]
    Invalid(#[from] ManifestError),
    #[error("invalid option tag: {0}")]
    InvalidTag(u8),
}

// ---------------------------------------------------------------------------
// Internal cursor-based reader for strict decoding
// ---------------------------------------------------------------------------

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.remaining() < n {
            return Err(DecodeError::UnexpectedEof);
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8, DecodeError> {
        let b = self.read_bytes(1)?;
        Ok(b[0])
    }

    fn read_u32_le(&mut self) -> Result<u32, DecodeError> {
        let b = self.read_bytes(4)?;
        Ok(u32::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_u64_le(&mut self) -> Result<u64, DecodeError> {
        let b = self.read_bytes(8)?;
        Ok(u64::from_le_bytes(b.try_into().unwrap()))
    }

    fn read_array32(&mut self) -> Result<[u8; 32], DecodeError> {
        let b = self.read_bytes(32)?;
        Ok(b.try_into().unwrap())
    }

    fn read_len_prefixed_bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let len = self.read_u32_le()? as usize;
        let b = self.read_bytes(len)?;
        Ok(b.to_vec())
    }

    fn read_string(&mut self) -> Result<String, DecodeError> {
        let bytes = self.read_len_prefixed_bytes()?;
        String::from_utf8(bytes).map_err(|_| DecodeError::InvalidUtf8)
    }
}

// ---------------------------------------------------------------------------
// Manifest encode / decode / compute_ref
// ---------------------------------------------------------------------------

impl Manifest {
    pub fn new(
        parent: Option<SnapshotRef>,
        icount: u64,
        virtual_ns: u64,
        mut memory: MemoryMap,
        mut devices: Vec<DeviceState>,
    ) -> Result<Self, ManifestError> {
        // Validate page_size is a non-zero power of two
        if memory.page_size == 0 || !memory.page_size.is_power_of_two() {
            return Err(ManifestError::InvalidPageSize);
        }

        // Sort regions by gpa
        memory.regions.sort_by_key(|r| r.gpa);

        // Check for overlapping regions
        for i in 0..memory.regions.len().saturating_sub(1) {
            let region = &memory.regions[i];
            let next = &memory.regions[i + 1];
            let end = region.gpa + region.pages.len() as u64 * memory.page_size as u64;
            if end > next.gpa {
                return Err(ManifestError::OverlappingRegions);
            }
        }

        // Sort devices by kind
        devices.sort_by(|a, b| a.kind.cmp(&b.kind));

        // Check for duplicate device kinds
        for i in 0..devices.len().saturating_sub(1) {
            if devices[i].kind == devices[i + 1].kind {
                return Err(ManifestError::DuplicateDeviceKind(devices[i].kind.clone()));
            }
        }

        Ok(Self {
            version: SNAPSHOT_MANIFEST_VERSION,
            parent,
            icount,
            virtual_ns,
            memory,
            devices,
        })
    }

    /// Encode to canonical bytes. Deterministic: same manifest ⇒ same bytes.
    ///
    /// Encoding order:
    /// 1. `version: u32` LE
    /// 2. `parent: Option<SnapshotRef>` — tag byte (0=None, 1=Some) + 32 bytes if Some
    /// 3. `icount: u64` LE
    /// 4. `virtual_ns: u64` LE
    /// 5. `memory.page_size: u32` LE
    /// 6. `memory.regions: u32 count` + for each: gpa u64 LE, pages u32 count + PageHash 32 bytes each
    /// 7. `devices: u32 count` + for each: kind (u32 len + UTF-8), blob (u32 len + bytes)
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // 1. version
        buf.extend_from_slice(&self.version.to_le_bytes());

        // 2. parent
        match &self.parent {
            None => buf.push(0u8),
            Some(r) => {
                buf.push(1u8);
                buf.extend_from_slice(&r.to_bytes());
            }
        }

        // 3. icount
        buf.extend_from_slice(&self.icount.to_le_bytes());

        // 4. virtual_ns
        buf.extend_from_slice(&self.virtual_ns.to_le_bytes());

        // 5. memory.page_size
        buf.extend_from_slice(&self.memory.page_size.to_le_bytes());

        // 6. memory.regions
        buf.extend_from_slice(&(self.memory.regions.len() as u32).to_le_bytes());
        for region in &self.memory.regions {
            buf.extend_from_slice(&region.gpa.to_le_bytes());
            buf.extend_from_slice(&(region.pages.len() as u32).to_le_bytes());
            for page in &region.pages {
                buf.extend_from_slice(page.as_bytes());
            }
        }

        // 7. devices
        buf.extend_from_slice(&(self.devices.len() as u32).to_le_bytes());
        for device in &self.devices {
            let kind_bytes = device.kind.as_bytes();
            buf.extend_from_slice(&(kind_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(kind_bytes);
            buf.extend_from_slice(&(device.blob.len() as u32).to_le_bytes());
            buf.extend_from_slice(&device.blob);
        }

        buf
    }

    /// Decode from canonical bytes. Strict: trailing bytes or bad tags are errors.
    /// After decoding all fields, re-validates invariants via `Manifest::new`.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut cur = Cursor::new(bytes);

        // 1. version
        let version = cur.read_u32_le()?;

        // 2. parent
        let parent_tag = cur.read_u8()?;
        let parent = match parent_tag {
            0 => None,
            1 => {
                let arr = cur.read_array32()?;
                Some(SnapshotRef::from_bytes(arr))
            }
            tag => return Err(DecodeError::InvalidTag(tag)),
        };

        // 3. icount
        let icount = cur.read_u64_le()?;

        // 4. virtual_ns
        let virtual_ns = cur.read_u64_le()?;

        // 5. memory.page_size
        let page_size = cur.read_u32_le()?;

        // 6. memory.regions
        let region_count = cur.read_u32_le()? as usize;
        let mut regions = Vec::with_capacity(region_count);
        for _ in 0..region_count {
            let gpa = cur.read_u64_le()?;
            let page_count = cur.read_u32_le()? as usize;
            let mut pages = Vec::with_capacity(page_count);
            for _ in 0..page_count {
                let arr = cur.read_array32()?;
                pages.push(PageHash::from_bytes(arr));
            }
            regions.push(MemoryRegion { gpa, pages });
        }

        // 7. devices
        let device_count = cur.read_u32_le()? as usize;
        let mut devices = Vec::with_capacity(device_count);
        for _ in 0..device_count {
            let kind = cur.read_string()?;
            let blob = cur.read_len_prefixed_bytes()?;
            devices.push(DeviceState { kind, blob });
        }

        // Strict: no trailing bytes
        if cur.pos < bytes.len() {
            return Err(DecodeError::TrailingBytes);
        }

        // Re-validate invariants — but Manifest::new re-sorts, so we need to
        // pass the decoded data and let it re-validate. Because the encoded
        // form already has sorted regions and devices (from a prior encode),
        // the round-trip should be stable. We reconstruct via new() to verify
        // invariants, preserving the version field.
        let memory = MemoryMap { page_size, regions };
        let validated = Manifest::new(parent, icount, virtual_ns, memory, devices)?;

        // Restore the original version (new() always stamps SNAPSHOT_MANIFEST_VERSION;
        // we want to preserve whatever version was in the wire bytes so decode is
        // the inverse of encode even for future version numbers).
        Ok(Manifest { version, ..validated })
    }

    /// Compute the BLAKE3 hash of the canonical encoding. This is the [`SnapshotRef`].
    pub fn compute_ref(&self) -> SnapshotRef {
        let encoded = self.encode();
        let hash = blake3::hash(&encoded);
        SnapshotRef::from_bytes(*hash.as_bytes())
    }
}

// ---------------------------------------------------------------------------
// Proptest strategies (behind `test-strategies` feature or `#[cfg(test)]`)
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "test-strategies"))]
pub mod strategies {
    use super::*;
    use proptest::prelude::*;

    fn arb_page_hash() -> impl Strategy<Value = PageHash> {
        any::<[u8; 32]>().prop_map(PageHash::from_bytes)
    }

    fn arb_snapshot_ref() -> impl Strategy<Value = SnapshotRef> {
        any::<[u8; 32]>().prop_map(SnapshotRef::from_bytes)
    }

    fn arb_region_at(gpa: u64) -> impl Strategy<Value = MemoryRegion> {
        proptest::collection::vec(arb_page_hash(), 0..=4).prop_map(move |pages| MemoryRegion {
            gpa,
            pages,
        })
    }

    fn arb_memory_map() -> impl Strategy<Value = MemoryMap> {
        // Generate 0-4 regions with non-overlapping GPAs.
        // Use a fixed stride of 4096 * 16 between region start GPAs so they never overlap
        // even with up to 4 pages each.
        (0_usize..=4_usize).prop_flat_map(|n| {
            let strategies: Vec<_> = (0..n)
                .map(|i| {
                    // Each region starts at i * 0x10000 to ensure no overlap
                    // (max pages=4, page_size=4096 ⇒ max region size = 0x4000 < 0x10000)
                    arb_region_at(i as u64 * 0x10000)
                })
                .collect();
            strategies.prop_map(|regions| MemoryMap {
                page_size: 4096,
                regions,
            })
        })
    }

    fn arb_devices() -> impl Strategy<Value = Vec<DeviceState>> {
        // Generate 0-3 devices with unique kinds chosen from a fixed set
        let kind_choices = prop_oneof![
            Just("vcpu"),
            Just("virtio-net"),
            Just("virtio-blk"),
            Just("nvme"),
            Just("rtc"),
        ];
        proptest::collection::vec(
            (kind_choices, proptest::collection::vec(any::<u8>(), 0..=8)).prop_map(
                |(kind, blob)| DeviceState {
                    kind: kind.to_string(),
                    blob,
                },
            ),
            0..=3,
        )
        .prop_map(|mut devices| {
            // deduplicate by kind (keep first occurrence)
            devices.sort_by(|a, b| a.kind.cmp(&b.kind));
            devices.dedup_by(|a, b| a.kind == b.kind);
            devices
        })
    }

    /// Strategy that generates valid [`Manifest`] instances.
    pub fn arb_manifest() -> impl Strategy<Value = Manifest> {
        let arb_parent = prop_oneof![Just(None), arb_snapshot_ref().prop_map(Some)];
        (
            arb_parent,
            any::<u64>(), // icount
            any::<u64>(), // virtual_ns
            arb_memory_map(),
            arb_devices(),
        )
            .prop_map(|(parent, icount, virtual_ns, memory, devices)| {
                // We know these are valid by construction; unwrap is safe.
                Manifest::new(parent, icount, virtual_ns, memory, devices)
                    .expect("arb_manifest: constructed invariant-safe manifest")
            })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use snapstore_types::PageHash;
    #[allow(unused_imports)]
    use proptest::{prop_assert, prop_assert_eq, prop_assert_ne};

    fn make_region(gpa: u64, num_pages: usize) -> MemoryRegion {
        MemoryRegion {
            gpa,
            pages: vec![PageHash::zero(); num_pages],
        }
    }

    fn make_device(kind: &str) -> DeviceState {
        DeviceState {
            kind: kind.to_string(),
            blob: vec![],
        }
    }

    // -----------------------------------------------------------------------
    // M2-WI1 tests (unchanged)
    // -----------------------------------------------------------------------

    #[test]
    fn valid_manifest_succeeds_and_is_sorted() {
        // Regions given out of order; devices given out of order
        let memory = MemoryMap {
            page_size: 4096,
            regions: vec![
                make_region(0x2000, 2), // gpa 0x2000..0x4000
                make_region(0x0000, 1), // gpa 0x0000..0x1000
            ],
        };
        let devices = vec![make_device("virtio-net"), make_device("block")];

        let m = Manifest::new(None, 42, 100, memory, devices).expect("should succeed");

        assert_eq!(m.version, SNAPSHOT_MANIFEST_VERSION);
        // regions sorted by gpa
        assert_eq!(m.memory.regions[0].gpa, 0x0000);
        assert_eq!(m.memory.regions[1].gpa, 0x2000);
        // devices sorted by kind
        assert_eq!(m.devices[0].kind, "block");
        assert_eq!(m.devices[1].kind, "virtio-net");
    }

    #[test]
    fn non_power_of_two_page_size_fails() {
        let memory = MemoryMap {
            page_size: 3000,
            regions: vec![],
        };
        let err = Manifest::new(None, 0, 0, memory, vec![]).unwrap_err();
        assert!(matches!(err, ManifestError::InvalidPageSize));
    }

    #[test]
    fn zero_page_size_fails() {
        let memory = MemoryMap {
            page_size: 0,
            regions: vec![],
        };
        let err = Manifest::new(None, 0, 0, memory, vec![]).unwrap_err();
        assert!(matches!(err, ManifestError::InvalidPageSize));
    }

    #[test]
    fn overlapping_regions_fails() {
        // region 0: gpa=0x0000, 2 pages @ 4096 => covers 0x0000..0x2000
        // region 1: gpa=0x1000 => starts inside region 0
        let memory = MemoryMap {
            page_size: 4096,
            regions: vec![make_region(0x0000, 2), make_region(0x1000, 1)],
        };
        let err = Manifest::new(None, 0, 0, memory, vec![]).unwrap_err();
        assert!(matches!(err, ManifestError::OverlappingRegions));
    }

    #[test]
    fn duplicate_device_kinds_fails() {
        let memory = MemoryMap {
            page_size: 4096,
            regions: vec![],
        };
        let devices = vec![make_device("block"), make_device("block")];
        let err = Manifest::new(None, 0, 0, memory, devices).unwrap_err();
        match err {
            ManifestError::DuplicateDeviceKind(k) => assert_eq!(k, "block"),
            _ => panic!("expected DuplicateDeviceKind"),
        }
    }

    #[test]
    fn minimal_manifest_succeeds() {
        let memory = MemoryMap {
            page_size: 4096,
            regions: vec![],
        };
        let m =
            Manifest::new(None, 0, 0, memory, vec![]).expect("minimal manifest should succeed");
        assert_eq!(m.version, SNAPSHOT_MANIFEST_VERSION);
        assert!(m.parent.is_none());
        assert!(m.memory.regions.is_empty());
        assert!(m.devices.is_empty());
    }

    // -----------------------------------------------------------------------
    // M2-WI2: Golden vector test
    // -----------------------------------------------------------------------
    // Build a fixed manifest and assert its encoding matches the expected bytes.
    // This catches accidental format drift across refactors.
    //
    // Manifest:
    //   version=1, parent=None, icount=42, virtual_ns=100,
    //   page_size=4096, one region at gpa=0 with one page hash=[0x01;32],
    //   one device kind="vcpu0" blob=vec![0x42]
    //
    // Expected encoding (all LE):
    //   [01 00 00 00]                 version=1
    //   [00]                          parent=None
    //   [2a 00 00 00 00 00 00 00]     icount=42
    //   [64 00 00 00 00 00 00 00]     virtual_ns=100
    //   [00 10 00 00]                 page_size=4096
    //   [01 00 00 00]                 regions count=1
    //   [00 00 00 00 00 00 00 00]     gpa=0
    //   [01 00 00 00]                 pages count=1
    //   [01*32]                       page hash
    //   [01 00 00 00]                 devices count=1
    //   [05 00 00 00]                 kind len=5
    //   [76 63 70 75 30]              "vcpu0"
    //   [01 00 00 00]                 blob len=1
    //   [42]                          blob byte
    #[test]
    fn golden_vector() {
        let memory = MemoryMap {
            page_size: 4096,
            regions: vec![MemoryRegion {
                gpa: 0,
                pages: vec![PageHash::from_bytes([0x01; 32])],
            }],
        };
        let devices = vec![DeviceState {
            kind: "vcpu0".to_string(),
            blob: vec![0x42],
        }];
        let m = Manifest::new(None, 42, 100, memory, devices).expect("golden manifest");

        let encoded = m.encode();

        #[rustfmt::skip]
        let expected: Vec<u8> = vec![
            // version = 1
            0x01, 0x00, 0x00, 0x00,
            // parent = None
            0x00,
            // icount = 42
            0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // virtual_ns = 100
            0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // page_size = 4096
            0x00, 0x10, 0x00, 0x00,
            // regions count = 1
            0x01, 0x00, 0x00, 0x00,
            // region 0 gpa = 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            // region 0 pages count = 1
            0x01, 0x00, 0x00, 0x00,
            // page hash [0x01; 32]
            0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
            0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
            0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
            0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
            // devices count = 1
            0x01, 0x00, 0x00, 0x00,
            // kind len = 5
            0x05, 0x00, 0x00, 0x00,
            // "vcpu0"
            0x76, 0x63, 0x70, 0x75, 0x30,
            // blob len = 1
            0x01, 0x00, 0x00, 0x00,
            // blob byte
            0x42,
        ];

        assert_eq!(
            encoded, expected,
            "golden vector mismatch — encoding format has changed!"
        );

        // Also verify round-trip
        let decoded = Manifest::decode(&encoded).expect("golden decode");
        assert_eq!(decoded, m);
    }

    // -----------------------------------------------------------------------
    // M2-WI2: Basic decode error cases
    // -----------------------------------------------------------------------

    #[test]
    fn decode_trailing_bytes_is_error() {
        let memory = MemoryMap {
            page_size: 4096,
            regions: vec![],
        };
        let m = Manifest::new(None, 0, 0, memory, vec![]).unwrap();
        let mut encoded = m.encode();
        encoded.push(0xff);
        assert!(matches!(Manifest::decode(&encoded), Err(DecodeError::TrailingBytes)));
    }

    #[test]
    fn decode_truncated_is_error() {
        let memory = MemoryMap {
            page_size: 4096,
            regions: vec![],
        };
        let m = Manifest::new(None, 0, 0, memory, vec![]).unwrap();
        let encoded = m.encode();
        assert!(encoded.len() > 1, "encoded must be non-trivial");
        let truncated = &encoded[..encoded.len() - 1];
        assert!(matches!(Manifest::decode(truncated), Err(DecodeError::UnexpectedEof)));
    }

    #[test]
    fn decode_invalid_option_tag_is_error() {
        // version (4 bytes) then a bad parent tag
        let mut buf = vec![0x01, 0x00, 0x00, 0x00]; // version=1
        buf.push(0x02); // invalid Option tag
        assert!(matches!(Manifest::decode(&buf), Err(DecodeError::InvalidTag(2))));
    }

    // -----------------------------------------------------------------------
    // M2-WI2: Proptest properties
    // -----------------------------------------------------------------------

    proptest::proptest! {
        #[test]
        fn round_trip_identity(m in strategies::arb_manifest()) {
            let encoded = m.encode();
            let decoded = Manifest::decode(&encoded).unwrap();
            prop_assert_eq!(m, decoded);
        }

        #[test]
        fn canonical_bytes(m in strategies::arb_manifest()) {
            let encoded = m.encode();
            let re_encoded = Manifest::decode(&encoded).unwrap().encode();
            prop_assert_eq!(encoded, re_encoded);
        }

        #[test]
        fn ref_stability_mutation(m in strategies::arb_manifest()) {
            let r1 = m.compute_ref();
            // mutate icount — must change ref
            let mut m2 = m.clone();
            m2.icount = m2.icount.wrapping_add(1);
            let r2 = m2.compute_ref();
            // Only assert if mutation actually changes the manifest
            if m.icount != m2.icount {
                prop_assert_ne!(r1, r2);
            }
        }

        #[test]
        fn strictness_trailing_byte(m in strategies::arb_manifest()) {
            let mut encoded = m.encode();
            encoded.push(0xff);
            let result = Manifest::decode(&encoded);
            prop_assert!(result.is_err());
        }

        #[test]
        fn strictness_truncation(m in strategies::arb_manifest()) {
            let encoded = m.encode();
            if encoded.len() > 1 {
                let truncated = &encoded[..encoded.len() - 1];
                let result = Manifest::decode(truncated);
                prop_assert!(result.is_err());
            }
        }
    }
}
