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
}

#[cfg(test)]
mod tests {
    use super::*;
    use snapstore_types::PageHash;

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
            regions: vec![
                make_region(0x0000, 2),
                make_region(0x1000, 1),
            ],
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
        let m = Manifest::new(None, 0, 0, memory, vec![]).expect("minimal manifest should succeed");
        assert_eq!(m.version, SNAPSHOT_MANIFEST_VERSION);
        assert!(m.parent.is_none());
        assert!(m.memory.regions.is_empty());
        assert!(m.devices.is_empty());
    }
}
