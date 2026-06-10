#![forbid(unsafe_code)]

#[cfg(feature = "proto")]
pub use determinism_proto::snapstore::v1::NodeMeta;

/// Fixed page size used throughout the snapshot store.
pub const PAGE_SIZE: usize = 4096;

/// A 32-byte content hash identifying a single page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PageHash(pub [u8; 32]);

impl PageHash {
    pub fn zero() -> Self {
        Self([0; 32])
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }

    pub fn from_bytes(b: [u8; 32]) -> Self {
        Self(b)
    }
}

/// An opaque identifier for a pack file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PackId(pub u32);

/// The location of a page within a specific pack file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageLoc {
    pub pack: PackId,
    pub offset: u64,
}

/// A 32-byte content-addressed reference to a snapshot root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRef(pub [u8; 32]);

impl SnapshotRef {
    pub fn zero() -> Self {
        Self([0; 32])
    }

    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }

    pub fn from_bytes(b: [u8; 32]) -> Self {
        Self(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_size_is_4096() {
        assert_eq!(PAGE_SIZE, 4096);
    }

    #[test]
    fn page_hash_round_trips() {
        let h = PageHash([0xab; 32]);
        assert_eq!(PageHash::from_bytes(h.to_bytes()), h);
    }

    #[test]
    fn snapshot_ref_round_trips() {
        let r = SnapshotRef([0xcd; 32]);
        assert_eq!(SnapshotRef::from_bytes(r.to_bytes()), r);
    }
}
