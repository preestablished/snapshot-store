#![forbid(unsafe_code)]

use snapstore_types::SnapshotRef;

pub const SNAPSHOT_MANIFEST_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub version: u32,
    pub ref_hint: SnapshotRef,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            version: SNAPSHOT_MANIFEST_VERSION,
            ref_hint: SnapshotRef::zero(),
        }
    }
}
