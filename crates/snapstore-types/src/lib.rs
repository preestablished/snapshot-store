#![forbid(unsafe_code)]

pub use determinism_proto::snapstore::v1::NodeMeta;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRef(pub [u8; 32]);

impl SnapshotRef {
    pub fn zero() -> Self {
        Self([0; 32])
    }
}
