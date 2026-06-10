#![forbid(unsafe_code)]

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

/// A 32-byte content hash identifying an input-log container.
///
/// BLAKE3 of the container bytes excluding the 32-byte footer (the footer
/// *is* this hash — see the input-log container codec).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LogId(pub [u8; 32]);

impl LogId {
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

/// A caller-assigned node identifier, unique within one experiment.
/// The experiment root is always node 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

impl NodeId {
    pub const ROOT: NodeId = NodeId(0);

    pub fn is_root(&self) -> bool {
        self.0 == 0
    }
}

/// Maximum length of an experiment id in bytes.
pub const EXPERIMENT_ID_MAX_BYTES: usize = 128;

/// A caller-chosen experiment identifier: 1..=128 bytes of UTF-8.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExperimentId(String);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ExperimentIdError {
    #[error("experiment id must not be empty")]
    Empty,
    #[error("experiment id exceeds {EXPERIMENT_ID_MAX_BYTES} bytes (got {0})")]
    TooLong(usize),
}

impl ExperimentId {
    /// Validates length on construction: 1..=128 UTF-8 bytes.
    pub fn new(s: impl Into<String>) -> Result<Self, ExperimentIdError> {
        let s = s.into();
        if s.is_empty() {
            return Err(ExperimentIdError::Empty);
        }
        if s.len() > EXPERIMENT_ID_MAX_BYTES {
            return Err(ExperimentIdError::TooLong(s.len()));
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for ExperimentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for ExperimentId {
    type Err = ExperimentIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

/// Lifecycle status of a tree node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum NodeStatus {
    Frontier = 0,
    Expanded = 1,
    Pruned = 2,
    Goal = 3,
}

impl NodeStatus {
    /// Fallible decode for the DB/proto path.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Frontier),
            1 => Some(Self::Expanded),
            2 => Some(Self::Pruned),
            3 => Some(Self::Goal),
            _ => None,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
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

    #[test]
    fn log_id_round_trips() {
        let l = LogId([0x5a; 32]);
        assert_eq!(LogId::from_bytes(l.to_bytes()), l);
    }

    #[test]
    fn node_id_root() {
        assert!(NodeId::ROOT.is_root());
        assert!(NodeId(0).is_root());
        assert!(!NodeId(1).is_root());
    }

    #[test]
    fn experiment_id_bounds() {
        assert_eq!(
            ExperimentId::new(""),
            Err(ExperimentIdError::Empty),
            "empty rejected"
        );
        assert!(ExperimentId::new("a").is_ok(), "1 byte accepted");
        let max = "x".repeat(EXPERIMENT_ID_MAX_BYTES);
        assert!(ExperimentId::new(max).is_ok(), "128 bytes accepted");
        let over = "x".repeat(EXPERIMENT_ID_MAX_BYTES + 1);
        assert_eq!(
            ExperimentId::new(over),
            Err(ExperimentIdError::TooLong(EXPERIMENT_ID_MAX_BYTES + 1)),
            "129 bytes rejected"
        );
        // Length is measured in bytes, not chars: 64 two-byte chars = 128 bytes.
        let two_byte = "é".repeat(64);
        assert_eq!(two_byte.len(), 128);
        assert!(ExperimentId::new(two_byte).is_ok());
        let two_byte_over = "é".repeat(65);
        assert!(ExperimentId::new(two_byte_over).is_err());
    }

    #[test]
    fn experiment_id_round_trips() {
        let id = ExperimentId::new("exp-001").unwrap();
        assert_eq!(id.as_str(), "exp-001");
        assert_eq!(id.to_string(), "exp-001");
        let parsed: ExperimentId = "exp-001".parse().unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn node_status_round_trips() {
        for status in [
            NodeStatus::Frontier,
            NodeStatus::Expanded,
            NodeStatus::Pruned,
            NodeStatus::Goal,
        ] {
            assert_eq!(NodeStatus::from_u8(status.as_u8()), Some(status));
        }
        assert_eq!(NodeStatus::from_u8(4), None);
        assert_eq!(NodeStatus::from_u8(255), None);
    }
}
