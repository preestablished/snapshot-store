use snapstore_types::NodeId;

/// Errors returned by [`crate::MetaDb`] operations.
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("io error: {0}")]
    Io(String),

    #[error("actor thread has died")]
    ActorDead,

    #[error("database schema version {found} is newer than supported {supported}")]
    FutureVersion { found: i64, supported: i64 },

    #[error("parent node not found or is pruned")]
    ParentNotFound,

    #[error("node already exists with different immutable fields")]
    AlreadyExists,

    #[error("nodes not found: {0:?}")]
    MissingNodes(Vec<NodeId>),

    #[error("root node (node_id=0) pruning disallowed without allow_root")]
    PruneRootDenied,

    #[error("CAS failed; current generation is {current}")]
    CasFailed { current: u64 },

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("blake3 mismatch: log_id does not match container content")]
    LogIdMismatch,

    #[error("input log too small (minimum required bytes)")]
    LogTooSmall,

    #[error("input log exceeds max size")]
    LogTooLarge,

    #[error("node status decode error: invalid byte {0}")]
    InvalidNodeStatus(u8),
}
