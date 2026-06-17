use snapstore_types::{PageHash, SnapshotRef};

/// Typed errors returned by the async and blocking `SnapstoreClient`.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The server reported that one or more pages referenced by a `PutSnapshot`
    /// are not durably stored. The caller must upload missing pages and retry.
    #[error("server reported missing pages (count={})", page_hashes.len())]
    MissingPages {
        page_hashes: Vec<PageHash>,
        /// Non-empty when the parent manifest itself is absent.
        parent_ref: Option<SnapshotRef>,
    },

    /// `UpdateNodes` rolled back because the listed node ids do not exist.
    #[error("server reported missing nodes: {:?}", node_ids)]
    MissingNodes { node_ids: Vec<u64> },

    /// A CAS operation (`PutMetadata`/`DeleteMetadata` with
    /// `expected_generation`) failed because the stored generation does not
    /// match. `current_generation == 0` means the key is absent.
    #[error("CAS failed: current generation is {current_generation}")]
    CasFailed { current_generation: u64 },

    /// `CreateNode` was called with `(experiment_id, node_id)` that already
    /// exists with different immutable fields, or a duplicate key was
    /// re-inserted with conflicting state.
    #[error("resource already exists")]
    AlreadyExists,

    /// The server replied with a gRPC status that did not carry a rich error
    /// detail decodable into one of the above variants.
    #[error("gRPC error: {0}")]
    Status(#[from] tonic::Status),

    /// Transport-level error: channel setup / connect / TLS / UDS.
    #[error("transport error: {0}")]
    Transport(String),

    /// A container returned by the server failed its BLAKE3 footer check,
    /// meaning the bytes were corrupted in transit or at rest.
    #[error("corrupt payload: {} — expected hash {} got {}", .0.context, .0.expected, .0.actual)]
    CorruptPayload(Box<CorruptDetail>),

    /// The server returned a `batch_blake3` that does not match the locally
    /// computed hash over the page hashes sent. This is a P0 integrity signal.
    #[error("batch_blake3 cross-check mismatch: expected {expected} got {actual}")]
    BatchBlake3Mismatch { expected: String, actual: String },

    /// A log container returned for a `GetInputLog` has a footer that does not
    /// match the requested `log_id`.
    #[error("input-log corrupt: expected log_id {expected} got {actual}")]
    CorruptInputLog { expected: String, actual: String },
}

/// Detail payload for `ClientError::CorruptPayload`.
#[derive(Debug)]
pub struct CorruptDetail {
    pub context: String,
    pub expected: String,
    pub actual: String,
}

impl ClientError {
    /// Build a `CorruptPayload` error from a snapshot ref and the two hash bytes.
    pub(crate) fn corrupt_snapshot(
        expected: &[u8],
        actual: &[u8],
        context: impl Into<String>,
    ) -> Self {
        ClientError::CorruptPayload(Box::new(CorruptDetail {
            context: context.into(),
            expected: hex(expected),
            actual: hex(actual),
        }))
    }

    /// Build a general `CorruptPayload` error when the expected/actual values
    /// are already formatted for the caller's context.
    pub(crate) fn corrupt_payload(
        context: impl Into<String>,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Self {
        ClientError::CorruptPayload(Box::new(CorruptDetail {
            context: context.into(),
            expected: expected.into(),
            actual: actual.into(),
        }))
    }

    /// Whether this error is non-retryable: caller-bug or explicit state conflict.
    pub fn is_non_retryable(&self) -> bool {
        matches!(
            self,
            ClientError::MissingPages { .. }
                | ClientError::MissingNodes { .. }
                | ClientError::CasFailed { .. }
                | ClientError::AlreadyExists
                | ClientError::BatchBlake3Mismatch { .. }
                | ClientError::CorruptPayload(_)
                | ClientError::CorruptInputLog { .. }
        )
    }
}

/// Resolve a `tonic::Status` into the richest available `ClientError`.
///
/// Tries to decode rich detail payloads first. Falls back to a plain
/// `ClientError::Status` if no structured detail is present or decoding fails.
pub fn decode_status(status: tonic::Status) -> ClientError {
    use tonic::Code;

    match status.code() {
        Code::AlreadyExists => return ClientError::AlreadyExists,
        Code::FailedPrecondition | Code::NotFound => {}
        _ => return ClientError::Status(status),
    }

    // Try to extract rich details
    if let Some(decoded) = crate::details::decode_details(&status) {
        return decoded;
    }

    ClientError::Status(status)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Convenience alias for results from this crate.
pub type ClientResult<T> = Result<T, ClientError>;
