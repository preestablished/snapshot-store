//! WI3 — structured error model: storage/meta error → `tonic::Status` with
//! rich `google.rpc.Status` detail payloads.
//!
//! # Wire format
//!
//! `tonic::Status::with_details` stores binary bytes in the
//! `grpc-status-details-bin` trailer.  We encode a prost
//! `google.rpc.Status` message whose `details` field carries typed `Any`
//! entries.  The client decodes these via `Status::details()`.
//!
//! Each `Any` entry uses the canonical type URL:
//! `type.googleapis.com/determinism.snapstore.v1.{TypeName}`.
//!
//! The client crate mirrors this encoding exactly — see the round-trip tests
//! in `tests/server.rs`.

use crate::snapstore_proto::{CurrentGeneration, MissingNodes, MissingPages};
use prost::Message;
use snapstore_meta::MetaError;
use snapstore_store::PutError;
use tonic::{Code, Status};

// ── Minimal google.rpc.Status ─────────────────────────────────────────────────

/// A local prost-encoded `google.rpc.Status` (wire-compatible with the real one).
#[derive(Clone, prost::Message)]
pub struct RpcStatus {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(message, repeated, tag = "3")]
    pub details: Vec<prost_types::Any>,
}

// ── Type-URL helpers ──────────────────────────────────────────────────────────

const TYPE_URL_MISSING_PAGES: &str = "type.googleapis.com/determinism.snapstore.v1.MissingPages";
const TYPE_URL_MISSING_NODES: &str = "type.googleapis.com/determinism.snapstore.v1.MissingNodes";
const TYPE_URL_CURRENT_GENERATION: &str =
    "type.googleapis.com/determinism.snapstore.v1.CurrentGeneration";

fn make_any<M: prost::Message>(type_url: &str, msg: &M) -> prost_types::Any {
    prost_types::Any {
        type_url: type_url.to_owned(),
        value: msg.encode_to_vec(),
    }
}

/// Build a `tonic::Status` that carries a `google.rpc.Status` detail payload
/// in the `grpc-status-details-bin` trailer.
pub fn status_with_detail(
    code: Code,
    message: impl Into<String>,
    details: Vec<prost_types::Any>,
) -> Status {
    let message = message.into();
    let rpc_status = RpcStatus {
        code: code as i32,
        message: message.clone(),
        details,
    };
    let detail_bytes = bytes::Bytes::from(rpc_status.encode_to_vec());
    Status::with_details(code, message, detail_bytes)
}

/// Decode a `google.rpc.Status` from the detail bytes of a `tonic::Status`.
/// Used by tests to verify round-trip encoding.
pub fn decode_status_details(status: &Status) -> Option<RpcStatus> {
    let bytes = status.details();
    if bytes.is_empty() {
        return None;
    }
    RpcStatus::decode(bytes).ok()
}

// ── PutError → Status ─────────────────────────────────────────────────────────

/// Map a `PutError` (from `SnapshotStore::put_snapshot`) to a gRPC `Status`.
pub fn put_error_to_status(e: PutError) -> Status {
    match e {
        PutError::MissingPages(hashes) => {
            let detail = MissingPages {
                page_hashes: hashes.iter().map(|h| h.to_bytes().to_vec()).collect(),
                parent_ref: vec![],
            };
            status_with_detail(
                Code::FailedPrecondition,
                format!("missing {} page(s) in pagestore", detail.page_hashes.len()),
                vec![make_any(TYPE_URL_MISSING_PAGES, &detail)],
            )
        }
        PutError::UnknownParent(r) => {
            let detail = MissingPages {
                page_hashes: vec![],
                parent_ref: r.to_bytes().to_vec(),
            };
            status_with_detail(
                Code::FailedPrecondition,
                "parent manifest not found",
                vec![make_any(TYPE_URL_MISSING_PAGES, &detail)],
            )
        }
        PutError::ParentRamMismatch => {
            Status::new(Code::InvalidArgument, "parent guest_ram_bytes mismatch")
        }
        PutError::Manifest(e) => {
            Status::new(Code::InvalidArgument, format!("manifest decode error: {e}"))
        }
        PutError::Io(e) => Status::new(Code::Internal, format!("I/O error: {e}")),
        PutError::PageStore(e) => Status::new(Code::Internal, format!("page store error: {e}")),
    }
}

// ── MetaError → Status ────────────────────────────────────────────────────────

/// Map a `MetaError` to a gRPC `Status`.
pub fn meta_error_to_status(e: MetaError) -> Status {
    match e {
        MetaError::MissingNodes(ids) => {
            let detail = MissingNodes {
                node_ids: ids.iter().map(|n| n.0).collect(),
            };
            status_with_detail(
                Code::NotFound,
                format!("nodes not found: {:?}", ids),
                vec![make_any(TYPE_URL_MISSING_NODES, &detail)],
            )
        }
        MetaError::CasFailed { current } => {
            let detail = CurrentGeneration {
                generation: current,
            };
            status_with_detail(
                Code::FailedPrecondition,
                format!("CAS failed; current generation is {current}"),
                vec![make_any(TYPE_URL_CURRENT_GENERATION, &detail)],
            )
        }
        MetaError::AlreadyExists => Status::new(Code::AlreadyExists, "node already exists"),
        MetaError::ParentNotFound => Status::new(
            Code::FailedPrecondition,
            "parent node not found or is pruned",
        ),
        MetaError::PruneRootDenied => Status::new(
            Code::FailedPrecondition,
            "root node pruning requires allow_root",
        ),
        MetaError::InvalidArgument(msg) => Status::new(Code::InvalidArgument, msg),
        MetaError::LogIdMismatch => Status::new(
            Code::InvalidArgument,
            "log_id does not match container content",
        ),
        MetaError::LogTooSmall => {
            Status::new(Code::InvalidArgument, "input log container too small")
        }
        MetaError::LogTooLarge => Status::new(
            Code::InvalidArgument,
            "input log container exceeds max size",
        ),
        MetaError::FutureVersion { found, supported } => Status::new(
            Code::Internal,
            format!("schema version {found} > supported {supported}"),
        ),
        MetaError::ActorDead => Status::new(Code::Internal, "meta actor thread died"),
        MetaError::Sqlite(e) => Status::new(Code::Internal, format!("sqlite error: {e}")),
        MetaError::Io(e) => Status::new(Code::Internal, format!("meta I/O error: {e}")),
        MetaError::InvalidNodeStatus(b) => {
            Status::new(Code::Internal, format!("invalid node status byte: {b}"))
        }
    }
}

// ── Decode helpers for tests ──────────────────────────────────────────────────

/// Decode the first `MissingPages` detail from a status.
pub fn decode_missing_pages(status: &Status) -> Option<MissingPages> {
    let rpc = decode_status_details(status)?;
    for detail in &rpc.details {
        if detail.type_url == TYPE_URL_MISSING_PAGES {
            return MissingPages::decode(detail.value.as_slice()).ok();
        }
    }
    None
}

/// Decode the first `MissingNodes` detail from a status.
pub fn decode_missing_nodes(status: &Status) -> Option<MissingNodes> {
    let rpc = decode_status_details(status)?;
    for detail in &rpc.details {
        if detail.type_url == TYPE_URL_MISSING_NODES {
            return MissingNodes::decode(detail.value.as_slice()).ok();
        }
    }
    None
}

/// Decode the first `CurrentGeneration` detail from a status.
pub fn decode_current_generation(status: &Status) -> Option<CurrentGeneration> {
    let rpc = decode_status_details(status)?;
    for detail in &rpc.details {
        if detail.type_url == TYPE_URL_CURRENT_GENERATION {
            return CurrentGeneration::decode(detail.value.as_slice()).ok();
        }
    }
    None
}
