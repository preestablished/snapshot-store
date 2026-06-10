//! gRPC rich-status detail encoding/decoding.
//!
//! The server side attaches typed error detail payloads to `tonic::Status`
//! using the `grpc-status-details-bin` trailing metadata.  The wire format
//! is a `google.rpc.Status` proto (code + message + repeated `Any` details).
//!
//! We use a minimal local prost struct (`RpcStatus`) that is wire-compatible
//! with `google.rpc.Status` (field tags 1, 2, 3), rather than importing the
//! full proto because tonic_types does not re-export a decodable pb::Status
//! in tonic 0.12's stable surface.
//!
//! Type URLs follow the convention:
//!   `type.googleapis.com/determinism.snapstore.v1.<MessageName>`

use prost::Message;
use prost_types::Any;
use snapstore_types::{PageHash, SnapshotRef};

use crate::error::ClientError;

// ── minimal google.rpc.Status clone ─────────────────────────────────────────

/// Wire-compatible with `google.rpc.Status` (tags 1=code, 2=message, 3=details).
#[derive(Clone, prost::Message)]
pub struct RpcStatus {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(message, repeated, tag = "3")]
    pub details: Vec<Any>,
}

// ── detail message wrappers ───────────────────────────────────────────────────

/// Mirrors `determinism.snapstore.v1.MissingPages`.
#[derive(Clone, prost::Message)]
pub struct MissingPagesPb {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub page_hashes: Vec<Vec<u8>>,
    #[prost(bytes = "vec", tag = "2")]
    pub parent_ref: Vec<u8>,
}

/// Mirrors `determinism.snapstore.v1.MissingNodes`.
#[derive(Clone, prost::Message)]
pub struct MissingNodesPb {
    #[prost(uint64, repeated, tag = "1")]
    pub node_ids: Vec<u64>,
}

/// Mirrors `determinism.snapstore.v1.CurrentGeneration`.
#[derive(Clone, prost::Message)]
pub struct CurrentGenerationPb {
    #[prost(uint64, tag = "1")]
    pub generation: u64,
}

// ── type URL constants ────────────────────────────────────────────────────────

pub const TYPE_URL_MISSING_PAGES: &str =
    "type.googleapis.com/determinism.snapstore.v1.MissingPages";
pub const TYPE_URL_MISSING_NODES: &str =
    "type.googleapis.com/determinism.snapstore.v1.MissingNodes";
pub const TYPE_URL_CURRENT_GENERATION: &str =
    "type.googleapis.com/determinism.snapstore.v1.CurrentGeneration";

// ── encoding helpers ──────────────────────────────────────────────────────────

/// Encode a `MissingPages` detail and attach it to the bytes that can be passed
/// to `tonic::Status::with_details`.
pub fn encode_missing_pages(page_hashes: &[PageHash], parent_ref: Option<&SnapshotRef>) -> Vec<u8> {
    let pb = MissingPagesPb {
        page_hashes: page_hashes.iter().map(|h| h.as_bytes().to_vec()).collect(),
        parent_ref: parent_ref
            .map(|r| r.to_bytes().to_vec())
            .unwrap_or_default(),
    };
    encode_detail_bytes(TYPE_URL_MISSING_PAGES, &pb)
}

/// Encode a `MissingNodes` detail.
pub fn encode_missing_nodes(node_ids: &[u64]) -> Vec<u8> {
    let pb = MissingNodesPb {
        node_ids: node_ids.to_vec(),
    };
    encode_detail_bytes(TYPE_URL_MISSING_NODES, &pb)
}

/// Encode a `CurrentGeneration` detail.
pub fn encode_current_generation(generation: u64) -> Vec<u8> {
    let pb = CurrentGenerationPb { generation };
    encode_detail_bytes(TYPE_URL_CURRENT_GENERATION, &pb)
}

/// Wrap a prost-encoded message inside an `RpcStatus` and return the binary.
fn encode_detail_bytes<M: Message>(type_url: &str, msg: &M) -> Vec<u8> {
    let any = Any {
        type_url: type_url.to_owned(),
        value: msg.encode_to_vec(),
    };
    let rpc_status = RpcStatus {
        code: 0,
        message: String::new(),
        details: vec![any],
    };
    rpc_status.encode_to_vec()
}

/// Build a `tonic::Status` carrying a single rich detail payload.
///
/// The detail bytes are placed in the `grpc-status-details-bin` header via
/// `tonic::Status::with_details`, which is the correct path for the server to
/// propagate rich status details to the client.
pub fn status_with_detail(
    code: tonic::Code,
    message: &str,
    detail_bytes: Vec<u8>,
) -> tonic::Status {
    tonic::Status::with_details(code, message, bytes::Bytes::from(detail_bytes))
}

// ── decoding ──────────────────────────────────────────────────────────────────

/// Attempt to decode the `grpc-status-details-bin` detail bytes from a tonic
/// `Status` into a typed `ClientError`.  Returns `None` if no decodable
/// detail is present.
///
/// Tonic 0.12 surfaces the `grpc-status-details-bin` header through
/// `Status::details()` (a `Bytes` field populated by `from_header_map`).
/// We decode that as an `RpcStatus` proto and inspect the `Any` entries.
pub fn decode_details(status: &tonic::Status) -> Option<ClientError> {
    let bytes = status.details();
    if bytes.is_empty() {
        return None;
    }
    let rpc_status = RpcStatus::decode(bytes).ok()?;

    for any in &rpc_status.details {
        match any.type_url.as_str() {
            TYPE_URL_MISSING_PAGES => {
                if let Ok(pb) = MissingPagesPb::decode(any.value.as_slice()) {
                    let page_hashes: Vec<PageHash> = pb
                        .page_hashes
                        .iter()
                        .filter_map(|b| {
                            let arr: [u8; 32] = b.as_slice().try_into().ok()?;
                            Some(PageHash::from_bytes(arr))
                        })
                        .collect();
                    let parent_ref = if pb.parent_ref.len() == 32 {
                        let arr: [u8; 32] = pb.parent_ref.as_slice().try_into().ok()?;
                        Some(SnapshotRef::from_bytes(arr))
                    } else {
                        None
                    };
                    return Some(ClientError::MissingPages {
                        page_hashes,
                        parent_ref,
                    });
                }
            }
            TYPE_URL_MISSING_NODES => {
                if let Ok(pb) = MissingNodesPb::decode(any.value.as_slice()) {
                    return Some(ClientError::MissingNodes {
                        node_ids: pb.node_ids,
                    });
                }
            }
            TYPE_URL_CURRENT_GENERATION => {
                if let Ok(pb) = CurrentGenerationPb::decode(any.value.as_slice()) {
                    return Some(ClientError::CasFailed {
                        current_generation: pb.generation,
                    });
                }
            }
            _ => {}
        }
    }
    None
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: round-trip an RpcStatus bytes blob through encode→attach→decode.
    fn round_trip_via_status(code: tonic::Code, detail_bytes: Vec<u8>) -> Option<ClientError> {
        let status = status_with_detail(code, "test", detail_bytes);
        decode_details(&status)
    }

    #[test]
    fn missing_pages_round_trip() {
        let hashes = vec![
            PageHash::from_bytes([0x11; 32]),
            PageHash::from_bytes([0x22; 32]),
        ];
        let parent = SnapshotRef::from_bytes([0xaa; 32]);
        let bytes = encode_missing_pages(&hashes, Some(&parent));
        let err =
            round_trip_via_status(tonic::Code::FailedPrecondition, bytes).expect("should decode");

        match err {
            ClientError::MissingPages {
                page_hashes,
                parent_ref,
            } => {
                assert_eq!(page_hashes.len(), 2);
                assert_eq!(page_hashes[0], PageHash::from_bytes([0x11; 32]));
                assert_eq!(page_hashes[1], PageHash::from_bytes([0x22; 32]));
                assert_eq!(parent_ref, Some(SnapshotRef::from_bytes([0xaa; 32])));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn missing_pages_no_parent_round_trip() {
        let hashes = vec![PageHash::from_bytes([0x33; 32])];
        let bytes = encode_missing_pages(&hashes, None);
        let err =
            round_trip_via_status(tonic::Code::FailedPrecondition, bytes).expect("should decode");

        match err {
            ClientError::MissingPages { parent_ref, .. } => {
                assert_eq!(parent_ref, None);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn missing_nodes_round_trip() {
        let node_ids = vec![1u64, 5, 99];
        let bytes = encode_missing_nodes(&node_ids);
        let err = round_trip_via_status(tonic::Code::NotFound, bytes).expect("should decode");

        match err {
            ClientError::MissingNodes { node_ids: decoded } => {
                assert_eq!(decoded, vec![1u64, 5, 99]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn current_generation_round_trip() {
        let bytes = encode_current_generation(42);
        let err =
            round_trip_via_status(tonic::Code::FailedPrecondition, bytes).expect("should decode");

        match err {
            ClientError::CasFailed { current_generation } => {
                assert_eq!(current_generation, 42);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn no_details_returns_none() {
        let status = tonic::Status::new(tonic::Code::Internal, "no details");
        assert!(decode_details(&status).is_none());
    }

    #[test]
    fn unknown_type_url_returns_none() {
        let any = Any {
            type_url: "type.googleapis.com/unknown.Type".to_owned(),
            value: vec![0xde, 0xad],
        };
        let rpc_status = RpcStatus {
            code: 9,
            message: "x".into(),
            details: vec![any],
        };
        let bytes = rpc_status.encode_to_vec();
        let status = status_with_detail(tonic::Code::FailedPrecondition, "x", bytes);
        assert!(decode_details(&status).is_none());
    }
}
