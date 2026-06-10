// `forbid` is too strong while tonic codegen is in the tree: include_proto
// expands to code we don't control. Manual code keeps the discipline via deny.
#![deny(unsafe_code)]

/// Generated `determinism.snapstore.v1` types and service stubs.
///
/// Single re-export seam: when control-plane fulfils the
/// adopt-snapstore-proto-v1 request, this module body swaps to a re-export
/// of the published crate and nothing else changes (phase-2 plan, risk 2).
pub mod snapstore_proto {
    tonic::include_proto!("determinism.snapstore.v1");
}
