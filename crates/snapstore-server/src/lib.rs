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

pub mod build_server;
pub mod config;
pub mod errors;
pub mod metrics;
pub mod service;
pub mod startup;

#[cfg(test)]
mod tests {
    use super::snapstore_proto;

    #[test]
    fn generated_service_types_exist() {
        // Server + client stubs and the error-detail messages all codegen.
        let _ = snapstore_proto::snapshot_store_client::SnapshotStoreClient::<
            tonic::transport::Channel,
        >::new;
        let detail = snapstore_proto::MissingPages {
            page_hashes: vec![vec![0u8; 32]],
            parent_ref: vec![],
        };
        assert_eq!(detail.page_hashes.len(), 1);
        let node = snapstore_proto::NodeMeta::default();
        assert_eq!(node.node_id, 0);
    }
}
