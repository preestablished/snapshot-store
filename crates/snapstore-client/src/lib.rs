#![forbid(unsafe_code)]

pub fn sample_put_snapshot() -> determinism_proto::snapstore::v1::PutSnapshotRequest {
    determinism_proto::snapstore::v1::PutSnapshotRequest {
        manifest: b"m0-manifest".to_vec(),
    }
}
