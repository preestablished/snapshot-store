//! Container build helpers.
//!
//! These functions compose `snapstore-manifest` primitives to construct
//! `.spm` snapshot-manifest containers and SILG input-log containers.
//!
//! **Dependency note**: `snapstore-client` depends on `snapstore-manifest`
//! here, which is a deliberate deviation from ARCHITECTURE.md §1's
//! "client = types + proto + localpath" rule.  `snapstore-manifest` is a
//! pure, I/O-free crate; this dependency is recorded as a decision in the
//! phase-2 plan (02-m4, WI4) and tracked in docs-drift item 05.

use snapstore_manifest::{input_log::InputLogContainer, DeviceBlob, Manifest, ManifestEntry};
use snapstore_types::{PageHash, SnapshotRef};

pub use snapstore_manifest::{ManifestEntry as ManifestEntryRe, ManifestError};

/// Build a `.spm` snapshot-manifest container from raw page data.
///
/// - `parent`: `None` for FULL manifests; `Some(ref)` for DELTA manifests.
/// - `guest_ram_bytes`: must be a multiple of 4096 and equal `pages.len() * 4096`
///   for FULL manifests.
/// - `pages`: `(page_index, 4096-byte payload)` pairs. For FULL manifests all
///   page indices 0..N must be present (the builder sorts and validates).
/// - `device_blob`: opaque device-state blob carried in the container footer.
///
/// Returns the encoded container bytes; the BLAKE3 footer **is** the
/// `SnapshotRef`.
pub fn build_snapshot_container(
    parent: Option<&SnapshotRef>,
    guest_ram_bytes: u64,
    pages: &[(u64, &[u8; 4096])],
    device_blob: DeviceBlob,
) -> Result<Vec<u8>, ManifestError> {
    let entries: Vec<ManifestEntry> = pages
        .iter()
        .map(|(idx, data)| ManifestEntry {
            page_index: *idx,
            page_hash: PageHash::from_bytes(*blake3::hash(*data).as_bytes()),
        })
        .collect();

    let manifest = match parent {
        None => Manifest::new_full(guest_ram_bytes, entries, device_blob)?,
        Some(p) => Manifest::new_delta(p.clone(), guest_ram_bytes, entries, device_blob)?,
    };

    Ok(manifest.encode())
}

/// Build a SILG input-log container.
///
/// `inner_version` is a caller-defined format tag; `payload` is the opaque
/// log bytes.  Returns the encoded container bytes; the BLAKE3 footer is the
/// `LogId`.
pub fn build_input_log_container(inner_version: u32, payload: &[u8]) -> Vec<u8> {
    InputLogContainer::encode(inner_version, payload)
}

/// Compute the `SnapshotRef` of an already-encoded container buffer.
pub fn snapshot_ref_of(container: &[u8]) -> SnapshotRef {
    Manifest::snapshot_ref(container)
}

/// Compute the `LogId` of an already-encoded SILG container buffer.
pub fn log_id_of(container: &[u8]) -> snapstore_types::LogId {
    InputLogContainer::log_id(container)
}

/// Extract just the page hashes from a list of `(page_index, payload)` pairs.
///
/// Used by `put_pages` to compute the local `batch_blake3` for cross-checking.
pub fn compute_batch_blake3(pages: &[(u64, &[u8; 4096])]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for (_, data) in pages {
        let ph = blake3::hash(*data);
        hasher.update(ph.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

/// Compute `batch_blake3` from a list of pre-computed page hashes, in the
/// order they were sent to the server.
pub fn compute_batch_blake3_from_hashes(hashes: &[[u8; 32]]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for h in hashes {
        hasher.update(h);
    }
    *hasher.finalize().as_bytes()
}
