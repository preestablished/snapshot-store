//! Gate S3 row: flatten of a 64-deep chain of 2k-entry deltas, warm — spec
//! target < 2 ms (pure CPU; gates at spec value on any hardware).

use criterion::{criterion_group, criterion_main, Criterion};
use snapstore_manifest::{flatten, DeviceBlob, Manifest, ManifestEntry};
use snapstore_types::{PageHash, SnapshotRef};

const GUEST_PAGES: u64 = 65_536; // 256 MiB of guest RAM
const DELTA_ENTRIES: u64 = 2_000;
const CHAIN_DEPTH: usize = 64;

fn hash_for(i: u64, salt: u64) -> PageHash {
    let mut b = [0u8; 32];
    b[..8].copy_from_slice(&i.to_le_bytes());
    b[8..16].copy_from_slice(&salt.to_le_bytes());
    PageHash::from_bytes(b)
}

fn empty_blob() -> DeviceBlob {
    DeviceBlob {
        format: 0,
        zstd: false,
        bytes: vec![],
        raw_len: 0,
    }
}

fn build_chain() -> Vec<Manifest> {
    let guest_ram_bytes = GUEST_PAGES * 4096;

    let full_entries: Vec<ManifestEntry> = (0..GUEST_PAGES)
        .map(|i| ManifestEntry {
            page_index: i,
            page_hash: hash_for(i, 0),
        })
        .collect();
    let root = Manifest::new_full(guest_ram_bytes, full_entries, empty_blob()).unwrap();
    let mut parent_ref = Manifest::snapshot_ref(&root.encode());

    let mut chain = vec![root];
    for depth in 1..CHAIN_DEPTH {
        // 2k dirty pages per epoch, spread deterministically.
        let entries: Vec<ManifestEntry> = (0..DELTA_ENTRIES)
            .map(|k| {
                let idx = (k * 31 + depth as u64 * 17) % GUEST_PAGES;
                ManifestEntry {
                    page_index: idx,
                    page_hash: hash_for(idx, depth as u64),
                }
            })
            .map(|e| (e.page_index, e))
            .collect::<std::collections::BTreeMap<_, _>>()
            .into_values()
            .collect();
        let delta = Manifest::new_delta(
            SnapshotRef::from_bytes(parent_ref.to_bytes()),
            guest_ram_bytes,
            entries,
            empty_blob(),
        )
        .unwrap();
        parent_ref = Manifest::snapshot_ref(&delta.encode());
        chain.push(delta);
    }
    chain
}

fn bench_flatten(c: &mut Criterion) {
    let chain = build_chain();
    // flatten() takes child-first order.
    let child_first: Vec<&Manifest> = chain.iter().rev().collect();

    c.bench_function("flatten_64_deep_2k_entry_chain", |b| {
        b.iter(|| {
            let merged = flatten(&child_first).unwrap();
            assert_eq!(merged.len() as u64, GUEST_PAGES);
            merged
        })
    });
}

criterion_group!(benches, bench_flatten);
criterion_main!(benches);
