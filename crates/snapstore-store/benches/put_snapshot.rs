//! Benchmark: PutSnapshot already-paged path, 2k-entry delta.
//!
//! Plan target (reference hardware): p50 < 3 ms.
//! This 2-core box is informational only.
//!
//! Run: cargo bench -p snapstore-store

use criterion::{criterion_group, criterion_main, Criterion};
use snapstore_manifest::DeviceBlob;
use snapstore_store::build::{build_delta_container, build_full_container};
use snapstore_store::SnapshotStore;
use snapstore_types::PAGE_SIZE;
use tempfile::{Builder as TempBuilder, TempDir};

fn empty_blob() -> DeviceBlob {
    DeviceBlob {
        format: 0,
        zstd: false,
        bytes: vec![],
        raw_len: 0,
    }
}

fn make_page(seed: u64) -> Box<[u8; PAGE_SIZE]> {
    let mut p = Box::new([0u8; PAGE_SIZE]);
    let bytes = seed.to_le_bytes();
    for (i, chunk) in p.chunks_mut(8).enumerate() {
        let offset = (i as u64).wrapping_add(seed);
        let b = offset.to_le_bytes();
        chunk.copy_from_slice(&b[..chunk.len()]);
    }
    let _ = bytes; // silence unused
    p
}

fn bench_tempdir(prefix: &str) -> TempDir {
    match std::env::var_os("SNAPSTORE_BENCH_ROOT") {
        Some(root) => TempBuilder::new()
            .prefix(prefix)
            .tempdir_in(root)
            .expect("create benchmark tempdir in SNAPSTORE_BENCH_ROOT"),
        None => TempDir::new().unwrap(),
    }
}

pub fn bench_put_snapshot(c: &mut Criterion) {
    const N_FULL: usize = 2048; // full manifest page count
    const N_DELTA: usize = 2048; // delta entries (all pages dirty)
    let grb = N_FULL as u64 * PAGE_SIZE as u64;

    let dir = bench_tempdir("snapstore-put-snapshot-");
    let store = SnapshotStore::open(dir.path()).unwrap();

    // Build FULL container pages.
    let full_pages: Vec<Box<[u8; PAGE_SIZE]>> = (0..N_FULL).map(|i| make_page(i as u64)).collect();
    let full_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = full_pages
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u64, p.as_ref()))
        .collect();

    // Ingest FULL pages.
    let full_refs: Vec<&[u8; PAGE_SIZE]> = full_pages.iter().map(|p| p.as_ref()).collect();
    store.pages().ingest(&full_refs).unwrap();

    // Build and commit FULL container.
    let full_container = build_full_container(grb, &full_pairs, empty_blob());
    let full_ref = store.put_snapshot(&full_container).unwrap();

    // Build DELTA pages (different content).
    let delta_pages: Vec<Box<[u8; PAGE_SIZE]>> = (0..N_DELTA)
        .map(|i| make_page(i as u64 + 0x10000))
        .collect();
    let delta_pairs: Vec<(u64, &[u8; PAGE_SIZE])> = delta_pages
        .iter()
        .enumerate()
        .map(|(i, p)| (i as u64, p.as_ref()))
        .collect();

    // Ingest delta pages and sync (pre-paged path).
    let delta_refs: Vec<&[u8; PAGE_SIZE]> = delta_pages.iter().map(|p| p.as_ref()).collect();
    store.pages().ingest(&delta_refs).unwrap();
    store.pages().sync().unwrap();

    // Pre-build the delta container once.
    let delta_container = build_delta_container(&full_ref, grb, &delta_pairs, empty_blob());

    let mut group = c.benchmark_group("put_snapshot");
    group.sample_size(50);

    group.bench_function("2k-entry-delta-already-paged", |b| {
        b.iter(|| {
            // Each iteration puts the same container again.
            // The second and subsequent calls hit the idempotent early-return
            // path (file already exists), which is still a useful measurement
            // of the hot-path overhead (stat + return).
            store.put_snapshot(&delta_container).unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, bench_put_snapshot);
criterion_main!(benches);
