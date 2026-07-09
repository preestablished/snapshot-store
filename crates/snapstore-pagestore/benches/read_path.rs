// ── Read-path benchmark (WI6 gate pre-check) ─────────────────────────────────
//
// Measures the warm-read throughput from page cache after a pre-populated store.
//
// Bench 1 (single_thread_warm_get):
//   - ~50k distinct pages spread across several packs (small max_pack_bytes to
//     force multiple rotations).
//   - Single-threaded random get() in the timed region.
//   - Reports: pages/s.
//
// Bench 2 (eight_thread_warm_get_batch):
//   - Same store setup as bench 1.
//   - 8 threads each calling get_batch() with random subsets.
//   - Reports: GB/s aggregate.
//
// Gate numbers (evaluated on the reference box, NOT here):
//   - S4 single-thread: ≥ 500k pages/s
//   - S4 8-thread:      ≥ 2.5 GB/s
// These benchmarks measure and report; they do not assert.

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use snapstore_pagestore::PageStore;
use snapstore_types::{PageHash, PAGE_SIZE};
use tempfile::{Builder as TempBuilder, TempDir};

// ── Store setup ───────────────────────────────────────────────────────────────

const TOTAL_PAGES: usize = 50_000;
const PAGES_PER_PACK: u64 = 1_000; // forces ~50 pack files
const BATCH_INGEST_SIZE: usize = 512;

fn bench_tempdir(prefix: &str) -> TempDir {
    match std::env::var_os("SNAPSTORE_BENCH_ROOT") {
        Some(root) => TempBuilder::new()
            .prefix(prefix)
            .tempdir_in(root)
            .expect("create benchmark tempdir in SNAPSTORE_BENCH_ROOT"),
        None => TempDir::new().unwrap(),
    }
}

/// Build a temporary store with TOTAL_PAGES unique pages spread across multiple
/// sealed packs.  Returns (dir, store, hashes) where dir must be kept alive.
fn build_warm_store() -> (TempDir, Arc<PageStore>, Vec<PageHash>) {
    use snapstore_pagestore::StoreOptions;
    use snapstore_types::PAGE_SIZE;

    let dir = bench_tempdir("snapstore-read-path-");

    // Force many pack rotations so we exercise cross-pack reads.
    let record_size = 37u64 + PAGE_SIZE as u64; // RECORD_HEADER_SIZE + PAGE_SIZE
    let pack_header = 20u64;
    let max_pack_bytes = pack_header + PAGES_PER_PACK * record_size;

    let opts = StoreOptions {
        max_pack_bytes,
        read_handle_cap: 256,
        ..StoreOptions::default()
    };

    let store = Arc::new(PageStore::open(dir.path(), opts).unwrap());

    // Pre-generate unique pages deterministically.
    let all_pages: Vec<Box<[u8; PAGE_SIZE]>> = (0..TOTAL_PAGES)
        .map(|i| {
            let mut p = Box::new([0u8; PAGE_SIZE]);
            p[0] = (i & 0xFF) as u8;
            p[1] = ((i >> 8) & 0xFF) as u8;
            p[2] = ((i >> 16) & 0xFF) as u8;
            p[3] = ((i >> 24) & 0xFF) as u8;
            for j in 4..PAGE_SIZE {
                p[j] = ((i ^ j).wrapping_add(0x5A)) as u8;
            }
            p
        })
        .collect();

    // Ingest in batches.
    let mut all_hashes: Vec<PageHash> = Vec::with_capacity(TOTAL_PAGES);
    for chunk in all_pages.chunks(BATCH_INGEST_SIZE) {
        let refs: Vec<&[u8; PAGE_SIZE]> = chunk.iter().map(|p| p.as_ref()).collect();
        let outcomes = store.ingest(&refs).unwrap();
        all_hashes.extend(outcomes.iter().map(|o| o.hash));
    }

    // Flush to disk so reads hit page-cache, not write buffers.
    store.sync().unwrap();

    (dir, store, all_hashes)
}

// ── Bench 1: single-thread warm random get ───────────────────────────────────

fn bench_single_thread_warm_get(c: &mut Criterion) {
    let (_dir, store, hashes) = build_warm_store();

    // Warm the page cache by doing one full scan before the timed region.
    for h in &hashes {
        store.get(h).unwrap().unwrap();
    }

    let n = hashes.len();
    // Use a simple pseudo-random walk to avoid sequential access patterns.
    let mut idx = 0usize;
    let stride = 7919; // prime, gives good coverage

    let mut group = c.benchmark_group("read_path");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.throughput(Throughput::Elements(TOTAL_PAGES as u64));

    group.bench_function("single_thread_warm_get", |b| {
        b.iter(|| {
            for _ in 0..TOTAL_PAGES {
                let h = &hashes[idx % n];
                criterion::black_box(store.get(h).unwrap().unwrap());
                idx = idx.wrapping_add(stride);
            }
        });
    });

    group.finish();
}

// ── Bench 2: 8-thread warm get_batch ─────────────────────────────────────────

fn bench_eight_thread_warm_get_batch(c: &mut Criterion) {
    let (_dir, store, hashes) = build_warm_store();

    // Warm page cache.
    for h in &hashes {
        store.get(h).unwrap().unwrap();
    }

    const THREADS: usize = 8;
    const BATCH_SIZE: usize = 256;
    // Total bytes measured: 8 threads × iters × BATCH_SIZE × PAGE_SIZE
    let per_iter_bytes = (THREADS * BATCH_SIZE * PAGE_SIZE) as u64;

    let store = Arc::clone(&store);
    let hashes = Arc::new(hashes);
    let n = hashes.len();

    let mut group = c.benchmark_group("read_path");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));
    group.throughput(Throughput::Bytes(per_iter_bytes));

    group.bench_function("eight_thread_warm_get_batch", |b| {
        b.iter(|| {
            let mut handles = Vec::with_capacity(THREADS);
            for t in 0..THREADS {
                let store_t = Arc::clone(&store);
                let hashes_t = Arc::clone(&hashes);
                let handle = std::thread::spawn(move || {
                    // Each thread uses a different starting offset to spread reads.
                    let start = (t * 997) % n;
                    let batch: Vec<PageHash> = (0..BATCH_SIZE)
                        .map(|i| hashes_t[(start + i * 7919) % n])
                        .collect();
                    let results = store_t.get_batch(&batch).unwrap();
                    // Consume results to prevent dead-code elimination.
                    criterion::black_box(results.iter().filter(|r| r.is_some()).count())
                });
                handles.push(handle);
            }
            for h in handles {
                h.join().unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_thread_warm_get,
    bench_eight_thread_warm_get_batch,
);
criterion_main!(benches);
