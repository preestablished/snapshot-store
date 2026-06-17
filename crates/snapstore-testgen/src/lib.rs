#![forbid(unsafe_code)]

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use snapstore_types::{PageHash, PAGE_SIZE};

/// Statistical profile describing a synthetic guest workload.
#[derive(Clone, Debug)]
pub struct GuestProfile {
    /// Total number of pages in guest memory.
    pub total_pages: usize,
    /// Fraction of pages that are all-zero (0.0..=1.0).
    pub zero_fraction: f64,
    /// Fraction of pages with low entropy (e.g. code/data patterns).
    pub text_fraction: f64,
    /// Fraction of pages with high entropy (random bytes).
    pub random_fraction: f64,
    /// Fraction of pages mutated per epoch (0.0..=1.0).
    pub dirty_rate: f64,
}

impl GuestProfile {
    /// Typical idle Linux VM: ~40% zero pages, ~40% text-like, ~20% random.
    pub fn idle_linux() -> Self {
        Self {
            total_pages: 262_144,
            zero_fraction: 0.4,
            text_fraction: 0.4,
            random_fraction: 0.2,
            dirty_rate: 0.05,
        }
    }

    /// Active workload: fewer zero pages, more entropy.
    pub fn busy_workload() -> Self {
        Self {
            total_pages: 262_144,
            zero_fraction: 0.1,
            text_fraction: 0.3,
            random_fraction: 0.6,
            dirty_rate: 0.2,
        }
    }

    /// All pages pairwise-distinct (no dedup hits possible). Used for G1 cold benchmark.
    pub fn all_unique() -> Self {
        Self {
            total_pages: 262_144,
            zero_fraction: 0.0,
            text_fraction: 0.0,
            random_fraction: 1.0,
            dirty_rate: 1.0,
        }
    }
}

/// A deterministic synthetic guest memory simulator.
pub struct SyntheticGuest {
    // Retained for future re-seeding of per-page RNGs during epoch resets.
    #[allow(dead_code)]
    seed: u64,
    profile: GuestProfile,
    pages: Vec<Box<[u8; PAGE_SIZE]>>,
    rng: ChaCha8Rng,
}

/// Generate a single page content using a per-page RNG derived from a combined seed.
///
/// For random pages, the page_index is folded into the seed so that each page has
/// unique content even across identical profiles -- satisfying the all_unique guarantee.
fn generate_page(
    page_rng: &mut ChaCha8Rng,
    zero_fraction: f64,
    text_fraction: f64,
    page_index: u64,
    global_seed: u64,
) -> Box<[u8; PAGE_SIZE]> {
    let roll: f64 = page_rng.gen();
    if roll < zero_fraction {
        Box::new([0u8; PAGE_SIZE])
    } else if roll < zero_fraction + text_fraction {
        let base_byte: u8 = page_rng.gen();
        let mut page = Box::new([0u8; PAGE_SIZE]);
        for i in 0..PAGE_SIZE {
            let variation: u8 = if i % 64 == 0 {
                page_rng.gen::<u8>() & 0x0F
            } else {
                0
            };
            page[i] = base_byte.wrapping_add(variation);
        }
        page
    } else {
        let unique_seed = (global_seed << 32) | (page_index & 0xFFFF_FFFF);
        let mut unique_rng = ChaCha8Rng::seed_from_u64(unique_seed);
        let mut page = Box::new([0u8; PAGE_SIZE]);
        for byte in page.iter_mut() {
            *byte = unique_rng.gen();
        }
        page
    }
}

impl SyntheticGuest {
    /// Create a new synthetic guest with deterministic content.
    ///
    /// Same seed + profile always produces byte-identical page streams.
    pub fn new(seed: u64, profile: GuestProfile) -> Self {
        let mut page_rng = ChaCha8Rng::seed_from_u64(seed);

        let total = profile.total_pages;
        let mut pages = Vec::with_capacity(total);

        for i in 0..total {
            let page = generate_page(
                &mut page_rng,
                profile.zero_fraction,
                profile.text_fraction,
                i as u64,
                seed,
            );
            pages.push(page);
        }

        let epoch_seed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let rng = ChaCha8Rng::seed_from_u64(epoch_seed);

        Self {
            seed,
            profile,
            pages,
            rng,
        }
    }

    /// Iterator over (page_index, page_bytes) for all pages.
    pub fn pages(&self) -> impl Iterator<Item = (u64, &[u8; PAGE_SIZE])> {
        self.pages
            .iter()
            .enumerate()
            .map(|(i, p)| (i as u64, p.as_ref()))
    }

    /// Advance one epoch: mutate dirty_rate fraction of pages.
    ///
    /// Returns the indices of mutated pages.
    pub fn step_epoch(&mut self) -> Vec<u64> {
        let total = self.profile.total_pages;
        let dirty_count = (self.profile.dirty_rate * total as f64) as usize;

        let mut indices: Vec<u64> = (0..total as u64).collect();
        for i in 0..dirty_count {
            let j = i + self.rng.gen_range(0..(total - i));
            indices.swap(i, j);
        }
        let dirty_indices: Vec<u64> = indices[..dirty_count].to_vec();

        for &idx in &dirty_indices {
            let page = &mut self.pages[idx as usize];
            for byte in page.iter_mut() {
                *byte = self.rng.gen();
            }
        }

        dirty_indices
    }

    /// Compute BLAKE3 hash of page at index.
    pub fn page_hash(&self, idx: usize) -> PageHash {
        let page_bytes = self.pages[idx].as_ref();
        PageHash(blake3::hash(page_bytes).into())
    }

    /// Total page count.
    pub fn total_pages(&self) -> usize {
        self.profile.total_pages
    }

    /// Get a view suitable for commit.
    ///
    /// Returns (gpa_base, page_data) slices where gpa_base is page_index * PAGE_SIZE.
    pub fn as_regions(&self) -> Vec<(u64, Vec<&[u8; PAGE_SIZE]>)> {
        let page_refs: Vec<&[u8; PAGE_SIZE]> = self.pages.iter().map(|p| p.as_ref()).collect();
        vec![(0u64, page_refs)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_PAGES: usize = 1024;

    fn small_idle_linux() -> GuestProfile {
        GuestProfile {
            total_pages: TEST_PAGES,
            ..GuestProfile::idle_linux()
        }
    }

    fn small_all_unique() -> GuestProfile {
        GuestProfile {
            total_pages: TEST_PAGES,
            ..GuestProfile::all_unique()
        }
    }

    #[test]
    fn determinism_same_seed_same_bytes() {
        let seed = 0xDEAD_BEEF_1234_5678u64;
        let g1 = SyntheticGuest::new(seed, small_idle_linux());
        let g2 = SyntheticGuest::new(seed, small_idle_linux());

        let mut all_identical = true;
        for ((i1, p1), (i2, p2)) in g1.pages().zip(g2.pages()) {
            assert_eq!(i1, i2, "page indices must match");
            if p1 != p2 {
                all_identical = false;
                break;
            }
        }
        assert!(
            all_identical,
            "same seed + profile must produce byte-identical pages"
        );
    }

    #[test]
    fn different_seeds_produce_different_output() {
        let g1 = SyntheticGuest::new(0x1111_1111_1111_1111u64, small_idle_linux());
        let g2 = SyntheticGuest::new(0x2222_2222_2222_2222u64, small_idle_linux());

        let mut all_identical = true;
        for ((_i1, p1), (_i2, p2)) in g1.pages().zip(g2.pages()) {
            if p1 != p2 {
                all_identical = false;
                break;
            }
        }
        assert!(
            !all_identical,
            "different seeds must produce different page content"
        );
    }

    #[test]
    fn distribution_idle_linux_fractions() {
        let profile = small_idle_linux();
        let guest = SyntheticGuest::new(42, profile.clone());

        let total = TEST_PAGES as f64;
        let mut zero_count = 0usize;
        let mut text_count = 0usize;
        let mut random_count = 0usize;

        for (_idx, page) in guest.pages() {
            let all_zero = page.iter().all(|&b| b == 0);
            if all_zero {
                zero_count += 1;
            } else {
                let mut seen = [false; 256];
                let mut unique = 0usize;
                for &b in page.iter() {
                    if !seen[b as usize] {
                        seen[b as usize] = true;
                        unique += 1;
                    }
                }
                if unique < 32 {
                    text_count += 1;
                } else {
                    random_count += 1;
                }
            }
        }

        let zero_frac = zero_count as f64 / total;
        let text_frac = text_count as f64 / total;
        let random_frac = random_count as f64 / total;

        assert!(
            (zero_frac - 0.4).abs() < 0.10,
            "zero_frac={:.3} expected ~0.4",
            zero_frac
        );
        assert!(
            (text_frac - 0.4).abs() < 0.10,
            "text_frac={:.3} expected ~0.4",
            text_frac
        );
        assert!(
            (random_frac - 0.2).abs() < 0.10,
            "random_frac={:.3} expected ~0.2",
            random_frac
        );
    }

    #[test]
    fn step_epoch_dirty_count_within_tolerance() {
        let profile = small_all_unique();
        let dirty_rate = profile.dirty_rate;
        let total = profile.total_pages;
        let mut guest = SyntheticGuest::new(99, profile);

        let dirty = guest.step_epoch();
        let expected = (dirty_rate * total as f64) as usize;
        let tolerance = (total as f64 * 0.01).ceil() as usize + 1;

        assert!(
            (dirty.len() as isize - expected as isize).unsigned_abs() <= tolerance,
            "dirty count {} expected {} +/-{}",
            dirty.len(),
            expected,
            tolerance
        );
    }

    #[test]
    fn step_epoch_modifies_pages() {
        let profile = small_all_unique();
        let mut guest = SyntheticGuest::new(77, profile);

        let snapshot: Vec<[u8; PAGE_SIZE]> = (0..TEST_PAGES).map(|i| *guest.pages[i]).collect();

        let dirty = guest.step_epoch();
        assert!(!dirty.is_empty(), "at least one page must be dirtied");

        let any_changed = dirty
            .iter()
            .any(|&idx| guest.pages[idx as usize].as_ref() != &snapshot[idx as usize]);
        assert!(any_changed, "step_epoch must actually modify page content");
    }
}
