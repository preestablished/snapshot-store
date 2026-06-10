//! Fuzz target for `Manifest::decode`.
//!
//! Build with: `cargo +nightly fuzz run manifest_decode`
//! (requires nightly toolchain and the `cargo-fuzz` cargo extension)

#![no_main]

use libfuzzer_sys::fuzz_target;
use snapstore_manifest::Manifest;

fuzz_target!(|data: &[u8]| {
    // All outcomes are acceptable: Ok(_) or Err(_).
    // Panics and stack overflows are bugs.
    let _ = Manifest::decode(data);
});
