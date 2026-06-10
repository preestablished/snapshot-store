#![forbid(unsafe_code)]

pub mod index;
pub mod ingest;
pub mod pack;

pub use ingest::{IngestOutcome, PageStore, StoreError, StoreOptions};
