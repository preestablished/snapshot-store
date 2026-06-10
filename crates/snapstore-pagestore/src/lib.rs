#![forbid(unsafe_code)]

pub mod pack;
pub mod index;
pub mod ingest;

pub use ingest::{IngestOutcome, PageStore, StoreError, StoreOptions};
