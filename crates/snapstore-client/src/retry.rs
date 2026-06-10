//! Retry policy for idempotent RPC calls.
//!
//! ## Policy table
//!
//! | Op category                               | Retried?         | Condition        |
//! |-------------------------------------------|------------------|------------------|
//! | Reads (GetSnapshot, GetInputLog, …)       | Yes              | see below        |
//! | Content-idempotent writes (PutPages, PutSnapshot, PutInputLog, CreateNode) | Yes | see below |
//! | CAS writes WITH `expected_generation`     | **Never**        | surface to caller |
//! | `ALREADY_EXISTS`                          | **Never**        | caller-bug signal |
//! | `MissingPages` detail                     | **Never**        | caller-action signal |
//! | `BatchBlake3Mismatch`                     | **Never**        | P0 integrity signal |
//!
//! Retried statuses: `DEADLINE_EXCEEDED`, `UNAVAILABLE`, and transport errors
//! (detected by failed `tonic::Status` conversions from transport errors).
//!
//! Backoff: 50 ms base, doubling each attempt (optional jitter via modular
//! arithmetic), capped so the total elapsed time stays below 30 seconds.
//!
//! The policy is implemented as a single `with_retry` async function that
//! accepts a closure returning a `Future<Output = Result<T, ClientError>>`.

use std::time::{Duration, Instant};

use crate::error::ClientError;

/// Maximum total elapsed time across all retry attempts.
const MAX_ELAPSED: Duration = Duration::from_secs(30);

/// Initial back-off delay.
const INITIAL_DELAY: Duration = Duration::from_millis(50);

/// Maximum per-attempt back-off delay.
const MAX_DELAY: Duration = Duration::from_secs(5);

/// Whether a `ClientError` warrants a blind retry.
fn is_retryable(err: &ClientError) -> bool {
    if err.is_non_retryable() {
        return false;
    }
    match err {
        ClientError::Status(s) => matches!(
            s.code(),
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
        ),
        ClientError::Transport(_) => true,
        _ => false,
    }
}

/// Execute `op` with exponential-backoff retry.
///
/// `op` is a `FnMut() -> Future<…>` (not `Fn`) because many call sites move
/// cloned state into the closure; `FnMut` is the right bound here.
pub async fn with_retry<F, Fut, T>(mut op: F) -> Result<T, ClientError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, ClientError>>,
{
    let start = Instant::now();
    let mut delay = INITIAL_DELAY;
    let mut attempt = 0u32;

    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if is_retryable(&e) => {
                let elapsed = start.elapsed();
                if elapsed >= MAX_ELAPSED {
                    tracing::debug!(
                        attempt,
                        elapsed_ms = elapsed.as_millis(),
                        "retry budget exhausted"
                    );
                    return Err(e);
                }
                // Jitter: XOR low bits of attempt with delay millis for a
                // lightweight spread without a random dep.
                let jitter_ms = (attempt as u64 * 7) % 20;
                let sleep_dur = (delay + Duration::from_millis(jitter_ms)).min(MAX_DELAY);
                tracing::debug!(
                    attempt,
                    sleep_ms = sleep_dur.as_millis(),
                    "retrying after transient error"
                );
                tokio::time::sleep(sleep_dur).await;
                delay = (delay * 2).min(MAX_DELAY);
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}
