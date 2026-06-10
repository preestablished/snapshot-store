//! SEQPACKET page-channel server (Linux-only, WI2).
//!
//! Listens on `page_channel_path` (mode 0660, removes stale file first) and
//! dispatches PUT_BATCH / GET_BATCH from co-located hypervisor workers.
//!
//! # Threading model
//!
//! The page-channel protocol uses the synchronous `recv_datagram`/`send_datagram`
//! API from `snapstore-localpath`.  Rather than bridging this into async with
//! `AsyncFd` (which would require spawning blocking tasks per datagram anyway),
//! the listener loop runs in a **single dedicated blocking thread** and spawns
//! **one blocking thread per accepted connection**.  This keeps the
//! implementation simple, avoids async cancellation complexity, and matches the
//! scale expectations for the fast path (a small number of co-located clients).
//!
//! # Backpressure
//!
//! A `Semaphore`-equivalent counter guards the maximum number of in-flight
//! pages through the ingest path.  When the gate would be exceeded the server
//! replies `ERROR OVERLOAD`; the client backs off (the operation is
//! content-idempotent).

// BorrowedFd::borrow_raw and OwnedFd::from_raw_fd require unsafe but are
// well-defined FFI patterns used on raw fds obtained from nix (which returns
// RawFd).
#![allow(unsafe_code)]

use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

use nix::sys::socket::{
    accept, bind, listen, socket, AddressFamily, Backlog, SockFlag, SockType, UnixAddr,
};

use snapstore_localpath::linux::{
    create_get_memfd, memfd_len, recv_datagram, seal_get_memfd, send_datagram, verify_put_seals,
};
use snapstore_localpath::proto::{
    batch_cross_check, decode_get_batch, decode_hdr, decode_put_batch, encode_error,
    encode_get_batch_data, encode_put_batch_ok, ErrorCode, GetReq, MsgKind, PutOkBody,
};
use snapstore_pagestore::PageStore;
use snapstore_store::SnapshotStore;
use snapstore_types::{PageHash, PAGE_SIZE};

use crate::metrics::Metrics;

// ── Public handle ─────────────────────────────────────────────────────────────

/// A running page-channel listener.  Dropping it does not immediately stop the
/// thread (the listener thread will exit on its next `accept` iteration when
/// the listening fd is dropped and returns EBADF/EINVAL).
pub struct PageChannelHandle {
    // Keep the OwnedFd alive so the listening thread can detect shutdown via
    // the accept error.
    _sock: OwnedFd,
}

// ── Entry point ───────────────────────────────────────────────────────────────

/// Start the page-channel listener in a dedicated blocking thread.
///
/// Returns `Err` only if the socket could not be bound; all per-connection
/// errors are logged and handled internally.
pub fn start(
    path: &Path,
    store: Arc<SnapshotStore>,
    metrics: Arc<Metrics>,
    ingest_queue_pages: u32,
    corrupt_cross_check_for_test: bool,
) -> std::io::Result<PageChannelHandle> {
    // Remove stale socket file.
    if path.exists() {
        std::fs::remove_file(path)?;
    }

    // Create the SEQPACKET listening socket.
    let sock = socket(
        AddressFamily::Unix,
        SockType::SeqPacket,
        SockFlag::SOCK_CLOEXEC,
        None,
    )
    .map_err(nix_to_io)?;

    let addr = UnixAddr::new(path).map_err(nix_to_io)?;
    bind(sock.as_raw_fd(), &addr).map_err(nix_to_io)?;

    // Set mode 0660 on the socket file.
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660))?;

    listen(&sock, Backlog::new(128).unwrap()).map_err(nix_to_io)?;

    // Shared in-flight pages counter.
    // Positive value = pages currently being processed; we check <= max before
    // accepting a new batch.  AtomicI64 so we can go briefly negative on
    // concurrent decrements without panicking (shouldn't happen, but safe).
    let in_flight = Arc::new(AtomicI64::new(0));

    let path_str = path.display().to_string();
    let listen_fd = dup_owned_fd(&sock)?;

    // Clone refs for the listener thread.
    let store_t = Arc::clone(&store);
    let metrics_t = Arc::clone(&metrics);
    let in_flight_t = Arc::clone(&in_flight);

    std::thread::Builder::new()
        .name("page-channel-listener".into())
        .spawn(move || {
            tracing::info!(path = %path_str, "page-channel listener started");
            accept_loop(
                listen_fd,
                store_t,
                metrics_t,
                in_flight_t,
                ingest_queue_pages,
                corrupt_cross_check_for_test,
            );
            tracing::info!(path = %path_str, "page-channel listener exiting");
        })
        .map_err(std::io::Error::other)?;

    Ok(PageChannelHandle { _sock: sock })
}

// ── Accept loop ───────────────────────────────────────────────────────────────

fn accept_loop(
    listen_fd: OwnedFd,
    store: Arc<SnapshotStore>,
    metrics: Arc<Metrics>,
    in_flight: Arc<AtomicI64>,
    ingest_queue_pages: u32,
    corrupt_cross_check_for_test: bool,
) {
    loop {
        // nix::sys::socket::accept returns RawFd; wrap it in OwnedFd immediately
        // so it is closed on drop/panic.
        let conn_fd = match accept(listen_fd.as_raw_fd()) {
            Ok(raw_fd) => unsafe { OwnedFd::from_raw_fd(raw_fd) },
            Err(nix::errno::Errno::EINVAL) | Err(nix::errno::Errno::EBADF) => {
                // Listening socket closed — normal shutdown.
                break;
            }
            Err(e) => {
                tracing::warn!(err = %e, "page-channel: accept error");
                continue;
            }
        };

        metrics.page_channel_clients.inc();

        let s = Arc::clone(&store);
        let m = Arc::clone(&metrics);
        let inf = Arc::clone(&in_flight);

        std::thread::Builder::new()
            .name("page-channel-conn".into())
            .spawn(move || {
                connection_loop(
                    conn_fd,
                    s,
                    m,
                    inf,
                    ingest_queue_pages,
                    corrupt_cross_check_for_test,
                );
            })
            .ok();
    }
}

// ── Per-connection loop ───────────────────────────────────────────────────────

fn connection_loop(
    conn: OwnedFd,
    store: Arc<SnapshotStore>,
    metrics: Arc<Metrics>,
    in_flight: Arc<AtomicI64>,
    ingest_queue_pages: u32,
    corrupt_cross_check_for_test: bool,
) {
    // SAFETY: BorrowedFd — the OwnedFd outlives every use in this function.
    // We borrow it for the duration of the connection.
    let borrowed = unsafe { BorrowedFd::borrow_raw(conn.as_raw_fd()) };

    loop {
        let dgram = match recv_datagram(borrowed) {
            Ok(Some(d)) => d,
            Ok(None) => {
                tracing::debug!("page-channel: client disconnected");
                break;
            }
            Err(e) => {
                tracing::warn!(err = %e, "page-channel: recv error, closing");
                break;
            }
        };

        let (hdr, body) = match decode_hdr(&dgram.bytes) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(err = %e, "page-channel: malformed datagram, closing");
                let _ = send_datagram(
                    borrowed,
                    &encode_error(0, ErrorCode::Invalid, &format!("bad header: {e}")),
                    None,
                );
                break; // close on protocol error
            }
        };

        match hdr.msg {
            MsgKind::PutBatch => {
                let close = handle_put_batch(
                    borrowed,
                    &hdr,
                    body,
                    dgram.fd,
                    store.pages(),
                    &metrics,
                    &in_flight,
                    ingest_queue_pages,
                    corrupt_cross_check_for_test,
                );
                if close {
                    break;
                }
            }
            MsgKind::GetBatch => {
                let close = handle_get_batch(borrowed, &hdr, body, store.pages(), &metrics);
                if close {
                    break;
                }
            }
            other => {
                tracing::warn!(msg = ?other, "page-channel: unexpected msg kind, closing");
                let _ = send_datagram(
                    borrowed,
                    &encode_error(
                        hdr.seq,
                        ErrorCode::Invalid,
                        &format!("unexpected msg kind {:?}", other),
                    ),
                    None,
                );
                break;
            }
        }
    }

    metrics.page_channel_clients.dec();
    // conn drops here, closing the fd.
}

// ── PUT_BATCH handler ─────────────────────────────────────────────────────────

/// Returns `true` if the connection should be closed after this message.
#[allow(clippy::too_many_arguments)]
fn handle_put_batch(
    conn: BorrowedFd<'_>,
    hdr: &snapstore_localpath::proto::PcHdr,
    body: &[u8],
    fd: Option<OwnedFd>,
    page_store: &PageStore,
    metrics: &Metrics,
    in_flight: &AtomicI64,
    ingest_queue_pages: u32,
    corrupt_cross_check_for_test: bool,
) -> bool {
    // Validate count.
    let count = match decode_put_batch(hdr, body) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(err = %e, "page-channel: PUT_BATCH decode error");
            let _ = send_datagram(
                conn,
                &encode_error(hdr.seq, ErrorCode::Invalid, &e.to_string()),
                None,
            );
            return true; // close
        }
    };

    // Must have an fd.
    let fd = match fd {
        Some(f) => f,
        None => {
            tracing::warn!("page-channel: PUT_BATCH without fd");
            let _ = send_datagram(
                conn,
                &encode_error(hdr.seq, ErrorCode::Invalid, "PUT_BATCH requires a memfd"),
                None,
            );
            return true; // close
        }
    };

    // Verify seals.
    match verify_put_seals(fd.as_fd()) {
        Ok(true) => {}
        Ok(false) => {
            let _ = send_datagram(
                conn,
                &encode_error(
                    hdr.seq,
                    ErrorCode::Invalid,
                    "memfd is not sealed with F_SEAL_WRITE|F_SEAL_SHRINK",
                ),
                None,
            );
            return false; // not a protocol error; keep connection alive
        }
        Err(e) => {
            let _ = send_datagram(
                conn,
                &encode_error(hdr.seq, ErrorCode::Invalid, &format!("F_GET_SEALS: {e}")),
                None,
            );
            return true;
        }
    }

    // Validate size.
    let fd_len = match memfd_len(fd.as_fd()) {
        Ok(l) => l,
        Err(e) => {
            let _ = send_datagram(
                conn,
                &encode_error(hdr.seq, ErrorCode::Invalid, &format!("fstat: {e}")),
                None,
            );
            return true;
        }
    };
    let expected_len = count as u64 * PAGE_SIZE as u64;
    if fd_len != expected_len {
        let _ = send_datagram(
            conn,
            &encode_error(
                hdr.seq,
                ErrorCode::Invalid,
                &format!("memfd size {} != count*4096 = {}", fd_len, expected_len),
            ),
            None,
        );
        return false;
    }

    // Backpressure check.
    let prev = in_flight.fetch_add(count as i64, Ordering::SeqCst);
    if prev + count as i64 > ingest_queue_pages as i64 {
        in_flight.fetch_sub(count as i64, Ordering::SeqCst);
        let _ = send_datagram(
            conn,
            &encode_error(
                hdr.seq,
                ErrorCode::Overload,
                &format!("ingest queue full ({prev} in-flight)"),
            ),
            None,
        );
        return false;
    }

    // Read pages from the memfd (pread, no unsafe mmap needed).
    let file = std::fs::File::from(fd);
    let mut pages: Vec<Box<[u8; PAGE_SIZE]>> = Vec::with_capacity(count as usize);
    for i in 0..count as usize {
        let mut page = Box::new([0u8; PAGE_SIZE]);
        if let Err(e) = file.read_exact_at(page.as_mut(), (i * PAGE_SIZE) as u64) {
            in_flight.fetch_sub(count as i64, Ordering::SeqCst);
            let _ = send_datagram(
                conn,
                &encode_error(hdr.seq, ErrorCode::Invalid, &format!("read page {i}: {e}")),
                None,
            );
            return true;
        }
        pages.push(page);
    }
    // fd (file) drops here — RAII close.

    // Convert to refs for ingest.
    let page_refs: Vec<&[u8; PAGE_SIZE]> = pages.iter().map(|p| p.as_ref()).collect();

    // Ingest via PageStore (hashes internally with rayon).
    let outcomes = match page_store.ingest(&page_refs) {
        Ok(o) => o,
        Err(e) => {
            in_flight.fetch_sub(count as i64, Ordering::SeqCst);
            tracing::error!(err = %e, "page-channel: ingest error");
            let _ = send_datagram(
                conn,
                &encode_error(hdr.seq, ErrorCode::Invalid, &format!("ingest: {e}")),
                None,
            );
            return true;
        }
    };

    in_flight.fetch_sub(count as i64, Ordering::SeqCst);

    let mut pages_new: u32 = 0;
    let mut pages_deduped: u32 = 0;
    let mut hashes: Vec<PageHash> = Vec::with_capacity(outcomes.len());
    for outcome in &outcomes {
        hashes.push(outcome.hash);
        if outcome.newly_written {
            pages_new += 1;
        } else {
            pages_deduped += 1;
        }
    }

    let mut batch_blake3 = batch_cross_check(&hashes);

    // Test hook: flip one byte so the client's cross-check fails.
    if corrupt_cross_check_for_test {
        batch_blake3[0] ^= 0xff;
        metrics.page_channel_crosscheck_mismatch.inc();
    }

    let payload_bytes = count as u64 * PAGE_SIZE as u64;
    metrics
        .page_channel_bytes
        .with_label_values(&["in"])
        .inc_by(payload_bytes as f64);
    metrics
        .page_channel_batches
        .with_label_values(&["put"])
        .inc();

    let body = PutOkBody {
        pages_new,
        pages_deduped,
        batch_blake3,
    };
    let reply = encode_put_batch_ok(hdr.seq, count, &body);
    if let Err(e) = send_datagram(conn, &reply, None) {
        tracing::warn!(err = %e, "page-channel: failed to send PUT_BATCH_OK");
    }

    false
}

// ── GET_BATCH handler ─────────────────────────────────────────────────────────

/// Returns `true` if the connection should be closed after this message.
fn handle_get_batch(
    conn: BorrowedFd<'_>,
    hdr: &snapstore_localpath::proto::PcHdr,
    body: &[u8],
    page_store: &PageStore,
    metrics: &Metrics,
) -> bool {
    let reqs: Vec<GetReq> = match decode_get_batch(hdr, body) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(err = %e, "page-channel: GET_BATCH decode error");
            let _ = send_datagram(
                conn,
                &encode_error(hdr.seq, ErrorCode::Invalid, &e.to_string()),
                None,
            );
            return true;
        }
    };

    let hashes: Vec<PageHash> = reqs.iter().map(|r| r.page_hash).collect();

    let results = match page_store.get_batch(&hashes) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(err = %e, "page-channel: get_batch error");
            let _ = send_datagram(
                conn,
                &encode_error(hdr.seq, ErrorCode::Invalid, &format!("get_batch: {e}")),
                None,
            );
            return true;
        }
    };

    // Check for missing pages; reply NOT_FOUND with the first missing hash.
    for (i, maybe) in results.iter().enumerate() {
        if maybe.is_none() {
            let first_missing = hex_from_bytes(hashes[i].as_bytes());
            let _ = send_datagram(
                conn,
                &encode_error(
                    hdr.seq,
                    ErrorCode::NotFound,
                    &format!("page not found: {first_missing}"),
                ),
                None,
            );
            return false;
        }
    }

    // All found — create memfd and write pages.
    let total_len = reqs.len() as u64 * PAGE_SIZE as u64;
    let file = match create_get_memfd(total_len) {
        Ok(f) => f,
        Err(e) => {
            let _ = send_datagram(
                conn,
                &encode_error(hdr.seq, ErrorCode::Invalid, &format!("create memfd: {e}")),
                None,
            );
            return true;
        }
    };

    for (i, maybe) in results.iter().enumerate() {
        let page_bytes = maybe.as_ref().unwrap();
        if let Err(e) = file.write_all_at(page_bytes, (i * PAGE_SIZE) as u64) {
            let _ = send_datagram(
                conn,
                &encode_error(hdr.seq, ErrorCode::Invalid, &format!("write page: {e}")),
                None,
            );
            return true;
        }
    }

    let sealed_fd = match seal_get_memfd(file) {
        Ok(f) => f,
        Err(e) => {
            let _ = send_datagram(
                conn,
                &encode_error(hdr.seq, ErrorCode::Invalid, &format!("seal memfd: {e}")),
                None,
            );
            return true;
        }
    };

    let reply_wire = encode_get_batch_data(hdr.seq, &reqs);
    if let Err(e) = send_datagram(conn, &reply_wire, Some(sealed_fd.as_fd())) {
        tracing::warn!(err = %e, "page-channel: failed to send GET_BATCH_DATA");
    }
    // sealed_fd drops here — RAII.

    let payload_bytes = reqs.len() as u64 * PAGE_SIZE as u64;
    metrics
        .page_channel_bytes
        .with_label_values(&["out"])
        .inc_by(payload_bytes as f64);
    metrics
        .page_channel_batches
        .with_label_values(&["get"])
        .inc();

    false
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn hex_from_bytes(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn nix_to_io(e: nix::errno::Errno) -> std::io::Error {
    std::io::Error::from_raw_os_error(e as i32)
}

/// Duplicate an `OwnedFd` so we can move it into the listener thread while
/// keeping the original alive in the `PageChannelHandle`.
fn dup_owned_fd(fd: &OwnedFd) -> std::io::Result<OwnedFd> {
    use std::os::fd::FromRawFd;
    let raw = nix::unistd::dup(fd.as_raw_fd()).map_err(nix_to_io)?;
    // SAFETY: `dup` returns a fresh fd owned by us.
    #[allow(unsafe_code)]
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

use std::os::fd::AsFd;
