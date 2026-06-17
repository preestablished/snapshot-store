//! Client half of the SEQPACKET page channel.
//!
//! [`PageChannelClient`] connects to the server's SEQPACKET socket and
//! provides:
//! - [`PageChannelClient::put_batch`] — upload pages via a sealed memfd.
//! - [`PageChannelClient::get_batch`] — fetch pages by hash into memfds.
//!
//! # Concurrency model (v1)
//!
//! A `Mutex<OwnedFd>` serialises the socket so that `&self` methods can be
//! used from multiple threads without an external lock.  One in-flight request
//! per client is correct for v1; the protocol is purely sequential (request →
//! reply) so pipelining is not needed here.  Callers that need parallel
//! throughput should create a pool of clients.

use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::fs::FileExt;
use std::sync::Mutex;

use nix::sys::socket::{connect, socket, AddressFamily, SockFlag, SockType, UnixAddr};

use snapstore_types::{PageHash, PAGE_SIZE};

use crate::{
    linux::{create_sealed_put_memfd, memfd_len, recv_datagram, send_datagram},
    proto::{
        batch_cross_check, decode_error, decode_get_batch_data, decode_hdr, decode_put_batch_ok,
        encode_get_batch, encode_put_batch, GetReq, MsgKind, GET_BATCH_MAX_PER_DATAGRAM,
        PUT_BATCH_MAX_PAGES,
    },
    ChannelError,
};

/// Outcome from a [`PageChannelClient::put_batch`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutOutcome {
    pub pages_new: u32,
    pub pages_deduped: u32,
}

/// A connected SEQPACKET page-channel client.
///
/// The socket is protected by a `Mutex`; all operations require `&self`
/// (no exclusive ownership needed by the caller) and are serialised
/// internally.  See module doc for the concurrency rationale.
pub struct PageChannelClient {
    sock: Mutex<OwnedFd>,
    seq: std::sync::atomic::AtomicU64,
}

impl PageChannelClient {
    /// Connect to the page-channel SEQPACKET socket at `path`.
    pub fn connect(path: &std::path::Path) -> Result<Self, ChannelError> {
        let sock = socket(
            AddressFamily::Unix,
            SockType::SeqPacket,
            SockFlag::SOCK_CLOEXEC,
            None,
        )
        .map_err(|e| ChannelError::Io(std::io::Error::from_raw_os_error(e as i32)))?;
        let addr = UnixAddr::new(path)
            .map_err(|e| ChannelError::Io(std::io::Error::from_raw_os_error(e as i32)))?;
        connect(sock.as_raw_fd(), &addr)
            .map_err(|e| ChannelError::Io(std::io::Error::from_raw_os_error(e as i32)))?;
        Ok(Self {
            sock: Mutex::new(sock),
            seq: std::sync::atomic::AtomicU64::new(0),
        })
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Upload a batch of pages (at most [`PUT_BATCH_MAX_PAGES`] per call).
    ///
    /// Creates a sealed memfd, writes all pages into it, sends a `PUT_BATCH`
    /// datagram with the fd attached, then receives and validates the
    /// `PUT_BATCH_OK` reply.
    ///
    /// # Cross-check
    ///
    /// The client independently computes `batch_blake3` over the per-page
    /// BLAKE3 hashes and compares it against the server's reply.  A mismatch
    /// returns [`ChannelError::CrossCheckMismatch`] — a P0 determinism bug
    /// that must never be retried silently.
    ///
    /// # Errors
    ///
    /// - [`ChannelError::Peer`] with code [`ErrorCode::Overload`] — the
    ///   server is backpressured; the caller should back off and retry (the
    ///   operation is content-idempotent).
    /// - [`ChannelError::CrossCheckMismatch`] — P0 fatal; do not retry.
    pub fn put_batch(&self, pages: &[&[u8; PAGE_SIZE]]) -> Result<PutOutcome, ChannelError> {
        assert!(
            pages.len() <= PUT_BATCH_MAX_PAGES as usize,
            "put_batch: chunk too large ({}); caller must split",
            pages.len()
        );
        assert!(!pages.is_empty(), "put_batch: empty batch");

        // Compute per-page hashes for the cross-check (rayon: serial blake3
        // alone caps the batch below the 1.5 GB/s transport gate).
        use rayon::prelude::*;
        let hashes: Vec<PageHash> = pages
            .par_iter()
            .map(|p| PageHash::from_bytes(*blake3::hash(*p).as_bytes()))
            .collect();
        let local_cross_check = batch_cross_check(&hashes);

        // Create and seal the memfd.
        let memfd = create_sealed_put_memfd(pages)?;

        let seq = self.next_seq();
        let wire = encode_put_batch(seq, pages.len() as u32);

        let sock = self.sock.lock().unwrap();
        send_datagram(sock.as_fd(), &wire, Some(memfd.as_fd()))?;
        drop(memfd); // fd sent; no need to keep it

        let dgram = recv_datagram(sock.as_fd())?.ok_or_else(|| {
            ChannelError::Protocol("server closed connection after PUT_BATCH".into())
        })?;

        let (hdr, body) = decode_hdr(&dgram.bytes)?;

        match hdr.msg {
            MsgKind::PutBatchOk => {
                let ok = decode_put_batch_ok(&hdr, body)?;
                if ok.batch_blake3 != local_cross_check {
                    return Err(ChannelError::CrossCheckMismatch {
                        expected: hex(&local_cross_check),
                        actual: hex(&ok.batch_blake3),
                    });
                }
                Ok(PutOutcome {
                    pages_new: ok.pages_new,
                    pages_deduped: ok.pages_deduped,
                })
            }
            MsgKind::Error => {
                let err = decode_error(&hdr, body)?;
                Err(ChannelError::Peer {
                    code: err.code,
                    detail: err.detail,
                })
            }
            other => Err(ChannelError::Protocol(format!(
                "unexpected reply to PUT_BATCH: {other:?}"
            ))),
        }
    }

    /// Fetch a batch of pages by hash.
    ///
    /// `reqs` is a slice of `(page_hash, dst_slot)` pairs.  The `dst_slot`
    /// is echoed in the reply and returned to the caller for scatter routing;
    /// the server never interprets it.
    ///
    /// Sends at most [`GET_BATCH_MAX_PER_DATAGRAM`] hashes per datagram and
    /// collects replies keyed by `seq`.  Returns `(dst_slot, page_bytes)`
    /// pairs in the same order as the input (sorted by input index).
    ///
    /// # Errors
    ///
    /// - [`ChannelError::Peer`] with code [`ErrorCode::NotFound`] if any hash
    ///   is unknown (the server names the first missing hash in `detail`).
    pub fn get_batch(&self, reqs: &[(PageHash, u64)]) -> Result<Vec<(u64, Vec<u8>)>, ChannelError> {
        if reqs.is_empty() {
            return Ok(vec![]);
        }

        // Build datagrams, each carrying at most GET_BATCH_MAX_PER_DATAGRAM entries.
        // Associate each chunk with a unique seq.
        let chunks: Vec<(u64, Vec<GetReq>)> = reqs
            .chunks(GET_BATCH_MAX_PER_DATAGRAM as usize)
            .map(|chunk| {
                let seq = self.next_seq();
                let get_reqs: Vec<GetReq> = chunk
                    .iter()
                    .map(|(hash, dst_slot)| GetReq {
                        page_hash: *hash,
                        dst_slot: *dst_slot,
                    })
                    .collect();
                (seq, get_reqs)
            })
            .collect();

        let sock = self.sock.lock().unwrap();

        // Send all GET_BATCH datagrams.
        for (seq, get_reqs) in &chunks {
            let wire = encode_get_batch(*seq, get_reqs);
            send_datagram(sock.as_fd(), &wire, None)?;
        }

        // Collect replies.  We expect exactly one reply datagram per sent
        // datagram, each carrying the fd.  Match by seq to handle any
        // reordering (in practice SEQPACKET is ordered, but the API
        // contract uses seq for correctness).
        let mut seq_to_reqs: std::collections::HashMap<u64, Vec<GetReq>> = chunks
            .iter()
            .map(|(seq, reqs)| (*seq, reqs.clone()))
            .collect();

        // Maintain insertion order for the output: map seq → offset into
        // result vec.  We'll build results in the order chunks were sent.
        let mut results: Vec<(u64, Vec<u8>)> = Vec::with_capacity(reqs.len());
        // Temporary storage keyed by seq, in the order replies arrive.
        let mut reply_pages: std::collections::HashMap<u64, Vec<(u64, Vec<u8>)>> =
            std::collections::HashMap::new();

        for _ in 0..chunks.len() {
            let dgram = recv_datagram(sock.as_fd())?.ok_or_else(|| {
                ChannelError::Protocol("server closed connection during GET_BATCH".into())
            })?;
            let (hdr, body) = decode_hdr(&dgram.bytes)?;

            match hdr.msg {
                MsgKind::GetBatchData => {
                    let echoed = decode_get_batch_data(&hdr, body)?;
                    let fd = dgram.fd.ok_or_else(|| {
                        ChannelError::Protocol("GET_BATCH_DATA arrived without fd".into())
                    })?;
                    let fd_len = memfd_len(fd.as_fd())?;
                    let expected_len = (echoed.len() as u64) * (PAGE_SIZE as u64);
                    if fd_len != expected_len {
                        return Err(ChannelError::Protocol(format!(
                            "GET_BATCH_DATA fd size {fd_len} != expected {expected_len}"
                        )));
                    }
                    // Bulk-read the whole reply memfd (per-page 4 KiB reads
                    // would cap GET_BATCH well below the 2.5 GB/s gate).
                    let file = std::fs::File::from(fd);
                    let mut all = vec![0u8; expected_len as usize];
                    const READ_CHUNK: usize = 4 * 1024 * 1024;
                    for (chunk_idx, chunk) in all.chunks_mut(READ_CHUNK).enumerate() {
                        file.read_exact_at(chunk, (chunk_idx * READ_CHUNK) as u64)?;
                    }
                    let pages_for_this_reply: Vec<(u64, Vec<u8>)> = echoed
                        .iter()
                        .zip(all.chunks_exact(PAGE_SIZE))
                        .map(|(req, page)| (req.dst_slot, page.to_vec()))
                        .collect();
                    reply_pages.insert(hdr.seq, pages_for_this_reply);
                }
                MsgKind::Error => {
                    let err = decode_error(&hdr, body)?;
                    return Err(ChannelError::Peer {
                        code: err.code,
                        detail: err.detail,
                    });
                }
                other => {
                    return Err(ChannelError::Protocol(format!(
                        "unexpected reply to GET_BATCH: {other:?}"
                    )));
                }
            }
        }

        // Reassemble in chunk order (maintaining input order).
        for (seq, _) in &chunks {
            if let Some(pages) = reply_pages.remove(seq) {
                results.extend(pages);
            }
        }

        // Remove seq from the map (not needed after replies collected).
        for (seq, _) in &chunks {
            seq_to_reqs.remove(seq);
        }

        Ok(results)
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// Allow unused import of AsRawFd for the connect call
use std::os::fd::AsRawFd;
