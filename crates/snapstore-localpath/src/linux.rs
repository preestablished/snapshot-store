//! Linux-only plumbing: SEQPACKET datagrams with `SCM_RIGHTS` fd-passing,
//! and memfd creation/sealing helpers.
//!
//! Everything here works on `OwnedFd`/`BorrowedFd` via nix's safe wrappers,
//! except the single justified `unsafe` adopting SCM_RIGHTS-received fds.

use std::io::{IoSlice, IoSliceMut};
use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::FileExt;

use nix::fcntl::{fcntl, FcntlArg, SealFlag};
use nix::sys::memfd::{memfd_create, MemFdCreateFlag};
use nix::sys::socket::{
    recvmsg, sendmsg, socketpair, AddressFamily, ControlMessage, ControlMessageOwned, MsgFlags,
    SockFlag, SockType,
};

use crate::proto::DATAGRAM_MAX;
use crate::ChannelError;
use snapstore_types::PAGE_SIZE;

// ── datagram I/O ──────────────────────────────────────────────────────────────

/// Send one datagram, optionally attaching a file descriptor via `SCM_RIGHTS`.
pub fn send_datagram(
    sock: BorrowedFd<'_>,
    bytes: &[u8],
    fd: Option<BorrowedFd<'_>>,
) -> Result<(), ChannelError> {
    debug_assert!(bytes.len() <= DATAGRAM_MAX);
    let iov = [IoSlice::new(bytes)];
    let raw_fds;
    let cmsgs: &[ControlMessage<'_>] = match &fd {
        Some(f) => {
            raw_fds = [f.as_raw_fd()];
            &[ControlMessage::ScmRights(&raw_fds)]
        }
        None => &[],
    };
    let sent =
        sendmsg::<()>(sock.as_raw_fd(), &iov, cmsgs, MsgFlags::empty(), None).map_err(io_err)?;
    if sent != bytes.len() {
        return Err(ChannelError::Protocol(format!(
            "short send: {sent} of {} bytes",
            bytes.len()
        )));
    }
    Ok(())
}

/// One received datagram: its payload bytes and any fd that rode along.
pub struct Datagram {
    pub bytes: Vec<u8>,
    pub fd: Option<OwnedFd>,
}

/// Receive one datagram (blocking). Returns `None` on orderly peer shutdown
/// (zero-length read with no data — SEQPACKET EOF).
pub fn recv_datagram(sock: BorrowedFd<'_>) -> Result<Option<Datagram>, ChannelError> {
    let mut buf = vec![0u8; DATAGRAM_MAX];
    let mut cmsg_buf = nix::cmsg_space!([std::os::fd::RawFd; 1]);
    let (len, fd) = {
        let mut iov = [IoSliceMut::new(&mut buf)];
        let msg = recvmsg::<()>(
            sock.as_raw_fd(),
            &mut iov,
            Some(&mut cmsg_buf),
            MsgFlags::empty(),
        )
        .map_err(io_err)?;

        if msg.flags.contains(MsgFlags::MSG_TRUNC) {
            return Err(ChannelError::Protocol(
                "datagram exceeded the 64 KiB cap (MSG_TRUNC)".into(),
            ));
        }

        let mut fd: Option<OwnedFd> = None;
        for cmsg in msg.cmsgs().map_err(io_err)? {
            if let ControlMessageOwned::ScmRights(fds) = cmsg {
                for raw in fds {
                    // nix hands us raw fds; take ownership of the first and
                    // close any extras (a peer must send at most one).
                    let owned = unsafe_owned_from_raw(raw);
                    if fd.is_none() {
                        fd = Some(owned);
                    }
                    // extras drop ⇒ closed
                }
            }
        }
        (msg.bytes, fd)
    };

    if len == 0 && fd.is_none() {
        return Ok(None); // peer closed
    }
    buf.truncate(len);
    Ok(Some(Datagram { bytes: buf, fd }))
}

// nix's ControlMessageOwned::ScmRights yields RawFd values that the kernel
// has installed into our fd table and that nobody else owns; adopting them
// into OwnedFd is the safe-and-required cleanup path. `std::os::fd::FromRawFd`
// is an unsafe trait fn, so route through the one place nix offers it safely.
fn unsafe_owned_from_raw(raw: std::os::fd::RawFd) -> OwnedFd {
    use std::os::fd::FromRawFd;
    // SAFETY-equivalent justification above; this is the single point where
    // a kernel-installed fd is adopted.
    #[allow(unsafe_code)]
    unsafe {
        OwnedFd::from_raw_fd(raw)
    }
}

fn io_err(e: nix::errno::Errno) -> ChannelError {
    ChannelError::Io(std::io::Error::from_raw_os_error(e as i32))
}

// ── memfd helpers ─────────────────────────────────────────────────────────────

/// Seals a PUT_BATCH memfd must carry (client side) and the server must
/// verify: the content can no longer change or shrink between the server's
/// hash-at-receive and the pack writer's later copy (TOCTOU close — plan 03
/// WI2; upstream doc issue filed to add this to API §4's protocol rules).
pub const REQUIRED_PUT_SEALS: &[SealFlag] = &[SealFlag::F_SEAL_WRITE, SealFlag::F_SEAL_SHRINK];

/// Create a memfd containing `count` pages and seal it for PUT_BATCH
/// (`F_SEAL_WRITE | F_SEAL_SHRINK`).
pub fn create_sealed_put_memfd(pages: &[&[u8; PAGE_SIZE]]) -> Result<OwnedFd, ChannelError> {
    let fd = memfd_create(
        c"snapstore-put-batch",
        MemFdCreateFlag::MFD_CLOEXEC | MemFdCreateFlag::MFD_ALLOW_SEALING,
    )
    .map_err(io_err)?;
    let file = std::fs::File::from(fd);
    for (i, page) in pages.iter().enumerate() {
        file.write_all_at(*page, (i * PAGE_SIZE) as u64)?;
    }
    let fd = OwnedFd::from(file);
    fcntl(
        fd.as_raw_fd(),
        FcntlArg::F_ADD_SEALS(SealFlag::F_SEAL_WRITE | SealFlag::F_SEAL_SHRINK),
    )
    .map_err(io_err)?;
    Ok(fd)
}

/// Create a memfd of `len` bytes for a GET_BATCH_DATA reply; the caller
/// writes pages then calls [`seal_get_memfd`].
pub fn create_get_memfd(len: u64) -> Result<std::fs::File, ChannelError> {
    let fd = memfd_create(
        c"snapstore-get-batch",
        MemFdCreateFlag::MFD_CLOEXEC | MemFdCreateFlag::MFD_ALLOW_SEALING,
    )
    .map_err(io_err)?;
    let file = std::fs::File::from(fd);
    file.set_len(len)?;
    Ok(file)
}

/// Seal a GET_BATCH_DATA memfd: size frozen (`F_SEAL_GROW | F_SEAL_SHRINK`).
pub fn seal_get_memfd(file: std::fs::File) -> Result<OwnedFd, ChannelError> {
    let fd = OwnedFd::from(file);
    fcntl(
        fd.as_raw_fd(),
        FcntlArg::F_ADD_SEALS(SealFlag::F_SEAL_GROW | SealFlag::F_SEAL_SHRINK),
    )
    .map_err(io_err)?;
    Ok(fd)
}

/// Server-side check on a received PUT_BATCH fd: reject unsealed memfds
/// (`ERROR INVALID`) — see [`REQUIRED_PUT_SEALS`].
pub fn verify_put_seals(fd: BorrowedFd<'_>) -> Result<bool, ChannelError> {
    let seals = fcntl(fd.as_raw_fd(), FcntlArg::F_GET_SEALS).map_err(io_err)?;
    let seals = SealFlag::from_bits_truncate(seals);
    Ok(seals.contains(SealFlag::F_SEAL_WRITE) && seals.contains(SealFlag::F_SEAL_SHRINK))
}

/// The current byte length of a received memfd.
pub fn memfd_len(fd: BorrowedFd<'_>) -> Result<u64, ChannelError> {
    let st = nix::sys::stat::fstat(fd.as_raw_fd()).map_err(io_err)?;
    Ok(st.st_size as u64)
}

/// A connected SEQPACKET socket pair (loopback tests and in-process use).
pub fn seqpacket_pair() -> Result<(OwnedFd, OwnedFd), ChannelError> {
    socketpair(
        AddressFamily::Unix,
        SockType::SeqPacket,
        None,
        SockFlag::SOCK_CLOEXEC,
    )
    .map_err(io_err)
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::*;
    use snapstore_types::PageHash;

    fn page(fill: u8) -> Box<[u8; PAGE_SIZE]> {
        Box::new([fill; PAGE_SIZE])
    }

    #[test]
    fn datagram_round_trip_no_fd() {
        let (a, b) = seqpacket_pair().unwrap();
        let wire = encode_put_batch(11, 3);
        send_datagram(a.as_fd(), &wire, None).unwrap();
        let got = recv_datagram(b.as_fd()).unwrap().unwrap();
        assert_eq!(got.bytes, wire);
        assert!(got.fd.is_none());
    }

    #[test]
    fn each_message_type_with_fd_round_trips() {
        let (a, b) = seqpacket_pair().unwrap();
        let p0 = page(0x11);
        let p1 = page(0x22);
        let pages: Vec<&[u8; PAGE_SIZE]> = vec![&p0, &p1];

        // PUT_BATCH with a sealed memfd.
        let memfd = create_sealed_put_memfd(&pages).unwrap();
        let wire = encode_put_batch(1, 2);
        send_datagram(a.as_fd(), &wire, Some(memfd.as_fd())).unwrap();
        let got = recv_datagram(b.as_fd()).unwrap().unwrap();
        let (hdr, body) = decode_hdr(&got.bytes).unwrap();
        assert_eq!(decode_put_batch(&hdr, body).unwrap(), 2);
        let rfd = got.fd.expect("fd must arrive");
        assert!(verify_put_seals(rfd.as_fd()).unwrap());
        assert_eq!(memfd_len(rfd.as_fd()).unwrap(), 2 * PAGE_SIZE as u64);
        // Content visible through the received fd.
        let f = std::fs::File::from(rfd);
        let mut buf = vec![0u8; PAGE_SIZE];
        f.read_exact_at(&mut buf, PAGE_SIZE as u64).unwrap();
        assert_eq!(buf, vec![0x22; PAGE_SIZE]);

        // PUT_BATCH_OK (no fd).
        let ok = PutOkBody {
            pages_new: 1,
            pages_deduped: 1,
            batch_blake3: batch_cross_check(&[PageHash([1; 32]), PageHash([2; 32])]),
        };
        send_datagram(b.as_fd(), &encode_put_batch_ok(1, 2, &ok), None).unwrap();
        let got = recv_datagram(a.as_fd()).unwrap().unwrap();
        let (hdr, body) = decode_hdr(&got.bytes).unwrap();
        assert_eq!(decode_put_batch_ok(&hdr, body).unwrap(), ok);

        // GET_BATCH (no fd) → GET_BATCH_DATA (fd, size-sealed).
        let reqs = vec![GetReq {
            page_hash: PageHash([7; 32]),
            dst_slot: 4,
        }];
        send_datagram(a.as_fd(), &encode_get_batch(2, &reqs), None).unwrap();
        let got = recv_datagram(b.as_fd()).unwrap().unwrap();
        let (hdr, body) = decode_hdr(&got.bytes).unwrap();
        assert_eq!(decode_get_batch(&hdr, body).unwrap(), reqs);

        let reply = create_get_memfd(PAGE_SIZE as u64).unwrap();
        reply.write_all_at(&[0x33; PAGE_SIZE], 0).unwrap();
        let reply = seal_get_memfd(reply).unwrap();
        send_datagram(
            b.as_fd(),
            &encode_get_batch_data(2, &reqs),
            Some(reply.as_fd()),
        )
        .unwrap();
        let got = recv_datagram(a.as_fd()).unwrap().unwrap();
        let (hdr, body) = decode_hdr(&got.bytes).unwrap();
        assert_eq!(decode_get_batch_data(&hdr, body).unwrap(), reqs);
        assert!(got.fd.is_some());

        // ERROR (no fd).
        send_datagram(
            a.as_fd(),
            &encode_error(3, ErrorCode::NotFound, "nope"),
            None,
        )
        .unwrap();
        let got = recv_datagram(b.as_fd()).unwrap().unwrap();
        let (hdr, body) = decode_hdr(&got.bytes).unwrap();
        let err = decode_error(&hdr, body).unwrap();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert_eq!(err.detail, "nope");
    }

    #[test]
    fn unsealed_memfd_fails_verification() {
        let fd = memfd_create(
            c"snapstore-unsealed",
            MemFdCreateFlag::MFD_CLOEXEC | MemFdCreateFlag::MFD_ALLOW_SEALING,
        )
        .unwrap();
        assert!(!verify_put_seals(fd.as_fd()).unwrap());
    }

    #[test]
    fn sealed_put_memfd_rejects_writes() {
        let p = page(0xaa);
        let fd = create_sealed_put_memfd(&[&p]).unwrap();
        let f = std::fs::File::from(fd);
        assert!(
            f.write_all_at(&[0u8; 8], 0).is_err(),
            "seal must block writes"
        );
    }

    #[test]
    fn peer_close_yields_none() {
        let (a, b) = seqpacket_pair().unwrap();
        drop(a);
        assert!(recv_datagram(b.as_fd()).unwrap().is_none());
    }
}
