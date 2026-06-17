//! Pure wire codec for the page channel (API §4).
//!
//! Everything here is bytes-in/bytes-out and host-portable; the
//! sendmsg/recvmsg + `SCM_RIGHTS` plumbing lives in [`crate::linux`].
//!
//! All integers are little-endian, packed exactly as written in the spec:
//!
//! ```text
//! PcHdr   { magic: u32 = 0x50434831 ("PCH1"), msg: u16, flags: u16,
//!           seq: u64, count: u32, reserved: u32 }            // 24 bytes
//! msg 1   PUT_BATCH      (fd = memfd; count pages; no body)
//! msg 2   PUT_BATCH_OK   body PutOkBody { pages_new: u32, pages_deduped: u32,
//!                                         batch_blake3: [u8; 32] }  // 40 bytes
//! msg 3   GET_BATCH      body count × GetReq { page_hash: [u8; 32],
//!                                              dst_slot: u64 }      // 40 bytes
//! msg 4   GET_BATCH_DATA (fd = memfd; body echoes the GetReq entries)
//! msg 5   ERROR          body ErrBody { code: u32, detail_len: u32, utf8… }
//! ```

use snapstore_types::PageHash;

// ── constants ─────────────────────────────────────────────────────────────────

/// Wire magic. Encoded little-endian, i.e. the bytes `31 48 43 50` on the wire.
pub const PC_MAGIC: u32 = 0x5043_4831; // "PCH1"

/// Header size on the wire.
pub const HDR_SIZE: usize = 24;

/// One `GetReq` / echoed entry on the wire.
pub const GET_REQ_SIZE: usize = 40;

/// `PutOkBody` size on the wire.
pub const PUT_OK_SIZE: usize = 40;

/// `ErrBody` fixed prefix (code + detail_len) before the UTF-8 detail.
pub const ERR_PREFIX_SIZE: usize = 8;

/// Maximum pages per PUT_BATCH (= 32 MiB of payload in the memfd).
pub const PUT_BATCH_MAX_PAGES: u32 = 8192;

/// Maximum GetReq entries per GET_BATCH datagram (fits the 64 KiB datagram
/// cap: 24 + 1500×40 = 60,024 bytes). Clients send multiple datagrams for
/// larger sets; `seq` orders them.
pub const GET_BATCH_MAX_PER_DATAGRAM: u32 = 1500;

/// Datagram size cap the protocol assumes.
pub const DATAGRAM_MAX: usize = 64 * 1024;

// Compile-time layout checks against the spec offsets.
const _: () = assert!(HDR_SIZE == 4 + 2 + 2 + 8 + 4 + 4);
const _: () = assert!(GET_REQ_SIZE == 32 + 8);
const _: () = assert!(PUT_OK_SIZE == 4 + 4 + 32);
const _: () =
    assert!(HDR_SIZE + (GET_BATCH_MAX_PER_DATAGRAM as usize) * GET_REQ_SIZE <= DATAGRAM_MAX);
const _: () = assert!(PUT_BATCH_MAX_PAGES as usize * 4096 == 32 * 1024 * 1024);

// ── message kinds ─────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum MsgKind {
    PutBatch = 1,
    PutBatchOk = 2,
    GetBatch = 3,
    GetBatchData = 4,
    Error = 5,
}

impl MsgKind {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            1 => Some(Self::PutBatch),
            2 => Some(Self::PutBatchOk),
            3 => Some(Self::GetBatch),
            4 => Some(Self::GetBatchData),
            5 => Some(Self::Error),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum ErrorCode {
    NotFound = 1,
    Invalid = 2,
    Overload = 3,
}

impl ErrorCode {
    pub fn from_u32(v: u32) -> Option<Self> {
        match v {
            1 => Some(Self::NotFound),
            2 => Some(Self::Invalid),
            3 => Some(Self::Overload),
            _ => None,
        }
    }
}

// ── wire structs ──────────────────────────────────────────────────────────────

/// The fixed 24-byte datagram header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PcHdr {
    pub msg: MsgKind,
    pub flags: u16,
    pub seq: u64,
    pub count: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PutOkBody {
    pub pages_new: u32,
    pub pages_deduped: u32,
    /// BLAKE3 over the concatenated per-page hashes in memfd order (the
    /// full hash list would blow the 64 KiB datagram cap).
    pub batch_blake3: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GetReq {
    pub page_hash: PageHash,
    /// Echoed metadata for client-side scatter; the server never interprets it.
    pub dst_slot: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrBody {
    pub code: ErrorCode,
    pub detail: String,
}

// ── errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireError {
    #[error("datagram shorter than header ({0} bytes)")]
    Truncated(usize),
    #[error("bad magic {0:#010x}")]
    BadMagic(u32),
    #[error("unknown msg kind {0}")]
    UnknownMsg(u16),
    #[error("nonzero flags {0:#06x}")]
    NonZeroFlags(u16),
    #[error("nonzero reserved {0:#010x}")]
    NonZeroReserved(u32),
    #[error("count {count} out of range for {ctx}")]
    CountOutOfRange { ctx: &'static str, count: u32 },
    #[error("body length {got} does not match header count (expected {expected})")]
    BodyLenMismatch { expected: usize, got: usize },
    #[error("unknown error code {0}")]
    UnknownErrorCode(u32),
    #[error("error detail is not valid UTF-8")]
    DetailNotUtf8,
    #[error("error detail length {0} exceeds datagram cap")]
    DetailTooLong(usize),
}

// ── header codec ──────────────────────────────────────────────────────────────

fn put_hdr(buf: &mut Vec<u8>, hdr: &PcHdr) {
    buf.extend_from_slice(&PC_MAGIC.to_le_bytes());
    buf.extend_from_slice(&(hdr.msg as u16).to_le_bytes());
    buf.extend_from_slice(&hdr.flags.to_le_bytes());
    buf.extend_from_slice(&hdr.seq.to_le_bytes());
    buf.extend_from_slice(&hdr.count.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // reserved
}

/// Decode and validate the fixed header; returns the header and the body.
pub fn decode_hdr(datagram: &[u8]) -> Result<(PcHdr, &[u8]), WireError> {
    if datagram.len() < HDR_SIZE {
        return Err(WireError::Truncated(datagram.len()));
    }
    let magic = u32::from_le_bytes(datagram[0..4].try_into().unwrap());
    if magic != PC_MAGIC {
        return Err(WireError::BadMagic(magic));
    }
    let msg_raw = u16::from_le_bytes(datagram[4..6].try_into().unwrap());
    let msg = MsgKind::from_u16(msg_raw).ok_or(WireError::UnknownMsg(msg_raw))?;
    let flags = u16::from_le_bytes(datagram[6..8].try_into().unwrap());
    if flags != 0 {
        return Err(WireError::NonZeroFlags(flags));
    }
    let seq = u64::from_le_bytes(datagram[8..16].try_into().unwrap());
    let count = u32::from_le_bytes(datagram[16..20].try_into().unwrap());
    let reserved = u32::from_le_bytes(datagram[20..24].try_into().unwrap());
    if reserved != 0 {
        return Err(WireError::NonZeroReserved(reserved));
    }
    Ok((
        PcHdr {
            msg,
            flags,
            seq,
            count,
        },
        &datagram[HDR_SIZE..],
    ))
}

// ── message encoders ──────────────────────────────────────────────────────────

/// msg 1 — header only; the page payload travels in the accompanying memfd.
pub fn encode_put_batch(seq: u64, count: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HDR_SIZE);
    put_hdr(
        &mut buf,
        &PcHdr {
            msg: MsgKind::PutBatch,
            flags: 0,
            seq,
            count,
        },
    );
    buf
}

/// msg 2.
pub fn encode_put_batch_ok(seq: u64, count: u32, body: &PutOkBody) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HDR_SIZE + PUT_OK_SIZE);
    put_hdr(
        &mut buf,
        &PcHdr {
            msg: MsgKind::PutBatchOk,
            flags: 0,
            seq,
            count,
        },
    );
    buf.extend_from_slice(&body.pages_new.to_le_bytes());
    buf.extend_from_slice(&body.pages_deduped.to_le_bytes());
    buf.extend_from_slice(&body.batch_blake3);
    buf
}

/// msg 3.
pub fn encode_get_batch(seq: u64, reqs: &[GetReq]) -> Vec<u8> {
    debug_assert!(reqs.len() <= GET_BATCH_MAX_PER_DATAGRAM as usize);
    let mut buf = Vec::with_capacity(HDR_SIZE + reqs.len() * GET_REQ_SIZE);
    put_hdr(
        &mut buf,
        &PcHdr {
            msg: MsgKind::GetBatch,
            flags: 0,
            seq,
            count: reqs.len() as u32,
        },
    );
    for r in reqs {
        buf.extend_from_slice(r.page_hash.as_bytes());
        buf.extend_from_slice(&r.dst_slot.to_le_bytes());
    }
    buf
}

/// msg 4 — same body shape as msg 3 (entries echoed); pages travel in the
/// accompanying memfd, request[i]'s page at offset i*4096.
pub fn encode_get_batch_data(seq: u64, reqs: &[GetReq]) -> Vec<u8> {
    let mut buf = encode_get_batch(seq, reqs);
    // Patch the msg kind: layout is identical apart from the kind field.
    buf[4..6].copy_from_slice(&(MsgKind::GetBatchData as u16).to_le_bytes());
    buf
}

/// msg 5.
pub fn encode_error(seq: u64, code: ErrorCode, detail: &str) -> Vec<u8> {
    let detail_bytes = detail.as_bytes();
    debug_assert!(HDR_SIZE + ERR_PREFIX_SIZE + detail_bytes.len() <= DATAGRAM_MAX);
    let mut buf = Vec::with_capacity(HDR_SIZE + ERR_PREFIX_SIZE + detail_bytes.len());
    put_hdr(
        &mut buf,
        &PcHdr {
            msg: MsgKind::Error,
            flags: 0,
            seq,
            count: 0,
        },
    );
    buf.extend_from_slice(&(code as u32).to_le_bytes());
    buf.extend_from_slice(&(detail_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(detail_bytes);
    buf
}

// ── body decoders (call after `decode_hdr` dispatched on `hdr.msg`) ──────────

/// msg 1: validates the count bound and the empty body.
pub fn decode_put_batch(hdr: &PcHdr, body: &[u8]) -> Result<u32, WireError> {
    debug_assert_eq!(hdr.msg, MsgKind::PutBatch);
    if hdr.count == 0 || hdr.count > PUT_BATCH_MAX_PAGES {
        return Err(WireError::CountOutOfRange {
            ctx: "PUT_BATCH",
            count: hdr.count,
        });
    }
    if !body.is_empty() {
        return Err(WireError::BodyLenMismatch {
            expected: 0,
            got: body.len(),
        });
    }
    Ok(hdr.count)
}

/// msg 2.
pub fn decode_put_batch_ok(hdr: &PcHdr, body: &[u8]) -> Result<PutOkBody, WireError> {
    debug_assert_eq!(hdr.msg, MsgKind::PutBatchOk);
    if body.len() != PUT_OK_SIZE {
        return Err(WireError::BodyLenMismatch {
            expected: PUT_OK_SIZE,
            got: body.len(),
        });
    }
    Ok(PutOkBody {
        pages_new: u32::from_le_bytes(body[0..4].try_into().unwrap()),
        pages_deduped: u32::from_le_bytes(body[4..8].try_into().unwrap()),
        batch_blake3: body[8..40].try_into().unwrap(),
    })
}

fn decode_get_reqs(hdr: &PcHdr, body: &[u8], ctx: &'static str) -> Result<Vec<GetReq>, WireError> {
    if hdr.count == 0 || hdr.count > GET_BATCH_MAX_PER_DATAGRAM {
        return Err(WireError::CountOutOfRange {
            ctx,
            count: hdr.count,
        });
    }
    let expected = hdr.count as usize * GET_REQ_SIZE;
    if body.len() != expected {
        return Err(WireError::BodyLenMismatch {
            expected,
            got: body.len(),
        });
    }
    let mut reqs = Vec::with_capacity(hdr.count as usize);
    for chunk in body.chunks_exact(GET_REQ_SIZE) {
        reqs.push(GetReq {
            page_hash: PageHash::from_bytes(chunk[0..32].try_into().unwrap()),
            dst_slot: u64::from_le_bytes(chunk[32..40].try_into().unwrap()),
        });
    }
    Ok(reqs)
}

/// msg 3.
pub fn decode_get_batch(hdr: &PcHdr, body: &[u8]) -> Result<Vec<GetReq>, WireError> {
    debug_assert_eq!(hdr.msg, MsgKind::GetBatch);
    decode_get_reqs(hdr, body, "GET_BATCH")
}

/// msg 4.
pub fn decode_get_batch_data(hdr: &PcHdr, body: &[u8]) -> Result<Vec<GetReq>, WireError> {
    debug_assert_eq!(hdr.msg, MsgKind::GetBatchData);
    decode_get_reqs(hdr, body, "GET_BATCH_DATA")
}

/// msg 5.
pub fn decode_error(hdr: &PcHdr, body: &[u8]) -> Result<ErrBody, WireError> {
    debug_assert_eq!(hdr.msg, MsgKind::Error);
    if body.len() < ERR_PREFIX_SIZE {
        return Err(WireError::BodyLenMismatch {
            expected: ERR_PREFIX_SIZE,
            got: body.len(),
        });
    }
    let code_raw = u32::from_le_bytes(body[0..4].try_into().unwrap());
    let code = ErrorCode::from_u32(code_raw).ok_or(WireError::UnknownErrorCode(code_raw))?;
    let detail_len = u32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
    if detail_len > DATAGRAM_MAX - HDR_SIZE - ERR_PREFIX_SIZE {
        return Err(WireError::DetailTooLong(detail_len));
    }
    let detail_bytes = &body[ERR_PREFIX_SIZE..];
    if detail_bytes.len() != detail_len {
        return Err(WireError::BodyLenMismatch {
            expected: ERR_PREFIX_SIZE + detail_len,
            got: body.len(),
        });
    }
    let detail = std::str::from_utf8(detail_bytes)
        .map_err(|_| WireError::DetailNotUtf8)?
        .to_owned();
    Ok(ErrBody { code, detail })
}

/// The cross-check hash carried in `PUT_BATCH_OK`: BLAKE3 over the
/// concatenated per-page hashes in memfd order. Both halves compute it —
/// a mismatch is a P0 determinism bug, never retried silently.
pub fn batch_cross_check(hashes: &[PageHash]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for h in hashes {
        hasher.update(h.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_layout_on_wire() {
        let buf = encode_put_batch(0xDEAD_BEEF_0123_4567, 42);
        assert_eq!(buf.len(), HDR_SIZE);
        // Magic 0x50434831 little-endian = bytes 31 48 43 50.
        assert_eq!(&buf[0..4], &[0x31, 0x48, 0x43, 0x50]);
        assert_eq!(&buf[4..6], &1u16.to_le_bytes()); // msg
        assert_eq!(&buf[6..8], &0u16.to_le_bytes()); // flags
        assert_eq!(&buf[8..16], &0xDEAD_BEEF_0123_4567u64.to_le_bytes());
        assert_eq!(&buf[16..20], &42u32.to_le_bytes());
        assert_eq!(&buf[20..24], &0u32.to_le_bytes()); // reserved
    }

    #[test]
    fn put_batch_round_trip() {
        let buf = encode_put_batch(7, 8192);
        let (hdr, body) = decode_hdr(&buf).unwrap();
        assert_eq!(hdr.msg, MsgKind::PutBatch);
        assert_eq!(hdr.seq, 7);
        assert_eq!(decode_put_batch(&hdr, body).unwrap(), 8192);
    }

    #[test]
    fn put_batch_count_bounds() {
        for bad in [0u32, PUT_BATCH_MAX_PAGES + 1] {
            let buf = encode_put_batch(0, bad);
            let (hdr, body) = decode_hdr(&buf).unwrap();
            assert!(matches!(
                decode_put_batch(&hdr, body),
                Err(WireError::CountOutOfRange { .. })
            ));
        }
    }

    #[test]
    fn put_batch_ok_round_trip() {
        let body_in = PutOkBody {
            pages_new: 100,
            pages_deduped: 28,
            batch_blake3: [0xab; 32],
        };
        let buf = encode_put_batch_ok(9, 128, &body_in);
        assert_eq!(buf.len(), HDR_SIZE + PUT_OK_SIZE);
        let (hdr, body) = decode_hdr(&buf).unwrap();
        assert_eq!(hdr.msg, MsgKind::PutBatchOk);
        assert_eq!(hdr.count, 128);
        assert_eq!(decode_put_batch_ok(&hdr, body).unwrap(), body_in);
    }

    #[test]
    fn get_batch_round_trip() {
        let reqs: Vec<GetReq> = (0..100)
            .map(|i| GetReq {
                page_hash: PageHash([i as u8; 32]),
                dst_slot: i * 3 + 1,
            })
            .collect();
        let buf = encode_get_batch(55, &reqs);
        assert_eq!(buf.len(), HDR_SIZE + 100 * GET_REQ_SIZE);
        let (hdr, body) = decode_hdr(&buf).unwrap();
        assert_eq!(hdr.msg, MsgKind::GetBatch);
        assert_eq!(hdr.seq, 55);
        assert_eq!(decode_get_batch(&hdr, body).unwrap(), reqs);
    }

    #[test]
    fn get_batch_data_round_trip() {
        let reqs = vec![GetReq {
            page_hash: PageHash([0x11; 32]),
            dst_slot: 99,
        }];
        let buf = encode_get_batch_data(2, &reqs);
        let (hdr, body) = decode_hdr(&buf).unwrap();
        assert_eq!(hdr.msg, MsgKind::GetBatchData);
        assert_eq!(decode_get_batch_data(&hdr, body).unwrap(), reqs);
    }

    #[test]
    fn error_round_trip() {
        for (code, detail) in [
            (ErrorCode::NotFound, "missing hash ab12"),
            (ErrorCode::Invalid, ""),
            (ErrorCode::Overload, "queue full"),
        ] {
            let buf = encode_error(3, code, detail);
            let (hdr, body) = decode_hdr(&buf).unwrap();
            assert_eq!(hdr.msg, MsgKind::Error);
            let err = decode_error(&hdr, body).unwrap();
            assert_eq!(err.code, code);
            assert_eq!(err.detail, detail);
        }
    }

    #[test]
    fn header_strictness() {
        // Truncated.
        assert!(matches!(
            decode_hdr(&[0u8; 10]),
            Err(WireError::Truncated(10))
        ));
        // Bad magic.
        let mut buf = encode_put_batch(0, 1);
        buf[0] ^= 0xff;
        assert!(matches!(decode_hdr(&buf), Err(WireError::BadMagic(_))));
        // Unknown msg kind.
        let mut buf = encode_put_batch(0, 1);
        buf[4..6].copy_from_slice(&99u16.to_le_bytes());
        assert!(matches!(decode_hdr(&buf), Err(WireError::UnknownMsg(99))));
        // Nonzero flags.
        let mut buf = encode_put_batch(0, 1);
        buf[6] = 1;
        assert!(matches!(decode_hdr(&buf), Err(WireError::NonZeroFlags(1))));
        // Nonzero reserved.
        let mut buf = encode_put_batch(0, 1);
        buf[20] = 1;
        assert!(matches!(
            decode_hdr(&buf),
            Err(WireError::NonZeroReserved(1))
        ));
    }

    #[test]
    fn body_strictness() {
        // PUT_BATCH with trailing body bytes.
        let mut buf = encode_put_batch(0, 1);
        buf.push(0);
        let (hdr, body) = decode_hdr(&buf).unwrap();
        assert!(matches!(
            decode_put_batch(&hdr, body),
            Err(WireError::BodyLenMismatch { .. })
        ));
        // GET_BATCH body length mismatch vs count.
        let reqs = vec![GetReq {
            page_hash: PageHash([0; 32]),
            dst_slot: 0,
        }];
        let mut buf = encode_get_batch(0, &reqs);
        buf.truncate(buf.len() - 1);
        let (hdr, body) = decode_hdr(&buf).unwrap();
        assert!(matches!(
            decode_get_batch(&hdr, body),
            Err(WireError::BodyLenMismatch { .. })
        ));
        // ERROR with unknown code.
        let mut buf = encode_error(0, ErrorCode::NotFound, "x");
        buf[HDR_SIZE..HDR_SIZE + 4].copy_from_slice(&7u32.to_le_bytes());
        let (hdr, body) = decode_hdr(&buf).unwrap();
        assert!(matches!(
            decode_error(&hdr, body),
            Err(WireError::UnknownErrorCode(7))
        ));
        // ERROR with detail_len disagreeing with the actual tail.
        let mut buf = encode_error(0, ErrorCode::Invalid, "abc");
        let dl = HDR_SIZE + 4;
        buf[dl..dl + 4].copy_from_slice(&2u32.to_le_bytes());
        let (hdr, body) = decode_hdr(&buf).unwrap();
        assert!(matches!(
            decode_error(&hdr, body),
            Err(WireError::BodyLenMismatch { .. })
        ));
        // ERROR with invalid UTF-8 detail.
        let mut buf = encode_error(0, ErrorCode::Invalid, "ab");
        let last = buf.len() - 1;
        buf[last] = 0xff;
        let (hdr, body) = decode_hdr(&buf).unwrap();
        assert!(matches!(
            decode_error(&hdr, body),
            Err(WireError::DetailNotUtf8)
        ));
    }

    #[test]
    fn cross_check_is_order_sensitive() {
        let a = PageHash([1; 32]);
        let b = PageHash([2; 32]);
        assert_ne!(batch_cross_check(&[a, b]), batch_cross_check(&[b, a]));
        assert_eq!(batch_cross_check(&[]), *blake3::hash(&[]).as_bytes());
    }
}
