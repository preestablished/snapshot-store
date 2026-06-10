// deny, not forbid: recv_datagram needs one `unsafe` to adopt SCM_RIGHTS fds
// the kernel installed into our table (OwnedFd::from_raw_fd) — that single
// site carries an allow + justification; everything else stays safe.
#![deny(unsafe_code)]

//! The fast-path page channel: SEQPACKET datagrams with memfd fd-passing
//! for co-located hypervisor workers (API §4 / phase-2 plan 03).
//!
//! Control stays on gRPC; only bulk page payloads ride this channel.
//!
//! Layering:
//! - [`proto`] — pure wire codec over byte slices, host-portable.
//! - [`linux`] — sendmsg/recvmsg + `SCM_RIGHTS` plumbing and memfd helpers,
//!   `cfg(target_os = "linux")` only. On other targets the channel reports
//!   [`Unsupported`](ChannelError::Unsupported) so the workspace stays green
//!   on the darwin dev machine; all acceptance runs happen on the Linux
//!   reference box.

pub mod proto;

#[cfg(target_os = "linux")]
pub mod linux;

/// Errors common to both halves of the channel.
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("page channel is only supported on Linux")]
    Unsupported,
    #[error("wire format error: {0}")]
    Wire(#[from] proto::WireError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("peer sent ERROR {code:?}: {detail}")]
    Peer {
        code: proto::ErrorCode,
        detail: String,
    },
    #[error("protocol violation: {0}")]
    Protocol(String),
}
