//! Shared helpers for encoding-shape tests: drive a `Writer`, then peek at
//! header/trailer/root to make assertions about emitted bytes.

use kahon::{BuildPolicy, Writer, WriterOptions};

/// Header is 6 bytes; trailer is 12 bytes (root_offset u64 + magic u32).
const HEADER_LEN: usize = 6;
const TRAILER_LEN: usize = 12;

/// Build a document by driving a fresh `Writer`. Panics on encode error so
/// individual tests stay focused on the assertion, not the plumbing.
///
/// Uses the compact (fanout=128, no padding) policy so byte-level tests can
/// reason about exact body bytes without accounting for page-alignment fill.
pub fn build<F: FnOnce(&mut Writer<&mut Vec<u8>>)>(f: F) -> Vec<u8> {
    let opts = WriterOptions {
        policy: BuildPolicy::compact(128),
        ..WriterOptions::default()
    };
    let mut buf = Vec::new();
    {
        let mut w = Writer::with_options(&mut buf, opts).unwrap();
        f(&mut w);
        w.finish().unwrap();
    }
    buf
}

/// Strip header and trailer; return just the body bytes. For a single-value
/// document, `body()` is the root value's full encoding.
pub fn body(buf: &[u8]) -> &[u8] {
    &buf[HEADER_LEN..buf.len() - TRAILER_LEN]
}

/// First byte of the root value (its tag).
pub fn root_byte(buf: &[u8]) -> u8 {
    buf[root_offset(buf) as usize]
}

/// Absolute byte offset of the root value, read from the trailer.
pub fn root_offset(buf: &[u8]) -> u64 {
    let off = buf.len() - TRAILER_LEN;
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}
