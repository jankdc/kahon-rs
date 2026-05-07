use crate::align::write_padding;
use crate::config::PageAlignment;
use crate::error::WriteError;
use crate::frame::{close_frame, register_into_frame, Frame};
use crate::raw_writer::RawWriter;
use crate::sink::{RewindableSink, WriteCtx};
use crate::types::MAGIC;

/// Closing bytes for a kahon document as it stands at a point in time.
///
/// A complete kahon document can be assembled by concatenating the
/// first [`prefix_len`](Self::prefix_len) bytes of the writer's sink
/// with [`bytes`](Self::bytes). Useful for publishing snapshots of an
/// in-progress document without disturbing the live writer.
#[derive(Debug, Clone)]
pub struct TrailerSnapshot {
    /// Number of bytes from the start of the writer's sink that
    /// participate in the snapshot.
    pub prefix_len: u64,
    /// Closing bytes to append after the prefix. Length is `>= 12`.
    pub bytes: Vec<u8>,
}

impl TrailerSnapshot {
    /// Total length of the assembled snapshot document.
    pub fn total_len(&self) -> u64 {
        self.prefix_len + self.bytes.len() as u64
    }

    /// Absolute offset of the root value in the assembled snapshot.
    pub fn root_offset(&self) -> u64 {
        let n = self.bytes.len();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.bytes[n - 12..n - 4]);
        u64::from_le_bytes(buf)
    }

    /// Absolute offset where the 12-byte trailer begins in the
    /// assembled snapshot. Equal to `total_len() - 12`.
    pub fn trailer_offset(&self) -> u64 {
        self.total_len() - 12
    }
}

impl<S: RewindableSink> RawWriter<S> {
    /// Synthesize the closing bytes for the document as it stands,
    /// without disturbing the writer. See [`TrailerSnapshot`].
    pub fn snapshot_trailer(&self) -> Result<TrailerSnapshot, WriteError> {
        if self.poisoned {
            return Err(WriteError::Poisoned);
        }
        if !self.frames.is_empty() && self.root_offset.is_some() {
            return Err(WriteError::MultipleRootValues);
        }

        let prefix_len = self.pos;
        let mut tail: Vec<u8> = Vec::new();
        let mut tail_pos = self.pos;
        let mut tail_padding = self.padding_written;
        let mut scratch = Vec::with_capacity(64);
        let mut frames = self.frames.clone();
        let mut root: Option<u64> = self.root_offset;

        // Drop pending object keys in the cloned frames; the key strings
        // already live in the prefix as orphaned bytes (kahon allows
        // unreferenced bytes in the body).
        for frame in frames.iter_mut() {
            if let Frame::Object(o) = frame {
                o.pending_key = None;
            }
        }

        // Close frames innermost-first into the side buffer. Each closed
        // frame's root is registered into its parent, or becomes the
        // document root if there is no parent.
        while let Some(frame) = frames.pop() {
            let mut ctx = WriteCtx {
                sink: &mut tail,
                pos: &mut tail_pos,
                scratch: &mut scratch,
                padding_written: &mut tail_padding,
            };
            let root_off = close_frame(frame, &self.opts.policy, &mut ctx)?;
            if let Some(parent) = frames.last_mut() {
                register_into_frame(parent, root_off, &self.opts, &mut ctx)?;
            } else {
                root = Some(root_off);
            }
        }

        let root = root.ok_or(WriteError::EmptyDocument)?;

        // Pad so the 12-byte trailer ends on a page boundary (mirrors finish).
        if let PageAlignment::Aligned { page_size } = self.opts.policy.align {
            let ps = page_size as u64;
            let target_mod = ps - 12;
            let cur_mod = tail_pos % ps;
            let pad = if cur_mod <= target_mod {
                target_mod - cur_mod
            } else {
                ps - cur_mod + target_mod
            };
            if pad > 0 {
                let mut ctx = WriteCtx {
                    sink: &mut tail,
                    pos: &mut tail_pos,
                    scratch: &mut scratch,
                    padding_written: &mut tail_padding,
                };
                write_padding(&mut ctx, pad as usize)?;
            }
        }

        let mut trailer = [0u8; 12];
        trailer[..8].copy_from_slice(&root.to_le_bytes());
        trailer[8..].copy_from_slice(&MAGIC);
        tail.extend_from_slice(&trailer);

        Ok(TrailerSnapshot {
            prefix_len,
            bytes: tail,
        })
    }
}
