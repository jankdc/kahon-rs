//! Checkpoint primitives and snapshot trailer.
//!
//! [`Checkpoint`] captures writer state so [`Writer::rollback`] can undo
//! everything written since. [`SnapshotTrailer`] synthesises the closing
//! bytes for an in-progress document so a reader can see a complete kahon
//! file without waiting for `finish()`.
//!
//! Both are powered by the same primitive: cloning the frame stack and
//! replaying state. Rollback restores the cloned stack onto the live writer;
//! `snapshot_trailer` drives a clone to closure into a side buffer, leaving
//! the live writer untouched.

use crate::align::write_padding;
use crate::config::PageAlignment;
use crate::error::WriteError;
use crate::frame::{close_frame, register_into_frame, Frame};
use crate::sink::{RewindableSink, Sink, WriteCtx};
use crate::types::MAGIC;
use crate::writer::Writer;

/// Closing bytes for a kahon document as it stands at a point in time.
///
/// Returned by [`Writer::snapshot_trailer`]. The bytes a reader needs to
/// see a complete kahon document are the first `prefix_len` bytes of
/// whatever the writer has emitted, concatenated with [`bytes`]. Useful for
/// publishing snapshots of an in-progress document without disturbing the
/// live writer.
///
/// `bytes` includes any closing B+tree internal nodes for open frames plus
/// the 12-byte trailer (spec §10.2), so its length is always `>= 12`.
///
/// [`bytes`]: TrailerSnapshot::bytes
#[derive(Debug, Clone)]
pub struct TrailerSnapshot {
    /// Number of bytes from the start of the writer's sink that participate
    /// in the snapshot. Equal to [`Writer::bytes_written`] at the call site.
    pub prefix_len: u64,
    /// Closing bytes to append after the prefix to form a complete kahon
    /// document. Length is `>= 12`.
    pub bytes: Vec<u8>,
}

impl TrailerSnapshot {
    /// Total length of the assembled snapshot document, in bytes.
    ///
    /// Equal to `prefix_len + bytes.len()`. Useful for `pread`-style
    /// consumers that need to know the doc's logical end before reading.
    pub fn total_len(&self) -> u64 {
        self.prefix_len + self.bytes.len() as u64
    }

    /// Absolute offset of the root value's type-code byte in the assembled
    /// snapshot.
    ///
    /// Recovered from the 12-byte trailer at the tail of [`bytes`]. Saves
    /// a consumer the round trip of reading the trailer themselves before
    /// jumping to the root.
    ///
    /// [`bytes`]: TrailerSnapshot::bytes
    pub fn root_offset(&self) -> u64 {
        // The trailer is the last 12 bytes of `bytes`: 8-byte little-endian
        // root_offset followed by the 4-byte "KAHN" magic.
        let n = self.bytes.len();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.bytes[n - 12..n - 4]);
        u64::from_le_bytes(buf)
    }

    /// Absolute offset where the 12-byte trailer begins in the assembled
    /// snapshot.
    ///
    /// Equal to `total_len() - 12`. Useful for consumers that want to
    /// `pread` the trailer directly.
    pub fn trailer_offset(&self) -> u64 {
        self.total_len() - 12
    }
}

/// A snapshot of writer state taken by [`Writer::checkpoint`].
///
/// Pass to [`Writer::rollback`] to discard everything written since the
/// checkpoint and restore the writer's frame stack, byte position, and
/// padding counter. To keep the writes, simply drop the `Checkpoint` - the
/// sink already holds the bytes; nothing else needs to happen.
///
/// Holds owned data; carries no borrow of the [`Writer`].
///
/// # Misuse
///
/// `rollback` validates that the checkpoint matches the writer's current
/// frame depth and that its captured position is `<=` the current position.
/// Mismatches return [`WriteError::InvalidCheckpoint`]. The two failure
/// modes the check catches:
///
/// - The checkpoint was taken at a shallower frame depth than the current
///   builder (e.g., a sub-builder calling `rollback` on its parent's
///   checkpoint while still alive).
/// - The checkpoint's position is no longer reachable - usually because a
///   later rollback already discarded those bytes.
pub struct Checkpoint {
    pos: u64,
    padding_written: u64,
    root_offset: Option<u64>,
    frames: Vec<Frame>,
}

impl<S: Sink> Writer<S> {
    /// Synthesize the closing bytes for the document as it stands, without
    /// modifying the writer.
    ///
    /// The first [`prefix_len`](SnapshotTrailer::prefix_len) bytes of the
    /// sink concatenated with the returned [`bytes`](SnapshotTrailer::bytes)
    /// form a complete kahon document. The live writer is unaffected:
    /// position, frames, padding counter, and root are untouched, so writing
    /// can continue and a future [`finish`](Writer::finish) call still
    /// produces the document the writer was building.
    ///
    /// Open container frames are closed into the side buffer in postorder
    /// (innermost first); the topmost open frame's root becomes the
    /// snapshot's root. Pending object keys (a key emitted with no paired
    /// value yet) are dropped from the snapshot - the key bytes already
    /// live in the prefix as orphaned (unreferenced) data, which kahon
    /// permits.
    ///
    /// Errors:
    /// - [`WriteError::Poisoned`] if the writer is poisoned.
    /// - [`WriteError::EmptyDocument`] if no value has been pushed yet.
    /// - [`WriteError::MultipleRootValues`] if a top-level value has been
    ///   pushed *and* a container frame is open (a structurally invalid
    ///   state for snapshotting).
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

impl<S: RewindableSink> Writer<S> {
    /// Snapshot the writer's current state. Pass the returned [`Checkpoint`]
    /// to [`Writer::rollback`] to undo writes made since this call, or drop
    /// it to keep the writes (the sink already holds the bytes).
    ///
    /// May be called at any frame depth; works inside an open
    /// [`ArrayBuilder`](crate::ArrayBuilder) or
    /// [`ObjectBuilder`](crate::ObjectBuilder) via the `checkpoint` method
    /// on those types. Nesting is supported in both directions: a checkpoint
    /// taken inside another checkpoint, and a checkpoint taken while a
    /// container builder is open.
    pub fn checkpoint(&mut self) -> Checkpoint {
        Checkpoint {
            pos: self.pos,
            padding_written: self.padding_written,
            root_offset: self.root_offset,
            frames: self.frames.clone(),
        }
    }

    /// Discard everything written since `cp` was taken and restore the
    /// writer's frame stack, byte position, and padding counter.
    ///
    /// Returns [`WriteError::InvalidCheckpoint`] if the checkpoint's frame
    /// depth doesn't match the writer's current depth, or if its captured
    /// position is greater than the current position. See [`Checkpoint`].
    pub fn rollback(&mut self, cp: Checkpoint) -> Result<(), WriteError> {
        if self.poisoned {
            return Err(WriteError::Poisoned);
        }
        if cp.pos > self.pos || cp.frames.len() != self.frames.len() {
            return Err(WriteError::InvalidCheckpoint);
        }
        self.sink.rewind_to(cp.pos)?;
        self.pos = cp.pos;
        self.padding_written = cp.padding_written;
        self.root_offset = cp.root_offset;
        self.frames = cp.frames;
        Ok(())
    }
}
