//! Speculative writes and snapshot trailer.
//!
//! [`Writer::try_write`] runs a closure within a save/restore boundary:
//! `Ok` keeps the writes, `Err` rolls everything back including the poison
//! flag.
//!

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

/// Internal save-state for [`Writer::try_write`]. Captures the
/// information needed to roll the writer back to the call site.
pub(crate) struct Checkpoint {
    pos: u64,
    padding_written: u64,
    root_offset: Option<u64>,
    frames: Vec<Frame>,
    poisoned: bool,
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
    /// Run `f` as a speculative write. If `f` returns `Ok`, the writes are
    /// kept; if `Err`, every byte written and every frame opened since
    /// the call is rolled back (poison flag included) and the error is
    /// propagated.
    ///
    /// ```ignore
    /// w.try_write(|w| {
    ///     w.push_str(&candidate)?;
    ///     if !is_valid(&candidate) { return Err(MyErr::Invalid); }
    ///     Ok(())
    /// })?;
    /// ```
    ///
    /// # What `try_write` does *not* do
    ///
    /// Despite the rollback semantics, this is **not** a database
    /// transaction. Specifically:
    ///
    /// - **There is no durability.** `try_write` does not flush or
    ///   fsync; an `Ok` return only means the bytes were accepted by the
    ///   sink. Use [`finish`](Self::finish) plus your own flush/fsync.
    ///
    /// # Errors
    ///
    /// - [`WriteError::Poisoned`] if the writer is already poisoned at
    ///   entry. Like every other op, `try_write` refuses to run on a
    ///   poisoned writer; once poisoned, the document is unrecoverable.
    /// - If `f` returns `Err`, `try_write` rolls back and propagates the
    ///   error. If rollback itself fails (sink truncate I/O error), the
    ///   writer is poisoned and the I/O error is returned via
    ///   `E::from(WriteError)`. The `E: From<WriteError>` bound exists
    ///   for this conversion; in practice, error enums that already
    ///   carry a `WriteError` variant satisfy it for free.
    pub fn try_write<F, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<WriteError>,
    {
        if self.poisoned {
            return Err(E::from(WriteError::Poisoned));
        }

        let cp = self.checkpoint();
        match f(self) {
            Ok(v) => {
                drop(cp);
                Ok(v)
            }
            Err(e) => {
                if let Err(rb) = self.rollback(cp) {
                    return Err(E::from(rb));
                }
                Err(e)
            }
        }
    }

    pub(crate) fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            pos: self.pos,
            padding_written: self.padding_written,
            root_offset: self.root_offset,
            frames: self.frames.clone(),
            poisoned: self.poisoned,
        }
    }

    pub(crate) fn rollback(&mut self, cp: Checkpoint) -> Result<(), WriteError> {
        if let Err(e) = self.sink.rewind_to(cp.pos) {
            // Sink and in-memory state are now inconsistent. Refuse to
            // continue silently.
            self.poisoned = true;
            return Err(WriteError::Io(e));
        }
        self.pos = cp.pos;
        self.padding_written = cp.padding_written;
        self.root_offset = cp.root_offset;
        self.frames = cp.frames;
        self.poisoned = cp.poisoned;
        Ok(())
    }
}
