//! Speculative writes and snapshot trailer.

use crate::error::WriteError;
use crate::frame::Frame;
use crate::raw_writer::RawWriter;
use crate::sink::RewindableSink;
use crate::Writer;

/// Save-state for [`RawWriter::checkpoint`] / [`RawWriter::rollback`].
///
/// Dropping a `Checkpoint` without rolling back commits the speculative
/// writes.
pub struct Checkpoint {
    pos: u64,
    padding_written: u64,
    root_offset: Option<u64>,
    frames: Vec<Frame>,
    poisoned: bool,
    pending_exts: Vec<(u64, usize)>,
}

impl<S: RewindableSink> RawWriter<S> {
    /// Capture a save-state for speculative writes. Pair with
    /// [`rollback`](Self::rollback) to revert; drop the [`Checkpoint`]
    /// to commit.
    pub fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            pos: self.pos,
            padding_written: self.padding_written,
            root_offset: self.root_offset,
            frames: self.frames.clone(),
            poisoned: self.poisoned,
            pending_exts: self.pending_exts.clone(),
        }
    }

    /// Restore the writer to a captured save-state.
    ///
    /// Returns [`WriteError::Io`] if the sink truncate fails; the
    /// writer is poisoned in that case.
    pub fn rollback(&mut self, cp: Checkpoint) -> Result<(), WriteError> {
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
        self.pending_exts = cp.pending_exts;
        Ok(())
    }
}

impl<S: RewindableSink> Writer<S> {
    /// Run `f` as a speculative write: on `Ok`, the writes commit; on
    /// `Err`, every byte and frame opened by `f` is rolled back and
    /// the error is propagated.
    ///
    /// Note: this is not a transaction - there is no `fsync`, and the
    /// rollback only undoes in-memory state and truncates the sink.
    ///
    /// ```ignore
    /// writer.try_write(|writer| {
    ///     writer.push_str(&candidate)?;
    ///     if !is_valid(&candidate) { return Err(MyErr::Invalid); }
    ///     Ok(())
    /// })?;
    /// ```
    ///
    /// If rollback itself fails (sink truncate I/O error), the writer
    /// is poisoned and the I/O error is returned via `E::from(WriteError)`.
    pub fn try_write<F, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<WriteError>,
    {
        if self.raw.poisoned {
            return Err(E::from(WriteError::Poisoned));
        }
        let cp = self.raw.checkpoint();
        match f(self) {
            Ok(v) => {
                drop(cp);
                Ok(v)
            }
            Err(e) => {
                if let Err(rb) = self.raw.rollback(cp) {
                    return Err(E::from(rb));
                }
                Err(e)
            }
        }
    }
}
