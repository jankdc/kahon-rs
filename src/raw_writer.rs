//! Flat, runtime-checked writer surface for advanced integrations.
//!
//! Mismatched frames, forgotten closes, and `push_key` outside an
//! object are runtime errors here. The builder API rules them out at
//! compile time; the flat API trades that guarantee for composability.
//!
//! # Conversion
//!
//! [`Writer::into_raw`] consumes a `Writer` and returns a `RawWriter`
//! over the same state. [`RawWriter::into_safe`] consumes the
//! `RawWriter` back into a `Writer`. Open frames carry across
//! conversion, so an adapter can drop into the builder API for a
//! sub-tree and return without unwinding.
//!
//! Because conversion is by value, you cannot hold both surfaces over
//! the same writer at once. Code that takes `&mut Writer` cannot
//! accidentally call flat methods, and code that takes
//! `&mut RawWriter` cannot accidentally open a builder.
//!
//! [`Writer`]: crate::Writer
//! [`ArrayBuilder`]: crate::ArrayBuilder
//! [`ObjectBuilder`]: crate::ObjectBuilder
//! [`Writer::into_raw`]: crate::Writer::into_raw

use crate::checkpoint::TrailerSnapshot;
use crate::error::WriteError;
use crate::frame::Frame;
use crate::sink::{RewindableSink, Sink};
use crate::writer::Writer;

/// Save-state for [`RawWriter`] backtracking.
///
/// Maps directly onto the closure-shaped [`Writer::try_write`] used by
/// the safe API.
///
/// [`Writer::try_write`]: crate::Writer::try_write
pub struct Checkpoint(crate::checkpoint::Checkpoint);

pub struct RawWriter<S: Sink> {
    inner: Writer<S>,
}

impl<S: Sink> RawWriter<S> {
    /// Promote a [`Writer`] to its flat form. The builder API becomes
    /// unavailable until [`into_safe`](Self::into_safe).
    pub fn from_writer(w: Writer<S>) -> Self {
        Self { inner: w }
    }

    /// Demote back to the safe builder API. Open frames are preserved;
    /// the next operation must close them in LIFO order or
    /// [`Writer::finish`] will reject with [`WriteError::Poisoned`] /
    /// equivalent state.
    pub fn into_safe(self) -> Writer<S> {
        self.inner
    }

    // ------------------------------------------------------------------
    // Scalars - delegate to Writer; semantics identical.
    // ------------------------------------------------------------------

    /// See [`Writer::push_null`](crate::Writer::push_null).
    pub fn push_null(&mut self) -> Result<(), WriteError> {
        self.inner.push_null()
    }

    /// See [`Writer::push_bool`](crate::Writer::push_bool).
    pub fn push_bool(&mut self, v: bool) -> Result<(), WriteError> {
        self.inner.push_bool(v)
    }

    /// See [`Writer::push_i64`](crate::Writer::push_i64).
    pub fn push_i64(&mut self, v: i64) -> Result<(), WriteError> {
        self.inner.push_i64(v)
    }

    /// See [`Writer::push_u64`](crate::Writer::push_u64).
    pub fn push_u64(&mut self, v: u64) -> Result<(), WriteError> {
        self.inner.push_u64(v)
    }

    /// See [`Writer::push_f64`](crate::Writer::push_f64).
    pub fn push_f64(&mut self, v: f64) -> Result<(), WriteError> {
        self.inner.push_f64(v)
    }

    /// See [`Writer::push_str`](crate::Writer::push_str).
    pub fn push_str(&mut self, s: &str) -> Result<(), WriteError> {
        self.inner.push_str(s)
    }

    // ------------------------------------------------------------------
    // Frames - the new flat surface.
    // ------------------------------------------------------------------

    /// Open an array frame. Pair with [`end_array`](Self::end_array).
    ///
    /// Returns [`WriteError::Poisoned`] if the writer is poisoned.
    pub fn begin_array(&mut self) -> Result<(), WriteError> {
        if self.inner.poisoned {
            return Err(WriteError::Poisoned);
        }
        self.inner.push_array_frame();
        Ok(())
    }

    /// Close the open array frame.
    ///
    /// Errors:
    /// - [`WriteError::Poisoned`] if the writer is poisoned.
    /// - [`WriteError::FrameMismatch`] if the top frame is not an
    ///   array (or no frame is open).
    pub fn end_array(&mut self) -> Result<(), WriteError> {
        if self.inner.poisoned {
            return Err(WriteError::Poisoned);
        }
        match self.inner.frames.last() {
            Some(Frame::Array(_)) => self.inner.close_array_frame(),
            _ => Err(WriteError::FrameMismatch),
        }
    }

    /// Open an object frame. Pair with [`end_object`](Self::end_object).
    /// Use [`push_key`](Self::push_key) before each value.
    ///
    /// Returns [`WriteError::Poisoned`] if the writer is poisoned.
    pub fn begin_object(&mut self) -> Result<(), WriteError> {
        if self.inner.poisoned {
            return Err(WriteError::Poisoned);
        }
        self.inner.push_object_frame();
        Ok(())
    }

    /// Close the open object frame.
    ///
    /// Errors:
    /// - [`WriteError::Poisoned`] if the writer is poisoned.
    /// - [`WriteError::FrameMismatch`] if the top frame is not an
    ///   object (or no frame is open).
    pub fn end_object(&mut self) -> Result<(), WriteError> {
        if self.inner.poisoned {
            return Err(WriteError::Poisoned);
        }
        match self.inner.frames.last() {
            Some(Frame::Object(_)) => self.inner.close_object_frame(),
            _ => Err(WriteError::FrameMismatch),
        }
    }

    /// Set the key for the next value pushed into the open object.
    ///
    /// Errors:
    /// - [`WriteError::Poisoned`] if the writer is poisoned.
    /// - [`WriteError::KeyOutsideObject`] if the top frame is not an
    ///   object.
    pub fn push_key(&mut self, key: &str) -> Result<(), WriteError> {
        if self.inner.poisoned {
            return Err(WriteError::Poisoned);
        }
        match self.inner.frames.last() {
            Some(Frame::Object(_)) => self.inner.set_pending_key(key),
            _ => Err(WriteError::KeyOutsideObject),
        }
    }

    // ------------------------------------------------------------------
    // Introspection - delegates.
    // ------------------------------------------------------------------

    /// See [`Writer::bytes_written`](crate::Writer::bytes_written).
    pub fn bytes_written(&self) -> u64 {
        self.inner.bytes_written()
    }

    /// See [`Writer::buffered_bytes`](crate::Writer::buffered_bytes).
    pub fn buffered_bytes(&self) -> usize {
        self.inner.buffered_bytes()
    }

    /// See [`Writer::padding_bytes_written`](crate::Writer::padding_bytes_written).
    pub fn padding_bytes_written(&self) -> u64 {
        self.inner.padding_bytes_written()
    }

    /// See [`Writer::snapshot_trailer`](crate::Writer::snapshot_trailer).
    pub fn snapshot_trailer(&self) -> Result<TrailerSnapshot, WriteError> {
        self.inner.snapshot_trailer()
    }

    /// See [`Writer::finish`](crate::Writer::finish). Errors if any
    /// frame is still open.
    pub fn finish(self) -> Result<S, WriteError> {
        self.inner.finish()
    }
}

impl<S: RewindableSink> RawWriter<S> {
    /// Capture a save-state for speculative writes. Cheap;
    /// non-mutating. Pair with [`rollback`](Self::rollback) to revert,
    /// or drop the [`Checkpoint`] to commit.
    pub fn checkpoint(&self) -> Checkpoint {
        Checkpoint(self.inner.checkpoint())
    }

    /// Restore the writer to the captured save-state. Truncates the
    /// sink, restores the frame stack, and clears the poison flag if
    /// it was clean at capture time.
    ///
    /// Errors with [`WriteError::Io`] if the sink truncate fails; the
    /// writer is poisoned in that case.
    pub fn rollback(&mut self, cp: Checkpoint) -> Result<(), WriteError> {
        self.inner.rollback(cp.0)
    }
}
