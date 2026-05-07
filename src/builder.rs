use crate::error::WriteError;
use crate::raw_writer::RawWriter;
use crate::sink::{RewindableSink, Sink};
use crate::writer::{Filled, Writer};

// ---------------------------------------------------------------------
// Root builders
//
// Root builders own the underlying RawWriter outright (instead of
// borrowing it like the nested builders do), because they have to
// transition the safe-API state from `Empty` to `Filled` on close - a
// type-level change that requires consuming `self`.
// ---------------------------------------------------------------------

/// Root-level array builder. Consumes a [`Writer<S, Empty>`] and yields
/// a [`Writer<S, Filled>`] when [`end`](Self::end) is called.
///
/// The `#[must_use]` lint catches the obvious footgun: dropping the
/// builder without calling `.end()` leaves the document incomplete (no
/// trailer ever gets written), so make sure the builder's value is
/// either bound or threaded through to `.end()`.
#[must_use = "the document is unfinished until you call .end() on the root builder"]
pub struct RootArrayBuilder<S: Sink> {
    w: RawWriter<S>,
    closed: bool,
}

impl<S: Sink> RootArrayBuilder<S> {
    pub(crate) fn new(mut w: RawWriter<S>) -> Self {
        w.push_array_frame();
        Self { w, closed: false }
    }

    /// Append `null`.
    pub fn push_null(&mut self) -> Result<(), WriteError> {
        self.w.push_null()
    }
    /// Append a boolean.
    pub fn push_bool(&mut self, v: bool) -> Result<(), WriteError> {
        self.w.push_bool(v)
    }
    /// Append a signed 64-bit integer.
    pub fn push_i64(&mut self, v: i64) -> Result<(), WriteError> {
        self.w.push_i64(v)
    }
    /// Append an unsigned 64-bit integer.
    pub fn push_u64(&mut self, v: u64) -> Result<(), WriteError> {
        self.w.push_u64(v)
    }
    /// Append a 64-bit float. NaN and ±∞ are rejected.
    pub fn push_f64(&mut self, v: f64) -> Result<(), WriteError> {
        self.w.push_f64(v)
    }
    /// Append a UTF-8 string.
    pub fn push_str(&mut self, s: &str) -> Result<(), WriteError> {
        self.w.push_str(s)
    }

    /// See [`Writer::bytes_written`].
    pub fn bytes_written(&self) -> u64 {
        self.w.bytes_written()
    }
    /// See [`Writer::buffered_bytes`].
    pub fn buffered_bytes(&self) -> usize {
        self.w.buffered_bytes()
    }

    /// Open a nested array.
    pub fn start_array<'b>(&'b mut self) -> ArrayBuilder<'b, S> {
        self.w.push_array_frame();
        ArrayBuilder::new(&mut self.w)
    }
    /// Open a nested object.
    pub fn start_object<'b>(&'b mut self) -> ObjectBuilder<'b, S> {
        self.w.push_object_frame();
        ObjectBuilder::new(&mut self.w)
    }

    /// Close the array, propagating any error from the final flush, and
    /// return a [`Writer<S, Filled>`] ready for `finish`.
    pub fn end(mut self) -> Result<Writer<S, Filled>, WriteError> {
        self.closed = true;
        self.w.close_array_frame()?;
        // Take ownership of `self.raw` without running Drop. Replacing
        // with a moved-out `RawWriter` would require it to be `Default`;
        // instead we just `mem::replace` via a `ManuallyDrop` dance.
        let raw = unsafe { std::ptr::read(&self.w) };
        std::mem::forget(self);
        Ok(Writer::from_raw(raw))
    }
}

impl<S: Sink> Drop for RootArrayBuilder<S> {
    fn drop(&mut self) {
        // Best-effort close so internal state is consistent. The writer
        // is owned here, so any error has nowhere to go - the caller
        // already lost access to the document by ignoring the
        // `must_use`. The doc on disk will lack a trailer.
        if !self.closed {
            let _ = self.w.close_array_frame();
        }
    }
}

impl<S: RewindableSink> RootArrayBuilder<S> {
    /// See [`RawWriter::try_write`](crate::raw::RawWriter::try_write).
    pub fn try_write<F, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<WriteError>,
    {
        if self.w.poisoned {
            return Err(E::from(WriteError::Poisoned));
        }
        let cp = self.w.checkpoint();
        match f(self) {
            Ok(v) => {
                drop(cp);
                Ok(v)
            }
            Err(e) => {
                if let Err(rb) = self.w.rollback(cp) {
                    return Err(E::from(rb));
                }
                Err(e)
            }
        }
    }
}

/// Root-level object builder. Consumes a [`Writer<S, Empty>`] and yields
/// a [`Writer<S, Filled>`] when [`end`](Self::end) is called.
///
/// See [`RootArrayBuilder`] for notes on the `must_use` invariant.
#[must_use = "the document is unfinished until you call .end() on the root builder"]
pub struct RootObjectBuilder<S: Sink> {
    w: RawWriter<S>,
    closed: bool,
}

impl<S: Sink> RootObjectBuilder<S> {
    pub(crate) fn new(mut w: RawWriter<S>) -> Self {
        w.push_object_frame();
        Self { w, closed: false }
    }

    /// Insert `null` at `key`.
    pub fn push_null(&mut self, key: &str) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_null()
    }
    /// Insert a boolean at `key`.
    pub fn push_bool(&mut self, key: &str, v: bool) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_bool(v)
    }
    /// Insert a signed 64-bit integer at `key`.
    pub fn push_i64(&mut self, key: &str, v: i64) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_i64(v)
    }
    /// Insert an unsigned 64-bit integer at `key`.
    pub fn push_u64(&mut self, key: &str, v: u64) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_u64(v)
    }
    /// Insert a 64-bit float at `key`. NaN and ±∞ are rejected.
    pub fn push_f64(&mut self, key: &str, v: f64) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_f64(v)
    }
    /// Insert a UTF-8 string at `key`.
    pub fn push_str(&mut self, key: &str, s: &str) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_str(s)
    }

    /// See [`Writer::bytes_written`].
    pub fn bytes_written(&self) -> u64 {
        self.w.bytes_written()
    }
    /// See [`Writer::buffered_bytes`].
    pub fn buffered_bytes(&self) -> usize {
        self.w.buffered_bytes()
    }

    /// Open a nested array under `key`.
    pub fn start_array<'b>(&'b mut self, key: &str) -> Result<ArrayBuilder<'b, S>, WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_array_frame();
        Ok(ArrayBuilder::new(&mut self.w))
    }
    /// Open a nested object under `key`.
    pub fn start_object<'b>(&'b mut self, key: &str) -> Result<ObjectBuilder<'b, S>, WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_object_frame();
        Ok(ObjectBuilder::new(&mut self.w))
    }

    /// Close the object, propagating any error from the final flush, and
    /// return a [`Writer<S, Filled>`] ready for `finish`.
    pub fn end(mut self) -> Result<Writer<S, Filled>, WriteError> {
        self.closed = true;
        self.w.close_object_frame()?;
        let raw = unsafe { std::ptr::read(&self.w) };
        std::mem::forget(self);
        Ok(Writer::from_raw(raw))
    }
}

impl<S: Sink> Drop for RootObjectBuilder<S> {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.w.close_object_frame();
        }
    }
}

impl<S: RewindableSink> RootObjectBuilder<S> {
    /// See [`RawWriter::try_write`](crate::raw::RawWriter::try_write).
    pub fn try_write<F, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<WriteError>,
    {
        if self.w.poisoned {
            return Err(E::from(WriteError::Poisoned));
        }
        let cp = self.w.checkpoint();
        match f(self) {
            Ok(v) => {
                drop(cp);
                Ok(v)
            }
            Err(e) => {
                if let Err(rb) = self.w.rollback(cp) {
                    return Err(E::from(rb));
                }
                Err(e)
            }
        }
    }
}

// ---------------------------------------------------------------------
// Nested builders - borrow `&mut RawWriter`. Unchanged in spirit from
// before the typestate refactor; only `Drop`/`end` semantics matter
// here, not state transitions.
// ---------------------------------------------------------------------

/// Handle for building a nested array. Borrows its parent writer
/// mutably; drops close the array automatically. Use
/// [`ArrayBuilder::end`] for explicit, error-propagating close.
pub struct ArrayBuilder<'a, S: Sink> {
    w: &'a mut RawWriter<S>,
    closed: bool,
}

impl<'a, S: Sink> ArrayBuilder<'a, S> {
    pub(crate) fn new(w: &'a mut RawWriter<S>) -> Self {
        Self { w, closed: false }
    }

    /// Append `null`.
    pub fn push_null(&mut self) -> Result<(), WriteError> {
        self.w.push_null()
    }
    /// Append a boolean.
    pub fn push_bool(&mut self, v: bool) -> Result<(), WriteError> {
        self.w.push_bool(v)
    }
    /// Append a signed 64-bit integer.
    pub fn push_i64(&mut self, v: i64) -> Result<(), WriteError> {
        self.w.push_i64(v)
    }
    /// Append an unsigned 64-bit integer.
    pub fn push_u64(&mut self, v: u64) -> Result<(), WriteError> {
        self.w.push_u64(v)
    }
    /// Append a 64-bit float. NaN and ±∞ are rejected.
    pub fn push_f64(&mut self, v: f64) -> Result<(), WriteError> {
        self.w.push_f64(v)
    }
    /// Append a UTF-8 string.
    pub fn push_str(&mut self, s: &str) -> Result<(), WriteError> {
        self.w.push_str(s)
    }

    /// See [`Writer::bytes_written`].
    pub fn bytes_written(&self) -> u64 {
        self.w.bytes_written()
    }
    /// See [`Writer::buffered_bytes`].
    pub fn buffered_bytes(&self) -> usize {
        self.w.buffered_bytes()
    }

    /// Open a nested array.
    pub fn start_array<'b>(&'b mut self) -> ArrayBuilder<'b, S> {
        self.w.push_array_frame();
        ArrayBuilder::new(&mut *self.w)
    }
    /// Open a nested object.
    pub fn start_object<'b>(&'b mut self) -> ObjectBuilder<'b, S> {
        self.w.push_object_frame();
        ObjectBuilder::new(&mut *self.w)
    }

    /// Close the array, propagating any error from the final flush.
    /// Prefer this over relying on `Drop` whenever you care about errors.
    pub fn end(mut self) -> Result<(), WriteError> {
        self.closed = true;
        self.w.close_array_frame()
    }
}

impl<S: Sink> Drop for ArrayBuilder<'_, S> {
    fn drop(&mut self) {
        if !self.closed && self.w.close_array_frame().is_err() {
            self.w.poisoned = true;
        }
    }
}

impl<S: RewindableSink> ArrayBuilder<'_, S> {
    /// See [`RawWriter::try_write`](crate::raw::RawWriter::try_write).
    pub fn try_write<F, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<WriteError>,
    {
        if self.w.poisoned {
            return Err(E::from(WriteError::Poisoned));
        }
        let cp = self.w.checkpoint();
        match f(self) {
            Ok(v) => {
                drop(cp);
                Ok(v)
            }
            Err(e) => {
                if let Err(rb) = self.w.rollback(cp) {
                    return Err(E::from(rb));
                }
                Err(e)
            }
        }
    }
}

/// Handle for building a nested object. Keys are passed positionally
/// before their value. Drops close the object automatically.
pub struct ObjectBuilder<'a, S: Sink> {
    w: &'a mut RawWriter<S>,
    closed: bool,
}

impl<'a, S: Sink> ObjectBuilder<'a, S> {
    pub(crate) fn new(w: &'a mut RawWriter<S>) -> Self {
        Self { w, closed: false }
    }

    /// Insert `null` at `key`.
    pub fn push_null(&mut self, key: &str) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_null()
    }
    /// Insert a boolean at `key`.
    pub fn push_bool(&mut self, key: &str, v: bool) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_bool(v)
    }
    /// Insert a signed 64-bit integer at `key`.
    pub fn push_i64(&mut self, key: &str, v: i64) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_i64(v)
    }
    /// Insert an unsigned 64-bit integer at `key`.
    pub fn push_u64(&mut self, key: &str, v: u64) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_u64(v)
    }
    /// Insert a 64-bit float at `key`. NaN and ±∞ are rejected.
    pub fn push_f64(&mut self, key: &str, v: f64) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_f64(v)
    }
    /// Insert a UTF-8 string at `key`.
    pub fn push_str(&mut self, key: &str, s: &str) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_str(s)
    }

    /// See [`Writer::bytes_written`].
    pub fn bytes_written(&self) -> u64 {
        self.w.bytes_written()
    }
    /// See [`Writer::buffered_bytes`].
    pub fn buffered_bytes(&self) -> usize {
        self.w.buffered_bytes()
    }

    /// Open a nested array under `key`.
    pub fn start_array<'b>(&'b mut self, key: &str) -> Result<ArrayBuilder<'b, S>, WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_array_frame();
        Ok(ArrayBuilder::new(&mut *self.w))
    }
    /// Open a nested object under `key`.
    pub fn start_object<'b>(&'b mut self, key: &str) -> Result<ObjectBuilder<'b, S>, WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_object_frame();
        Ok(ObjectBuilder::new(&mut *self.w))
    }

    /// Close the object, propagating any error from the final flush.
    /// Prefer this over relying on `Drop` whenever you care about errors.
    pub fn end(mut self) -> Result<(), WriteError> {
        self.closed = true;
        self.w.close_object_frame()
    }
}

impl<S: Sink> Drop for ObjectBuilder<'_, S> {
    fn drop(&mut self) {
        if !self.closed && self.w.close_object_frame().is_err() {
            self.w.poisoned = true;
        }
    }
}

impl<S: RewindableSink> ObjectBuilder<'_, S> {
    /// See [`RawWriter::try_write`](crate::raw::RawWriter::try_write).
    pub fn try_write<F, T, E>(&mut self, f: F) -> Result<T, E>
    where
        F: FnOnce(&mut Self) -> Result<T, E>,
        E: From<WriteError>,
    {
        if self.w.poisoned {
            return Err(E::from(WriteError::Poisoned));
        }
        let cp = self.w.checkpoint();
        match f(self) {
            Ok(v) => {
                drop(cp);
                Ok(v)
            }
            Err(e) => {
                if let Err(rb) = self.w.rollback(cp) {
                    return Err(E::from(rb));
                }
                Err(e)
            }
        }
    }
}
