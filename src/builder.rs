use crate::error::WriteError;
use crate::raw_writer::RawWriter;
use crate::sink::{RewindableSink, Sink};
use crate::writer::{Filled, Writer};

/// Root-level array builder. Returned by
/// [`Writer::start_array`](crate::Writer::start_array). Call
/// [`.end()`](Self::end) to recover the [`Writer`] for `finish` -
/// dropping without `.end()` leaves the document with no trailer.
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
    /// Append a 64-bit float. Returns [`WriteError::NaNOrInfinity`] for
    /// NaN or ±∞.
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

    /// Close the array and return a [`Writer`] ready for
    /// [`finish`](Writer::finish).
    pub fn end(mut self) -> Result<Writer<S, Filled>, WriteError> {
        self.closed = true;
        self.w.close_array_frame()?;
        let raw = unsafe { std::ptr::read(&self.w) };
        std::mem::forget(self);
        Ok(Writer::from_raw(raw))
    }
}

impl<S: Sink> Drop for RootArrayBuilder<S> {
    fn drop(&mut self) {
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

/// Root-level object builder. Returned by
/// [`Writer::start_object`](crate::Writer::start_object). Call
/// [`.end()`](Self::end) to recover the [`Writer`] for `finish` -
/// dropping without `.end()` leaves the document with no trailer.
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
    /// Insert a 64-bit float at `key`. Returns
    /// [`WriteError::NaNOrInfinity`] for NaN or ±∞.
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

    /// Close the object and return a [`Writer`] ready for
    /// [`finish`](Writer::finish).
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

/// Handle for a nested array. Closes on drop; call
/// [`.end()?`](Self::end) instead to surface close errors as a
/// `Result`.
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
    /// Append a 64-bit float. Returns [`WriteError::NaNOrInfinity`] for
    /// NaN or ±∞.
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

    /// Explicitly close the array, surfacing any close error.
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

/// Handle for a nested object. Keys are passed positionally before
/// their value. Closes on drop; call [`.end()?`](Self::end) to surface
/// close errors as a `Result`.
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
    /// Insert a 64-bit float at `key`. Returns
    /// [`WriteError::NaNOrInfinity`] for NaN or ±∞.
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

    /// Explicitly close the object, surfacing any close error.
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
