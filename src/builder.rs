use crate::error::WriteError;
use crate::sink::Sink;
use crate::writer::Writer;

/// Handle for building an array. Borrows its parent `Writer` mutably; drops
/// close the array automatically. Use [`ArrayBuilder::end`] for explicit,
/// error-propagating close.
pub struct ArrayBuilder<'a, S: Sink> {
    w: &'a mut Writer<S>,
    closed: bool,
}

impl<'a, S: Sink> ArrayBuilder<'a, S> {
    pub(crate) fn new(w: &'a mut Writer<S>) -> Self {
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

/// Handle for building an object. Keys are passed positionally before their
/// value. Drops close the object automatically.
pub struct ObjectBuilder<'a, S: Sink> {
    w: &'a mut Writer<S>,
    closed: bool,
}

impl<'a, S: Sink> ObjectBuilder<'a, S> {
    pub(crate) fn new(w: &'a mut Writer<S>) -> Self {
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
