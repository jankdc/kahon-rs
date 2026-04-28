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

    pub fn push_null(&mut self) -> Result<(), WriteError> {
        self.w.push_null()
    }
    pub fn push_bool(&mut self, v: bool) -> Result<(), WriteError> {
        self.w.push_bool(v)
    }
    pub fn push_i64(&mut self, v: i64) -> Result<(), WriteError> {
        self.w.push_i64(v)
    }
    pub fn push_u64(&mut self, v: u64) -> Result<(), WriteError> {
        self.w.push_u64(v)
    }
    pub fn push_f64(&mut self, v: f64) -> Result<(), WriteError> {
        self.w.push_f64(v)
    }
    pub fn push_str(&mut self, s: &str) -> Result<(), WriteError> {
        self.w.push_str(s)
    }

    pub fn bytes_written(&self) -> u64 {
        self.w.bytes_written()
    }

    pub fn buffered_bytes(&self) -> usize {
        self.w.buffered_bytes()
    }

    pub fn start_array<'b>(&'b mut self) -> ArrayBuilder<'b, S> {
        self.w.push_array_frame();
        ArrayBuilder::new(&mut *self.w)
    }
    pub fn start_object<'b>(&'b mut self) -> ObjectBuilder<'b, S> {
        self.w.push_object_frame();
        ObjectBuilder::new(&mut *self.w)
    }

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

    pub fn push_null(&mut self, key: &str) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_null()
    }
    pub fn push_bool(&mut self, key: &str, v: bool) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_bool(v)
    }
    pub fn push_i64(&mut self, key: &str, v: i64) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_i64(v)
    }
    pub fn push_u64(&mut self, key: &str, v: u64) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_u64(v)
    }
    pub fn push_f64(&mut self, key: &str, v: f64) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_f64(v)
    }
    pub fn push_str(&mut self, key: &str, s: &str) -> Result<(), WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_str(s)
    }

    pub fn bytes_written(&self) -> u64 {
        self.w.bytes_written()
    }

    pub fn buffered_bytes(&self) -> usize {
        self.w.buffered_bytes()
    }

    pub fn start_array<'b>(&'b mut self, key: &str) -> Result<ArrayBuilder<'b, S>, WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_array_frame();
        Ok(ArrayBuilder::new(&mut *self.w))
    }
    pub fn start_object<'b>(&'b mut self, key: &str) -> Result<ObjectBuilder<'b, S>, WriteError> {
        self.w.set_pending_key(key)?;
        self.w.push_object_frame();
        Ok(ObjectBuilder::new(&mut *self.w))
    }

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
