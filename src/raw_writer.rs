//! Flat, writer surface for advanced integrations.
//!
//! Use this when the typed builder API in [`Writer`](crate::Writer) is
//! awkward.
//! Mismatched frames and `push_key` outside an object surface as
//! runtime errors instead of compile errors.

use crate::align::write_padding;
use crate::bplus::ArrayBPlusBuilder;
use crate::config::{PageAlignment, WriterOptions};
use crate::encode::{
    write_ext, write_f64, write_false, write_integer, write_null, write_string, write_true,
};
use crate::error::WriteError;
use crate::frame::{close_frame_with_prefix, Frame, ObjectState};
use crate::sink::{Sink, WriteCtx};
use crate::types::{FLAGS, MAGIC, VERSION};

/// Flat, runtime-checked streaming writer over a [`Sink`].
///
/// Most users want [`Writer`](crate::Writer) and its builders. Reach
/// for `RawWriter` when the typed API is awkward: FFI, async parsers,
/// storage adapters.
pub struct RawWriter<S: Sink> {
    pub(crate) sink: S,
    pub(crate) pos: u64,
    scratch: Vec<u8>,
    pub(crate) padding_written: u64,
    pub(crate) frames: Vec<Frame>,
    pub(crate) opts: WriterOptions,
    pub(crate) root_offset: Option<u64>,
    pub(crate) poisoned: bool,
    pub(crate) pending_exts: Vec<(Vec<u8>, usize)>,
}

impl<S: Sink> RawWriter<S> {
    /// Create a raw writer with default options.
    ///
    /// I/O errors from the initial header write are deferred and
    /// surface on the first push or `finish`.
    pub fn new(sink: S) -> Self {
        // Default policy is statically known-valid; unwrap is safe.
        Self::with_options(sink, WriterOptions::default())
            .expect("default WriterOptions must validate")
    }

    /// Create a raw writer with caller-supplied [`WriterOptions`].
    ///
    /// Returns [`WriteError::InvalidOption`] if the options fail
    /// validation.
    pub fn with_options(sink: S, opts: WriterOptions) -> Result<Self, WriteError> {
        opts.policy.validate()?;
        let mut w = Self {
            sink,
            pos: 0,
            scratch: Vec::with_capacity(64),
            padding_written: 0,
            frames: Vec::new(),
            opts,
            root_offset: None,
            poisoned: false,
            pending_exts: Vec::new(),
        };
        // If the header write fails, defer the error: poison now and surface
        // it on the first push/finish.
        if w.write_header().is_err() {
            w.poisoned = true;
        }
        Ok(w)
    }

    /// Total bytes emitted to the sink so far.
    pub fn bytes_written(&self) -> u64 {
        self.pos
    }

    /// Approximate live in-memory footprint of the writer's working
    /// buffers. Useful for profiling memory across configurations.
    pub fn buffered_bytes(&self) -> usize {
        let mut total = self.scratch.capacity();
        total += self.frames.capacity() * std::mem::size_of::<Frame>();
        for f in &self.frames {
            total += match f {
                Frame::Array(b) => b.buffered_bytes(),
                Frame::Object(o) => {
                    let mut t =
                        o.current_run.capacity() * std::mem::size_of::<(Vec<u8>, u64, u64)>();
                    t += o.current_run_key_bytes;
                    t += o.runs_cascade.buffered_bytes();
                    if let Some((kb, _)) = &o.pending_key {
                        t += kb.capacity();
                    }
                    t
                }
            };
        }
        total += self.pending_exts.capacity() * std::mem::size_of::<(Vec<u8>, usize)>();
        for (bytes, _) in &self.pending_exts {
            total += bytes.capacity();
        }
        total
    }

    /// Bytes of unreferenced padding emitted under
    /// [`PageAlignment::Aligned`] (zero otherwise).
    pub fn padding_bytes_written(&self) -> u64 {
        self.padding_written
    }

    /// Push a `null` (root, array element, or object value).
    pub fn push_null(&mut self) -> Result<(), WriteError> {
        self.check_ready()?;
        let off = self.emit_scalar(|b| {
            write_null(b);
            Ok(())
        })?;
        self.register_value(off)
    }

    /// Push a boolean.
    pub fn push_bool(&mut self, v: bool) -> Result<(), WriteError> {
        self.check_ready()?;
        let off = self.emit_scalar(|b| {
            if v {
                write_true(b);
            } else {
                write_false(b);
            }
            Ok(())
        })?;
        self.register_value(off)
    }

    /// Push a signed 64-bit integer.
    pub fn push_i64(&mut self, v: i64) -> Result<(), WriteError> {
        self.check_ready()?;
        let off = self.emit_scalar(|b| write_integer(b, v as i128))?;
        self.register_value(off)
    }

    /// Push an unsigned 64-bit integer.
    pub fn push_u64(&mut self, v: u64) -> Result<(), WriteError> {
        self.check_ready()?;
        let off = self.emit_scalar(|b| write_integer(b, v as i128))?;
        self.register_value(off)
    }

    /// Push a 64-bit float.
    ///
    /// Returns [`WriteError::NaNOrInfinity`] for NaN or ±∞.
    pub fn push_f64(&mut self, v: f64) -> Result<(), WriteError> {
        self.check_ready()?;
        let off = self.emit_scalar(|b| write_f64(b, v))?;
        self.register_value(off)
    }

    /// Push a UTF-8 string.
    pub fn push_str(&mut self, s: &str) -> Result<(), WriteError> {
        self.check_ready()?;
        let off = self.emit_scalar(|b| {
            write_string(b, s);
            Ok(())
        })?;
        self.register_value(off)
    }

    /// Open an extension wrapper at the current value position. The
    /// next value emitted - scalar, string, or container - becomes the
    /// extension's payload and the parent slot points at the extension
    /// tag byte rather than the payload's own offset.
    ///
    /// Extensions are opaque to the format: `ext_id` is meaningful only
    /// to the consumer, and any structured per-extension information
    /// must be carried inside the payload value (e.g. an object or
    /// array). The writer assigns no semantics to `ext_id`.
    ///
    /// Extensions nest: calling `push_extension` again before the
    /// payload writes a second extension header whose tag becomes the
    /// outer extension's payload, and so on. The outermost extension's
    /// offset wins for the parent slot.
    ///
    /// No-payload cases encode `Null` per spec; call [`push_null`](Self::push_null)
    /// after `push_extension` to produce that shape.
    ///
    /// Returns [`WriteError::MultipleRootValues`] at root level if a
    /// root value was already pushed, [`WriteError::KeyOutsideObject`]
    /// inside an object without a pending key, and
    /// [`WriteError::ExtensionWithoutPayload`] later if no payload
    /// follows before the enclosing frame closes.
    pub fn push_extension(&mut self, ext_id: u64) -> Result<(), WriteError> {
        self.check_ready()?;
        let depth = self.frames.len();
        let nesting_at_depth: bool = self.pending_exts.last().is_some_and(|(_, d)| *d == depth);
        match self.frames.last() {
            None => {
                if self.root_offset.is_some() && !nesting_at_depth {
                    return Err(WriteError::MultipleRootValues);
                }
            }
            Some(Frame::Object(o)) => {
                if o.pending_key.is_none() && !nesting_at_depth {
                    return Err(WriteError::KeyOutsideObject);
                }
            }
            Some(Frame::Array(_)) => {}
        }
        if nesting_at_depth {
            let (bytes, _) = self.pending_exts.last_mut().unwrap();
            write_ext(bytes, ext_id);
        } else {
            let mut bytes = Vec::with_capacity(1);
            write_ext(&mut bytes, ext_id);
            self.pending_exts.push((bytes, depth));
        }
        Ok(())
    }

    /// Open an array frame. Pair with [`end_array`](Self::end_array).
    pub fn begin_array(&mut self) -> Result<(), WriteError> {
        if self.poisoned {
            return Err(WriteError::Poisoned);
        }
        if self.frames.is_empty() && self.root_offset.is_some() {
            return Err(WriteError::MultipleRootValues);
        }
        self.push_array_frame();
        Ok(())
    }

    /// Close the open array frame.
    ///
    /// Returns [`WriteError::FrameMismatch`] if the top frame is not an
    /// array.
    pub fn end_array(&mut self) -> Result<(), WriteError> {
        if self.poisoned {
            return Err(WriteError::Poisoned);
        }
        match self.frames.last() {
            Some(Frame::Array(_)) => self.close_array_frame(),
            _ => Err(WriteError::FrameMismatch),
        }
    }

    /// Open an object frame. Pair with [`end_object`](Self::end_object);
    /// call [`push_key`](Self::push_key) before each value.
    pub fn begin_object(&mut self) -> Result<(), WriteError> {
        if self.poisoned {
            return Err(WriteError::Poisoned);
        }
        if self.frames.is_empty() && self.root_offset.is_some() {
            return Err(WriteError::MultipleRootValues);
        }
        self.push_object_frame();
        Ok(())
    }

    /// Close the open object frame.
    ///
    /// Returns [`WriteError::FrameMismatch`] if the top frame is not an
    /// object.
    pub fn end_object(&mut self) -> Result<(), WriteError> {
        if self.poisoned {
            return Err(WriteError::Poisoned);
        }
        match self.frames.last() {
            Some(Frame::Object(_)) => self.close_object_frame(),
            _ => Err(WriteError::FrameMismatch),
        }
    }

    /// Set the key for the next value pushed into the open object.
    ///
    /// Returns [`WriteError::KeyOutsideObject`] if the top frame is not
    /// an object.
    pub fn push_key(&mut self, key: &str) -> Result<(), WriteError> {
        if self.poisoned {
            return Err(WriteError::Poisoned);
        }
        match self.frames.last() {
            Some(Frame::Object(_)) => self.set_pending_key(key),
            _ => Err(WriteError::KeyOutsideObject),
        }
    }

    /// Finalize the document and return the underlying sink.
    ///
    /// Returns [`WriteError::EmptyDocument`] if no root was pushed.
    /// Errors if any container frames are still open.
    pub fn finish(mut self) -> Result<S, WriteError> {
        if self.poisoned {
            return Err(WriteError::Poisoned);
        }
        if !self.frames.is_empty() {
            self.poisoned = true;
            return Err(WriteError::Poisoned);
        }
        if !self.pending_exts.is_empty() {
            self.poisoned = true;
            return Err(WriteError::ExtensionWithoutPayload);
        }
        let root = self.root_offset.ok_or(WriteError::EmptyDocument)?;

        // Pad so the 12-byte trailer ends on a page boundary.
        if let PageAlignment::Aligned { page_size } = self.opts.policy.align {
            let ps = page_size as u64;
            let target_mod = ps - 12;
            let cur_mod = self.pos % ps;
            let pad = if cur_mod <= target_mod {
                target_mod - cur_mod
            } else {
                ps - cur_mod + target_mod
            };
            if pad > 0 {
                let mut ctx = WriteCtx {
                    sink: &mut self.sink,
                    pos: &mut self.pos,
                    scratch: &mut self.scratch,
                    padding_written: &mut self.padding_written,
                };
                write_padding(&mut ctx, pad as usize)?;
            }
        }

        let mut trailer = [0u8; 12];
        trailer[..8].copy_from_slice(&root.to_le_bytes());
        trailer[8..].copy_from_slice(&MAGIC);
        self.sink.write_all(&trailer)?;
        self.pos += trailer.len() as u64;
        Ok(self.sink)
    }

    // ------------------------------------------------------------------
    // Frame primitives - shared with the builder API.
    // ------------------------------------------------------------------

    pub(crate) fn push_array_frame(&mut self) {
        self.frames.push(Frame::Array(ArrayBPlusBuilder::new()));
    }

    pub(crate) fn push_object_frame(&mut self) {
        self.frames.push(Frame::Object(ObjectState::default()));
    }

    pub(crate) fn set_pending_key(&mut self, key: &str) -> Result<(), WriteError> {
        let depth = self.frames.len();
        if self.pending_exts.last().is_some_and(|(_, d)| *d == depth) {
            return Err(WriteError::ExtensionWithoutPayload);
        }
        let off = self.emit_scalar(|b| {
            write_string(b, key);
            Ok(())
        })?;
        match self.frames.last_mut() {
            Some(Frame::Object(o)) => {
                o.set_pending_key(key.as_bytes().to_vec(), off);
                Ok(())
            }
            _ => unreachable!(
                "set_pending_key is pub(crate) and only called from ObjectBuilder \
                 with an Object frame on top"
            ),
        }
    }

    pub(crate) fn close_array_frame(&mut self) -> Result<(), WriteError> {
        if self.poisoned {
            return Err(WriteError::Poisoned);
        }
        let inner_depth = self.frames.len();
        if self
            .pending_exts
            .last()
            .is_some_and(|(_, d)| *d == inner_depth)
        {
            self.poisoned = true;
            return Err(WriteError::ExtensionWithoutPayload);
        }
        let Some(frame @ Frame::Array(_)) = self.frames.pop() else {
            unreachable!("top frame is not an array")
        };
        let slot_depth = self.frames.len();
        let prefix = if self
            .pending_exts
            .last()
            .is_some_and(|(_, d)| *d == slot_depth)
        {
            Some(self.pending_exts.pop().unwrap().0)
        } else {
            None
        };
        let mut ctx = WriteCtx {
            sink: &mut self.sink,
            pos: &mut self.pos,
            scratch: &mut self.scratch,
            padding_written: &mut self.padding_written,
        };
        let slot_off =
            close_frame_with_prefix(frame, &self.opts.policy, &mut ctx, prefix.as_deref())?;
        self.register_value(slot_off)
    }

    pub(crate) fn close_object_frame(&mut self) -> Result<(), WriteError> {
        if self.poisoned {
            return Err(WriteError::Poisoned);
        }
        let inner_depth = self.frames.len();
        if self
            .pending_exts
            .last()
            .is_some_and(|(_, d)| *d == inner_depth)
        {
            self.poisoned = true;
            return Err(WriteError::ExtensionWithoutPayload);
        }
        let Some(frame @ Frame::Object(_)) = self.frames.pop() else {
            unreachable!("top frame is not an object")
        };
        let slot_depth = self.frames.len();
        let prefix = if self
            .pending_exts
            .last()
            .is_some_and(|(_, d)| *d == slot_depth)
        {
            Some(self.pending_exts.pop().unwrap().0)
        } else {
            None
        };
        let mut ctx = WriteCtx {
            sink: &mut self.sink,
            pos: &mut self.pos,
            scratch: &mut self.scratch,
            padding_written: &mut self.padding_written,
        };
        let slot_off =
            close_frame_with_prefix(frame, &self.opts.policy, &mut ctx, prefix.as_deref())?;
        self.register_value(slot_off)
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    fn write_header(&mut self) -> Result<(), WriteError> {
        let header = [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3], VERSION, FLAGS];
        self.sink.write_all(&header)?;
        self.pos += header.len() as u64;
        Ok(())
    }

    fn check_ready(&self) -> Result<(), WriteError> {
        if self.poisoned {
            return Err(WriteError::Poisoned);
        }
        Ok(())
    }

    /// Serialize a scalar into `scratch` and emit it via callback.
    /// Flushes any pending extension header bytes immediately before the
    /// scalar so the payload tag lands at `ext_off + 1 + E` (spec §9).
    /// Returns the slot offset (the outermost ext tag if one was flushed,
    /// else the scalar's own tag byte).
    fn emit_scalar(
        &mut self,
        build: impl FnOnce(&mut Vec<u8>) -> Result<(), WriteError>,
    ) -> Result<u64, WriteError> {
        let depth = self.frames.len();
        let ext_off = if self.pending_exts.last().is_some_and(|(_, d)| *d == depth) {
            let (ext_bytes, _) = self.pending_exts.pop().unwrap();
            let off = self.pos;
            self.sink.write_all(&ext_bytes)?;
            self.pos += ext_bytes.len() as u64;
            Some(off)
        } else {
            None
        };
        self.scratch.clear();
        build(&mut self.scratch)?;
        let scalar_off = self.pos;
        self.sink.write_all(&self.scratch)?;
        self.pos += self.scratch.len() as u64;
        Ok(ext_off.unwrap_or(scalar_off))
    }

    /// Route a completed value's offset to the current context: the root slot,
    /// an open array frame, or an open object frame awaiting a key.
    fn register_value(&mut self, off: u64) -> Result<(), WriteError> {
        let run_buffer = self.opts.object_sort_window;
        let policy = &self.opts.policy;
        let mut ctx = WriteCtx {
            sink: &mut self.sink,
            pos: &mut self.pos,
            scratch: &mut self.scratch,
            padding_written: &mut self.padding_written,
        };
        match self.frames.last_mut() {
            None => {
                if self.root_offset.is_some() {
                    return Err(WriteError::MultipleRootValues);
                }
                self.root_offset = Some(off);
                Ok(())
            }
            Some(Frame::Array(a)) => a.push(off, policy, &mut ctx),
            Some(Frame::Object(o)) => o.accept_value(off, run_buffer, policy, &mut ctx),
        }
    }
}
