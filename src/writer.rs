use crate::align::write_padding;
use crate::bplus::{ArrayBPlusBuilder, ObjectBPlusBuilder, ObjectCascade, ObjectLeafItem};
use crate::encode::{write_f64, write_false, write_integer, write_null, write_string, write_true};
use crate::error::WriteError;
use crate::sink::{Sink, WriteCtx};
use crate::types::{EMPTY_ARRAY, EMPTY_OBJECT, FLAGS, MAGIC, VERSION};

/// How the writer decides when to close a B+tree node and flush it to disk.
#[derive(Clone, Debug)]
pub enum NodeSizing {
    /// Fixed entry-count cap. Predictable, smallest files. Each leaf or
    /// internal node closes once it accumulates `n` entries.
    Fanout(usize),

    /// Target on-disk byte budget per node. The writer derives an
    /// effective fanout per flush from the offset width currently in
    /// use, sizing nodes to fill (but not exceed) the target. Recommended
    /// value for disk-resident files: 4096 (one OS page). See spec §13.3.
    ///
    /// If a single entry would exceed the target, the writer waits for a 
    /// second entry rather than emitting an `m=1` node.
    TargetBytes(usize),
}

/// Whether to pad the body for page-cache friendliness on disk.
///
/// Padding bytes are `Null` tags (`0x00`) that no offset references; they
/// are invisible to a reader that traverses the document by following
/// offsets from the root.
#[derive(Clone, Debug)]
pub enum PageAlignment {
    /// No padding. Tightest file size; preferred for in-memory or
    /// network use where cache locality is not a concern.
    None,

    /// Pad so that:
    ///   1. No container node ≤ `page_size` straddles a page boundary.
    ///   2. The 12-byte trailer lands in the file's last `page_size`-aligned
    ///      page, i.e. `file_size % page_size == 0`.
    Aligned { page_size: usize },
}

/// Bundle of layout knobs threaded through B+tree construction.
///
/// Combine via [`BuildPolicy::compact`] (in-memory friendly) or
/// [`BuildPolicy::disk_aligned`] (file-backed), or build directly from
/// [`NodeSizing`] and [`PageAlignment`] for fine control.
#[derive(Clone, Debug)]
pub struct BuildPolicy {
    /// How node fanout is decided when flushing.
    pub sizing: NodeSizing,
    /// Whether to pad container layout for page-cache friendliness.
    pub align: PageAlignment,
}

impl BuildPolicy {
    /// Minimum sane `TargetBytes`. Below 64, internal nodes can't fit two
    /// entries at W=8 with header overhead, which would force violations
    /// of the `m >= 2` floor.
    pub const MIN_TARGET_BYTES: usize = 64;

    /// Tight, predictable layout: fixed fanout, no padding. Best for
    /// fixture tests and in-memory pipelines.
    pub fn compact(fanout: usize) -> Self {
        Self {
            sizing: NodeSizing::Fanout(fanout),
            align: PageAlignment::None,
        }
    }

    /// Disk-tuned layout: each node targets one page, trailer is
    /// page-aligned. Suitable default for files that will be `pread`-ed
    /// or memory-mapped.
    pub fn disk_aligned(page_size: usize) -> Self {
        Self {
            sizing: NodeSizing::TargetBytes(page_size),
            align: PageAlignment::Aligned { page_size },
        }
    }

    pub(crate) fn validate(&self) -> Result<(), WriteError> {
        match self.sizing {
            NodeSizing::Fanout(f) if f < 2 => {
                return Err(WriteError::InvalidOption(
                    "fanout must be >= 2 (spec §8 invariant 3)",
                ));
            }
            NodeSizing::TargetBytes(t) if t < Self::MIN_TARGET_BYTES => {
                return Err(WriteError::InvalidOption("target_node_bytes must be >= 64"));
            }
            _ => {}
        }
        if let PageAlignment::Aligned { page_size } = self.align {
            if !page_size.is_power_of_two() || page_size < 64 {
                return Err(WriteError::InvalidOption(
                    "page_size must be a power of two and >= 64",
                ));
            }
        }
        Ok(())
    }
}

impl Default for BuildPolicy {
    fn default() -> Self {
        // In-memory friendly out of the box: fixed fanout, no padding,
        // tightest output. Opt into `disk_aligned(page_size)` for files
        // that will be `pread`-ed or memory-mapped.
        Self::compact(128)
    }
}

/// Top-level writer configuration.
///
/// Combine with [`Writer::with_options`] to override the defaults.
/// `WriterOptions::default()` gives an in-memory–friendly setup
/// (fixed fanout, no padding, tightest output).
#[derive(Clone, Debug)]
pub struct WriterOptions {
    /// Max entries buffered (and sorted) per object run before flushing.
    /// Larger values give modestly faster keyed lookups (fewer runs to walk
    /// at internal nodes) at proportional memory cost. Sweet spot is roughly
    /// the effective fanout squared; below that, lookups pay; above, you
    /// only burn memory.
    pub object_sort_window: usize,

    /// Node-sizing and page-alignment knobs. See [`BuildPolicy`].
    pub policy: BuildPolicy,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            object_sort_window: 16_384,
            policy: BuildPolicy::default(),
        }
    }
}

// ============================================================================
// Frame state
// ============================================================================

#[derive(Default)]
struct ObjectState {
    current_run: Vec<(Vec<u8>, u64, u64)>, // key_bytes, key_off, val_off
    /// Streaming cross-run merge tree. Each completed run pushes one
    /// `ObjEntry` here; the cascade auto-flushes a level when it reaches
    /// fanout, so memory is bounded by O(depth × fanout × key_size) — not
    /// by total run count.
    runs_cascade: ObjectCascade,
    pending_key: Option<(Vec<u8>, u64)>,
    /// Sum of `kb.capacity()` across `current_run`. Maintained incrementally
    /// so `Writer::buffered_bytes()` is O(1) instead of O(current_run.len()).
    current_run_key_bytes: usize,
}

impl ObjectState {
    fn set_pending_key(&mut self, key_bytes: Vec<u8>, key_off: u64) -> Result<(), WriteError> {
        if self.pending_key.is_some() {
            return Err(WriteError::MisuseObjectKey);
        }
        self.pending_key = Some((key_bytes, key_off));
        Ok(())
    }

    /// Pair a just-emitted value offset with the pending key; flush the run
    /// once it reaches `run_buffer`.
    fn accept_value<S: Sink>(
        &mut self,
        val_off: u64,
        run_buffer: usize,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<(), WriteError> {
        let (kb, koff) = self
            .pending_key
            .take()
            .ok_or(WriteError::MisuseObjectValue)?;
        self.current_run_key_bytes += kb.capacity();
        self.current_run.push((kb, koff, val_off));
        if self.current_run.len() >= run_buffer {
            self.flush_run(policy, ctx)?;
        }
        Ok(())
    }

    fn flush_run<S: Sink>(
        &mut self,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<(), WriteError> {
        let mut run = std::mem::take(&mut self.current_run);
        self.current_run_key_bytes = 0;
        if run.is_empty() {
            return Ok(());
        }
        run.sort_by(|a, b| a.0.cmp(&b.0));
        for w in run.windows(2) {
            if w[0].0 == w[1].0 {
                return Err(WriteError::DuplicateKey);
            }
        }
        let mut builder = ObjectBPlusBuilder::new();
        for (kb, k, v) in run {
            builder.push(
                ObjectLeafItem {
                    key_off: k,
                    val_off: v,
                    key_bytes: kb,
                },
                policy,
                ctx,
            )?;
        }
        let entry = builder
            .finalize(policy, ctx)?
            .expect("non-empty run yields a root");
        // Stream the run's root entry into the cross-run cascade. The
        // cascade's `should_flush_object_internal` predicate auto-emits an
        // internal node once a level fills, so memory stays bounded by the
        // cascade depth — independent of run count.
        self.runs_cascade.push(1, entry, policy, ctx)
    }

    /// Flush the pending run (if any) and return the object root offset.
    /// Writes the `EMPTY_OBJECT` singleton if nothing was ever pushed.
    fn finalize<S: Sink>(
        mut self,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<u64, WriteError> {
        if self.pending_key.is_some() {
            return Err(WriteError::MisuseObjectValue);
        }
        if !self.current_run.is_empty() {
            self.flush_run(policy, ctx)?;
        }
        // The cascade's `finalize` bubbles a lone entry up as a carry without
        // emitting a wrapper — preserving the "single run is the root" fast
        // path naturally.
        match self.runs_cascade.finalize(policy, ctx)? {
            None => {
                let off = *ctx.pos;
                ctx.sink.write_all(&[EMPTY_OBJECT])?;
                *ctx.pos += 1;
                Ok(off)
            }
            Some(root) => Ok(root.node_off),
        }
    }
}

enum Frame {
    Array(ArrayBPlusBuilder),
    Object(ObjectState),
}

/// Streaming writer for a single Kahon document.
///
/// A `Writer` wraps any [`Sink`] (every [`std::io::Write`] qualifies) and
/// lets you push exactly one root value — a scalar, an array, or an
/// object — before [`finish`](Writer::finish) emits the trailer.
///
/// Values are written as they arrive; container nodes are buffered only
/// until they fill (per [`BuildPolicy`]) and then flushed. Peak memory
/// is bounded by tree depth, not document size.
///
/// See the [crate-level docs](crate) for a full example.
pub struct Writer<S: Sink> {
    sink: S,
    pos: u64,
    scratch: Vec<u8>,
    padding_written: u64,
    frames: Vec<Frame>,
    opts: WriterOptions,
    root_offset: Option<u64>,
    pub(crate) poisoned: bool,
    finished: bool,
}

impl<S: Sink> Writer<S> {
    /// Create a writer with default options ([`WriterOptions::default`]).
    ///
    /// The header is written eagerly; if that initial write fails, the
    /// writer is poisoned and the error surfaces on the next operation.
    pub fn new(sink: S) -> Self {
        // Default policy is statically known-valid; unwrap is safe.
        Self::with_options(sink, WriterOptions::default())
            .expect("default WriterOptions must validate")
    }

    /// Create a writer with caller-supplied [`WriterOptions`].
    ///
    /// Returns [`WriteError::InvalidOption`] if the policy is malformed
    /// (fanout < 2, target bytes < 64, or non–power-of-two page size).
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
            finished: false,
        };
        // If the header write fails, defer the error: poison now and surface
        // it on the first push/finish.
        if w.write_header().is_err() {
            w.poisoned = true;
        }
        Ok(w)
    }

    /// Total bytes emitted to the sink so far, including the header,
    /// every flushed value/node, and any alignment padding.
    pub fn bytes_written(&self) -> u64 {
        self.pos
    }

    /// Total bytes of unreferenced filler emitted by the page-alignment
    /// policy (zero when [`PageAlignment::None`] is in effect). Useful for
    /// quantifying the cost of disk-friendly layout.
    pub fn padding_bytes_written(&self) -> u64 {
        self.padding_written
    }

    /// Approximate live in-memory footprint of the writer's working buffers:
    /// the scratch encoding buffer, plus per-frame B+tree buffers and object
    /// run state. Useful for profiling peak memory across configurations.
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
        total
    }

    /// Finalize the document by writing the 12-byte trailer and return
    /// the underlying sink.
    ///
    /// Errors if the writer is poisoned, has already been finished, has
    /// open container builders, or never received a root value
    /// ([`WriteError::EmptyDocument`]).
    pub fn finish(mut self) -> Result<S, WriteError> {
        if self.poisoned {
            return Err(WriteError::Poisoned);
        }
        if self.finished {
            return Err(WriteError::AlreadyFinished);
        }
        if !self.frames.is_empty() {
            self.poisoned = true;
            return Err(WriteError::Poisoned);
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
        self.finished = true;
        Ok(self.sink)
    }

    /// Push a `null` as the document root, or as the next array
    /// element / object value.
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

    /// Push a signed 64-bit integer. Encoded in the narrowest tag that
    /// fits the value.
    pub fn push_i64(&mut self, v: i64) -> Result<(), WriteError> {
        self.check_ready()?;
        let off = self.emit_scalar(|b| write_integer(b, v as i128))?;
        self.register_value(off)
    }

    /// Push an unsigned 64-bit integer. Values up to `2^64 - 1` are
    /// representable; encoded in the narrowest tag that fits.
    pub fn push_u64(&mut self, v: u64) -> Result<(), WriteError> {
        self.check_ready()?;
        let off = self.emit_scalar(|b| write_integer(b, v as i128))?;
        self.register_value(off)
    }

    /// Push a 64-bit float.
    ///
    /// Returns [`WriteError::NaNOrInfinity`] for NaN or ±∞, and
    /// [`WriteError::FloatPrecisionLoss`] if the value cannot be
    /// represented losslessly in the chosen narrower encoding.
    pub fn push_f64(&mut self, v: f64) -> Result<(), WriteError> {
        self.check_ready()?;
        let off = self.emit_scalar(|b| write_f64(b, v))?;
        self.register_value(off)
    }

    /// Push a UTF-8 string. The bytes are written as-is; no escaping is
    /// applied.
    pub fn push_str(&mut self, s: &str) -> Result<(), WriteError> {
        self.check_ready()?;
        let off = self.emit_scalar(|b| {
            write_string(b, s);
            Ok(())
        })?;
        self.register_value(off)
    }

    /// Open an array. The returned [`ArrayBuilder`](crate::ArrayBuilder) borrows the writer
    /// mutably; close it via `.end()` (errors propagated) or by drop
    /// (errors poison the writer).
    pub fn start_array(&mut self) -> crate::builder::ArrayBuilder<'_, S> {
        self.push_array_frame();
        crate::builder::ArrayBuilder::new(self)
    }

    /// Open an object. The returned [`ObjectBuilder`](crate::ObjectBuilder) borrows the writer
    /// mutably; close it via `.end()` (errors propagated) or by drop
    /// (errors poison the writer).
    pub fn start_object(&mut self) -> crate::builder::ObjectBuilder<'_, S> {
        self.push_object_frame();
        crate::builder::ObjectBuilder::new(self)
    }

    pub(crate) fn push_array_frame(&mut self) {
        self.frames.push(Frame::Array(ArrayBPlusBuilder::new()));
    }

    pub(crate) fn push_object_frame(&mut self) {
        self.frames.push(Frame::Object(ObjectState::default()));
    }

    pub(crate) fn set_pending_key(&mut self, key: &str) -> Result<(), WriteError> {
        let off = self.emit_scalar(|b| {
            write_string(b, key);
            Ok(())
        })?;
        match self.frames.last_mut() {
            Some(Frame::Object(o)) => o.set_pending_key(key.as_bytes().to_vec(), off),
            _ => Err(WriteError::MisuseObjectKey),
        }
    }

    pub(crate) fn close_array_frame(&mut self) -> Result<(), WriteError> {
        if self.poisoned {
            return Err(WriteError::Poisoned);
        }
        let Some(Frame::Array(builder)) = self.frames.pop() else {
            unreachable!("top frame is not an array")
        };
        // Borrow opts.policy through a snapshot pointer-copy to avoid
        // overlapping borrows with self.ctx().
        let policy = self.opts.policy.clone();
        let root_off = match builder.finalize(&policy, &mut self.ctx())? {
            Some((_, off)) => off,
            None => {
                let off = self.pos;
                self.sink.write_all(&[EMPTY_ARRAY])?;
                self.pos += 1;
                off
            }
        };
        self.register_value(root_off)
    }

    pub(crate) fn close_object_frame(&mut self) -> Result<(), WriteError> {
        if self.poisoned {
            return Err(WriteError::Poisoned);
        }
        let Some(Frame::Object(obj)) = self.frames.pop() else {
            unreachable!("top frame is not an object")
        };
        let policy = self.opts.policy.clone();
        let root_off = obj.finalize(&policy, &mut self.ctx())?;
        self.register_value(root_off)
    }

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
        if self.finished {
            return Err(WriteError::AlreadyFinished);
        }
        Ok(())
    }

    /// Bundle a fresh `WriteCtx` over the writer's owned state.
    fn ctx(&mut self) -> WriteCtx<'_, S> {
        WriteCtx {
            sink: &mut self.sink,
            pos: &mut self.pos,
            scratch: &mut self.scratch,
            padding_written: &mut self.padding_written,
        }
    }

    /// Serialize a scalar into `scratch` and emit it via callback.
    /// Returns the offset where the value's tag byte was written.
    fn emit_scalar(
        &mut self,
        build: impl FnOnce(&mut Vec<u8>) -> Result<(), WriteError>,
    ) -> Result<u64, WriteError> {
        self.scratch.clear();
        build(&mut self.scratch)?;
        let off = self.pos;
        self.sink.write_all(&self.scratch)?;
        self.pos += self.scratch.len() as u64;
        Ok(off)
    }

    /// Route a completed value's offset to the current context: the root slot,
    /// an open array frame, or an open object frame awaiting a key.
    fn register_value(&mut self, off: u64) -> Result<(), WriteError> {
        let policy = self.opts.policy.clone();
        let run_buffer = self.opts.object_sort_window;
        // Disjoint borrows: frames is taken via last_mut(); ctx borrows the
        // other writer fields directly.
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
            Some(Frame::Array(a)) => a.push(off, &policy, &mut ctx),
            Some(Frame::Object(o)) => o.accept_value(off, run_buffer, &policy, &mut ctx),
        }
    }
}
