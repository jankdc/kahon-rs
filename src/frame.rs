//! Per-container builder state held during postorder body emission.
//!
//! Spec §14.2: "Per open JSON container, keep a frame with a level stack."
//! A [`Frame`] is one such state - either an array under construction
//! ([`ArrayBPlusBuilder`]) or an object whose pairs are being buffered into
//! sorted runs and folded into a cross-run cascade ([`ObjectState`]).
//!
//! The [`close_frame`] and [`register_into_frame`] helpers are shared between
//! the live close path (`Writer::close_array_frame` / `close_object_frame`)
//! and `Writer::snapshot_trailer`'s out-of-band close into a side buffer -
//! the latter takes a clone of the frame stack so the live writer is
//! unaffected.
//!

use crate::bplus::{ArrayBPlusBuilder, ObjectBPlusBuilder, ObjectCascade, ObjectLeafItem};
use crate::config::{BuildPolicy, WriterOptions};
use crate::error::WriteError;
use crate::sink::{Sink, WriteCtx};
use crate::types::{EMPTY_ARRAY, EMPTY_OBJECT};

#[derive(Default, Clone)]
pub(crate) struct ObjectState {
    pub(crate) current_run: Vec<(Vec<u8>, u64, u64)>, // key_bytes, key_off, val_off
    /// Streaming cross-run merge tree. Each completed run pushes one
    /// `ObjEntry` here; the cascade auto-flushes a level when it reaches
    /// fanout, so memory is bounded by O(depth × fanout × key_size) - not
    /// by total run count.
    pub(crate) runs_cascade: ObjectCascade,
    pub(crate) pending_key: Option<(Vec<u8>, u64)>,
    /// Sum of `kb.capacity()` across `current_run`. Maintained incrementally
    /// so `Writer::buffered_bytes()` is O(1) instead of O(current_run.len()).
    pub(crate) current_run_key_bytes: usize,
}

impl ObjectState {
    pub(crate) fn set_pending_key(&mut self, key_bytes: Vec<u8>, key_off: u64) {
        debug_assert!(
            self.pending_key.is_none(),
            "ObjectBuilder consumes pending_key on every push; double-set is unreachable"
        );
        self.pending_key = Some((key_bytes, key_off));
    }

    /// Pair a just-emitted value offset with the pending key; flush the run
    /// once it reaches `run_buffer`.
    pub(crate) fn accept_value<S: Sink>(
        &mut self,
        val_off: u64,
        run_buffer: usize,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<(), WriteError> {
        let (kb, koff) = self.pending_key.take().expect(
            "ObjectBuilder always sets pending_key before producing a value; None is unreachable",
        );
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
        let mut deduped: Vec<(Vec<u8>, u64, u64)> = Vec::with_capacity(run.len());
        for entry in run {
            match deduped.last_mut() {
                Some(last) if last.0 == entry.0 => *last = entry,
                _ => deduped.push(entry),
            }
        }
        let mut builder = ObjectBPlusBuilder::new();
        for (kb, k, v) in deduped {
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
        // cascade depth - independent of run count.
        self.runs_cascade.push(1, entry, policy, ctx)
    }

    /// Flush the pending run (if any) and return the object root offset.
    /// Writes the `EMPTY_OBJECT` singleton if nothing was ever pushed.
    fn finalize<S: Sink>(
        mut self,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<u64, WriteError> {
        debug_assert!(
            self.pending_key.is_none(),
            "ObjectBuilder consumes pending_key before close; Some is unreachable"
        );
        if !self.current_run.is_empty() {
            self.flush_run(policy, ctx)?;
        }
        // The cascade's `finalize` bubbles a lone entry up as a carry without
        // emitting a wrapper - preserving the "single run is the root" fast
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

#[derive(Clone)]
pub(crate) enum Frame {
    Array(ArrayBPlusBuilder),
    Object(ObjectState),
}

/// Drive a single frame's B+tree to its root, emitting any pending leaf and
/// internal nodes through `ctx`. Returns the root's absolute offset (the
/// type-code byte of the closed container).
///
/// For an empty array/object, emits the `EMPTY_ARRAY`/`EMPTY_OBJECT`
/// singleton (spec §7.0) and returns its offset.
///
/// Used by both the live close path (`close_array_frame`/`close_object_frame`)
/// and `snapshot_trailer`'s out-of-band close into a side buffer.
pub(crate) fn close_frame<S: Sink>(
    frame: Frame,
    policy: &BuildPolicy,
    ctx: &mut WriteCtx<'_, S>,
) -> Result<u64, WriteError> {
    match frame {
        Frame::Array(builder) => match builder.finalize(policy, ctx)? {
            Some((_, off)) => Ok(off),
            None => {
                let off = *ctx.pos;
                ctx.sink.write_all(&[EMPTY_ARRAY])?;
                *ctx.pos += 1;
                Ok(off)
            }
        },
        Frame::Object(obj) => obj.finalize(policy, ctx),
    }
}

/// Register a completed value's offset into the given (parent) frame.
/// Used by `snapshot_trailer` to fold a closed frame's root into its parent
/// during out-of-band closure.
pub(crate) fn register_into_frame<S: Sink>(
    frame: &mut Frame,
    off: u64,
    opts: &WriterOptions,
    ctx: &mut WriteCtx<'_, S>,
) -> Result<(), WriteError> {
    match frame {
        Frame::Array(a) => a.push(off, &opts.policy, ctx),
        Frame::Object(o) => o.accept_value(off, opts.object_sort_window, &opts.policy, ctx),
    }
}
