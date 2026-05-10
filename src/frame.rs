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

use crate::align::pad_for_node;
use crate::bplus::{
    emit_array_internal_bytes, emit_object_internal_bytes, ArrayBPlusBuilder, ObjEntry,
    ObjectBPlusBuilder, ObjectCascade, ObjectLeafItem,
};
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

    /// Flush any pending run and return the cascaded root entry, or `None`
    /// if no key/value pair was ever pushed. The caller writes the
    /// `EMPTY_OBJECT` singleton on `None`.
    fn finalize_entry<S: Sink>(
        mut self,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<Option<ObjEntry>, WriteError> {
        debug_assert!(
            self.pending_key.is_none(),
            "ObjectBuilder consumes pending_key before close; Some is unreachable"
        );
        if !self.current_run.is_empty() {
            self.flush_run(policy, ctx)?;
        }
        self.runs_cascade.finalize(policy, ctx)
    }
}

#[derive(Clone)]
pub(crate) enum Frame {
    Array(ArrayBPlusBuilder),
    Object(ObjectState),
}

/// Drive a single frame's B+tree to its root, optionally emitting an
/// extension header sequence immediately before the root's type-code byte
/// (spec §9 requires the payload at `ext_off + 1 + E`).
///
/// When `prefix` is `Some`, the closed container's root is wrapped in a
/// single-entry internal node so the extension header sits adjacent to a
/// type-code byte we control. The returned offset points at the start of
/// the prefix (the outermost extension tag); without a prefix, it points
/// at the original root tag.
///
/// For an empty array/object, emits the `EMPTY_ARRAY`/`EMPTY_OBJECT`
/// singleton (spec §7.0) directly after any prefix.
///
/// Used by both the live close path (`close_array_frame`/`close_object_frame`)
/// and `snapshot_trailer`'s out-of-band close into a side buffer.
pub(crate) fn close_frame_with_prefix<S: Sink>(
    frame: Frame,
    policy: &BuildPolicy,
    ctx: &mut WriteCtx<'_, S>,
    prefix: Option<&[u8]>,
) -> Result<u64, WriteError> {
    match frame {
        Frame::Array(builder) => match builder.finalize(policy, ctx)? {
            Some((sub_total, root_off)) => match prefix {
                None => Ok(root_off),
                Some(p) => emit_ext_wrapped_array(ctx, p, sub_total, root_off, &policy.align),
            },
            None => emit_empty_singleton(ctx, prefix, EMPTY_ARRAY),
        },
        Frame::Object(obj) => match obj.finalize_entry(policy, ctx)? {
            Some(entry) => match prefix {
                None => Ok(entry.node_off),
                Some(p) => emit_ext_wrapped_object(ctx, p, entry, &policy.align),
            },
            None => emit_empty_singleton(ctx, prefix, EMPTY_OBJECT),
        },
    }
}

fn emit_empty_singleton<S: Sink>(
    ctx: &mut WriteCtx<'_, S>,
    prefix: Option<&[u8]>,
    tag: u8,
) -> Result<u64, WriteError> {
    let prefix = prefix.unwrap_or(&[]);
    let slot_off = *ctx.pos;
    if !prefix.is_empty() {
        ctx.sink.write_all(prefix)?;
        *ctx.pos += prefix.len() as u64;
    }
    ctx.sink.write_all(&[tag])?;
    *ctx.pos += 1;
    Ok(slot_off)
}

fn emit_ext_wrapped_array<S: Sink>(
    ctx: &mut WriteCtx<'_, S>,
    prefix: &[u8],
    sub_total: u64,
    root_off: u64,
    align: &crate::config::PageAlignment,
) -> Result<u64, WriteError> {
    ctx.scratch.clear();
    ctx.scratch.extend_from_slice(prefix);
    emit_array_internal_bytes(ctx.scratch, &[(sub_total, root_off)]);
    pad_for_node(ctx, ctx.scratch.len(), align)?;
    let slot_off = *ctx.pos;
    ctx.sink.write_all(ctx.scratch)?;
    *ctx.pos += ctx.scratch.len() as u64;
    Ok(slot_off)
}

fn emit_ext_wrapped_object<S: Sink>(
    ctx: &mut WriteCtx<'_, S>,
    prefix: &[u8],
    entry: ObjEntry,
    align: &crate::config::PageAlignment,
) -> Result<u64, WriteError> {
    ctx.scratch.clear();
    ctx.scratch.extend_from_slice(prefix);
    emit_object_internal_bytes(ctx.scratch, &[entry]);
    pad_for_node(ctx, ctx.scratch.len(), align)?;
    let slot_off = *ctx.pos;
    ctx.sink.write_all(ctx.scratch)?;
    *ctx.pos += ctx.scratch.len() as u64;
    Ok(slot_off)
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
