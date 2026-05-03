//! Streaming B+tree subtree emission for arrays and objects.
//!
//! Arrays and objects share the same bulk-loaded B+tree shape, but object
//! internal nodes carry per-child key-range fences (`key_off_lo`,
//! `key_off_hi`, spec §7.4) so readers can short-circuit run scans. The
//! array path stays a 2-tuple `(sub_total, node_off)` cascade; the object
//! path uses a richer entry that carries the fence offsets and the actual
//! key bytes (needed to compute parent fences when sibling runs interleave
//! lexicographically).

use crate::align::pad_for_node;
use crate::encode::{smallest_width, write_uintw, write_varuint};
use crate::error::WriteError;
use crate::sink::{Sink, WriteCtx};
use crate::types::{ARRAY_INTERNAL_TAG, ARRAY_LEAF_TAG, OBJECT_INTERNAL_TAG, OBJECT_LEAF_TAG};
use crate::writer::{BuildPolicy, NodeSizing, PageAlignment};

// ============================================================================
// Size predicates
// ============================================================================

/// Encoded byte length of an LEB128 varuint.
#[inline]
fn varuint_size(v: u64) -> usize {
    let bits = (64 - v.leading_zeros()) as usize;
    bits.div_ceil(7).max(1)
}

fn projected_array_leaf_bytes(items: &[u64]) -> usize {
    let max_off = items.iter().copied().max().unwrap_or(0);
    let (_, width) = smallest_width(max_off);
    1 + varuint_size(items.len() as u64) + items.len() * width
}

fn projected_object_leaf_bytes(items: &[ObjectLeafItem]) -> usize {
    let max_off = items
        .iter()
        .map(|it| it.key_off.max(it.val_off))
        .max()
        .unwrap_or(0);
    let (_, width) = smallest_width(max_off);
    1 + varuint_size(items.len() as u64) + items.len() * 2 * width
}

fn projected_array_internal_bytes(pairs: &[(u64, u64)]) -> usize {
    let max_off = pairs.iter().map(|&(_, o)| o).max().unwrap_or(0);
    let total: u64 = pairs.iter().map(|&(st, _)| st).sum();
    let (_, width) = smallest_width(max_off);
    1 + varuint_size(total) + varuint_size(pairs.len() as u64) + pairs.len() * (8 + width)
}

fn projected_object_internal_bytes(entries: &[ObjEntry]) -> usize {
    let max_off = entries.iter().map(ObjEntry::max_offset).max().unwrap_or(0);
    let total: u64 = entries.iter().map(|e| e.sub_total).sum();
    let (_, width) = smallest_width(max_off);
    1 + varuint_size(total) + varuint_size(entries.len() as u64) + entries.len() * (8 + 3 * width)
}

fn should_flush_array_leaf(items: &[u64], sizing: &NodeSizing) -> bool {
    if items.is_empty() {
        return false;
    }
    match sizing {
        NodeSizing::Fanout(f) => items.len() >= *f,
        NodeSizing::TargetBytes(t) => projected_array_leaf_bytes(items) >= *t,
    }
}

fn should_flush_object_leaf(items: &[ObjectLeafItem], sizing: &NodeSizing) -> bool {
    if items.is_empty() {
        return false;
    }
    match sizing {
        NodeSizing::Fanout(f) => items.len() >= *f,
        NodeSizing::TargetBytes(t) => projected_object_leaf_bytes(items) >= *t,
    }
}

fn should_flush_array_internal(pairs: &[(u64, u64)], sizing: &NodeSizing) -> bool {
    if pairs.len() < 2 {
        return false;
    }
    match sizing {
        NodeSizing::Fanout(f) => pairs.len() >= *f,
        NodeSizing::TargetBytes(t) => projected_array_internal_bytes(pairs) >= *t,
    }
}

fn should_flush_object_internal(entries: &[ObjEntry], sizing: &NodeSizing) -> bool {
    if entries.len() < 2 {
        return false;
    }
    match sizing {
        NodeSizing::Fanout(f) => entries.len() >= *f,
        NodeSizing::TargetBytes(t) => projected_object_internal_bytes(entries) >= *t,
    }
}

// ============================================================================
// Leaf items
// ============================================================================

/// Object-leaf entry kept alongside its key bytes; the bytes live until the
/// leaf flushes, at which point the leaf's first/last entries become its
/// fence and bubble into the cascade.
#[derive(Clone)]
pub(crate) struct ObjectLeafItem {
    pub(crate) key_off: u64,
    pub(crate) val_off: u64,
    pub(crate) key_bytes: Vec<u8>,
}

// ============================================================================
// Leaf / internal emission
// ============================================================================

fn emit_array_leaf<S: Sink>(
    ctx: &mut WriteCtx<'_, S>,
    offs: &[u64],
    align: &PageAlignment,
) -> Result<u64, WriteError> {
    let max_off = offs.iter().copied().max().unwrap_or(0);
    let (w, width) = smallest_width(max_off);
    ctx.scratch.clear();
    ctx.scratch.push(ARRAY_LEAF_TAG | w);
    write_varuint(ctx.scratch, offs.len() as u64);
    for &o in offs {
        write_uintw(ctx.scratch, o, width);
    }
    pad_for_node(ctx, ctx.scratch.len(), align)?;
    let node_off = *ctx.pos;
    ctx.sink.write_all(ctx.scratch)?;
    *ctx.pos += ctx.scratch.len() as u64;
    Ok(node_off)
}

fn emit_object_leaf<S: Sink>(
    ctx: &mut WriteCtx<'_, S>,
    items: &[ObjectLeafItem],
    align: &PageAlignment,
) -> Result<u64, WriteError> {
    let mut max_off = 0u64;
    for it in items {
        if it.key_off > max_off {
            max_off = it.key_off;
        }
        if it.val_off > max_off {
            max_off = it.val_off;
        }
    }
    let (w, width) = smallest_width(max_off);
    ctx.scratch.clear();
    ctx.scratch.push(OBJECT_LEAF_TAG | w);
    write_varuint(ctx.scratch, items.len() as u64);
    for it in items {
        write_uintw(ctx.scratch, it.key_off, width);
        write_uintw(ctx.scratch, it.val_off, width);
    }
    pad_for_node(ctx, ctx.scratch.len(), align)?;
    let node_off = *ctx.pos;
    ctx.sink.write_all(ctx.scratch)?;
    *ctx.pos += ctx.scratch.len() as u64;
    Ok(node_off)
}

fn emit_array_internal<S: Sink>(
    ctx: &mut WriteCtx<'_, S>,
    pairs: &[(u64, u64)],
    align: &PageAlignment,
) -> Result<u64, WriteError> {
    let mut max_off = 0u64;
    let mut total = 0u64;
    for &(st, off) in pairs {
        total = total.saturating_add(st);
        if off > max_off {
            max_off = off;
        }
    }
    let (w, width) = smallest_width(max_off);
    ctx.scratch.clear();
    ctx.scratch.push(ARRAY_INTERNAL_TAG | w);
    write_varuint(ctx.scratch, total);
    write_varuint(ctx.scratch, pairs.len() as u64);
    for &(st, off) in pairs {
        ctx.scratch.extend_from_slice(&st.to_le_bytes());
        write_uintw(ctx.scratch, off, width);
    }
    pad_for_node(ctx, ctx.scratch.len(), align)?;
    let node_off = *ctx.pos;
    ctx.sink.write_all(ctx.scratch)?;
    *ctx.pos += ctx.scratch.len() as u64;
    Ok(node_off)
}

fn emit_object_internal<S: Sink>(
    ctx: &mut WriteCtx<'_, S>,
    entries: &[ObjEntry],
    align: &PageAlignment,
) -> Result<u64, WriteError> {
    let max_off = entries.iter().map(ObjEntry::max_offset).max().unwrap_or(0);
    let (w, width) = smallest_width(max_off);
    let total: u64 = entries.iter().map(|e| e.sub_total).sum();
    ctx.scratch.clear();
    ctx.scratch.push(OBJECT_INTERNAL_TAG | w);
    write_varuint(ctx.scratch, total);
    write_varuint(ctx.scratch, entries.len() as u64);
    for e in entries {
        ctx.scratch.extend_from_slice(&e.sub_total.to_le_bytes());
        write_uintw(ctx.scratch, e.key_lo_off, width);
        write_uintw(ctx.scratch, e.key_hi_off, width);
        write_uintw(ctx.scratch, e.node_off, width);
    }
    pad_for_node(ctx, ctx.scratch.len(), align)?;
    let node_off = *ctx.pos;
    ctx.sink.write_all(ctx.scratch)?;
    *ctx.pos += ctx.scratch.len() as u64;
    Ok(node_off)
}

// ============================================================================
// Array internal-level cascade
// ============================================================================

#[derive(Default)]
pub(crate) struct ArrayCascade {
    levels: Vec<Vec<(u64, u64)>>,
}

impl ArrayCascade {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.levels.iter().all(|lv| lv.is_empty())
    }

    pub(crate) fn buffered_bytes(&self) -> usize {
        let mut t = self.levels.capacity() * std::mem::size_of::<Vec<(u64, u64)>>();
        for lv in &self.levels {
            t += lv.capacity() * std::mem::size_of::<(u64, u64)>();
        }
        t
    }

    pub(crate) fn push<S: Sink>(
        &mut self,
        level: usize,
        pair: (u64, u64),
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<(), WriteError> {
        debug_assert!(level >= 1);
        while self.levels.len() < level {
            self.levels.push(Vec::new());
        }
        self.levels[level - 1].push(pair);
        if !should_flush_array_internal(&self.levels[level - 1], &policy.sizing) {
            return Ok(());
        }
        let pairs = std::mem::take(&mut self.levels[level - 1]);
        let total: u64 = pairs.iter().map(|&(st, _)| st).sum();
        let off = emit_array_internal(ctx, &pairs, &policy.align)?;
        self.push(level + 1, (total, off), policy, ctx)
    }

    pub(crate) fn finalize<S: Sink>(
        mut self,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<Option<(u64, u64)>, WriteError> {
        let mut carry: Option<(u64, u64)> = None;
        for i in 0..self.levels.len() {
            if let Some(p) = carry.take() {
                self.levels[i].push(p);
            }
            let pairs = std::mem::take(&mut self.levels[i]);
            carry = match pairs.len() {
                0 => None,
                1 => Some(pairs[0]),
                _ => {
                    let total: u64 = pairs.iter().map(|&(st, _)| st).sum();
                    let off = emit_array_internal(ctx, &pairs, &policy.align)?;
                    Some((total, off))
                }
            };
        }
        Ok(carry)
    }
}

// ============================================================================
// Object internal-level cascade
// ============================================================================

/// Object-internal cascade entry: spec §7.4 wire fields plus the actual
/// `[key_lo, key_hi]` byte sequences. The bytes are needed when sibling
/// runs interleave lexicographically (cross-run merge in
/// `ObjectState::finalize`); within a single run the leaves arrive in
/// sorted order, so this collapses to taking the first/last child's fence.
#[derive(Clone)]
pub(crate) struct ObjEntry {
    pub(crate) sub_total: u64,
    pub(crate) node_off: u64,
    pub(crate) key_lo_off: u64,
    pub(crate) key_hi_off: u64,
    pub(crate) key_lo: Vec<u8>,
    pub(crate) key_hi: Vec<u8>,
}

impl ObjEntry {
    fn max_offset(&self) -> u64 {
        self.node_off.max(self.key_lo_off).max(self.key_hi_off)
    }
}

#[derive(Default)]
pub(crate) struct ObjectCascade {
    levels: Vec<Vec<ObjEntry>>,
}

impl ObjectCascade {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.levels.iter().all(|lv| lv.is_empty())
    }

    pub(crate) fn buffered_bytes(&self) -> usize {
        let mut t = self.levels.capacity() * std::mem::size_of::<Vec<ObjEntry>>();
        for lv in &self.levels {
            t += lv.capacity() * std::mem::size_of::<ObjEntry>();
            for e in lv {
                t += e.key_lo.capacity() + e.key_hi.capacity();
            }
        }
        t
    }

    pub(crate) fn push<S: Sink>(
        &mut self,
        level: usize,
        entry: ObjEntry,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<(), WriteError> {
        debug_assert!(level >= 1);
        while self.levels.len() < level {
            self.levels.push(Vec::new());
        }
        self.levels[level - 1].push(entry);
        if !should_flush_object_internal(&self.levels[level - 1], &policy.sizing) {
            return Ok(());
        }
        let entries = std::mem::take(&mut self.levels[level - 1]);
        let parent = emit_and_merge_object(ctx, entries, &policy.align)?;
        self.push(level + 1, parent, policy, ctx)
    }

    pub(crate) fn finalize<S: Sink>(
        mut self,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<Option<ObjEntry>, WriteError> {
        let mut carry: Option<ObjEntry> = None;
        for i in 0..self.levels.len() {
            if let Some(p) = carry.take() {
                self.levels[i].push(p);
            }
            let entries = std::mem::take(&mut self.levels[i]);
            carry = match entries.len() {
                0 => None,
                1 => Some(entries.into_iter().next().unwrap()),
                _ => Some(emit_and_merge_object(ctx, entries, &policy.align)?),
            };
        }
        Ok(carry)
    }
}

/// Emit one object-internal node and fold its children's fences into the
/// resulting parent entry. Lexicographic min/max across children uses the
/// stored key bytes; within a single run they are already monotone, so this
/// reduces to first/last in practice.
fn emit_and_merge_object<S: Sink>(
    ctx: &mut WriteCtx<'_, S>,
    mut entries: Vec<ObjEntry>,
    align: &PageAlignment,
) -> Result<ObjEntry, WriteError> {
    let off = emit_object_internal(ctx, &entries, align)?;
    let sub_total: u64 = entries.iter().map(|e| e.sub_total).sum();
    let mut lo_idx = 0usize;
    let mut hi_idx = 0usize;
    for i in 1..entries.len() {
        if entries[i].key_lo < entries[lo_idx].key_lo {
            lo_idx = i;
        }
        if entries[i].key_hi > entries[hi_idx].key_hi {
            hi_idx = i;
        }
    }
    let key_lo_off = entries[lo_idx].key_lo_off;
    let key_hi_off = entries[hi_idx].key_hi_off;
    // Take ownership of the chosen lo/hi byte buffers without cloning.
    let key_lo = std::mem::take(&mut entries[lo_idx].key_lo);
    let key_hi = std::mem::take(&mut entries[hi_idx].key_hi);
    Ok(ObjEntry {
        sub_total,
        node_off: off,
        key_lo_off,
        key_hi_off,
        key_lo,
        key_hi,
    })
}

// ============================================================================
// Array B+tree builder
// ============================================================================

pub(crate) struct ArrayBPlusBuilder {
    leaf: Vec<u64>,
    cascade: ArrayCascade,
}

impl Default for ArrayBPlusBuilder {
    fn default() -> Self {
        Self {
            leaf: Vec::new(),
            cascade: ArrayCascade::new(),
        }
    }
}

impl ArrayBPlusBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn buffered_bytes(&self) -> usize {
        self.leaf.capacity() * std::mem::size_of::<u64>() + self.cascade.buffered_bytes()
    }

    pub(crate) fn push<S: Sink>(
        &mut self,
        item: u64,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<(), WriteError> {
        self.leaf.push(item);
        if should_flush_array_leaf(&self.leaf, &policy.sizing) {
            self.spill_leaf(policy, ctx)?;
        }
        Ok(())
    }

    fn spill_leaf<S: Sink>(
        &mut self,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<(), WriteError> {
        let leaf = std::mem::take(&mut self.leaf);
        let count = leaf.len() as u64;
        let off = emit_array_leaf(ctx, &leaf, &policy.align)?;
        self.cascade.push(1, (count, off), policy, ctx)
    }

    /// Returns `Some((total, root_off))` if anything was pushed; `None` if empty.
    pub(crate) fn finalize<S: Sink>(
        mut self,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<Option<(u64, u64)>, WriteError> {
        if self.leaf.is_empty() && self.cascade.is_empty() {
            return Ok(None);
        }
        if !self.leaf.is_empty() {
            let leaf = std::mem::take(&mut self.leaf);
            let count = leaf.len() as u64;
            let off = emit_array_leaf(ctx, &leaf, &policy.align)?;
            // Append to level 1 without short-circuiting cascade: finalize
            // is the m>=2 gatekeeper.
            while self.cascade.levels.is_empty() {
                self.cascade.levels.push(Vec::new());
            }
            self.cascade.levels[0].push((count, off));
        }
        self.cascade.finalize(policy, ctx)
    }
}

// ============================================================================
// Object B+tree builder
// ============================================================================

pub(crate) struct ObjectBPlusBuilder {
    leaf: Vec<ObjectLeafItem>,
    cascade: ObjectCascade,
}

impl Default for ObjectBPlusBuilder {
    fn default() -> Self {
        Self {
            leaf: Vec::new(),
            cascade: ObjectCascade::new(),
        }
    }
}

impl ObjectBPlusBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Items MUST be pushed in ascending UTF-8 byte order of `key_bytes`.
    pub(crate) fn push<S: Sink>(
        &mut self,
        item: ObjectLeafItem,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<(), WriteError> {
        self.leaf.push(item);
        if should_flush_object_leaf(&self.leaf, &policy.sizing) {
            self.spill_leaf(policy, ctx)?;
        }
        Ok(())
    }

    fn spill_leaf<S: Sink>(
        &mut self,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<(), WriteError> {
        let leaf = std::mem::take(&mut self.leaf);
        let count = leaf.len() as u64;
        let leaf_off = emit_object_leaf(ctx, &leaf, &policy.align)?;
        let entry = leaf_to_entry(leaf, count, leaf_off);
        self.cascade.push(1, entry, policy, ctx)
    }

    /// Returns `Some(ObjEntry)` if anything was pushed; `None` if empty.
    /// The caller writes the empty-object singleton on `None`.
    pub(crate) fn finalize<S: Sink>(
        mut self,
        policy: &BuildPolicy,
        ctx: &mut WriteCtx<'_, S>,
    ) -> Result<Option<ObjEntry>, WriteError> {
        if self.leaf.is_empty() && self.cascade.is_empty() {
            return Ok(None);
        }
        if !self.leaf.is_empty() {
            let leaf = std::mem::take(&mut self.leaf);
            let count = leaf.len() as u64;
            let leaf_off = emit_object_leaf(ctx, &leaf, &policy.align)?;
            let entry = leaf_to_entry(leaf, count, leaf_off);
            if self.cascade.levels.is_empty() {
                self.cascade.levels.push(Vec::new());
            }
            self.cascade.levels[0].push(entry);
        }
        self.cascade.finalize(policy, ctx)
    }
}

/// Build a cascade entry from a sorted leaf: the leaf's first/last items'
/// keys are its fence.
fn leaf_to_entry(mut leaf: Vec<ObjectLeafItem>, count: u64, leaf_off: u64) -> ObjEntry {
    debug_assert!(!leaf.is_empty());
    let last = leaf.pop().unwrap();
    let key_hi_off = last.key_off;
    let key_hi = last.key_bytes;
    if leaf.is_empty() {
        // Single-entry leaf: lo == hi (both offset and bytes).
        ObjEntry {
            sub_total: count,
            node_off: leaf_off,
            key_lo_off: key_hi_off,
            key_hi_off,
            key_lo: key_hi.clone(),
            key_hi,
        }
    } else {
        let first = leaf.into_iter().next().unwrap();
        ObjEntry {
            sub_total: count,
            node_off: leaf_off,
            key_lo_off: first.key_off,
            key_hi_off,
            key_lo: first.key_bytes,
            key_hi,
        }
    }
}
