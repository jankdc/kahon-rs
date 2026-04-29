//! Test-only reference decoder for the Kahon format. Yields a
//! `serde_json::Value` plus structural metadata for invariant assertions.

use serde_json::{Map, Number, Value};

#[derive(Debug, PartialEq)]
pub enum ReadError {
    BadMagic,
    BadVersion,
    BadFlags,
    BadRootOffset,
    UnknownTag(u8),
    Truncated,
    BadUtf8,
    BadFloat,
    UnsortedKeys,
    InternalFanoutTooSmall,
    SubTotalMismatch,
    OffsetOutOfBounds,
    OverlongVarint,
    /// Container declared a wider offset slot than necessary (spec §7, §8.8).
    NonMinimalWidth {
        declared: u8,
        needed: u8,
    },
    /// A stored offset is >= the containing node's own offset (postorder violation, spec §9).
    PostorderViolation {
        node_off: usize,
        child_off: usize,
    },
    /// An array-leaf or object-leaf used n=0 instead of EmptyArray/EmptyObject (writer bug per §7.0).
    EmptyLeafNotSingleton,
    /// An internal node's m exceeded the writer's declared fanout (writer property).
    FanoutExceeded {
        m: u64,
        ceiling: u64,
    },
}

#[derive(Debug, Default, Clone)]
pub struct Stats {
    /// Count of containers using each width code (0..=3).
    pub width_histogram: [u64; 4],
    /// Largest m observed at any internal node.
    pub max_internal_fanout: u64,
    /// Number of internal nodes traversed.
    pub internal_nodes: u64,
    /// Number of leaf container nodes traversed.
    pub leaf_nodes: u64,
}

#[derive(Debug)]
pub struct Decoded {
    pub value: Value,
    pub stats: Stats,
}

/// Decode without a fanout ceiling check. Use [`decode_with_fanout`] in
/// property tests to also assert m <= writer's chosen fanout.
pub fn decode(buf: &[u8]) -> Result<Decoded, ReadError> {
    decode_inner(buf, None)
}

/// Look up `key` in the root object and return the offset of the matching
/// value tag, or `None` if absent. Searches all runs of internal nodes per
/// spec §8.5 (key ranges across siblings MAY overlap). Within a leaf, keys
/// are sorted, so binary search applies.
pub fn lookup_key(buf: &[u8], key: &[u8]) -> Result<Option<usize>, ReadError> {
    if buf.len() < 18 {
        return Err(ReadError::Truncated);
    }
    if &buf[0..4] != b"KAHN" {
        return Err(ReadError::BadMagic);
    }
    let n = buf.len();
    let root_off = u64::from_le_bytes(buf[n - 12..n - 4].try_into().unwrap()) as usize;
    let body_end = n - 12;
    lookup_in_object(buf, root_off, body_end, key)
}

fn lookup_in_object(
    buf: &[u8],
    off: usize,
    body_end: usize,
    key: &[u8],
) -> Result<Option<usize>, ReadError> {
    if off >= body_end {
        return Err(ReadError::OffsetOutOfBounds);
    }
    let tag = buf[off];
    let w = (tag & 0x03) as usize;
    let width = 1usize << w;
    match tag & 0xFC {
        0x80 => {
            let mut p = off + 1;
            let n = read_varuint(buf, &mut p, body_end)? as usize;
            let pairs_start = p;
            let (mut lo, mut hi) = (0usize, n);
            while lo < hi {
                let mid = (lo + hi) / 2;
                let pair_off = pairs_start + mid * 2 * width;
                need(buf, pair_off + 2 * width, body_end)?;
                let k_off = read_uintw(buf, pair_off, width) as usize;
                let mid_key = read_string_bytes(buf, k_off, body_end)?;
                use std::cmp::Ordering::*;
                match mid_key.cmp(key) {
                    Less => lo = mid + 1,
                    Greater => hi = mid,
                    Equal => {
                        let v_off = read_uintw(buf, pair_off + width, width) as usize;
                        return Ok(Some(v_off));
                    }
                }
            }
            Ok(None)
        }
        0x84 => {
            let mut p = off + 1;
            let _total = read_varuint(buf, &mut p, body_end)?;
            let m = read_varuint(buf, &mut p, body_end)? as usize;
            // Last-wins across sibling leaves: scan all in-range children
            // and keep the most recently traversed match. Children are stored
            // in run-insertion order, so the last hit is the latest write.
            let mut latest: Option<usize> = None;
            for _ in 0..m {
                need(buf, p + 8 + 3 * width, body_end)?;
                p += 8;
                let key_lo_off = read_uintw(buf, p, width) as usize;
                p += width;
                let key_hi_off = read_uintw(buf, p, width) as usize;
                p += width;
                let child = read_uintw(buf, p, width) as usize;
                p += width;
                // Fence short-circuit: skip children whose key range cannot
                // contain the lookup key (spec §7.4).
                let lo = read_string_bytes(buf, key_lo_off, body_end)?;
                if lo > key {
                    continue;
                }
                let hi = read_string_bytes(buf, key_hi_off, body_end)?;
                if hi < key {
                    continue;
                }
                if let Some(v) = lookup_in_object(buf, child, body_end, key)? {
                    latest = Some(v);
                }
            }
            Ok(latest)
        }
        0x34 => Ok(None),
        _ => Err(ReadError::UnknownTag(tag)),
    }
}

/// Decode and additionally assert every internal node's m is <= `fanout`,
/// and every leaf's n is <= `fanout`. This is a *writer*-side property
/// (the spec only mandates m >= 2), but is meaningful for fuzz testing.
pub fn decode_with_fanout(buf: &[u8], fanout: usize) -> Result<Decoded, ReadError> {
    decode_inner(buf, Some(fanout as u64))
}

fn decode_inner(buf: &[u8], fanout_ceiling: Option<u64>) -> Result<Decoded, ReadError> {
    if buf.len() < 18 {
        return Err(ReadError::Truncated);
    }
    if &buf[0..4] != b"KAHN" {
        return Err(ReadError::BadMagic);
    }
    if buf[4] != 0x01 {
        return Err(ReadError::BadVersion);
    }
    if buf[5] != 0x00 {
        return Err(ReadError::BadFlags);
    }
    let n = buf.len();
    if &buf[n - 4..] != b"KAHN" {
        return Err(ReadError::BadMagic);
    }
    let root_off = u64::from_le_bytes(buf[n - 12..n - 4].try_into().unwrap()) as usize;
    if root_off < 6 || root_off > n - 13 {
        return Err(ReadError::BadRootOffset);
    }
    let body_end = n - 12;
    let mut stats = Stats::default();
    let mut ctx = Ctx {
        stats: &mut stats,
        fanout_ceiling,
    };
    let value = decode_value(buf, root_off, body_end, &mut ctx)?;
    Ok(Decoded { value, stats })
}

struct Ctx<'a> {
    stats: &'a mut Stats,
    fanout_ceiling: Option<u64>,
}

fn decode_value(
    buf: &[u8],
    off: usize,
    body_end: usize,
    ctx: &mut Ctx<'_>,
) -> Result<Value, ReadError> {
    if off >= body_end {
        return Err(ReadError::OffsetOutOfBounds);
    }
    let tag = buf[off];
    match tag {
        0x00 => Ok(Value::Null),
        0x01 => Ok(Value::Bool(false)),
        0x02 => Ok(Value::Bool(true)),
        0x03..=0x12 => Ok(Value::Number(Number::from(2_i64 - tag as i64))),
        0x13..=0x32 => Ok(Value::Number(Number::from((tag - 0x13) as u64))),
        0x33 => Ok(Value::Array(Vec::new())),
        0x34 => Ok(Value::Object(Map::new())),
        0x40 => {
            need(buf, off + 2, body_end)?;
            Ok(Value::Number(Number::from(buf[off + 1] as u64)))
        }
        0x41 => {
            need(buf, off + 3, body_end)?;
            let v = u16::from_le_bytes(buf[off + 1..off + 3].try_into().unwrap());
            Ok(Value::Number(Number::from(v as u64)))
        }
        0x42 => {
            need(buf, off + 5, body_end)?;
            let v = u32::from_le_bytes(buf[off + 1..off + 5].try_into().unwrap());
            Ok(Value::Number(Number::from(v as u64)))
        }
        0x43 => {
            need(buf, off + 9, body_end)?;
            let v = u64::from_le_bytes(buf[off + 1..off + 9].try_into().unwrap());
            Ok(Value::Number(Number::from(v)))
        }
        0x44 => {
            need(buf, off + 2, body_end)?;
            Ok(Value::Number(Number::from(buf[off + 1] as i8 as i64)))
        }
        0x45 => {
            need(buf, off + 3, body_end)?;
            let v = i16::from_le_bytes(buf[off + 1..off + 3].try_into().unwrap());
            Ok(Value::Number(Number::from(v as i64)))
        }
        0x46 => {
            need(buf, off + 5, body_end)?;
            let v = i32::from_le_bytes(buf[off + 1..off + 5].try_into().unwrap());
            Ok(Value::Number(Number::from(v as i64)))
        }
        0x47 => {
            need(buf, off + 9, body_end)?;
            let v = i64::from_le_bytes(buf[off + 1..off + 9].try_into().unwrap());
            Ok(Value::Number(Number::from(v)))
        }
        0x50 => {
            need(buf, off + 5, body_end)?;
            let v = f32::from_le_bytes(buf[off + 1..off + 5].try_into().unwrap()) as f64;
            Number::from_f64(v)
                .map(Value::Number)
                .ok_or(ReadError::BadFloat)
        }
        0x51 => {
            need(buf, off + 9, body_end)?;
            let v = f64::from_le_bytes(buf[off + 1..off + 9].try_into().unwrap());
            Number::from_f64(v)
                .map(Value::Number)
                .ok_or(ReadError::BadFloat)
        }
        0x60..=0x6E => {
            let len = (tag - 0x60) as usize + 1;
            need(buf, off + 1 + len, body_end)?;
            let s = std::str::from_utf8(&buf[off + 1..off + 1 + len])
                .map_err(|_| ReadError::BadUtf8)?
                .to_string();
            Ok(Value::String(s))
        }
        0x6F => {
            let mut p = off + 1;
            let len = read_varuint(buf, &mut p, body_end)? as usize;
            need(buf, p + len, body_end)?;
            let s = std::str::from_utf8(&buf[p..p + len])
                .map_err(|_| ReadError::BadUtf8)?
                .to_string();
            Ok(Value::String(s))
        }
        0x70..=0x77 => decode_array(buf, off, body_end, ctx),
        0x80..=0x87 => decode_object(buf, off, body_end, ctx),
        _ => Err(ReadError::UnknownTag(tag)),
    }
}

fn decode_array(
    buf: &[u8],
    off: usize,
    body_end: usize,
    ctx: &mut Ctx<'_>,
) -> Result<Value, ReadError> {
    let mut value_offs = Vec::new();
    collect_array_values(buf, off, body_end, ctx, &mut value_offs)?;
    let mut out = Vec::with_capacity(value_offs.len());
    for o in value_offs {
        out.push(decode_value(buf, o, body_end, ctx)?);
    }
    Ok(Value::Array(out))
}

fn collect_array_values(
    buf: &[u8],
    off: usize,
    body_end: usize,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<usize>,
) -> Result<u64, ReadError> {
    let tag = buf[off];
    let w = tag & 0x03;
    let width = 1usize << w;
    ctx.stats.width_histogram[w as usize] += 1;
    match tag & 0xFC {
        0x70 => {
            ctx.stats.leaf_nodes += 1;
            let mut p = off + 1;
            let n = read_varuint(buf, &mut p, body_end)?;
            if n == 0 {
                return Err(ReadError::EmptyLeafNotSingleton);
            }
            check_fanout(n, ctx.fanout_ceiling)?;
            let mut max_off: u64 = 0;
            for _ in 0..n {
                need(buf, p + width, body_end)?;
                let child = read_uintw(buf, p, width);
                p += width;
                check_postorder(off, child as usize)?;
                max_off = max_off.max(child);
                out.push(child as usize);
            }
            check_minimal_width(max_off, w)?;
            Ok(n)
        }
        0x74 => {
            ctx.stats.internal_nodes += 1;
            let mut p = off + 1;
            let total = read_varuint(buf, &mut p, body_end)?;
            let m = read_varuint(buf, &mut p, body_end)?;
            if m < 2 {
                return Err(ReadError::InternalFanoutTooSmall);
            }
            check_fanout(m, ctx.fanout_ceiling)?;
            ctx.stats.max_internal_fanout = ctx.stats.max_internal_fanout.max(m);
            let mut sum = 0u64;
            let mut max_off: u64 = 0;
            let mut child_offs = Vec::with_capacity(m as usize);
            for _ in 0..m {
                need(buf, p + 8 + width, body_end)?;
                let sub = u64::from_le_bytes(buf[p..p + 8].try_into().unwrap());
                p += 8;
                let child = read_uintw(buf, p, width);
                p += width;
                check_postorder(off, child as usize)?;
                max_off = max_off.max(child);
                sum += sub;
                child_offs.push((sub, child as usize));
            }
            if sum != total {
                return Err(ReadError::SubTotalMismatch);
            }
            check_minimal_width(max_off, w)?;
            for (sub, c) in child_offs {
                let got = collect_array_values(buf, c, body_end, ctx, out)?;
                if got != sub {
                    return Err(ReadError::SubTotalMismatch);
                }
            }
            Ok(total)
        }
        _ => Err(ReadError::UnknownTag(tag)),
    }
}

fn decode_object(
    buf: &[u8],
    off: usize,
    body_end: usize,
    ctx: &mut Ctx<'_>,
) -> Result<Value, ReadError> {
    let mut pairs = Vec::new();
    collect_object_pairs(buf, off, body_end, ctx, &mut pairs)?;
    // Last-wins on duplicates: traversal visits children in stored order,
    // which matches run-insertion order in the writer's cross-run cascade,
    // so a later occurrence overwrites an earlier one. Within a single
    // leaf the writer guarantees strict-sorted unique keys (§7.3, §8
    // invariant 4); duplicates only ever surface across sibling leaves.
    let mut map = Map::new();
    for (k_off, v_off) in pairs {
        let k = match decode_value(buf, k_off, body_end, ctx)? {
            Value::String(s) => s,
            _ => return Err(ReadError::UnknownTag(buf[k_off])),
        };
        let v = decode_value(buf, v_off, body_end, ctx)?;
        map.insert(k, v);
    }
    Ok(Value::Object(map))
}

fn collect_object_pairs(
    buf: &[u8],
    off: usize,
    body_end: usize,
    ctx: &mut Ctx<'_>,
    out: &mut Vec<(usize, usize)>,
) -> Result<u64, ReadError> {
    let tag = buf[off];
    let w = tag & 0x03;
    let width = 1usize << w;
    ctx.stats.width_histogram[w as usize] += 1;
    match tag & 0xFC {
        0x80 => {
            ctx.stats.leaf_nodes += 1;
            let mut p = off + 1;
            let n = read_varuint(buf, &mut p, body_end)?;
            if n == 0 {
                return Err(ReadError::EmptyLeafNotSingleton);
            }
            check_fanout(n, ctx.fanout_ceiling)?;
            let mut prev_key: Option<Vec<u8>> = None;
            let mut max_off: u64 = 0;
            for _ in 0..n {
                need(buf, p + 2 * width, body_end)?;
                let k_off = read_uintw(buf, p, width);
                p += width;
                let v_off = read_uintw(buf, p, width);
                p += width;
                check_postorder(off, k_off as usize)?;
                check_postorder(off, v_off as usize)?;
                max_off = max_off.max(k_off).max(v_off);
                let key_bytes = read_string_bytes(buf, k_off as usize, body_end)?;
                if let Some(prev) = &prev_key {
                    if key_bytes <= prev.as_slice() {
                        return Err(ReadError::UnsortedKeys);
                    }
                }
                prev_key = Some(key_bytes.to_vec());
                out.push((k_off as usize, v_off as usize));
            }
            check_minimal_width(max_off, w)?;
            Ok(n)
        }
        0x84 => {
            ctx.stats.internal_nodes += 1;
            let mut p = off + 1;
            let total = read_varuint(buf, &mut p, body_end)?;
            let m = read_varuint(buf, &mut p, body_end)?;
            if m < 2 {
                return Err(ReadError::InternalFanoutTooSmall);
            }
            check_fanout(m, ctx.fanout_ceiling)?;
            ctx.stats.max_internal_fanout = ctx.stats.max_internal_fanout.max(m);
            let mut sum = 0u64;
            let mut max_off: u64 = 0;
            let mut child_offs = Vec::with_capacity(m as usize);
            for _ in 0..m {
                need(buf, p + 8 + 3 * width, body_end)?;
                let sub = u64::from_le_bytes(buf[p..p + 8].try_into().unwrap());
                p += 8;
                let key_lo_off = read_uintw(buf, p, width);
                p += width;
                let key_hi_off = read_uintw(buf, p, width);
                p += width;
                let child = read_uintw(buf, p, width);
                p += width;
                check_postorder(off, child as usize)?;
                check_postorder(off, key_lo_off as usize)?;
                check_postorder(off, key_hi_off as usize)?;
                max_off = max_off.max(child).max(key_lo_off).max(key_hi_off);
                sum += sub;
                child_offs.push((sub, child as usize));
            }
            if sum != total {
                return Err(ReadError::SubTotalMismatch);
            }
            check_minimal_width(max_off, w)?;
            for (sub, c) in child_offs {
                let before = out.len() as u64;
                let got = collect_object_pairs(buf, c, body_end, ctx, out)?;
                if got != sub || (out.len() as u64 - before) != sub {
                    return Err(ReadError::SubTotalMismatch);
                }
            }
            Ok(total)
        }
        _ => Err(ReadError::UnknownTag(tag)),
    }
}

fn smallest_w(max_off: u64) -> u8 {
    if max_off <= u8::MAX as u64 {
        0
    } else if max_off <= u16::MAX as u64 {
        1
    } else if max_off <= u32::MAX as u64 {
        2
    } else {
        3
    }
}

fn check_minimal_width(max_off: u64, declared: u8) -> Result<(), ReadError> {
    let needed = smallest_w(max_off);
    if declared != needed {
        Err(ReadError::NonMinimalWidth { declared, needed })
    } else {
        Ok(())
    }
}

fn check_postorder(node_off: usize, child_off: usize) -> Result<(), ReadError> {
    if child_off >= node_off {
        Err(ReadError::PostorderViolation {
            node_off,
            child_off,
        })
    } else {
        Ok(())
    }
}

fn check_fanout(m: u64, ceiling: Option<u64>) -> Result<(), ReadError> {
    if let Some(c) = ceiling {
        if m > c {
            return Err(ReadError::FanoutExceeded { m, ceiling: c });
        }
    }
    Ok(())
}

fn read_string_bytes(buf: &[u8], off: usize, body_end: usize) -> Result<&[u8], ReadError> {
    if off >= body_end {
        return Err(ReadError::OffsetOutOfBounds);
    }
    let tag = buf[off];
    match tag {
        0x60..=0x6E => {
            let len = (tag - 0x60) as usize + 1;
            need(buf, off + 1 + len, body_end)?;
            Ok(&buf[off + 1..off + 1 + len])
        }
        0x6F => {
            let mut p = off + 1;
            let len = read_varuint(buf, &mut p, body_end)? as usize;
            need(buf, p + len, body_end)?;
            Ok(&buf[p..p + len])
        }
        _ => Err(ReadError::UnknownTag(tag)),
    }
}

fn read_varuint(buf: &[u8], p: &mut usize, body_end: usize) -> Result<u64, ReadError> {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    for i in 0..10 {
        if *p >= body_end {
            return Err(ReadError::Truncated);
        }
        let b = buf[*p];
        *p += 1;
        v |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(v);
        }
        shift += 7;
        if i == 9 {
            return Err(ReadError::OverlongVarint);
        }
    }
    Err(ReadError::OverlongVarint)
}

fn read_uintw(buf: &[u8], p: usize, width: usize) -> u64 {
    let mut bytes = [0u8; 8];
    bytes[..width].copy_from_slice(&buf[p..p + width]);
    u64::from_le_bytes(bytes)
}

fn need(_buf: &[u8], end_exclusive: usize, body_end: usize) -> Result<(), ReadError> {
    if end_exclusive > body_end {
        Err(ReadError::OffsetOutOfBounds)
    } else {
        Ok(())
    }
}
