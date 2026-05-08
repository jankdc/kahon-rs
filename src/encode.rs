//! Scalar, string, and varuint encoders. All functions append to `buf`.

use crate::error::WriteError;
use crate::types::{
    FALSE, FLOAT32, FLOAT64, INT16, INT32, INT64, INT8, NULL, STRING, SUM, TINY_NEG_INT,
    TINY_STRING, TINY_SUM, TINY_UINT, TRUE, UINT16, UINT32, UINT64, UINT8,
};

// Varuint continuation bit and payload mask
const VARUINT_CONT: u8 = 0x80;
const VARUINT_MASK: u8 = 0x7F;

pub(crate) fn write_null(buf: &mut Vec<u8>) {
    buf.push(NULL);
}

pub(crate) fn write_false(buf: &mut Vec<u8>) {
    buf.push(FALSE);
}

pub(crate) fn write_true(buf: &mut Vec<u8>) {
    buf.push(TRUE);
}

pub(crate) fn write_varuint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= VARUINT_CONT as u64 {
        buf.push(((v as u8) & VARUINT_MASK) | VARUINT_CONT);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Append `v` as a little-endian unsigned integer occupying `width` bytes.
/// `width` must be 1, 2, 4, or 8, and `v` must fit.
pub(crate) fn write_uintw(buf: &mut Vec<u8>, v: u64, width: usize) {
    let bytes = v.to_le_bytes();
    buf.extend_from_slice(&bytes[..width]);
}

/// Write an integer choosing the smallest tag.
///
/// `v` must lie in `[i64::MIN, u64::MAX]`. The public API enforces this
/// by construction (`push_i64`/`push_u64` widen from `i64`/`u64`).
pub(crate) fn write_integer(buf: &mut Vec<u8>, v: i128) -> Result<(), WriteError> {
    debug_assert!(v >= i128::from(i64::MIN) && v <= i128::from(u64::MAX));

    if (0..=31).contains(&v) {
        buf.push(TINY_UINT + v as u8);
        return Ok(());
    }
    if (-16..=-1).contains(&v) {
        buf.push((i128::from(TINY_NEG_INT) - v) as u8);
        return Ok(());
    }

    if v >= 0 {
        let u = v as u128; // fits since v <= u64::MAX
        if u <= u8::MAX as u128 {
            buf.push(UINT8);
            buf.push(u as u8);
        } else if u <= u16::MAX as u128 {
            buf.push(UINT16);
            buf.extend_from_slice(&(u as u16).to_le_bytes());
        } else if u <= u32::MAX as u128 {
            buf.push(UINT32);
            buf.extend_from_slice(&(u as u32).to_le_bytes());
        } else {
            buf.push(UINT64);
            buf.extend_from_slice(&(u as u64).to_le_bytes());
        }
    } else {
        // v in [i64::MIN, -17]
        let s = v as i64; // fits
        if s >= i8::MIN as i64 {
            buf.push(INT8);
            buf.push(s as i8 as u8);
        } else if s >= i16::MIN as i64 {
            buf.push(INT16);
            buf.extend_from_slice(&(s as i16).to_le_bytes());
        } else if s >= i32::MIN as i64 {
            buf.push(INT32);
            buf.extend_from_slice(&(s as i32).to_le_bytes());
        } else {
            buf.push(INT64);
            buf.extend_from_slice(&s.to_le_bytes());
        }
    }
    Ok(())
}

/// Write a float per §5.3. Narrows to f32 if bit-exact round-trip holds,
/// else f64. Rejects NaN/Infinity.
pub(crate) fn write_f64(buf: &mut Vec<u8>, v: f64) -> Result<(), WriteError> {
    if v.is_nan() || v.is_infinite() {
        return Err(WriteError::NaNOrInfinity);
    }
    // Narrowing check: f32 round-trip yielding bit-exact v, including sign of zero.
    let f32v = v as f32;
    let widened = f32v as f64;
    if widened.to_bits() == v.to_bits() {
        buf.push(FLOAT32);
        buf.extend_from_slice(&f32v.to_le_bytes());
    } else {
        buf.push(FLOAT64);
        buf.extend_from_slice(&v.to_le_bytes());
    }
    Ok(())
}

/// TinyString for lengths 1..=15, generic `STRING` otherwise.
pub(crate) fn write_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len();
    if (1..=15).contains(&len) {
        buf.push(TINY_STRING + (len as u8 - 1));
    } else {
        buf.push(STRING);
        write_varuint(buf, len as u64);
    }
    buf.extend_from_slice(bytes);
}

/// Write a sum header per §9. TinySum (0xC0..0xCF) for index ≤ 15,
/// generic Sum (0xD0) + varuint otherwise. Caller supplies the payload
/// value bytes immediately after.
pub(crate) fn write_sum(buf: &mut Vec<u8>, index: u64) {
    if index <= 15 {
        buf.push(TINY_SUM + index as u8);
    } else {
        buf.push(SUM);
        write_varuint(buf, index);
    }
}

/// Offset width code `w` and the corresponding byte count `W = 2^w`.
pub(crate) fn smallest_width(max_off: u64) -> (u8, usize) {
    if max_off <= u8::MAX as u64 {
        (0, 1)
    } else if max_off <= u16::MAX as u64 {
        (1, 2)
    } else if max_off <= u32::MAX as u64 {
        (2, 4)
    } else {
        (3, 8)
    }
}
