mod common;

use common::encode::{body, build, root_byte, root_offset};
use kahon::{WriteError, Writer};

#[test]
fn integer_width_selection() {
    // (value, expected tag byte). Covers SmallInt, UInt8/16/32, Int8/16.
    let cases: &[(i128, u8)] = &[
        (0, 0x13),
        (31, 0x32),
        (-1, 0x03),
        (-16, 0x12),
        (32, 0x40), // UInt8
        (255, 0x40),
        (256, 0x41), // UInt16
        (65535, 0x41),
        (65536, 0x42), // UInt32
        (-17, 0x44),   // Int8
        (-128, 0x44),
        (-129, 0x45), // Int16
    ];
    for &(v, tag) in cases {
        let buf = build(|w| {
            if v >= 0 {
                w.push_u64(v as u64).unwrap();
            } else {
                w.push_i64(v as i64).unwrap();
            }
        });
        assert_eq!(root_byte(&buf), tag, "value {} expected tag {:#x}", v, tag);
    }
}

#[test]
fn integer_range_boundaries_accepted() {
    // integer range is [-2^63, 2^64-1]. Both endpoints must encode.
    for &v in &[u64::MAX, 0, i64::MAX as u64] {
        let mut buf = Vec::new();
        let mut w = Writer::new(&mut buf);
        w.push_u64(v).unwrap();
        w.finish().unwrap();
    }
    for &v in &[i64::MIN, -1, 0, i64::MAX] {
        let mut buf = Vec::new();
        let mut w = Writer::new(&mut buf);
        w.push_i64(v).unwrap();
        w.finish().unwrap();
    }
}

#[test]
fn integer_root_scalar_emits_tag_then_payload() {
    // UInt8 tag (0x40) + raw byte 0x2A.
    let buf = build(|w| w.push_i64(42).unwrap());
    assert_eq!(body(&buf), &[0x40, 0x2A]);
}

#[test]
fn float_narrows_to_f32_when_round_trip_exact() {
    let buf = build(|w| w.push_f64(2.0).unwrap());
    assert_eq!(root_byte(&buf), 0x50, "2.0 must encode as Float32");
}

#[test]
fn float_stays_f64_when_round_trip_lossy() {
    let buf = build(|w| w.push_f64(0.1).unwrap());
    assert_eq!(root_byte(&buf), 0x51, "0.1 must encode as Float64");
}

#[test]
fn float_negative_zero_preserved_as_f32() {
    let buf = build(|w| w.push_f64(-0.0).unwrap());
    assert_eq!(root_byte(&buf), 0x50);
    let off = root_offset(&buf) as usize;
    assert_eq!(&buf[off + 1..off + 5], &[0x00, 0x00, 0x00, 0x80]);
}

#[test]
fn float_nan_rejected() {
    let mut buf = Vec::new();
    let mut w = Writer::new(&mut buf);
    assert!(matches!(
        w.push_f64(f64::NAN),
        Err(WriteError::NaNOrInfinity)
    ));
}

#[test]
fn float_infinity_rejected() {
    let mut buf = Vec::new();
    let mut w = Writer::new(&mut buf);
    assert!(matches!(
        w.push_f64(f64::INFINITY),
        Err(WriteError::NaNOrInfinity)
    ));
    assert!(matches!(
        w.push_f64(f64::NEG_INFINITY),
        Err(WriteError::NaNOrInfinity)
    ));
}

#[test]
fn string_empty_uses_generic_tag_with_zero_length() {
    let buf = build(|w| w.push_str("").unwrap());
    assert_eq!(body(&buf), &[0x6F, 0x00]);
}

#[test]
fn string_one_byte_uses_tinystring_tag() {
    let buf = build(|w| w.push_str("a").unwrap());
    assert_eq!(body(&buf), &[0x60, 0x61]);
}

#[test]
fn string_fifteen_bytes_uses_max_tinystring_tag() {
    let s = "a".repeat(15);
    let buf = build(|w| w.push_str(&s).unwrap());
    assert_eq!(body(&buf)[0], 0x6E);
}

#[test]
fn string_sixteen_bytes_promotes_to_generic_tag() {
    let s = "a".repeat(16);
    let buf = build(|w| w.push_str(&s).unwrap());
    assert_eq!(body(&buf)[0], 0x6F);
    assert_eq!(body(&buf)[1], 0x10); // varuint length 16
}
