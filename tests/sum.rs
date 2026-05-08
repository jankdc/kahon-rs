mod common;

use kahon::raw::RawWriter;
use kahon::WriteError;

fn doc<F>(build: F) -> Vec<u8>
where
    F: FnOnce(&mut RawWriter<&mut Vec<u8>>) -> Result<(), WriteError>,
{
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = RawWriter::new(&mut buf);
        build(&mut w).unwrap();
        w.finish().unwrap();
    }
    buf
}

fn body(doc: &[u8]) -> &[u8] {
    &doc[6..doc.len() - 12]
}

fn root_offset(doc: &[u8]) -> u64 {
    let n = doc.len();
    u64::from_le_bytes(doc[n - 12..n - 4].try_into().unwrap())
}

#[test]
fn tinysum_null_matches_vector() {
    // sum/tinysum-null: TinySum index 0 wrapping Null
    // bytes_hex: 4B41484E 0200 C0 00 0600000000000000 4B41484E
    let buf = doc(|w| {
        w.push_sum(0)?;
        w.push_null()
    });
    assert_eq!(body(&buf), &[0xC0, 0x00]);
    assert_eq!(root_offset(&buf), 6);
}

#[test]
fn tinysum_int_matches_vector() {
    // sum/tinysum-int: index 3 wrapping integer 1 → C3 14
    let buf = doc(|w| {
        w.push_sum(3)?;
        w.push_i64(1)
    });
    assert_eq!(body(&buf), &[0xC3, 0x14]);
    assert_eq!(root_offset(&buf), 6);
}

#[test]
fn tinysum_max_index_matches_vector() {
    // sum/tinysum-max-index: index 15 → CF 14
    let buf = doc(|w| {
        w.push_sum(15)?;
        w.push_i64(1)
    });
    assert_eq!(body(&buf), &[0xCF, 0x14]);
}

#[test]
fn generic_sum_index_16_matches_vector() {
    // sum/generic-index-16: smallest index that requires the generic form
    // → D0 10 14
    let buf = doc(|w| {
        w.push_sum(16)?;
        w.push_i64(1)
    });
    assert_eq!(body(&buf), &[0xD0, 0x10, 0x14]);
}

#[test]
fn generic_sum_large_index_uses_varuint() {
    // index 300 = 0xAC 0x02 in LEB128
    let buf = doc(|w| {
        w.push_sum(300)?;
        w.push_i64(1)
    });
    assert_eq!(body(&buf), &[0xD0, 0xAC, 0x02, 0x14]);
}

#[test]
fn sum_inside_array_matches_vector() {
    // sum/in-array: [<TinySum 0 of 1>] → C0 14 70 01 06
    // root is the array (offset 8); array slot[0] = 6 (the sum tag).
    let buf = doc(|w| {
        w.begin_array()?;
        w.push_sum(0)?;
        w.push_i64(1)?;
        w.end_array()
    });
    assert_eq!(body(&buf), &[0xC0, 0x14, 0x70, 0x01, 0x06]);
    assert_eq!(root_offset(&buf), 8);
}

#[test]
fn nested_sums_at_same_depth_collapse_to_outer_offset() {
    // push_sum, push_sum, push_int → outer (s1) wraps inner (s2) wraps int.
    // Bytes: C1 C2 14 ; root points at s1 = offset 6.
    let buf = doc(|w| {
        w.push_sum(1)?;
        w.push_sum(2)?;
        w.push_i64(1)
    });
    assert_eq!(body(&buf), &[0xC1, 0xC2, 0x14]);
    assert_eq!(root_offset(&buf), 6);
}

#[test]
fn sum_wrapping_array_makes_root_the_sum() {
    // push_sum at root, then [1] as the payload. Root is the sum tag; the
    // array tag follows, with its own offsets unchanged.
    let buf = doc(|w| {
        w.push_sum(5)?;
        w.begin_array()?;
        w.push_i64(1)?;
        w.end_array()
    });
    // C5 14 70 01 07  -- sum at 6, int at 7, array at 8.
    assert_eq!(body(&buf), &[0xC5, 0x14, 0x70, 0x01, 0x07]);
    assert_eq!(root_offset(&buf), 6);
}

#[test]
fn nested_sums_across_frames_keep_separate_pending() {
    // push_sum at root, begin_array, push_sum (array slot's wrapper),
    // push_int. Outer sum's payload is the array; inner sum's payload is
    // the int. Array slot must point at inner sum (offset 8), not the int.
    // Document root must point at outer sum (offset 6), not the array.
    let buf = doc(|w| {
        w.push_sum(0)?;
        w.begin_array()?;
        w.push_sum(1)?;
        w.push_i64(2)?;
        w.end_array()
    });
    assert_eq!(body(&buf), &[0xC0, 0xC1, 0x15, 0x70, 0x01, 0x07]);
    assert_eq!(root_offset(&buf), 6);
}

#[test]
fn finish_with_pending_root_sum_errors() {
    let mut buf: Vec<u8> = Vec::new();
    let mut w = RawWriter::new(&mut buf);
    w.push_sum(0).unwrap();
    let err = w.finish().unwrap_err();
    assert!(matches!(err, WriteError::SumWithoutPayload), "{err:?}");
}

#[test]
fn end_array_with_pending_sum_errors_and_poisons() {
    let mut buf: Vec<u8> = Vec::new();
    let mut w = RawWriter::new(&mut buf);
    w.begin_array().unwrap();
    w.push_sum(0).unwrap();
    let err = w.end_array().unwrap_err();
    assert!(matches!(err, WriteError::SumWithoutPayload), "{err:?}");
    // Subsequent ops fail with Poisoned.
    assert!(matches!(w.push_i64(1), Err(WriteError::Poisoned)));
}

#[test]
fn push_key_with_pending_sum_errors() {
    // Inside an object, push_key after a key+sum (where sum awaits payload)
    // is invalid - the sum's payload must be a value, not another key.
    let mut buf: Vec<u8> = Vec::new();
    let mut w = RawWriter::new(&mut buf);
    w.begin_object().unwrap();
    w.push_key("k").unwrap();
    w.push_sum(0).unwrap();
    let err = w.push_key("k2").unwrap_err();
    assert!(matches!(err, WriteError::SumWithoutPayload), "{err:?}");
}

#[test]
fn second_root_after_completed_sum_errors() {
    let mut buf: Vec<u8> = Vec::new();
    let mut w = RawWriter::new(&mut buf);
    w.push_sum(0).unwrap();
    w.push_i64(1).unwrap();
    let err = w.push_sum(0).unwrap_err();
    assert!(matches!(err, WriteError::MultipleRootValues), "{err:?}");
}
