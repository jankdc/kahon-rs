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
fn tinyext_null_matches_vector() {
    // ext/tinyext-null: TinyExt id 0 wrapping Null
    // bytes_hex: 4B41484E 0200 C0 00 0600000000000000 4B41484E
    let buf = doc(|w| {
        w.push_extension(0)?;
        w.push_null()
    });
    assert_eq!(body(&buf), &[0xC0, 0x00]);
    assert_eq!(root_offset(&buf), 6);
}

#[test]
fn tinyext_int_matches_vector() {
    // ext/tinyext-int: id 3 wrapping integer 1 → C3 14
    let buf = doc(|w| {
        w.push_extension(3)?;
        w.push_i64(1)
    });
    assert_eq!(body(&buf), &[0xC3, 0x14]);
    assert_eq!(root_offset(&buf), 6);
}

#[test]
fn tinyext_max_id_matches_vector() {
    // ext/tinyext-max-id: id 15 → CF 14
    let buf = doc(|w| {
        w.push_extension(15)?;
        w.push_i64(1)
    });
    assert_eq!(body(&buf), &[0xCF, 0x14]);
}

#[test]
fn generic_ext_id_16_matches_vector() {
    // ext/generic-id-16: smallest id that requires the generic form
    // → D0 10 14
    let buf = doc(|w| {
        w.push_extension(16)?;
        w.push_i64(1)
    });
    assert_eq!(body(&buf), &[0xD0, 0x10, 0x14]);
}

#[test]
fn generic_ext_large_id_uses_varuint() {
    // id 300 = 0xAC 0x02 in LEB128
    let buf = doc(|w| {
        w.push_extension(300)?;
        w.push_i64(1)
    });
    assert_eq!(body(&buf), &[0xD0, 0xAC, 0x02, 0x14]);
}

#[test]
fn ext_inside_array_matches_vector() {
    // ext/in-array: [<TinyExt 0 of 1>] → C0 14 70 01 06
    // root is the array (offset 8); array slot[0] = 6 (the ext tag).
    let buf = doc(|w| {
        w.begin_array()?;
        w.push_extension(0)?;
        w.push_i64(1)?;
        w.end_array()
    });
    assert_eq!(body(&buf), &[0xC0, 0x14, 0x70, 0x01, 0x06]);
    assert_eq!(root_offset(&buf), 8);
}

#[test]
fn nested_exts_at_same_depth_collapse_to_outer_offset() {
    // push_extension, push_extension, push_int → outer (e1) wraps inner
    // (e2) wraps int. Bytes: C1 C2 14 ; root points at e1 = offset 6.
    let buf = doc(|w| {
        w.push_extension(1)?;
        w.push_extension(2)?;
        w.push_i64(1)
    });
    assert_eq!(body(&buf), &[0xC1, 0xC2, 0x14]);
    assert_eq!(root_offset(&buf), 6);
}

#[test]
fn ext_wrapping_array_makes_root_the_ext() {
    // push_extension at root, then [1] as the payload. Root is the ext
    // tag; the array tag follows, with its own offsets unchanged.
    let buf = doc(|w| {
        w.push_extension(5)?;
        w.begin_array()?;
        w.push_i64(1)?;
        w.end_array()
    });
    // C5 14 70 01 07  -- ext at 6, int at 7, array at 8.
    assert_eq!(body(&buf), &[0xC5, 0x14, 0x70, 0x01, 0x07]);
    assert_eq!(root_offset(&buf), 6);
}

#[test]
fn nested_exts_across_frames_keep_separate_pending() {
    // push_extension at root, begin_array, push_extension (array slot's
    // wrapper), push_int. Outer ext's payload is the array; inner ext's
    // payload is the int. Array slot must point at inner ext (offset 8),
    // not the int. Document root must point at outer ext (offset 6),
    // not the array.
    let buf = doc(|w| {
        w.push_extension(0)?;
        w.begin_array()?;
        w.push_extension(1)?;
        w.push_i64(2)?;
        w.end_array()
    });
    assert_eq!(body(&buf), &[0xC0, 0xC1, 0x15, 0x70, 0x01, 0x07]);
    assert_eq!(root_offset(&buf), 6);
}

#[test]
fn finish_with_pending_root_ext_errors() {
    let mut buf: Vec<u8> = Vec::new();
    let mut w = RawWriter::new(&mut buf);
    w.push_extension(0).unwrap();
    let err = w.finish().unwrap_err();
    assert!(
        matches!(err, WriteError::ExtensionWithoutPayload),
        "{err:?}"
    );
}

#[test]
fn end_array_with_pending_ext_errors_and_poisons() {
    let mut buf: Vec<u8> = Vec::new();
    let mut w = RawWriter::new(&mut buf);
    w.begin_array().unwrap();
    w.push_extension(0).unwrap();
    let err = w.end_array().unwrap_err();
    assert!(
        matches!(err, WriteError::ExtensionWithoutPayload),
        "{err:?}"
    );
    // Subsequent ops fail with Poisoned.
    assert!(matches!(w.push_i64(1), Err(WriteError::Poisoned)));
}

#[test]
fn push_key_with_pending_ext_errors() {
    // Inside an object, push_key after a key+ext (where ext awaits
    // payload) is invalid - the ext's payload must be a value, not
    // another key.
    let mut buf: Vec<u8> = Vec::new();
    let mut w = RawWriter::new(&mut buf);
    w.begin_object().unwrap();
    w.push_key("k").unwrap();
    w.push_extension(0).unwrap();
    let err = w.push_key("k2").unwrap_err();
    assert!(
        matches!(err, WriteError::ExtensionWithoutPayload),
        "{err:?}"
    );
}

#[test]
fn second_root_after_completed_ext_errors() {
    let mut buf: Vec<u8> = Vec::new();
    let mut w = RawWriter::new(&mut buf);
    w.push_extension(0).unwrap();
    w.push_i64(1).unwrap();
    let err = w.push_extension(0).unwrap_err();
    assert!(matches!(err, WriteError::MultipleRootValues), "{err:?}");
}
