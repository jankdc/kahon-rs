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
    // push_extension at root, then [1] as the payload. Per spec §9, the
    // payload's type-code byte must be at ext_off + 1 + E. Because a
    // container's root B+tree node is emitted last in postorder, the
    // writer wraps the original root in a single-entry internal node and
    // places the ext bytes immediately before that wrapper - so the byte
    // at ext_off + 1 is the wrapper's tag.
    let buf = doc(|w| {
        w.push_extension(5)?;
        w.begin_array()?;
        w.push_i64(1)?;
        w.end_array()
    });
    // 14            -- int 1 at offset 6
    // 70 01 06      -- original array leaf at offset 7, references int
    // C5            -- ext id 5 at offset 10 (slot points here)
    // 74 01 01 ..   -- single-entry wrapper internal at offset 11
    assert_eq!(
        body(&buf),
        &[
            0x14, 0x70, 0x01, 0x06, 0xC5, 0x74, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x07,
        ]
    );
    assert_eq!(root_offset(&buf), 10);
}

#[test]
fn nested_exts_across_frames_keep_separate_pending() {
    // push_extension at root, begin_array, push_extension (array slot's
    // wrapper), push_int. Outer ext wraps the array; inner ext wraps the
    // int. The inner ext is the array's leaf entry (scalar payload, ext
    // adjacent to int). The outer ext sits adjacent to a single-entry
    // internal-node wrapper of the array root.
    let buf = doc(|w| {
        w.push_extension(0)?;
        w.begin_array()?;
        w.push_extension(1)?;
        w.push_i64(2)?;
        w.end_array()
    });
    // C1 15         -- inner ext at 6, int 2 at 7
    // 70 01 06      -- array leaf at 8, child = inner ext
    // C0            -- outer ext at 11 (slot points here)
    // 74 ..         -- wrapper internal at 12
    assert_eq!(
        body(&buf),
        &[
            0xC1, 0x15, 0x70, 0x01, 0x06, 0xC0, 0x74, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x08,
        ]
    );
    assert_eq!(root_offset(&buf), 11);
}

#[test]
fn ext_wrapping_object_payload_lands_at_ext_off_plus_one() {
    // Regression for the v0.6.0 bug: push_extension followed by an
    // object payload used to record ext_off as the slot offset while
    // emitting the ext tag in front of the object's first child rather
    // than its root tag. Reading ext.payload() then handed back the key
    // string instead of the object.
    //
    // With the fix, the ext bytes sit immediately before the wrapper
    // internal-node tag, so the reader sees:
    //   slot -> ext tag -> wrapper tag -> object root -> {a: hi}
    let buf = doc(|w| {
        w.push_extension(0)?;
        w.begin_object()?;
        w.push_key("a")?;
        w.push_str("hi")?;
        w.end_object()
    });
    // 60 61         -- key "a" at offset 6
    // 61 68 69      -- value "hi" at offset 8
    // 80 01 06 08   -- object leaf at offset 11 (key_off=6, val_off=8)
    // C0            -- ext id 0 at offset 15 (slot points here)
    // 84 ..         -- wrapper object internal at offset 16, child = leaf at 11
    assert_eq!(
        body(&buf),
        &[
            0x60, 0x61, 0x61, 0x68, 0x69, 0x80, 0x01, 0x06, 0x08, 0xC0, 0x84, 0x01, 0x01, 0x01,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x06, 0x0B,
        ]
    );
    assert_eq!(root_offset(&buf), 15);
    // Spec §9 invariant: byte at ext_off + 1 is the payload's type-code byte.
    let ext_off = root_offset(&buf) as usize;
    assert_eq!(buf[ext_off], 0xC0);
    assert_eq!(buf[ext_off + 1] & 0xFC, 0x84); // OBJECT_INTERNAL_TAG (low 2 bits = width)
}

#[test]
fn ext_wrapping_empty_object_places_ext_before_singleton_tag() {
    // No fields = empty-object singleton. The ext bytes must still sit
    // immediately before the EMPTY_OBJECT tag so a reader walking
    // ext.payload() sees 0x34, not stray pre-ext bytes.
    let buf = doc(|w| {
        w.push_extension(7)?;
        w.begin_object()?;
        w.end_object()
    });
    // C7 34 -- ext at 6, EMPTY_OBJECT at 7.
    assert_eq!(body(&buf), &[0xC7, 0x34]);
    assert_eq!(root_offset(&buf), 6);
}

#[test]
fn ext_wrapping_empty_array_places_ext_before_singleton_tag() {
    let buf = doc(|w| {
        w.push_extension(2)?;
        w.begin_array()?;
        w.end_array()
    });
    // C2 33 -- ext at 6, EMPTY_ARRAY at 7.
    assert_eq!(body(&buf), &[0xC2, 0x33]);
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

#[test]
fn pending_ext_defers_bytes_written_and_shows_in_buffered_bytes() {
    let mut buf: Vec<u8> = Vec::new();
    let mut w = RawWriter::new(&mut buf);
    let pos_before = w.bytes_written();
    let buf_before = w.buffered_bytes();

    w.push_extension(0).unwrap();
    assert_eq!(
        w.bytes_written(),
        pos_before,
        "push_extension must not advance bytes_written; ext bytes are deferred"
    );
    assert!(
        w.buffered_bytes() > buf_before,
        "buffered_bytes must include the deferred ext header (was {}, now {})",
        buf_before,
        w.buffered_bytes()
    );

    // Stacking another ext at the same depth grows the deferred buffer
    // further but still does not advance bytes_written.
    let buf_one = w.buffered_bytes();
    w.push_extension(1).unwrap();
    assert_eq!(w.bytes_written(), pos_before);
    assert!(w.buffered_bytes() > buf_one);

    // Once the payload lands, bytes_written advances by ext bytes + scalar.
    w.push_i64(0).unwrap();
    let advanced = w.bytes_written() - pos_before;
    // Two TinyExt bytes (one each for ids 0 and 1) plus one scalar tag.
    assert_eq!(advanced, 3);

    w.finish().unwrap();
}

#[test]
fn pending_ext_for_object_payload_appears_in_buffered_bytes_across_frame() {
    let mut buf: Vec<u8> = Vec::new();
    let mut w = RawWriter::new(&mut buf);

    let before_ext = w.buffered_bytes();
    w.push_extension(0).unwrap();
    let with_ext_pending = w.buffered_bytes();
    assert!(
        with_ext_pending > before_ext,
        "deferred ext header must appear in buffered_bytes ({} -> {})",
        before_ext,
        with_ext_pending
    );

    w.begin_object().unwrap();
    let inside_open_object = w.buffered_bytes();
    assert!(
        inside_open_object >= with_ext_pending,
        "pending ext at outer depth must persist while object is open"
    );

    w.push_key("a").unwrap();
    w.push_str("hi").unwrap();

    w.end_object().unwrap();
    w.finish().unwrap();

    let mut control_buf: Vec<u8> = Vec::new();
    {
        let mut c = RawWriter::new(&mut control_buf);
        c.begin_object().unwrap();
        c.push_key("a").unwrap();
        c.push_str("hi").unwrap();
        c.end_object().unwrap();
        c.finish().unwrap();
    }
    // ext-wrapped doc must be larger by exactly the ext byte (1) + the
    // single-entry object internal wrapper (14 bytes for w=0 here).
    assert_eq!(buf.len(), control_buf.len() + 1 + 14);
}
