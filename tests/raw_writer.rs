//! Tests for the flat `RawWriter` API.

mod common;

use common::reader;
use kahon::raw::RawWriter;
use kahon::{WriteError, Writer};
use serde_json::json;

#[test]
fn flat_matches_builder_byte_for_byte() {
    let mut buf_builder: Vec<u8> = Vec::new();
    {
        let w = Writer::new(&mut buf_builder);
        let mut o = w.start_object();
        o.push_i64("hp", 80).unwrap();
        o.push_bool("enraged", true).unwrap();
        {
            let mut a = o.start_array("weapons").unwrap();
            a.push_str("fist").unwrap();
            {
                let mut inner = a.start_object();
                inner.push_str("name", "great axe").unwrap();
                inner.push_i64("damage", 15).unwrap();
                inner.end().unwrap();
            }
            a.end().unwrap();
        }
        let w = o.end().unwrap();
        w.finish().unwrap();
    }

    let mut buf_flat: Vec<u8> = Vec::new();
    {
        let mut r = RawWriter::new(&mut buf_flat);
        r.begin_object().unwrap();
        r.push_key("hp").unwrap();
        r.push_i64(80).unwrap();
        r.push_key("enraged").unwrap();
        r.push_bool(true).unwrap();
        r.push_key("weapons").unwrap();
        r.begin_array().unwrap();
        r.push_str("fist").unwrap();
        r.begin_object().unwrap();
        r.push_key("name").unwrap();
        r.push_str("great axe").unwrap();
        r.push_key("damage").unwrap();
        r.push_i64(15).unwrap();
        r.end_object().unwrap();
        r.end_array().unwrap();
        r.end_object().unwrap();
        r.finish().unwrap();
    }

    assert_eq!(buf_builder, buf_flat);
    let decoded = reader::decode(&buf_flat).unwrap().value;
    assert_eq!(
        decoded,
        json!({
            "hp": 80,
            "enraged": true,
            "weapons": ["fist", { "name": "great axe", "damage": 15 }]
        })
    );
}

#[test]
fn end_array_when_object_open_errors() {
    let mut buf: Vec<u8> = Vec::new();
    let mut r = RawWriter::new(&mut buf);
    r.begin_object().unwrap();
    let err = r.end_array().unwrap_err();
    assert!(matches!(err, WriteError::FrameMismatch));
}

#[test]
fn end_object_when_array_open_errors() {
    let mut buf: Vec<u8> = Vec::new();
    let mut r = RawWriter::new(&mut buf);
    r.begin_array().unwrap();
    let err = r.end_object().unwrap_err();
    assert!(matches!(err, WriteError::FrameMismatch));
}

#[test]
fn end_array_with_no_open_frame_errors() {
    let mut buf: Vec<u8> = Vec::new();
    let mut r = RawWriter::new(&mut buf);
    let err = r.end_array().unwrap_err();
    assert!(matches!(err, WriteError::FrameMismatch));
}

#[test]
fn push_key_outside_object_errors() {
    let mut buf: Vec<u8> = Vec::new();
    let mut r = RawWriter::new(&mut buf);
    let err = r.push_key("nope").unwrap_err();
    assert!(matches!(err, WriteError::KeyOutsideObject));

    r.begin_array().unwrap();
    let err = r.push_key("nope").unwrap_err();
    assert!(matches!(err, WriteError::KeyOutsideObject));
}

#[test]
fn rollback_reverts_speculative_writes() {
    let mut buf: Vec<u8> = Vec::new();
    let mut r = RawWriter::new(&mut buf);
    r.begin_array().unwrap();
    r.push_i64(1).unwrap();

    let cp = r.checkpoint();
    r.push_i64(999).unwrap();
    r.push_i64(1000).unwrap();
    r.rollback(cp).unwrap();

    r.push_i64(2).unwrap();
    r.end_array().unwrap();
    r.finish().unwrap();

    let decoded = reader::decode(&buf).unwrap().value;
    assert_eq!(decoded, json!([1, 2]));
}

#[test]
fn rollback_reverts_open_frame() {
    let mut buf: Vec<u8> = Vec::new();
    let mut r = RawWriter::new(&mut buf);
    r.begin_object().unwrap();
    r.push_key("kept").unwrap();
    r.push_i64(1).unwrap();

    let cp = r.checkpoint();
    // Open a nested array and partially fill it, then roll back.
    r.push_key("scratch").unwrap();
    r.begin_array().unwrap();
    r.push_i64(99).unwrap();
    r.rollback(cp).unwrap();

    // After rollback we should be back inside the object with no nested frame.
    r.push_key("also").unwrap();
    r.push_i64(2).unwrap();
    r.end_object().unwrap();
    r.finish().unwrap();

    let decoded = reader::decode(&buf).unwrap().value;
    assert_eq!(decoded, json!({ "kept": 1, "also": 2 }));
}

#[test]
fn rollback_matches_builder_try_write_semantics() {
    // Same speculative work, one via `try_write` on a builder, one via
    // checkpoint/rollback on RawWriter. Bytes must match.
    #[derive(Debug)]
    struct MyErr;
    impl From<WriteError> for MyErr {
        fn from(_: WriteError) -> Self {
            MyErr
        }
    }

    let mut buf_a: Vec<u8> = Vec::new();
    {
        let w = Writer::new(&mut buf_a);
        let mut a = w.start_array();
        a.push_i64(1).unwrap();
        let _: Result<(), MyErr> = a.try_write(|a| {
            a.push_i64(99)?;
            Err(MyErr)
        });
        a.push_i64(2).unwrap();
        let w = a.end().unwrap();
        w.finish().unwrap();
    }

    let mut buf_b: Vec<u8> = Vec::new();
    {
        let mut r = RawWriter::new(&mut buf_b);
        r.begin_array().unwrap();
        r.push_i64(1).unwrap();
        let cp = r.checkpoint();
        r.push_i64(99).unwrap();
        r.rollback(cp).unwrap();
        r.push_i64(2).unwrap();
        r.end_array().unwrap();
        r.finish().unwrap();
    }

    assert_eq!(buf_a, buf_b);
    let decoded = reader::decode(&buf_b).unwrap().value;
    assert_eq!(decoded, json!([1, 2]));
}

#[test]
fn raw_byte_for_byte_matches_builder() {
    // Writing the same logical document via the builder API and the flat
    // API must produce byte-identical output.
    let mut buf_pure: Vec<u8> = Vec::new();
    {
        let w = Writer::new(&mut buf_pure);
        let mut o = w.start_object();
        o.push_i64("a", 1).unwrap();
        {
            let mut a = o.start_array("nested").unwrap();
            a.push_i64(2).unwrap();
            a.push_i64(3).unwrap();
            a.end().unwrap();
        }
        let w = o.end().unwrap();
        w.finish().unwrap();
    }

    let mut buf_raw: Vec<u8> = Vec::new();
    {
        let mut r = RawWriter::new(&mut buf_raw);
        r.begin_object().unwrap();
        r.push_key("a").unwrap();
        r.push_i64(1).unwrap();
        r.push_key("nested").unwrap();
        r.begin_array().unwrap();
        r.push_i64(2).unwrap();
        r.push_i64(3).unwrap();
        r.end_array().unwrap();
        r.end_object().unwrap();
        r.finish().unwrap();
    }

    assert_eq!(buf_raw, buf_pure);
}

#[test]
fn finish_with_open_frame_errors() {
    let mut buf: Vec<u8> = Vec::new();
    let mut r = RawWriter::new(&mut buf);
    r.begin_array().unwrap();
    r.push_i64(1).unwrap();
    // Don't close the array.
    let err = r.finish().unwrap_err();
    assert!(matches!(err, WriteError::Poisoned));
}
