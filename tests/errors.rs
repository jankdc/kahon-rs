//! Error-path tests for `WriteError` variants reachable through the public API.
//!
//! With the typestate-guarded `Writer`, the "no root pushed" and "second
//! root pushed" cases are compile-time errors (no `finish` on
//! `Writer<Empty>`, no `push_*` on `Writer<Filled>`), so they live in
//! the trybuild compile-fail suite rather than here. The corresponding
//! runtime variants `WriteError::EmptyDocument` and
//! `WriteError::MultipleRootValues` remain reachable through
//! `RawWriter`; see `tests/raw_writer.rs`.

use kahon::raw::RawWriter;
use kahon::WriteError;

#[test]
fn raw_finish_with_no_root_returns_empty_document() {
    let mut buf = Vec::new();
    let r = RawWriter::new(&mut buf);
    let res = r.finish();
    assert!(
        matches!(res, Err(WriteError::EmptyDocument)),
        "expected EmptyDocument, got {:?}",
        res.err()
    );
}

#[test]
fn raw_second_top_level_push_returns_multiple_root_values() {
    let mut buf = Vec::new();
    let mut r = RawWriter::new(&mut buf);
    r.push_i64(1).unwrap();
    let res = r.push_i64(2);
    assert!(
        matches!(res, Err(WriteError::MultipleRootValues)),
        "expected MultipleRootValues, got {:?}",
        res.err()
    );
}

#[test]
fn raw_begin_array_after_root_returns_multiple_root_values() {
    let mut buf = Vec::new();
    let mut r = RawWriter::new(&mut buf);
    r.push_i64(1).unwrap();
    let res = r.begin_array();
    assert!(
        matches!(res, Err(WriteError::MultipleRootValues)),
        "expected MultipleRootValues, got {:?}",
        res.err()
    );
}

#[test]
fn raw_begin_object_after_root_returns_multiple_root_values() {
    let mut buf = Vec::new();
    let mut r = RawWriter::new(&mut buf);
    r.push_i64(1).unwrap();
    let res = r.begin_object();
    assert!(
        matches!(res, Err(WriteError::MultipleRootValues)),
        "expected MultipleRootValues, got {:?}",
        res.err()
    );
}

#[test]
fn raw_second_root_across_scalar_variants_returns_multiple_root_values() {
    type Push = fn(&mut RawWriter<&mut Vec<u8>>) -> Result<(), WriteError>;
    let pushes: &[(&str, Push)] = &[
        ("null", |w| w.push_null()),
        ("bool", |w| w.push_bool(true)),
        ("i64", |w| w.push_i64(1)),
        ("u64", |w| w.push_u64(1)),
        ("f64", |w| w.push_f64(1.0)),
        ("str", |w| w.push_str("x")),
    ];
    for (a_name, a) in pushes {
        for (b_name, b) in pushes {
            let mut buf = Vec::new();
            let mut r = RawWriter::new(&mut buf);
            a(&mut r).unwrap();
            let res = b(&mut r);
            assert!(
                matches!(res, Err(WriteError::MultipleRootValues)),
                "{a_name} then {b_name}: expected MultipleRootValues, got {:?}",
                res.err()
            );
        }
    }
}

#[test]
fn raw_scalar_then_container_returns_multiple_root_values() {
    let mut buf = Vec::new();
    let mut r = RawWriter::new(&mut buf);
    r.push_str("root").unwrap();
    assert!(matches!(
        r.begin_array(),
        Err(WriteError::MultipleRootValues)
    ));
    assert!(matches!(
        r.begin_object(),
        Err(WriteError::MultipleRootValues)
    ));
}

#[test]
fn raw_container_then_scalar_returns_multiple_root_values() {
    let mut buf = Vec::new();
    let mut r = RawWriter::new(&mut buf);
    r.begin_object().unwrap();
    r.end_object().unwrap();
    let res = r.push_bool(false);
    assert!(
        matches!(res, Err(WriteError::MultipleRootValues)),
        "expected MultipleRootValues, got {:?}",
        res.err()
    );
}

#[test]
fn raw_begin_array_after_closed_container_returns_multiple_root_values() {
    let mut buf = Vec::new();
    let mut r = RawWriter::new(&mut buf);
    r.begin_array().unwrap();
    r.end_array().unwrap();
    let res = r.begin_array();
    assert!(
        matches!(res, Err(WriteError::MultipleRootValues)),
        "expected MultipleRootValues, got {:?}",
        res.err()
    );
}
