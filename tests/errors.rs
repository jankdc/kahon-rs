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
