//! Error-path tests for `WriteError` variants reachable through the public API.
//!
//! Some variants (`MisuseObjectKey`, `MisuseObjectValue`, `AlreadyFinished`,
//! `IntegerOutOfRange`) are not reachable today: the builder borrow-checker
//! prevents misuse, `finish()` consumes the writer, and `push_i64`/`push_u64`
//! cover exactly the spec range. Those would only fire if a future API
//! exposed `i128` or raw-frame manipulation.
//!
//! Each test follows the same shape: drive the writer to the failure point,
//! capture the result, assert the variant.

mod common;

use kahon::{WriteError, Writer, WriterOptions};

#[test]
fn finish_with_no_root_returns_empty_document() {
    let mut buf = Vec::new();
    let w = Writer::new(&mut buf);
    let res = w.finish();
    assert!(
        matches!(res, Err(WriteError::EmptyDocument)),
        "expected EmptyDocument, got {:?}",
        res.err()
    );
}

#[test]
fn second_top_level_push_returns_multiple_root_values() {
    let mut buf = Vec::new();
    let mut w = Writer::new(&mut buf);
    w.push_i64(1).unwrap();
    let res = w.push_i64(2);
    assert!(
        matches!(res, Err(WriteError::MultipleRootValues)),
        "expected MultipleRootValues, got {:?}",
        res.err()
    );
}

#[test]
fn duplicate_key_within_run_caught_at_close() {
    let mut buf = Vec::new();
    let mut w = Writer::new(&mut buf);
    let mut o = w.start_object();
    o.push_i64("a", 1).unwrap();
    o.push_i64("a", 2).unwrap();
    let res = o.end();
    assert!(
        matches!(res, Err(WriteError::DuplicateKey)),
        "expected DuplicateKey, got {:?}",
        res.err()
    );
}

// Cross-run duplicate keys are the caller's contract per lib.rs §"Key-uniqueness
// contract": the writer enforces within-run uniqueness only. We document the
// contract by asserting the writer accepts the input and the strict reader
// rejects the resulting file.
#[test]
fn cross_run_duplicate_produces_non_conforming_file() {
    let mut buf = Vec::new();
    let opts = WriterOptions {
        object_sort_window: 1, // every key flushes its own run
        ..Default::default()
    };
    {
        let mut w = Writer::with_options(&mut buf, opts).unwrap();
        let mut o = w.start_object();
        o.push_i64("a", 1).unwrap();
        o.push_i64("a", 2).unwrap();
        o.end().unwrap();
        w.finish().unwrap();
    }
    let res = common::reader::decode(&buf);
    assert!(
        matches!(res, Err(common::reader::ReadError::DuplicateKey(_))),
        "expected reader DuplicateKey, got {:?}",
        res.map(|d| d.value)
    );
}
