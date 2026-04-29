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

use kahon::{WriteError, Writer};

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

