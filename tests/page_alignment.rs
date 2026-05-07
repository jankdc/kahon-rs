//! Verifies the disk-friendly writer policies introduced for spec §13:
//! `NodeSizing::TargetBytes` and `PageAlignment::Aligned`.

mod common;

use common::{driver, reader};
use kahon::{BuildPolicy, NodeSizing, PageAlignment, WriteError, Writer, WriterOptions};
use serde_json::{json, Value};

const PAGE: usize = 4096;

fn aligned_opts() -> WriterOptions {
    WriterOptions {
        policy: BuildPolicy::disk_aligned(PAGE),
        ..WriterOptions::default()
    }
}

fn big_doc() -> Value {
    let mut arr = Vec::with_capacity(2_000);
    for i in 0..2_000 {
        arr.push(json!({"k": i, "name": format!("item-{i}")}));
    }
    Value::Array(arr)
}

#[test]
fn aligned_file_size_is_page_multiple() {
    let bytes = driver::encode(&big_doc(), aligned_opts()).unwrap();
    assert_eq!(
        bytes.len() % PAGE,
        0,
        "file size {} should be a {} multiple",
        bytes.len(),
        PAGE
    );
}

#[test]
fn aligned_file_decodes_round_trip() {
    let value = big_doc();
    let bytes = driver::encode(&value, aligned_opts()).unwrap();
    let decoded = reader::decode(&bytes).expect("strict decode");
    assert_eq!(decoded.value, value);
}

#[test]
fn target_bytes_decodes_without_alignment() {
    // TargetBytes alone (no padding) must still produce a spec-conforming file.
    let opts = WriterOptions {
        policy: BuildPolicy {
            sizing: NodeSizing::TargetBytes(PAGE),
            align: PageAlignment::None,
        },
        ..WriterOptions::default()
    };
    let value = big_doc();
    let bytes = driver::encode(&value, opts).unwrap();
    let decoded = reader::decode(&bytes).expect("strict decode");
    assert_eq!(decoded.value, value);
}

#[test]
fn aligned_writer_reports_padding_bytes() {
    let opts = aligned_opts();
    let mut buf = Vec::new();
    let padding;
    {
        let w = Writer::with_options(&mut buf, opts).unwrap();
        let mut a = w.start_array();
        for i in 0..2_000i64 {
            a.push_i64(i).unwrap();
        }
        let w = a.end().unwrap();
        // Pre-finish: any padding inserted between nodes is reflected here.
        // After finish: trailer padding is also included.
        let pre = w.padding_bytes_written();
        w.finish().unwrap();
        // After finish, we can no longer query the writer; capture via outer.
        padding = pre;
    }
    // File-size sanity: total padding == file_size - (header + body_bytes_excluding_padding + trailer).
    // We can't recompute "body excluding padding" without re-encoding, but we
    // can at least assert the pre-finish counter is non-negative and the
    // file_size is still page-aligned.
    assert!(padding < buf.len() as u64);
    assert_eq!(buf.len() % PAGE, 0);
}

#[test]
fn target_bytes_below_minimum_rejected() {
    let opts = WriterOptions {
        policy: BuildPolicy {
            sizing: NodeSizing::TargetBytes(32),
            align: PageAlignment::None,
        },
        ..WriterOptions::default()
    };
    let mut buf = Vec::new();
    let res = Writer::with_options(&mut buf, opts);
    assert!(matches!(res, Err(WriteError::InvalidOption(_))));
}

#[test]
fn fanout_below_two_rejected() {
    let opts = WriterOptions {
        policy: BuildPolicy {
            sizing: NodeSizing::Fanout(1),
            align: PageAlignment::None,
        },
        ..WriterOptions::default()
    };
    let mut buf = Vec::new();
    let res = Writer::with_options(&mut buf, opts);
    assert!(matches!(res, Err(WriteError::InvalidOption(_))));
}

#[test]
fn page_size_must_be_power_of_two() {
    let opts = WriterOptions {
        policy: BuildPolicy {
            sizing: NodeSizing::TargetBytes(PAGE),
            align: PageAlignment::Aligned { page_size: 3000 },
        },
        ..WriterOptions::default()
    };
    let mut buf = Vec::new();
    let res = Writer::with_options(&mut buf, opts);
    assert!(matches!(res, Err(WriteError::InvalidOption(_))));
}

#[test]
fn padding_bytes_are_null_tags() {
    // Encode a small doc that will need at least some trailer padding to
    // reach a page boundary, then sweep the body for any non-null byte that
    // is not part of an emitted value or the trailer.
    let value = json!({"hello": "world"});
    let bytes = driver::encode(&value, aligned_opts()).unwrap();
    assert_eq!(bytes.len() % PAGE, 0);
    // The trailer is the last 12 bytes; the root_offset (first 8) plus magic
    // (last 4) must be intact.
    assert_eq!(&bytes[bytes.len() - 4..], b"KAHN");
    // Spot-check: the byte immediately before the trailer should be a Null
    // tag (the trailer-padding region).
    assert_eq!(
        bytes[bytes.len() - 13],
        0x00,
        "expected Null padding byte before trailer"
    );
}
