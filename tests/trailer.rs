//! Tests for `RawWriter::snapshot_trailer`.

mod common;

use common::reader;
use kahon::raw::RawWriter;
use kahon::{BuildPolicy, NodeSizing, PageAlignment, WriterOptions};
use serde_json::{json, Value};

fn opts(policy: BuildPolicy) -> WriterOptions {
    WriterOptions {
        policy,
        ..WriterOptions::default()
    }
}

fn policies() -> Vec<(&'static str, BuildPolicy)> {
    vec![
        ("compact(2)", BuildPolicy::compact(2)),
        ("compact(128)", BuildPolicy::compact(128)),
        (
            "disk_aligned(64)",
            BuildPolicy {
                sizing: NodeSizing::TargetBytes(64),
                align: PageAlignment::Aligned { page_size: 64 },
            },
        ),
        ("disk_aligned(4096)", BuildPolicy::disk_aligned(4096)),
    ]
}

fn decode(buf: &[u8]) -> Value {
    reader::decode(buf).expect("decode").value
}

#[test]
fn snapshot_trailer_yields_valid_document() {
    for (name, policy) in policies() {
        let mut buf = Vec::new();
        let snapshot = {
            let mut r = RawWriter::with_options(&mut buf, opts(policy.clone())).unwrap();
            r.begin_array().unwrap();
            r.push_i64(10).unwrap();
            r.push_str("hello").unwrap();
            r.end_array().unwrap();
            r.snapshot_trailer().unwrap()
        };

        let mut composed = buf[..snapshot.prefix_len as usize].to_vec();
        composed.extend_from_slice(&snapshot.bytes);
        let value = decode(&composed);
        assert_eq!(value, json!([10, "hello"]), "policy={name}");
    }
}

#[test]
fn snapshot_accessors_match_assembled_doc() {
    // total_len, root_offset, and trailer_offset must agree with what a
    // reader would compute from the assembled bytes.
    let mut buf = Vec::new();
    let snap = {
        let mut r = RawWriter::with_options(&mut buf, opts(BuildPolicy::compact(128))).unwrap();
        r.begin_array().unwrap();
        r.push_i64(7).unwrap();
        r.push_str("hi").unwrap();
        r.end_array().unwrap();
        r.snapshot_trailer().unwrap()
    };

    let mut composed = buf[..snap.prefix_len as usize].to_vec();
    composed.extend_from_slice(&snap.bytes);

    assert_eq!(snap.total_len() as usize, composed.len());
    assert_eq!(snap.trailer_offset(), snap.total_len() - 12);

    let trailer_start = snap.trailer_offset() as usize;
    let stored_root = u64::from_le_bytes(
        composed[trailer_start..trailer_start + 8]
            .try_into()
            .unwrap(),
    );
    assert_eq!(snap.root_offset(), stored_root);

    // Byte at root_offset is an array tag (spec §5: 0x70..=0x77).
    let tag = composed[snap.root_offset() as usize];
    assert!(
        (0x70..=0x77).contains(&tag),
        "expected array tag at root, got {tag:#x}"
    );
}

#[test]
fn snapshot_with_unfinalized_run_still_valid() {
    // Small fanout so the B+tree cascade is not a single leaf - snapshot
    // must close the cascade correctly into the side buffer.
    let mut buf = Vec::new();
    let snap = {
        let mut r = RawWriter::with_options(&mut buf, opts(BuildPolicy::compact(2))).unwrap();
        r.begin_array().unwrap();
        for i in 0..5 {
            r.push_i64(i).unwrap();
        }
        r.end_array().unwrap();
        r.snapshot_trailer().unwrap()
    };
    let mut composed = buf[..snap.prefix_len as usize].to_vec();
    composed.extend_from_slice(&snap.bytes);
    assert_eq!(decode(&composed), json!([0, 1, 2, 3, 4]));
}

#[test]
fn snapshot_trailer_is_non_destructive() {
    // Snapshot mid-stream, then keep writing; the final document must be
    // byte-identical to a control run that never called snapshot_trailer.
    for (name, policy) in policies() {
        let mut buf_with = Vec::new();
        {
            let mut r = RawWriter::with_options(&mut buf_with, opts(policy.clone())).unwrap();
            r.begin_array().unwrap();
            r.push_i64(1).unwrap();
            r.push_i64(2).unwrap();
            r.end_array().unwrap();
            let _snap = r.snapshot_trailer().unwrap();
            r.finish().unwrap();
        }

        let mut buf_no = Vec::new();
        {
            let mut r = RawWriter::with_options(&mut buf_no, opts(policy)).unwrap();
            r.begin_array().unwrap();
            r.push_i64(1).unwrap();
            r.push_i64(2).unwrap();
            r.end_array().unwrap();
            r.finish().unwrap();
        }

        assert_eq!(buf_with, buf_no, "policy={name}");
    }
}

#[test]
fn snapshot_with_open_frames_assembles_correctly() {
    // snapshot_trailer should close cloned frames into the side buffer
    // without disturbing the live writer's frame stack.
    let mut buf = Vec::new();
    let snap = {
        let mut r = RawWriter::with_options(&mut buf, opts(BuildPolicy::compact(4))).unwrap();
        r.begin_array().unwrap();
        r.push_i64(1).unwrap();
        r.begin_array().unwrap();
        r.push_i64(2).unwrap();
        r.push_i64(3).unwrap();
        // Snapshot with both array frames still open.
        r.snapshot_trailer().unwrap()
    };
    let mut composed = buf[..snap.prefix_len as usize].to_vec();
    composed.extend_from_slice(&snap.bytes);
    assert_eq!(decode(&composed), json!([1, [2, 3]]));
}

#[test]
fn snapshot_drops_pending_object_key() {
    // A pending key (set via push_key without a value yet) is dropped by
    // snapshot_trailer; the key's bytes remain as orphaned data in the
    // prefix, which kahon allows.
    let mut buf = Vec::new();
    let snap = {
        let mut r = RawWriter::with_options(&mut buf, opts(BuildPolicy::compact(4))).unwrap();
        r.begin_object().unwrap();
        r.push_key("a").unwrap();
        r.push_i64(1).unwrap();
        r.push_key("dangling").unwrap();
        // No value pushed for "dangling" - snapshot drops the pending key.
        r.snapshot_trailer().unwrap()
    };
    let mut composed = buf[..snap.prefix_len as usize].to_vec();
    composed.extend_from_slice(&snap.bytes);
    assert_eq!(decode(&composed), json!({"a": 1}));
}
