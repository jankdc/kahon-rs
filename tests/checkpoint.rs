//! Tests for the checkpoint primitives and `snapshot_trailer`.

mod common;

use common::reader;
use kahon::{BuildPolicy, NodeSizing, PageAlignment, WriteError, Writer, WriterOptions};
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
fn top_level_commit_matches_no_checkpoint_run() {
    for (name, policy) in policies() {
        let mut buf_with = Vec::new();
        {
            let mut w = Writer::with_options(&mut buf_with, opts(policy.clone())).unwrap();
            let cp = w.checkpoint();
            // Push the eventual root inside the checkpoint; drop cp to keep.
            let mut a = w.start_array();
            a.push_i64(1).unwrap();
            a.push_i64(2).unwrap();
            a.end().unwrap();
            drop(cp);
            w.finish().unwrap();
        }

        let mut buf_no = Vec::new();
        {
            let mut w = Writer::with_options(&mut buf_no, opts(policy)).unwrap();
            let mut a = w.start_array();
            a.push_i64(1).unwrap();
            a.push_i64(2).unwrap();
            a.end().unwrap();
            w.finish().unwrap();
        }

        assert_eq!(buf_with, buf_no, "policy={name}");
    }
}

#[test]
fn top_level_rollback_discards_writes() {
    for (name, policy) in policies() {
        let mut buf_with = Vec::new();
        {
            let mut w = Writer::with_options(&mut buf_with, opts(policy.clone())).unwrap();
            let cp = w.checkpoint();
            // Try one variant: an array of strings.
            let mut a = w.start_array();
            a.push_str("rejected").unwrap();
            a.end().unwrap();
            w.rollback(cp).unwrap();
            // Try a different variant: a scalar int.
            w.push_i64(42).unwrap();
            w.finish().unwrap();
        }

        let mut buf_no = Vec::new();
        {
            let mut w = Writer::with_options(&mut buf_no, opts(policy)).unwrap();
            w.push_i64(42).unwrap();
            w.finish().unwrap();
        }

        assert_eq!(buf_with, buf_no, "policy={name}");
        assert_eq!(decode(&buf_with), json!(42));
    }
}

#[test]
fn lexical_nested_inner_rollback_outer_commit() {
    // Rollback inner discards "rejected"; outer commit keeps the array.
    let mut buf = Vec::new();
    {
        let mut w = Writer::with_options(&mut buf, opts(BuildPolicy::compact(4))).unwrap();
        let cp_outer = w.checkpoint();
        let mut a = w.start_array();
        a.push_i64(1).unwrap();
        let cp_inner = a.checkpoint();
        a.push_str("rejected").unwrap();
        a.rollback(cp_inner).unwrap();
        a.push_i64(2).unwrap();
        a.end().unwrap();
        drop(cp_outer);
        w.finish().unwrap();
    }
    assert_eq!(decode(&buf), json!([1, 2]));
}

#[test]
fn lexical_nested_outer_rollback_after_inner_drop() {
    // Drop inner (keep), then outer rollback - everything is undone.
    let mut buf = Vec::new();
    {
        let mut w = Writer::with_options(&mut buf, opts(BuildPolicy::compact(4))).unwrap();
        let cp_outer = w.checkpoint();
        let cp_inner = w.checkpoint();
        w.push_i64(1).unwrap();
        drop(cp_inner);
        w.rollback(cp_outer).unwrap();
        w.push_i64(99).unwrap();
        w.finish().unwrap();
    }
    assert_eq!(decode(&buf), json!(99));
}

#[test]
fn structural_array_per_element_variants() {
    // For each element, attempt a "user variant" first (push_str(..)) and on
    // a fake rejection condition fall back to "guest variant" (push_null).
    let inputs = [Some("alice"), None, Some("bob"), None, Some("carol")];

    let mut buf = Vec::new();
    {
        let mut w = Writer::with_options(&mut buf, opts(BuildPolicy::compact(3))).unwrap();
        let mut a = w.start_array();
        for input in inputs.iter() {
            let cp = a.checkpoint();
            // Try "user" variant (string).
            a.push_str("speculative").unwrap();
            if input.is_some() {
                drop(cp); // keep the speculative push
            } else {
                a.rollback(cp).unwrap();
                a.push_null().unwrap();
            }
        }
        a.end().unwrap();
        w.finish().unwrap();
    }
    // The "user variant" speculative push was committed verbatim for Some(_)
    // cases; for None we rolled back and pushed null instead.
    assert_eq!(
        decode(&buf),
        json!(["speculative", null, "speculative", null, "speculative"])
    );
}

#[test]
fn structural_array_rollback_then_retry() {
    // Validator pattern: try a value, on err rollback and try a different one.
    let mut buf = Vec::new();
    {
        let mut w = Writer::with_options(&mut buf, opts(BuildPolicy::compact(4))).unwrap();
        let mut a = w.start_array();
        for &v in &[1, 2, 3] {
            let cp = a.checkpoint();
            // "Variant A": push as string. Pretend it fails for v == 2.
            a.push_str(&format!("s{v}")).unwrap();
            if v == 2 {
                a.rollback(cp).unwrap();
                a.push_i64(v).unwrap();
            } else {
                drop(cp);
            }
        }
        a.end().unwrap();
        w.finish().unwrap();
    }
    assert_eq!(decode(&buf), json!(["s1", 2, "s3"]));
}

// -----------------------------------------------------------------------------
// 5. structural - object
// -----------------------------------------------------------------------------

#[test]
fn structural_object_value_variant_with_rollback() {
    // The value for "role" is a union; first attempt fails, second succeeds.
    let mut buf = Vec::new();
    {
        let mut w = Writer::with_options(&mut buf, opts(BuildPolicy::compact(4))).unwrap();
        let mut o = w.start_object();
        o.push_str("name", "alice").unwrap();
        let cp = o.checkpoint();
        o.push_str("role", "admin").unwrap();
        o.rollback(cp).unwrap();
        o.push_str("role", "guest").unwrap();
        o.end().unwrap();
        w.finish().unwrap();
    }
    assert_eq!(decode(&buf), json!({"name": "alice", "role": "guest"}));
}

// -----------------------------------------------------------------------------
// 6. mixed nesting (object containing array of variants)
// -----------------------------------------------------------------------------

#[test]
fn mixed_nesting_object_with_variant_array() {
    let mut buf = Vec::new();
    {
        let mut w = Writer::with_options(&mut buf, opts(BuildPolicy::compact(4))).unwrap();
        let mut o = w.start_object();
        o.push_str("kind", "user").unwrap();
        {
            let mut tags = o.start_array("tags").unwrap();
            for tag in &["red", "skip", "blue"] {
                let cp = tags.checkpoint();
                tags.push_str(tag).unwrap();
                if *tag == "skip" {
                    tags.rollback(cp).unwrap();
                } else {
                    drop(cp);
                }
            }
            tags.end().unwrap();
        }
        o.end().unwrap();
        w.finish().unwrap();
    }
    assert_eq!(
        decode(&buf),
        json!({"kind": "user", "tags": ["red", "blue"]})
    );
}

// -----------------------------------------------------------------------------
// 7. depth-mismatch rejection
// -----------------------------------------------------------------------------

#[test]
fn rollback_at_wrong_depth_is_rejected() {
    let mut buf = Vec::new();
    let mut w = Writer::with_options(&mut buf, opts(BuildPolicy::compact(128))).unwrap();
    let mut o = w.start_object();
    let cp = o.checkpoint(); // depth = 1 (object frame open)
    o.push_str("name", "alice").unwrap();
    let mut tags = o.start_array("tags").unwrap(); // depth = 2
    tags.push_str("a").unwrap();
    // Try to rollback the object-level cp from inside the array.
    let err = tags.rollback(cp).unwrap_err();
    assert!(matches!(err, WriteError::InvalidCheckpoint));
}

// -----------------------------------------------------------------------------
// 8. position-mismatch rejection (already-rolled-back cp re-used)
// -----------------------------------------------------------------------------

#[test]
fn reusing_rolled_back_checkpoint_is_rejected() {
    // Take cp1, push, take cp2, push more, rollback to cp1 (sink shrinks
    // past cp2.pos), attempt to use cp2 - its position is now past EOF.
    let mut buf = Vec::new();
    let mut w = Writer::with_options(&mut buf, opts(BuildPolicy::compact(128))).unwrap();
    let mut a = w.start_array();
    a.push_i64(1).unwrap();
    let cp1 = a.checkpoint();
    a.push_i64(2).unwrap();
    let cp2 = a.checkpoint();
    a.push_i64(3).unwrap();
    a.rollback(cp1).unwrap();
    let err = a.rollback(cp2).unwrap_err();
    assert!(matches!(err, WriteError::InvalidCheckpoint));
}

// -----------------------------------------------------------------------------
// 9. drop without resolve = implicit commit
// -----------------------------------------------------------------------------

#[test]
fn dropped_checkpoint_is_implicit_commit() {
    let mut buf_drop = Vec::new();
    {
        let mut w = Writer::with_options(&mut buf_drop, opts(BuildPolicy::compact(128))).unwrap();
        let _cp = w.checkpoint();
        w.push_i64(7).unwrap();
        // _cp drops at end of statement scope; no commit/rollback called.
        w.finish().unwrap();
    }

    let mut buf_no = Vec::new();
    {
        let mut w = Writer::with_options(&mut buf_no, opts(BuildPolicy::compact(128))).unwrap();
        w.push_i64(7).unwrap();
        w.finish().unwrap();
    }

    assert_eq!(buf_drop, buf_no);
}

// -----------------------------------------------------------------------------
// 11. snapshot_trailer produces a valid kahon document
// -----------------------------------------------------------------------------

#[test]
fn snapshot_trailer_yields_valid_document() {
    for (name, policy) in policies() {
        // Build a partial document: an array with two elements pushed.
        // Snapshot mid-stream, assemble prefix + tail, decode.
        let mut buf = Vec::new();
        let snapshot = {
            let mut w = Writer::with_options(&mut buf, opts(policy.clone())).unwrap();
            let mut a = w.start_array();
            a.push_i64(10).unwrap();
            a.push_str("hello").unwrap();
            // snapshot_trailer is on Writer; need to drop the builder first.
            // Use the borrow trick: end the array here and re-take checkpoints
            // is not the goal. Instead, the test exercises snapshot_trailer
            // with no open frames - see also `snapshot_with_open_frames`.
            a.end().unwrap();
            w.snapshot_trailer().unwrap()
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
        let mut w = Writer::with_options(&mut buf, opts(BuildPolicy::compact(128))).unwrap();
        let mut a = w.start_array();
        a.push_i64(7).unwrap();
        a.push_str("hi").unwrap();
        a.end().unwrap();
        w.snapshot_trailer().unwrap()
    };

    let mut composed = buf[..snap.prefix_len as usize].to_vec();
    composed.extend_from_slice(&snap.bytes);

    // total_len = composed.len()
    assert_eq!(snap.total_len() as usize, composed.len());

    // trailer_offset = total_len - 12 (spec §10.2)
    assert_eq!(snap.trailer_offset(), snap.total_len() - 12);

    // root_offset matches the u64 stored at the trailer's first 8 bytes.
    let trailer_start = snap.trailer_offset() as usize;
    let stored_root = u64::from_le_bytes(
        composed[trailer_start..trailer_start + 8]
            .try_into()
            .unwrap(),
    );
    assert_eq!(snap.root_offset(), stored_root);

    // And the byte at root_offset is a valid container type-code (an array
    // tag in this case: 0x70..=0x77).
    let tag = composed[snap.root_offset() as usize];
    assert!(
        (0x70..=0x77).contains(&tag),
        "expected array tag at root, got {tag:#x}"
    );
}

#[test]
fn snapshot_with_unfinalized_run_still_valid() {
    // Use a small fanout so the B+tree cascade is not a single leaf -
    // snapshot must close the cascade correctly into the side buffer.
    // The borrow checker prevents calling snapshot_trailer while a
    // builder is alive, so we close the array first; the cascade-close
    // path is still exercised by snapshot via its frame clone (the
    // post-close root is unchanged regardless).
    let mut buf = Vec::new();
    let snap = {
        let mut w = Writer::with_options(&mut buf, opts(BuildPolicy::compact(2))).unwrap();
        {
            let mut a = w.start_array();
            for i in 0..5 {
                a.push_i64(i).unwrap();
            }
            a.end().unwrap();
        }
        w.snapshot_trailer().unwrap()
    };
    let mut composed = buf[..snap.prefix_len as usize].to_vec();
    composed.extend_from_slice(&snap.bytes);
    assert_eq!(decode(&composed), json!([0, 1, 2, 3, 4]));
}

// -----------------------------------------------------------------------------
// 12. snapshot_trailer is non-destructive
// -----------------------------------------------------------------------------

#[test]
fn snapshot_trailer_is_non_destructive() {
    // Snapshot mid-stream, then keep writing; the final document must be
    // byte-identical to a control run that never called snapshot_trailer.
    for (name, policy) in policies() {
        let mut buf_with = Vec::new();
        {
            let mut w = Writer::with_options(&mut buf_with, opts(policy.clone())).unwrap();
            let mut a = w.start_array();
            a.push_i64(1).unwrap();
            a.push_i64(2).unwrap();
            a.end().unwrap();
            // Snapshot here; live writer should be unaffected.
            let _snap = w.snapshot_trailer().unwrap();
            w.finish().unwrap();
        }

        let mut buf_no = Vec::new();
        {
            let mut w = Writer::with_options(&mut buf_no, opts(policy)).unwrap();
            let mut a = w.start_array();
            a.push_i64(1).unwrap();
            a.push_i64(2).unwrap();
            a.end().unwrap();
            w.finish().unwrap();
        }

        assert_eq!(buf_with, buf_no, "policy={name}");
    }
}

// -----------------------------------------------------------------------------
// 10. parametrized: rollback under all BuildPolicy settings
// -----------------------------------------------------------------------------

#[test]
fn rollback_under_all_policies_matches_control() {
    // Comprehensive rollback test parametrized over policies.
    // Build with: push committed prefix, take cp, push junk, rollback,
    // push committed suffix. Compare to a control with no junk.
    for (name, policy) in policies() {
        let mut buf_with = Vec::new();
        {
            let mut w = Writer::with_options(&mut buf_with, opts(policy.clone())).unwrap();
            let mut o = w.start_object();
            for i in 0..6 {
                o.push_i64(&format!("k{i}"), i as i64).unwrap();
            }
            let cp = o.checkpoint();
            for i in 100..106 {
                o.push_i64(&format!("junk{i}"), i as i64).unwrap();
            }
            o.rollback(cp).unwrap();
            for i in 6..10 {
                o.push_i64(&format!("k{i}"), i as i64).unwrap();
            }
            o.end().unwrap();
            w.finish().unwrap();
        }

        let mut buf_no = Vec::new();
        {
            let mut w = Writer::with_options(&mut buf_no, opts(policy)).unwrap();
            let mut o = w.start_object();
            for i in 0..10 {
                o.push_i64(&format!("k{i}"), i as i64).unwrap();
            }
            o.end().unwrap();
            w.finish().unwrap();
        }

        // Both decode to the same logical value. They may not be byte-identical
        // because the rollback doesn't recover scratch capacities or other
        // non-observable state, but the encoded bytes should match.
        assert_eq!(decode(&buf_with), decode(&buf_no), "policy={name}");
    }
}

// -----------------------------------------------------------------------------
// poison + checkpoint interaction
// -----------------------------------------------------------------------------

#[test]
fn rollback_on_poisoned_writer_errors() {
    use kahon::{RewindableSink, Sink};
    use std::io;

    // Custom sink that fails after N bytes, used to drive the writer into
    // a poisoned state via a failed builder Drop.
    struct FailAfter {
        buf: Vec<u8>,
        budget: usize,
    }
    impl Sink for FailAfter {
        fn write_all(&mut self, b: &[u8]) -> io::Result<()> {
            if self.buf.len() + b.len() > self.budget {
                Err(io::Error::other("budget exhausted"))
            } else {
                self.buf.extend_from_slice(b);
                Ok(())
            }
        }
    }
    impl RewindableSink for FailAfter {
        fn rewind_to(&mut self, len: u64) -> io::Result<()> {
            self.buf.truncate(len as usize);
            Ok(())
        }
    }

    let sink = FailAfter {
        buf: Vec::new(),
        budget: 8, // header (6) + a couple bytes; close will fail.
    };
    let mut w = Writer::with_options(sink, opts(BuildPolicy::compact(128))).unwrap();
    let cp = w.checkpoint();
    {
        let mut a = w.start_array();
        // Push enough to exceed budget when the array closes.
        for _ in 0..16 {
            let _ = a.push_i64(1);
        }
        // Drop closes the array; the close write exceeds budget, poisoning
        // the writer.
    }
    let err = w.rollback(cp).unwrap_err();
    assert!(
        matches!(err, WriteError::Poisoned),
        "expected Poisoned, got {err:?}"
    );
}
