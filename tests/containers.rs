//! Container encoding rules: empty-singleton tags, offset-width promotion,
//! object key sort order, multi-run flush, leaf streaming, large-N smoke.
//! Helpers come from `common::encode`.

mod common;

use common::encode::{body, build, root_byte, root_offset};
use kahon::{BuildPolicy, Writer, WriterOptions};

#[test]
fn empty_array_encodes_as_singleton_tag() {
    let buf = build(|w| {
        w.start_array().end().unwrap();
    });
    assert_eq!(body(&buf), &[0x33]);
}

#[test]
fn empty_object_encodes_as_singleton_tag() {
    let buf = build(|w| {
        w.start_object().end().unwrap();
    });
    assert_eq!(body(&buf), &[0x34]);
}

#[test]
fn array_singleton_matches_conformance_bytes() {
    // container/array-singleton: [1] → tag 0x70, varuint n=1, off[0]=0x06.
    // The TinyUInt 1 (0x14) lives at body offset 0x06; the leaf at 0x07.
    let buf = build(|w| {
        let mut a = w.start_array();
        a.push_i64(1).unwrap();
        a.end().unwrap();
    });
    assert_eq!(body(&buf), &[0x14, 0x70, 0x01, 0x06]);
    assert_eq!(root_offset(&buf), 0x07);
}

#[test]
fn array_three_matches_conformance_bytes() {
    // container/array-three: [1,2,3] → three TinyUInts at 0x06..0x08, then
    // tag 0x70, varuint n=3, off[0..3] = 0x06,0x07,0x08.
    let buf = build(|w| {
        let mut a = w.start_array();
        a.push_i64(1).unwrap();
        a.push_i64(2).unwrap();
        a.push_i64(3).unwrap();
        a.end().unwrap();
    });
    assert_eq!(
        body(&buf),
        &[0x14, 0x15, 0x16, 0x70, 0x03, 0x06, 0x07, 0x08]
    );
    assert_eq!(root_offset(&buf), 0x09);
}

#[test]
fn array_promotes_offset_width_when_child_offset_exceeds_255() {
    // A 300-byte string forces the second child's offset above 255, so the
    // leaf must declare 2-byte offset slots (w=1 → tag 0x71).
    let big = "x".repeat(300);
    let buf = build(|w| {
        let mut a = w.start_array();
        a.push_str(&big).unwrap();
        a.push_i64(1).unwrap();
        a.end().unwrap();
    });
    assert_eq!(root_byte(&buf), 0x71);
}

#[test]
fn array_with_small_fanout_produces_internal_node() {
    let opts = WriterOptions {
        policy: BuildPolicy::compact(2),
        ..Default::default()
    };
    let mut buf = Vec::new();
    {
        let mut w = Writer::with_options(&mut buf, opts).unwrap();
        let mut a = w.start_array();
        for i in 0i64..5 {
            a.push_i64(i).unwrap();
        }
        a.end().unwrap();
        w.finish().unwrap();
    }
    let tag = root_byte(&buf);
    assert!(
        (0x74..=0x77).contains(&tag),
        "expected array-internal tag, got {:#x}",
        tag
    );
}

#[test]
fn array_spills_leaf_to_sink_once_fanout_is_reached() {
    // With fanout=2, the second push closes the leaf and writes it through
    // to the sink — bytes_written must advance past just the raw scalars.
    let opts = WriterOptions {
        policy: BuildPolicy::compact(2),
        ..Default::default()
    };
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = Writer::with_options(&mut buf, opts).unwrap();
        let mut a = w.start_array();
        a.push_i64(0).unwrap(); // 1B scalar @ pos 6
        a.push_i64(1).unwrap(); // 1B scalar @ pos 7, then leaf spill
                                // header(6) + 2 scalars(2) + leaf(tag + varuint n=2 + 2×1B offsets = 4B) = 12.
        assert_eq!(a.bytes_written(), 12, "leaf should have spilled");
        a.push_i64(2).unwrap();
        a.end().unwrap();
        w.finish().unwrap();
    }
}

#[test]
fn object_single_pair_matches_conformance_bytes() {
    // container/object-single: {"a":1} → key "a" at 0x06 (TinyString 0x60 0x61),
    // value 1 at 0x08 (TinyUInt 0x14), then tag 0x80, varuint n=1, off pairs.
    let buf = build(|w| {
        let mut o = w.start_object();
        o.push_i64("a", 1).unwrap();
        o.end().unwrap();
    });
    assert_eq!(
        body(&buf),
        &[0x60, 0x61, 0x14, 0x80, 0x01, 0x06, 0x08]
    );
    assert_eq!(root_offset(&buf), 0x09);
}

#[test]
fn object_two_sorted_emits_kv_interleaved() {
    // {"a":1,"b":2} — both layouts are spec-conforming. The upstream
    // conformance vector container/object-two-sorted groups keys then values
    // ([60 61 60 62 14 15 80 02 06 0A 08 0B]); this writer streams k,v,k,v
    // because each value is emitted before the next key. The leaf still
    // satisfies §7.3 (keys strictly UTF-8-sorted in the offset array).
    let buf = build(|w| {
        let mut o = w.start_object();
        o.push_i64("a", 1).unwrap();
        o.push_i64("b", 2).unwrap();
        o.end().unwrap();
    });
    assert_eq!(
        body(&buf),
        &[0x60, 0x61, 0x14, 0x60, 0x62, 0x15, 0x80, 0x02, 0x06, 0x08, 0x09, 0x0B]
    );
    assert_eq!(root_offset(&buf), 0x0C);
}

#[test]
fn object_sorts_keys_within_a_run_regardless_of_push_order() {
    // Push "b" before "a"; the single-run leaf must place "a" first.
    let buf = build(|w| {
        let mut o = w.start_object();
        o.push_i64("b", 2).unwrap();
        o.push_i64("a", 1).unwrap();
        o.end().unwrap();
    });
    let tag = root_byte(&buf);
    assert!((0x80..=0x83).contains(&tag), "expected object-leaf tag");

    // Layout at width 0: tag, varuint n=2, (key_off, val_off) pairs (1B each).
    // First key offset lives at root_off + 2.
    let root_off = root_offset(&buf) as usize;
    let first_key_off = buf[root_off + 2] as usize;
    assert_eq!(&buf[first_key_off..first_key_off + 2], &[0x60, 0x61]); // "a"
}

#[test]
fn object_with_multiple_runs_produces_internal_node() {
    // run buffer of 2 → keys flush in 3 runs: [c,a]→[a,c], [d,b]→[b,d], [e].
    // Three runs require an internal merge node above the run leaves.
    let opts = WriterOptions {
        object_sort_window: 2,
        policy: BuildPolicy::compact(128),
    };
    let mut buf = Vec::new();
    {
        let mut w = Writer::with_options(&mut buf, opts).unwrap();
        let mut o = w.start_object();
        o.push_i64("c", 1).unwrap();
        o.push_i64("a", 2).unwrap();
        o.push_i64("d", 3).unwrap();
        o.push_i64("b", 4).unwrap();
        o.push_i64("e", 5).unwrap();
        o.end().unwrap();
        w.finish().unwrap();
    }
    let tag = root_byte(&buf);
    assert!(
        (0x84..=0x87).contains(&tag),
        "expected object-internal tag, got {:#x}",
        tag
    );
}

#[test]
fn duplicate_key_within_run_resolves_last_wins() {
    let mut buf = Vec::new();
    {
        let mut w = Writer::new(&mut buf);
        let mut o = w.start_object();
        o.push_i64("a", 1).unwrap();
        o.push_i64("a", 2).unwrap();
        o.end().unwrap();
        w.finish().unwrap();
    }
    let decoded = common::reader::decode(&buf).expect("decode last-wins doc");
    assert_eq!(decoded.value, serde_json::json!({ "a": 2 }));
}

#[test]
fn duplicate_cross_run_resolves_last_wins() {
    // object_sort_window=1 forces every key into its own run, so duplicates
    // land in sibling leaves under an internal node. Reader traversal order
    // matches run-insertion order, so structural decode and keyed lookup
    // both surface the latest write.
    let mut buf = Vec::new();
    let opts = WriterOptions {
        object_sort_window: 1,
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
    let decoded = common::reader::decode(&buf).expect("decode cross-run last-wins");
    assert_eq!(decoded.value, serde_json::json!({ "a": 2 }));

    let v_off = common::reader::lookup_key(&buf, b"a")
        .expect("lookup ok")
        .expect("key present");
    assert_eq!(buf[v_off], 0x15, "TinyUInt 2 (latest write)");
}

#[test]
fn large_array_streams_without_buffering_whole_input() {
    // Smoke test: 10k tiny ints with default fanout encode end-to-end and the
    // root is an array-internal node (i.e. the B+tree path engaged).
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = Writer::new(&mut buf);
        let mut a = w.start_array();
        for i in 0..10_000i64 {
            a.push_i64(i).unwrap();
        }
        a.end().unwrap();
        w.finish().unwrap();
    }
    let tag = root_byte(&buf);
    assert!(
        (0x74..=0x77).contains(&tag),
        "expected array-internal tag, got {:#x}",
        tag
    );
}
