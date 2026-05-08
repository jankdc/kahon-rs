//! Spec §8 invariant 3: every internal B+tree node must reference m >= 2
//! children. The walker (`common::walker`) traverses the emitted tree and
//! fails the test on any m=1 internal node.

mod common;

use common::encode::root_offset;
use common::walker::assert_no_m_below_2;
use kahon::{BuildPolicy, Empty, Filled, Writer, WriterOptions};

/// Build a doc with custom options and return the bytes; small wrapper so the
/// invariant tests below stay focused on the structural assertion.
fn build_with<F>(opts: WriterOptions, f: F) -> Vec<u8>
where
    F: for<'a> FnOnce(Writer<&'a mut Vec<u8>, Empty>) -> Writer<&'a mut Vec<u8>, Filled>,
{
    let mut buf = Vec::new();
    {
        let w = Writer::with_options(&mut buf, opts).unwrap();
        let w = f(w);
        w.finish().unwrap();
    }
    buf
}

#[test]
fn array_with_uneven_levels_keeps_m_at_least_2() {
    // fanout=2, 5 items: leaves [2,2,1] would naively climb to an m=1 node.
    // Array path bubbles the lone pair up - must stay valid.
    let opts = WriterOptions {
        policy: BuildPolicy::compact(2),
        ..Default::default()
    };
    let buf = build_with(opts, |w| {
        let mut a = w.start_array();
        for i in 0i64..5 {
            a.push_i64(i).unwrap();
        }
        a.end().unwrap()
    });
    assert_no_m_below_2(&buf, root_offset(&buf));
}

#[test]
fn single_run_object_with_uneven_levels_keeps_m_at_least_2() {
    // 5 sorted pairs at fanout=2: emit_object_subtree chunks → 3 leaves; the
    // climb must avoid producing a trailing m=1 internal.
    let opts = WriterOptions {
        policy: BuildPolicy::compact(2),
        object_sort_window: 1024, // hold all 5 in one run
    };
    let buf = build_with(opts, |w| {
        let mut o = w.start_object();
        for &k in &["a", "b", "c", "d", "e"] {
            o.push_i64(k, 0).unwrap();
        }
        o.end().unwrap()
    });
    assert_no_m_below_2(&buf, root_offset(&buf));
}

#[test]
fn multi_run_object_with_trailing_singleton_keeps_m_at_least_2() {
    // 3 runs at fanout=2 from 5 keys: [a,b], [c,d], [e]. The trailing
    // single-run chunk must not appear as an m=1 internal node.
    let opts = WriterOptions {
        policy: BuildPolicy::compact(2),
        object_sort_window: 2,
    };
    let buf = build_with(opts, |w| {
        let mut o = w.start_object();
        for &k in &["a", "b", "c", "d", "e"] {
            o.push_i64(k, 0).unwrap();
        }
        o.end().unwrap()
    });
    assert_no_m_below_2(&buf, root_offset(&buf));
}
