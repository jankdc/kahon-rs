//! Property tests via `proptest`.
//!
//! Strategies generate arbitrary JSON values and `WriterOptions`. Properties:
//!
//! 1. Round-trip: `decode(encode(v, opts)) == v`.
//! 2. Determinism: same `(v, opts)` → byte-identical output.
//! 3. Spec + writer invariants: enforced inline by `reader::decode_with_fanout`
//!    (m ≥ 2, m ≤ fanout, sub_total consistent, postorder, minimal offset
//!    width, sorted+unique object-leaf keys, no n=0 leaves).

mod common;

use common::{driver, reader};
use kahon::{BuildPolicy, NodeSizing, PageAlignment, WriterOptions};
use proptest::prelude::*;
use serde_json::{Map, Number, Value};

/// Finite f64 generator. Excludes NaN/Infinity (writer rejects them by spec).
fn arb_finite_f64() -> impl Strategy<Value = f64> {
    any::<f64>().prop_filter("finite", |f| f.is_finite())
}

fn arb_leaf() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|i| Value::Number(Number::from(i))),
        arb_finite_f64().prop_map(|f| {
            // from_f64 returns None only for non-finite, already filtered.
            Value::Number(Number::from_f64(f).unwrap())
        }),
        ".{0,16}".prop_map(Value::String),
    ]
}

fn arb_value() -> impl Strategy<Value = Value> {
    arb_leaf().prop_recursive(
        4,  // up to 4 levels of nesting
        48, // up to 48 total nodes
        6,  // each collection up to 6 items
        |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
                prop::collection::vec(("[a-z]{1,4}", inner), 0..6).prop_map(|kvs| {
                    let mut m = Map::new();
                    for (k, v) in kvs {
                        m.insert(k, v); // duplicate keys silently overwrite
                    }
                    Value::Object(m)
                }),
            ]
        },
    )
}

fn arb_opts() -> impl Strategy<Value = (WriterOptions, usize)> {
    // Returns (opts, fanout_ceiling) so the property test can pass the
    // ceiling to the strict reader. Fanout-mode lets us keep that ceiling
    // exact; TargetBytes mode would make the per-node fanout vary.
    (2usize..16, 1usize..16).prop_map(|(fanout, run)| {
        let opts = WriterOptions {
            object_sort_window: run,
            policy: BuildPolicy {
                sizing: NodeSizing::Fanout(fanout),
                align: PageAlignment::None,
            },
        };
        (opts, fanout)
    })
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    /// encode -> decode round-trips structurally; the strict reader enforces
    /// every spec + writer invariant during the decode.
    #[test]
    fn roundtrip(value in arb_value(), pair in arb_opts()) {
        let (opts, fanout) = pair;
        let bytes = driver::encode(&value, opts).expect("encode");
        let decoded = reader::decode_with_fanout(&bytes, fanout)
            .expect("decode_with_fanout");
        prop_assert_eq!(decoded.value, value);
    }

    /// Same input + options -> byte-identical output.
    #[test]
    fn deterministic(value in arb_value(), pair in arb_opts()) {
        let (opts, _) = pair;
        let a = driver::encode(&value, opts.clone()).expect("encode 1");
        let b = driver::encode(&value, opts).expect("encode 2");
        prop_assert_eq!(a, b);
    }
}
