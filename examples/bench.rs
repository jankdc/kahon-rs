//! Benchmark: Kahon's performance profile across workload shapes, and how the
//! writer knobs (`fanout`, `object_sort_window`) trade off keyed-lookup speed
//! against writer memory.
//!
//! Run: `cargo run --release --example bench`
//! Optional sizing args: `<n_records> <big_keys> <fat_keys>`.

use std::time::{Duration, Instant};

use kahon::{ArrayBuilder, BuildPolicy, NodeSizing, ObjectBuilder, Writer, WriterOptions};
use serde_json::{json, Value};

#[allow(dead_code)]
#[path = "../tests/common/reader.rs"]
mod reader;

// ============================================================================
// Workloads
// ============================================================================

/// Array of records: each record has scalar fields, a small tag object, and a
/// numeric array. Object fanout (~8 keys) is too small to ever trigger a run
/// flush, so this shape is insensitive to sort_window.
fn build_doc(n_records: usize) -> Value {
    let records: Vec<Value> = (0..n_records)
        .map(|i| {
            let tags: Value = (0..8)
                .map(|t| (format!("tag_{t}"), Value::String(format!("v_{}_{}", i, t))))
                .collect::<serde_json::Map<_, _>>()
                .into();
            let samples: Vec<Value> = (0..16)
                .map(|s| json!((i as f64) * 0.125 + (s as f64)))
                .collect();
            json!({
                "id": i as i64,
                "name": format!("record-{i:06}"),
                "active": i % 3 != 0,
                "score": (i as f64) * 1.5,
                "bucket": format!("bucket-{}", i % 37),
                "tags": tags,
                "samples": samples,
                "note": if i % 5 == 0 { Value::Null } else { json!(format!("note for {i}")) },
            })
        })
        .collect();
    Value::Array(records)
}

/// Single object with `n_keys` small entries. The shape that exercises
/// `object_sort_window` (the only knob with a tunable read-side effect).
fn build_big_object(n_keys: usize) -> Value {
    let mut map = serde_json::Map::with_capacity(n_keys);
    for i in 0..n_keys {
        map.insert(
            format!("key_{i:08}"),
            json!({"i": i as i64, "v": format!("val-{i}")}),
        );
    }
    Value::Object(map)
}

/// Big-object with ~1 KB values. Stresses offset-width selection (deeper into
/// u32 territory) and full-decode value reconstruction.
fn build_big_object_fat(n_keys: usize) -> Value {
    let mut map = serde_json::Map::with_capacity(n_keys);
    for i in 0..n_keys {
        let tags: Value = (0..16)
            .map(|t| {
                (
                    format!("tag_{t:02}"),
                    Value::String(format!("v_{i}_{t}_filler")),
                )
            })
            .collect::<serde_json::Map<_, _>>()
            .into();
        let samples: Vec<Value> = (0..32).map(|s| json!((i + s) as f64 * 0.5)).collect();
        let blob = format!("blob-{i:08}-{}", "x".repeat(256));
        map.insert(
            format!("key_{i:08}"),
            json!({
                "id": i as i64,
                "name": format!("record-{i:08}"),
                "tags": tags,
                "samples": samples,
                "blob": blob,
            }),
        );
    }
    Value::Object(map)
}

// ============================================================================
// Writer drivers (recursive over serde_json::Value)
// ============================================================================

fn write_root(w: &mut Writer<&mut Vec<u8>>, v: &Value, peak: &mut usize) {
    match v {
        Value::Array(items) => {
            let mut b = w.start_array();
            for item in items {
                push_into_array(&mut b, item, peak);
            }
            b.end().unwrap();
        }
        Value::Object(map) => {
            let mut b = w.start_object();
            for (k, vv) in map {
                push_into_object(&mut b, k, vv, peak);
            }
            b.end().unwrap();
        }
        Value::Null => w.push_null().unwrap(),
        Value::Bool(x) => w.push_bool(*x).unwrap(),
        Value::String(s) => w.push_str(s).unwrap(),
        Value::Number(n) => push_number_root(w, n),
    }
    *peak = (*peak).max(w.buffered_bytes());
}

fn push_into_array(b: &mut ArrayBuilder<'_, &mut Vec<u8>>, v: &Value, peak: &mut usize) {
    match v {
        Value::Array(items) => {
            let mut inner = b.start_array();
            for item in items {
                push_into_array(&mut inner, item, peak);
            }
            inner.end().unwrap();
        }
        Value::Object(map) => {
            let mut inner = b.start_object();
            for (kk, vv) in map {
                push_into_object(&mut inner, kk, vv, peak);
            }
            inner.end().unwrap();
        }
        Value::Null => b.push_null().unwrap(),
        Value::Bool(x) => b.push_bool(*x).unwrap(),
        Value::String(s) => b.push_str(s).unwrap(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                b.push_i64(i).unwrap();
            } else if let Some(u) = n.as_u64() {
                b.push_u64(u).unwrap();
            } else {
                b.push_f64(n.as_f64().unwrap()).unwrap();
            }
        }
    }
    *peak = (*peak).max(b.buffered_bytes());
}

fn push_into_object(b: &mut ObjectBuilder<'_, &mut Vec<u8>>, k: &str, v: &Value, peak: &mut usize) {
    match v {
        Value::Array(items) => {
            let mut inner = b.start_array(k).unwrap();
            for item in items {
                push_into_array(&mut inner, item, peak);
            }
            inner.end().unwrap();
        }
        Value::Object(map) => {
            let mut inner = b.start_object(k).unwrap();
            for (kk, vv) in map {
                push_into_object(&mut inner, kk, vv, peak);
            }
            inner.end().unwrap();
        }
        Value::Null => b.push_null(k).unwrap(),
        Value::Bool(x) => b.push_bool(k, *x).unwrap(),
        Value::String(s) => b.push_str(k, s).unwrap(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                b.push_i64(k, i).unwrap();
            } else if let Some(u) = n.as_u64() {
                b.push_u64(k, u).unwrap();
            } else {
                b.push_f64(k, n.as_f64().unwrap()).unwrap();
            }
        }
    }
    *peak = (*peak).max(b.buffered_bytes());
}

fn push_number_root(w: &mut Writer<&mut Vec<u8>>, n: &serde_json::Number) {
    if let Some(i) = n.as_i64() {
        w.push_i64(i).unwrap();
    } else if let Some(u) = n.as_u64() {
        w.push_u64(u).unwrap();
    } else {
        w.push_f64(n.as_f64().unwrap()).unwrap();
    }
}

// ============================================================================
// Measurement
// ============================================================================

struct Outcome {
    write: Duration,
    read_full: Duration,
    lookup: Option<Duration>,
    out_bytes: usize,
    peak: usize,
}

fn run_one(
    doc: &Value,
    opts: WriterOptions,
    lookup_keys: Option<&[String]>,
    out_capacity: usize,
) -> Outcome {
    let mut buf = Vec::with_capacity(out_capacity);
    let mut peak = 0usize;

    let t = Instant::now();
    {
        let mut w = Writer::with_options(&mut buf, opts).expect("valid options");
        peak = peak.max(w.buffered_bytes());
        write_root(&mut w, doc, &mut peak);
        w.finish().unwrap();
    }
    let write = t.elapsed();

    let t = Instant::now();
    reader::decode(&buf).expect("decode");
    let read_full = t.elapsed();

    let lookup = lookup_keys.map(|keys| {
        let t = Instant::now();
        let mut hits = 0u64;
        for k in keys {
            if reader::lookup_key(&buf, k.as_bytes())
                .expect("lookup")
                .is_some()
            {
                hits += 1;
            }
        }
        let total = t.elapsed();
        assert_eq!(hits as usize, keys.len(), "every sampled key must hit");
        total / keys.len() as u32
    });

    Outcome {
        write,
        read_full,
        lookup,
        out_bytes: buf.len(),
        peak,
    }
}

// ============================================================================
// Output
// ============================================================================

fn human_bytes(n: usize) -> String {
    const U: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < U.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{} {}", n, U[i])
    } else {
        format!("{:.2} {}", v, U[i])
    }
}

fn fmt_time(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.1} µs", ns as f64 / 1e3)
    } else if ns < 1_000_000_000 {
        format!("{:.1} ms", ns as f64 / 1e6)
    } else {
        format!("{:.2} s", ns as f64 / 1e9)
    }
}

fn sample_keys(n_keys: usize, n_samples: usize) -> Vec<String> {
    (0..n_samples)
        .map(|i| {
            let idx = (i.wrapping_mul(2654435761) >> 8) % n_keys;
            format!("key_{idx:08}")
        })
        .collect()
}

// ============================================================================
// Reports
// ============================================================================

struct Workload<'a> {
    label: &'a str,
    doc: &'a Value,
    json_size: usize,
    lookup_keys: Option<&'a [String]>,
}

fn print_baseline(workloads: &[Workload<'_>]) {
    let opts = WriterOptions::default();
    let sizing_label = match opts.policy.sizing {
        NodeSizing::Fanout(f) => format!("Fanout({})", f),
        NodeSizing::TargetBytes(t) => format!("TargetBytes({})", t),
    };
    println!(
        "=== baseline (default: sizing={}, sortwin={}) ===",
        sizing_label, opts.object_sort_window
    );
    println!();
    println!(
        "{:>16}  {:>10}  {:>9}  {:>11}  {:>9}  {:>9}  {:>10}  {:>9}",
        "workload", "json", "write", "write_thru", "read_full", "lookup", "output", "peak",
    );
    println!("{}", "-".repeat(96));

    for w in workloads {
        let r = run_one(w.doc, WriterOptions::default(), w.lookup_keys, w.json_size);
        let thru_bps = (w.json_size as f64 / r.write.as_secs_f64()) as usize;
        let lookup = r.lookup.map(fmt_time).unwrap_or_else(|| "-".to_string());
        println!(
            "{:>16}  {:>10}  {:>9}  {:>9}/s  {:>9}  {:>9}  {:>10}  {:>9}",
            w.label,
            human_bytes(w.json_size),
            fmt_time(r.write),
            human_bytes(thru_bps),
            fmt_time(r.read_full),
            lookup,
            human_bytes(r.out_bytes),
            human_bytes(r.peak),
        );
    }
}

/// Tuning sensitivity on big-object: the shape where knobs actually matter.
/// Carefully chosen `(fanout, sortwin)` pairs walk the lookup ↔ memory curve.
fn print_tuning(big: &Value, big_json_len: usize, big_lookups: &[String]) {
    println!();
    println!("=== tuning sensitivity (big-object) ===");
    println!();
    println!(
        "{:>8}  {:>8}  {:>10}  {:>10}   {}",
        "fanout", "sortwin", "lookup", "peak", "note"
    );
    println!("{}", "-".repeat(60));

    let configs: &[(usize, usize, &str)] = &[
        (32, 32, "min memory"),
        (32, 1024, ""),
        (128, 128, ""),
        (128, 16_384, "default"),
        (256, 256, ""),
        (256, 65_536, "max useful lookup"),
    ];

    for &(fanout, sortwin, note) in configs {
        let opts = WriterOptions {
            policy: BuildPolicy::compact(fanout),
            object_sort_window: sortwin,
        };
        let r = run_one(big, opts, Some(big_lookups), big_json_len);
        println!(
            "{:>8}  {:>8}  {:>10}  {:>10}   {}",
            fanout,
            sortwin,
            fmt_time(r.lookup.unwrap()),
            human_bytes(r.peak),
            note,
        );
    }
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    let n_records: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5_000);
    let big_keys: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);
    let fat_keys: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000);

    let doc = build_doc(n_records);
    let big = build_big_object(big_keys);
    let fat = build_big_object_fat(fat_keys);

    let doc_json = serde_json::to_vec(&doc).unwrap();
    let big_json = serde_json::to_vec(&big).unwrap();
    let fat_json = serde_json::to_vec(&fat).unwrap();

    let big_lookups = sample_keys(big_keys, 1024);
    let fat_lookups = sample_keys(fat_keys, 1024);

    let workloads = [
        Workload {
            label: "array-of-records",
            doc: &doc,
            json_size: doc_json.len(),
            lookup_keys: None,
        },
        Workload {
            label: "big-object",
            doc: &big,
            json_size: big_json.len(),
            lookup_keys: Some(&big_lookups),
        },
        Workload {
            label: "big-object-fat",
            doc: &fat,
            json_size: fat_json.len(),
            lookup_keys: Some(&fat_lookups),
        },
    ];

    print_baseline(&workloads);
    print_tuning(&big, big_json.len(), &big_lookups);
}
