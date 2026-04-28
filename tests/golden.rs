//! Golden-file test harness.
//!
//! For each `tests/fixtures/**/*.json`:
//!   1. Parse the JSON, derive `WriterOptions` from filename suffixes
//!      (`.fanoutN`, `.sortwinN`).
//!   2. Encode via `kahon::Writer`.
//!   3. Compare against the sibling `.kahon` file. Set `UPDATE_GOLDEN=1` to
//!      regenerate (also auto-creates the file if missing).
//!   4. Round-trip the bytes through the test reference reader and assert
//!      structural equality with the source JSON value.

mod common;

use common::{driver, reader};
use kahon::{BuildPolicy, NodeSizing, WriterOptions};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn collect_json(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            collect_json(&p, out);
        } else if p.extension().and_then(|s| s.to_str()) == Some("json") {
            out.push(p);
        }
    }
}

/// Parse `WriterOptions` overrides from the filename stem. Recognized suffixes
/// (in any order, separated by `.`):
///   - `fanoutN`   -> NodeSizing::Fanout(N)
///   - `sortwinN`  -> WriterOptions.object_sort_window = N
///
/// Goldens use the compact policy (fixed fanout, no page padding) so the
/// emitted bytes stay stable regardless of the library's `Default` policy.
///
/// e.g. `multi_run.fanout2.sortwin2.json`
fn opts_from_filename(path: &Path) -> WriterOptions {
    let mut opts = WriterOptions {
        policy: BuildPolicy::compact(128),
        ..WriterOptions::default()
    };
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    for segment in stem.split('.') {
        if let Some(n) = segment.strip_prefix("fanout") {
            if let Ok(v) = n.parse::<usize>() {
                opts.policy.sizing = NodeSizing::Fanout(v);
            }
        } else if let Some(n) = segment.strip_prefix("sortwin") {
            if let Ok(v) = n.parse::<usize>() {
                opts.object_sort_window = v;
            }
        }
    }
    opts
}

fn run_fixture(json_path: &Path) {
    let kahon_path = json_path.with_extension("kahon");
    let json_text = fs::read_to_string(json_path)
        .unwrap_or_else(|e| panic!("read {}: {}", json_path.display(), e));
    let value: Value = serde_json::from_str(&json_text)
        .unwrap_or_else(|e| panic!("parse {}: {}", json_path.display(), e));
    let opts = opts_from_filename(json_path);

    let bytes = driver::encode(&value, opts.clone())
        .unwrap_or_else(|e| panic!("encode {}: {:?}", json_path.display(), e));

    let update = std::env::var_os("UPDATE_GOLDEN").is_some();
    if update || !kahon_path.exists() {
        fs::write(&kahon_path, &bytes).expect("write golden");
        if !update {
            eprintln!(
                "note: created missing golden {} ({} bytes)",
                kahon_path.display(),
                bytes.len()
            );
        }
    } else {
        let expected = fs::read(&kahon_path).expect("read golden");
        assert_eq!(
            bytes,
            expected,
            "byte mismatch for {} (set UPDATE_GOLDEN=1 to regenerate)",
            json_path.display()
        );
    }

    // Round-trip: decoded value must structurally equal the source JSON.
    let decoded = reader::decode(&bytes)
        .unwrap_or_else(|e| panic!("decode {}: {:?}", json_path.display(), e));
    assert_eq!(
        decoded.value,
        value,
        "round-trip mismatch for {}",
        json_path.display()
    );
}

#[test]
fn all_fixtures() {
    let mut paths = Vec::new();
    collect_json(&fixtures_dir(), &mut paths);
    paths.sort();
    assert!(
        !paths.is_empty(),
        "no fixtures found under {}",
        fixtures_dir().display()
    );
    for p in paths {
        run_fixture(&p);
    }
}
