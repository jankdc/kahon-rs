mod common;

use common::reader;
use kahon::raw::RawWriter;
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
fn try_write_ok_matches_no_try_write_run() {
    for (name, policy) in policies() {
        let mut buf_with = Vec::new();
        {
            let mut r = RawWriter::with_options(&mut buf_with, opts(policy.clone())).unwrap();
            r.try_write(|r| -> Result<(), WriteError> {
                r.begin_array()?;
                r.push_i64(1)?;
                r.push_i64(2)?;
                r.end_array()?;
                Ok(())
            })
            .unwrap();
            r.finish().unwrap();
        }

        let mut buf_no = Vec::new();
        {
            let w = Writer::with_options(&mut buf_no, opts(policy)).unwrap();
            let mut a = w.start_array();
            a.push_i64(1).unwrap();
            a.push_i64(2).unwrap();
            let w = a.end().unwrap();
            w.finish().unwrap();
        }

        assert_eq!(buf_with, buf_no, "policy={name}");
    }
}

#[test]
fn try_write_err_discards_writes() {
    for (name, policy) in policies() {
        let mut buf_with = Vec::new();
        {
            let mut r = RawWriter::with_options(&mut buf_with, opts(policy.clone())).unwrap();
            // Try one variant: an array of strings. Force rollback.
            let _ = r.try_write(|r| -> Result<(), WriteError> {
                r.begin_array()?;
                r.push_str("rejected")?;
                r.end_array()?;
                Err(WriteError::EmptyDocument)
            });
            // Try a different variant: a scalar int.
            r.push_i64(42).unwrap();
            r.finish().unwrap();
        }

        let mut buf_no = Vec::new();
        {
            let w = Writer::with_options(&mut buf_no, opts(policy)).unwrap();
            let w = w.push_i64(42).unwrap();
            w.finish().unwrap();
        }

        assert_eq!(buf_with, buf_no, "policy={name}");
        assert_eq!(decode(&buf_with), json!(42));
    }
}

#[test]
fn try_write_nested_inner_err_outer_ok() {
    // Inner try_write returns Err -> "rejected" is undone. Outer
    // try_write returns Ok -> the array with [1, 2] is kept.
    let mut buf = Vec::new();
    {
        let mut r = RawWriter::with_options(&mut buf, opts(BuildPolicy::compact(4))).unwrap();
        r.try_write(|r| -> Result<(), WriteError> {
            r.begin_array()?;
            r.push_i64(1)?;
            let _ = r.try_write(|r| -> Result<(), WriteError> {
                r.push_str("rejected")?;
                Err(WriteError::EmptyDocument)
            });
            r.push_i64(2)?;
            r.end_array()?;
            Ok(())
        })
        .unwrap();
        r.finish().unwrap();
    }
    assert_eq!(decode(&buf), json!([1, 2]));
}

#[test]
fn try_write_nested_inner_ok_outer_err_undoes_everything() {
    // Inner commits a scalar; outer then errors -> the inner's scalar is
    // undone along with everything else. Recover by pushing a fresh root.
    let mut buf = Vec::new();
    {
        let mut r = RawWriter::with_options(&mut buf, opts(BuildPolicy::compact(4))).unwrap();
        let _ = r.try_write(|r| -> Result<(), WriteError> {
            r.try_write(|r| -> Result<(), WriteError> {
                r.push_i64(1)?;
                Ok(())
            })?;
            Err(WriteError::EmptyDocument)
        });
        r.push_i64(99).unwrap();
        r.finish().unwrap();
    }
    assert_eq!(decode(&buf), json!(99));
}

#[test]
fn try_write_array_per_element_variants() {
    // For each element: try the "user" variant (string) inside a
    // try_write; on rejection, fall back to the "guest" variant (null).
    let inputs = [Some("alice"), None, Some("bob"), None, Some("carol")];

    let mut buf = Vec::new();
    {
        let w = Writer::with_options(&mut buf, opts(BuildPolicy::compact(3))).unwrap();
        let mut a = w.start_array();
        for input in inputs.iter() {
            let attempted = a.try_write(|a| -> Result<(), WriteError> {
                a.push_str("speculative")?;
                if input.is_some() {
                    Ok(())
                } else {
                    Err(WriteError::EmptyDocument) // force rollback
                }
            });
            if attempted.is_err() {
                a.push_null().unwrap();
            }
        }
        let w = a.end().unwrap();
        w.finish().unwrap();
    }
    assert_eq!(
        decode(&buf),
        json!(["speculative", null, "speculative", null, "speculative"])
    );
}

#[test]
fn try_write_array_rollback_then_retry() {
    // Validator pattern: try variant A, on err rollback and try variant B.
    let mut buf = Vec::new();
    {
        let w = Writer::with_options(&mut buf, opts(BuildPolicy::compact(4))).unwrap();
        let mut a = w.start_array();
        for &v in &[1, 2, 3] {
            let attempted = a.try_write(|a| -> Result<(), WriteError> {
                a.push_str(&format!("s{v}"))?;
                if v == 2 {
                    Err(WriteError::EmptyDocument)
                } else {
                    Ok(())
                }
            });
            if attempted.is_err() {
                a.push_i64(v).unwrap();
            }
        }
        let w = a.end().unwrap();
        w.finish().unwrap();
    }
    assert_eq!(decode(&buf), json!(["s1", 2, "s3"]));
}

#[test]
fn try_write_object_value_variant_with_rollback() {
    let mut buf = Vec::new();
    {
        let w = Writer::with_options(&mut buf, opts(BuildPolicy::compact(4))).unwrap();
        let mut o = w.start_object();
        o.push_str("name", "alice").unwrap();
        let _ = o.try_write(|o| -> Result<(), WriteError> {
            o.push_str("role", "admin")?;
            Err(WriteError::EmptyDocument)
        });
        o.push_str("role", "guest").unwrap();
        let w = o.end().unwrap();
        w.finish().unwrap();
    }
    assert_eq!(decode(&buf), json!({"name": "alice", "role": "guest"}));
}

#[test]
fn try_write_mixed_nesting_object_with_variant_array() {
    let mut buf = Vec::new();
    {
        let w = Writer::with_options(&mut buf, opts(BuildPolicy::compact(4))).unwrap();
        let mut o = w.start_object();
        o.push_str("kind", "user").unwrap();
        {
            let mut tags = o.start_array("tags").unwrap();
            for tag in &["red", "skip", "blue"] {
                let _ = tags.try_write(|t| -> Result<(), WriteError> {
                    t.push_str(tag)?;
                    if *tag == "skip" {
                        Err(WriteError::EmptyDocument)
                    } else {
                        Ok(())
                    }
                });
            }
            tags.end().unwrap();
        }
        let w = o.end().unwrap();
        w.finish().unwrap();
    }
    assert_eq!(
        decode(&buf),
        json!({"kind": "user", "tags": ["red", "blue"]})
    );
}

#[test]
fn try_write_rollback_under_all_policies_matches_control() {
    for (name, policy) in policies() {
        let mut buf_with = Vec::new();
        {
            let w = Writer::with_options(&mut buf_with, opts(policy.clone())).unwrap();
            let mut o = w.start_object();
            for i in 0..6 {
                o.push_i64(&format!("k{i}"), i as i64).unwrap();
            }
            let _ = o.try_write(|o| -> Result<(), WriteError> {
                for i in 100..106 {
                    o.push_i64(&format!("junk{i}"), i as i64)?;
                }
                Err(WriteError::EmptyDocument)
            });
            for i in 6..10 {
                o.push_i64(&format!("k{i}"), i as i64).unwrap();
            }
            let w = o.end().unwrap();
            w.finish().unwrap();
        }

        let mut buf_no = Vec::new();
        {
            let w = Writer::with_options(&mut buf_no, opts(policy)).unwrap();
            let mut o = w.start_object();
            for i in 0..10 {
                o.push_i64(&format!("k{i}"), i as i64).unwrap();
            }
            let w = o.end().unwrap();
            w.finish().unwrap();
        }

        // Same logical value; not necessarily byte-identical (rollback
        // doesn't recover scratch capacities or other non-observable state).
        assert_eq!(decode(&buf_with), decode(&buf_no), "policy={name}");
    }
}

#[test]
fn try_write_recovers_from_poison() {
    // A failure inside the closure may poison the writer (e.g., a
    // builder's Drop close emits to a sink that errors). The try_write
    // rolls back to the entry state, including clearing the poison flag,
    // and propagates the user's Err.
    use kahon::{RewindableSink, Sink};
    use std::io;

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
    let mut r = RawWriter::with_options(sink, opts(BuildPolicy::compact(128))).unwrap();
    let outcome = r.try_write(|r| -> Result<(), WriteError> {
        r.begin_array()?;
        for _ in 0..16 {
            let _ = r.push_i64(1);
        }
        // The close will fail and poison the writer mid-closure; we
        // return an explicit Err so try_write takes the rollback branch.
        Err(WriteError::Io(io::Error::other("forced rollback")))
    });
    assert!(outcome.is_err());
    // After rollback, position is restored to post-header and poison is
    // cleared - the writer is fundamentally usable again.
    assert_eq!(r.bytes_written(), 6, "position restored to post-header");
}

#[test]
fn try_write_rejects_already_poisoned_writer() {
    // Once the writer is poisoned outside a try_write call, subsequent
    // try_write attempts must fail fast with Poisoned - they do not run
    // the closure, do not take a checkpoint, and do not silently recover.
    use kahon::{RewindableSink, Sink};
    use std::io;

    struct AlwaysFails;
    impl Sink for AlwaysFails {
        fn write_all(&mut self, _: &[u8]) -> io::Result<()> {
            Err(io::Error::other("nope"))
        }
    }
    impl RewindableSink for AlwaysFails {
        fn rewind_to(&mut self, _: u64) -> io::Result<()> {
            Ok(())
        }
    }

    // Construct a writer; the header write fails and poisons it.
    let mut r = RawWriter::with_options(AlwaysFails, opts(BuildPolicy::compact(128))).unwrap();
    // Confirm precondition: writer is poisoned.
    assert!(matches!(r.push_i64(1), Err(WriteError::Poisoned)));

    // try_write must refuse, returning Poisoned without invoking the closure.
    let mut closure_ran = false;
    let outcome: Result<(), WriteError> = r.try_write(|_r| {
        closure_ran = true;
        Ok(())
    });
    assert!(matches!(outcome, Err(WriteError::Poisoned)));
    assert!(
        !closure_ran,
        "try_write must not run f on a poisoned writer"
    );
}
