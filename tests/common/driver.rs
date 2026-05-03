//! Drive a `kahon::Writer` from a `serde_json::Value`. The int/float
//! distinction is taken from `serde_json::Number`, which preserves the
//! source token's classification (presence of `.` or `e`/`E`).

use kahon::{ArrayBuilder, ObjectBuilder, WriteError, Writer, WriterOptions};
use serde_json::{Number, Value};

pub fn encode(value: &Value, opts: WriterOptions) -> Result<Vec<u8>, WriteError> {
    let mut buf = Vec::new();
    {
        let mut w = Writer::with_options(&mut buf, opts)?;
        write_value(&mut w, value)?;
        w.finish()?;
    }
    Ok(buf)
}

fn write_value<S: kahon::Sink>(w: &mut Writer<S>, value: &Value) -> Result<(), WriteError> {
    match value {
        Value::Null => w.push_null(),
        Value::Bool(b) => w.push_bool(*b),
        Value::Number(n) => write_number_root(w, n),
        Value::String(s) => w.push_str(s),
        Value::Array(items) => {
            let mut a = w.start_array();
            for item in items {
                write_in_array(&mut a, item)?;
            }
            a.end()
        }
        Value::Object(map) => {
            let mut o = w.start_object();
            for (k, v) in map {
                write_in_object(&mut o, k, v)?;
            }
            o.end()
        }
    }
}

fn write_in_array<S: kahon::Sink>(
    a: &mut ArrayBuilder<'_, S>,
    value: &Value,
) -> Result<(), WriteError> {
    match value {
        Value::Null => a.push_null(),
        Value::Bool(b) => a.push_bool(*b),
        Value::Number(n) => write_number_in_array(a, n),
        Value::String(s) => a.push_str(s),
        Value::Array(items) => {
            let mut nested = a.start_array();
            for item in items {
                write_in_array(&mut nested, item)?;
            }
            nested.end()
        }
        Value::Object(map) => {
            let mut nested = a.start_object();
            for (k, v) in map {
                write_in_object(&mut nested, k, v)?;
            }
            nested.end()
        }
    }
}

fn write_in_object<S: kahon::Sink>(
    o: &mut ObjectBuilder<'_, S>,
    key: &str,
    value: &Value,
) -> Result<(), WriteError> {
    match value {
        Value::Null => o.push_null(key),
        Value::Bool(b) => o.push_bool(key, *b),
        Value::Number(n) => write_number_in_object(o, key, n),
        Value::String(s) => o.push_str(key, s),
        Value::Array(items) => {
            let mut nested = o.start_array(key)?;
            for item in items {
                write_in_array(&mut nested, item)?;
            }
            nested.end()
        }
        Value::Object(map) => {
            let mut nested = o.start_object(key)?;
            for (k, v) in map {
                write_in_object(&mut nested, k, v)?;
            }
            nested.end()
        }
    }
}

fn write_number_root<S: kahon::Sink>(w: &mut Writer<S>, n: &Number) -> Result<(), WriteError> {
    if let Some(i) = n.as_i64() {
        if !n.to_string().contains('.') && !n.to_string().contains(['e', 'E']) {
            return w.push_i64(i);
        }
    }
    if let Some(u) = n.as_u64() {
        if !n.to_string().contains('.') && !n.to_string().contains(['e', 'E']) {
            return w.push_u64(u);
        }
    }
    let f = n
        .as_f64()
        .expect("serde_json Number always converts to f64");
    w.push_f64(f)
}

fn write_number_in_array<S: kahon::Sink>(
    a: &mut ArrayBuilder<'_, S>,
    n: &Number,
) -> Result<(), WriteError> {
    let s = n.to_string();
    let is_float = s.contains('.') || s.contains(['e', 'E']);
    if !is_float {
        if let Some(u) = n.as_u64() {
            return a.push_u64(u);
        }
        if let Some(i) = n.as_i64() {
            return a.push_i64(i);
        }
    }
    a.push_f64(
        n.as_f64()
            .expect("serde_json Number always converts to f64"),
    )
}

fn write_number_in_object<S: kahon::Sink>(
    o: &mut ObjectBuilder<'_, S>,
    key: &str,
    n: &Number,
) -> Result<(), WriteError> {
    let s = n.to_string();
    let is_float = s.contains('.') || s.contains(['e', 'E']);
    if !is_float {
        if let Some(u) = n.as_u64() {
            return o.push_u64(key, u);
        }
        if let Some(i) = n.as_i64() {
            return o.push_i64(key, i);
        }
    }
    o.push_f64(
        key,
        n.as_f64()
            .expect("serde_json Number always converts to f64"),
    )
}
