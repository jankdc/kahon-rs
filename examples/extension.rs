//! Encoding a tagged union as a kahon extension.
//!
//! The kahon format treats extensions as opaque single-value wrappers:
//! the writer emits an `ext_id` and one payload value, and assigns no
//! meaning to either. Schema-blind readers stream past the wrapper and
//! recover plain JSON; schema-aware consumers (this library's caller)
//! interpret `ext_id` and decode the payload however they like.
//!
//! This example shows the recommended pattern: the consumer picks
//! `ext_id`s for its variants, and packs any per-variant fields into a
//! structured payload (here, a small object). The writer carries the
//! bytes; meaning lives entirely on the consumer side.
//!
//! Variants encoded:
//!   ext_id 1: Circle { radius }
//!   ext_id 2: Rectangle { w, h }

use kahon::raw::RawWriter;
use kahon::WriteError;

const EXT_CIRCLE: u64 = 1;
const EXT_RECTANGLE: u64 = 2;

enum Shape {
    Circle { radius: f64 },
    Rectangle { w: f64, h: f64 },
}

fn write_shape<S: kahon::Sink>(w: &mut RawWriter<S>, s: &Shape) -> Result<(), WriteError> {
    match s {
        Shape::Circle { radius } => {
            w.push_extension(EXT_CIRCLE)?;
            w.begin_object()?;
            w.push_key("radius")?;
            w.push_f64(*radius)?;
            w.end_object()
        }
        Shape::Rectangle { w: width, h } => {
            w.push_extension(EXT_RECTANGLE)?;
            w.begin_object()?;
            w.push_key("w")?;
            w.push_f64(*width)?;
            w.push_key("h")?;
            w.push_f64(*h)?;
            w.end_object()
        }
    }
}

fn main() {
    let shapes = [
        Shape::Circle { radius: 1.5 },
        Shape::Rectangle { w: 4.0, h: 2.0 },
        Shape::Circle { radius: 10.0 },
    ];

    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = RawWriter::new(&mut buf);
        w.begin_array().unwrap();
        for s in &shapes {
            write_shape(&mut w, s).unwrap();
        }
        w.end_array().unwrap();
        w.finish().unwrap();
    }

    println!("{} shapes serialized in {} bytes", shapes.len(), buf.len());
}
