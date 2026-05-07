//! Streaming writer for the [Kahon binary format] - a JSON-shaped
//! container with random-access B+tree arrays and objects.
//!
//! Writer memory stays bounded by tree depth, not document size, so
//! arbitrarily large documents stream through without buffering the
//! whole thing.
//!
//! [Kahon binary format]: https://github.com/jankdc/kahon
//!
//! # Quick start
//!
//! ```
//! use kahon::Writer;
//!
//! # fn main() -> Result<(), kahon::WriteError> {
//! let mut buf: Vec<u8> = Vec::new();
//!
//! let mut monster = Writer::new(&mut buf).start_object();
//! monster.push_i64("hp", 80)?;
//! monster.push_bool("enraged", true)?;
//!
//! {
//!     let mut weapons = monster.start_array("weapons")?;
//!     weapons.push_str("fist")?;
//!
//!     let mut axe = weapons.start_object();
//!     axe.push_str("name", "great axe")?;
//!     axe.push_i64("damage", 15)?;
//!     // nested builders auto-close on drop
//! }
//!
//! monster.end()?.finish()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Building a document
//!
//! A [`Writer`] holds exactly one root value: push a scalar, or open
//! the root with [`start_array`](Writer::start_array) /
//! [`start_object`](Writer::start_object) to get a
//! [`RootArrayBuilder`] / [`RootObjectBuilder`]. Inside a container,
//! [`ArrayBuilder`] / [`ObjectBuilder`] let you append more scalars and
//! open nested containers. Object methods take the key positionally
//! (`obj.push_i64("hp", 80)`); duplicate keys resolve last-wins.
//!
//! Finish the document by calling [`.end()`](RootObjectBuilder::end) on
//! the root builder, then [`.finish()`](Writer::finish) on the writer
//! it returns. The compiler enforces exactly-one-root and rejects
//! `finish` before a root is written.
//!
//! # Closing builders: `Drop` vs `end`
//!
//! Nested [`ArrayBuilder`] / [`ObjectBuilder`] close on drop - handy on
//! the happy path, but a close error has nowhere to go and poisons the
//! writer instead. Call `.end()?` to surface that error as a `Result`.
//!
//! Root builders ([`RootArrayBuilder`], [`RootObjectBuilder`]) are
//! `#[must_use]`: dropping one without `.end()` leaves the document
//! without a trailer.
//!
//! # Tuning the layout
//!
//! ```
//! use kahon::{BuildPolicy, Writer, WriterOptions};
//!
//! # fn main() -> Result<(), kahon::WriteError> {
//! # let mut sink: Vec<u8> = Vec::new();
//! let opts = WriterOptions {
//!     policy: BuildPolicy::disk_aligned(4096),
//!     ..Default::default()
//! };
//! let w = Writer::with_options(&mut sink, opts)?;
//! # let _ = w.push_null()?;
//! # Ok(())
//! # }
//! ```
//!
//! The default produces the tightest output and suits in-memory or
//! network use. [`BuildPolicy::disk_aligned`] adds a small amount of
//! padding for a layout friendlier to `pread` / `mmap`.
//!
//! # Sinks
//!
//! Any [`std::io::Write`] is a [`Sink`] - `Vec<u8>`, `File`,
//! `BufWriter`, etc. all work without adapters.
//!
//! # Errors
//!
//! Fallible operations return [`WriteError`]. After any error mid-
//! document, the writer is *poisoned* and further calls fail fast with
//! [`WriteError::Poisoned`] instead of producing a malformed file.

mod align;
mod bplus;
mod builder;
mod checkpoint;
mod config;
mod encode;
mod error;
mod frame;
mod raw_writer;
mod sink;
mod types;
mod writer;

/// Flat, runtime-checked writer surface for advanced integrations
/// (FFI bridges, async stream parsers, storage adapters). Most users
/// want [`Writer`] and its builders.
pub mod raw {
    pub use crate::checkpoint::Checkpoint;
    pub use crate::raw_writer::RawWriter;
}

pub use builder::{ArrayBuilder, ObjectBuilder, RootArrayBuilder, RootObjectBuilder};
pub use checkpoint::TrailerSnapshot;
pub use config::{BuildPolicy, NodeSizing, PageAlignment, WriterOptions};
pub use error::WriteError;
pub use sink::{RewindableSink, Sink};
pub use writer::{Empty, Filled, Writer};

/// Convenience alias for `std::result::Result<T, WriteError>`.
pub type Result<T> = std::result::Result<T, WriteError>;
