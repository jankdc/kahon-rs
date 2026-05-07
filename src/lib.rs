//! A streaming writer for the [Kahon binary format].
//!
//! Kahon is a JSON-shaped binary container designed for random access:
//! arrays and objects are laid out as on-disk B+trees, so a reader can
//! index into a million-element array or look up a key in a million-key
//! object without scanning the document. Values are written as they
//! arrive and containers reference children by back-offset, so writer
//! memory stays bounded by tree depth, not document size.
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
//! let w = Writer::new(&mut buf);
//!
//! // `start_object` consumes the empty `Writer` and returns a root
//! // builder; nested builders below borrow it `&mut`.
//! let mut monster = w.start_object();
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
//!     // `axe` and `weapons` auto-close on drop.
//! }
//!
//! // `.end()` on the root builder returns the writer in its `Filled`
//! // state, which is the only state with `finish`.
//! let w = monster.end()?;
//! w.finish()?; // writes the 12-byte trailer
//! # Ok(())
//! # }
//! ```
//!
//! # Building a document
//!
//! Construction follows a flexbuffer-style builder pattern with one
//! twist: the root push is *typestate-guarded*. A [`Writer<S, Empty>`]
//! exposes `push_*` (scalars) and `start_array` / `start_object`
//! (containers); each consumes `self` and produces either a
//! [`Writer<S, Filled>`] (scalars) or a root builder
//! ([`RootArrayBuilder`] / [`RootObjectBuilder`]) which itself yields a
//! [`Writer<S, Filled>`] on close. Only `Writer<S, Filled>` has
//! [`finish`](Writer::finish), so "exactly one root value per document"
//! is enforced at compile time.
//!
//! - Scalars are pushed via `push_null`, `push_bool`, `push_i64`,
//!   `push_u64`, `push_f64`, `push_str`.
//! - Containers are opened with `start_array` / `start_object`. The
//!   *root* call returns a [`RootArrayBuilder`] / [`RootObjectBuilder`]
//!   (which owns the writer outright); nested calls inside builders
//!   return [`ArrayBuilder`] / [`ObjectBuilder`] (which borrow their
//!   parent mutably).
//! - Object keys are passed positionally before the value
//!   (`obj.push_i64("hp", 80)`). Duplicate keys within an object's
//!   sort window resolve last-wins (the latest push for a given key
//!   replaces earlier ones).
//!
//! # Closing builders: `Drop` vs `end`
//!
//! *Nested* builders ([`ArrayBuilder`], [`ObjectBuilder`]) close their
//! container on `Drop`, which is convenient for the happy path. If the
//! close encounters a write error, `Drop` has nowhere to surface it —
//! the writer is poisoned and the error is reported on the next
//! operation. Call [`ArrayBuilder::end`] / [`ObjectBuilder::end`] to
//! close explicitly and propagate errors as a `Result`.
//!
//! *Root* builders ([`RootArrayBuilder`], [`RootObjectBuilder`]) are
//! marked `#[must_use]`. Calling `.end()` is the only way to recover
//! the [`Writer<S, Filled>`] you need to call `finish` on — dropping a
//! root builder leaves the document truncated (no trailer).
//!
//! # Tuning the layout
//!
//! [`WriterOptions`] selects how B+tree nodes are sized and whether the
//! body is padded for page-cache friendliness:
//!
//! ```
//! use kahon::{BuildPolicy, Writer, WriterOptions};
//!
//! # fn main() -> Result<(), kahon::WriteError> {
//! # let mut sink: Vec<u8> = Vec::new();
//! // Disk-tuned: each B+tree node targets one page, trailer is page-aligned.
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
//! The default ([`BuildPolicy::compact`] with fanout 128) produces the
//! tightest output and is best for in-memory or network use.
//! [`BuildPolicy::disk_aligned`] trades a small amount of unreferenced
//! padding for a layout that plays well with the page cache when files
//! are `pread`-ed or memory-mapped.
//!
//! # Sinks
//!
//! Any type implementing [`std::io::Write`] is also a [`Sink`] via a
//! blanket impl, so you can write to a `Vec<u8>`, a `File`, a
//! `BufWriter`, or any other writer without adapters.
//!
//! # Errors
//!
//! All fallible operations return [`WriteError`]. Once an error occurs
//! mid-document the writer is *poisoned*: subsequent operations fail
//! fast with [`WriteError::Poisoned`] rather than producing a malformed
//! file.

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
/// (storage-engine adapters, async stream parsers, FFI bridges).
/// Most users want [`Writer`] and its builders. See [`raw`] module
/// docs for details.
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
