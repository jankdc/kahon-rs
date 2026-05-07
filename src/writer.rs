use std::marker::PhantomData;

use crate::{RootArrayBuilder, RootObjectBuilder};
use crate::config::WriterOptions;
use crate::error::WriteError;
use crate::raw_writer::RawWriter;
use crate::sink::Sink;

/// State marker for a [`Writer`] that has not yet received its root value.
///
/// In this state the writer exposes the `push_*` and `start_*` methods.
/// Each of those consumes the writer and produces either a
/// [`Writer<S, Filled>`] (scalars) or a root builder
/// ([`RootArrayBuilder`](crate::RootArrayBuilder) /
/// [`RootObjectBuilder`](crate::RootObjectBuilder)) which itself yields a
/// [`Writer<S, Filled>`] on close. The "root pushed exactly once"
/// invariant is therefore enforced at compile time: a
/// [`Writer<S, Empty>`] has no `finish`, and a `Writer<S, Filled>` has no
/// way to push a second root.
pub struct Empty;

/// State marker for a [`Writer`] whose root value has been written.
///
/// In this state the only operations available are
/// [`finish`](Writer::finish) (consumes the writer, emits the trailer,
/// returns the sink) and [`snapshot_trailer`](Writer::snapshot_trailer)
/// (synthesizes closing bytes without disturbing the writer).
pub struct Filled;

/// Builder-pattern writer for a single Kahon document.
///
/// `Writer` is a thin newtype over [`RawWriter`](crate::raw::RawWriter);
/// the underlying writer holds the state and primitive operations, while
/// `Writer` adds the typed builder API on top. For applications that
/// need a flat, runtime-checked surface (FFI, async parsers, storage
/// adapters), use [`RawWriter`](crate::raw::RawWriter) directly.
///
/// See the [crate-level docs](crate) for a full example.
pub struct Writer<S: Sink, State = Empty> {
    pub(crate) raw: RawWriter<S>,
    _state: PhantomData<State>,
}

// ---------------------------------------------------------------------
// Introspection - available in any state.
// ---------------------------------------------------------------------

impl<S: Sink, State> Writer<S, State> {
    /// Total bytes emitted to the sink so far, including the header,
    /// every flushed value/node, and any alignment padding.
    pub fn bytes_written(&self) -> u64 {
        self.raw.bytes_written()
    }

    /// Total bytes of unreferenced filler emitted by the page-alignment
    /// policy (zero when [`PageAlignment::None`](crate::PageAlignment::None)
    /// is in effect). Useful for quantifying the cost of disk-friendly layout.
    pub fn padding_bytes_written(&self) -> u64 {
        self.raw.padding_bytes_written()
    }

    /// Approximate live in-memory footprint of the writer's working buffers.
    pub fn buffered_bytes(&self) -> usize {
        self.raw.buffered_bytes()
    }
}

impl<S: Sink> Writer<S, Empty> {
    /// Create a writer with default options ([`WriterOptions::default`]).
    ///
    /// The header is written eagerly; if that initial write fails, the
    /// writer is poisoned and the error surfaces on the next operation.
    pub fn new(sink: S) -> Self {
        Self {
            raw: RawWriter::new(sink),
            _state: PhantomData,
        }
    }

    /// Create a writer with caller-supplied [`WriterOptions`].
    ///
    /// Returns [`WriteError::InvalidOption`] if the policy is malformed
    /// (fanout < 2, target bytes < 64, or non–power-of-two page size).
    pub fn with_options(sink: S, opts: WriterOptions) -> Result<Self, WriteError> {
        Ok(Self {
            raw: RawWriter::with_options(sink, opts)?,
            _state: PhantomData,
        })
    }

    /// Push a `null` as the document root. Consumes the writer; the
    /// returned `Writer<S, Filled>` only allows `finish` /
    /// `snapshot_trailer`.
    pub fn push_null(mut self) -> Result<Writer<S, Filled>, WriteError> {
        self.raw.push_null()?;
        Ok(self.into_filled())
    }

    /// Push a boolean as the document root.
    pub fn push_bool(mut self, v: bool) -> Result<Writer<S, Filled>, WriteError> {
        self.raw.push_bool(v)?;
        Ok(self.into_filled())
    }

    /// Push a signed 64-bit integer as the document root. Encoded in the
    /// narrowest tag that fits the value.
    pub fn push_i64(mut self, v: i64) -> Result<Writer<S, Filled>, WriteError> {
        self.raw.push_i64(v)?;
        Ok(self.into_filled())
    }

    /// Push an unsigned 64-bit integer as the document root.
    pub fn push_u64(mut self, v: u64) -> Result<Writer<S, Filled>, WriteError> {
        self.raw.push_u64(v)?;
        Ok(self.into_filled())
    }

    /// Push a 64-bit float as the document root. NaN and ±∞ are rejected.
    pub fn push_f64(mut self, v: f64) -> Result<Writer<S, Filled>, WriteError> {
        self.raw.push_f64(v)?;
        Ok(self.into_filled())
    }

    /// Push a UTF-8 string as the document root.
    pub fn push_str(mut self, s: &str) -> Result<Writer<S, Filled>, WriteError> {
        self.raw.push_str(s)?;
        Ok(self.into_filled())
    }

    /// Open the root container as an array.
    pub fn start_array(self) -> RootArrayBuilder<S> {
        RootArrayBuilder::new(self.into_raw())
    }

    /// Open the root container as an object.
    pub fn start_object(self) -> RootObjectBuilder<S> {
        RootObjectBuilder::new(self.into_raw())
    }

    fn into_filled(self) -> Writer<S, Filled> {
        Writer {
            raw: self.raw,
            _state: PhantomData,
        }
    }

    /// Promote the underlying raw writer (for use with the `start_array`
    /// / `start_object` constructors on the root builders).
    pub(crate) fn into_raw(self) -> RawWriter<S> {
        self.raw
    }
}

impl<S: Sink> Writer<S, Filled> {
    /// Reconstruct a `Writer<S, Filled>` from a raw writer that has its
    /// root value set. Used by root builders' `end()`.
    pub(crate) fn from_raw(raw: RawWriter<S>) -> Self {
        Self {
            raw,
            _state: PhantomData,
        }
    }

    /// Finalize the document by writing the 12-byte trailer and return
    /// the underlying sink.
    pub fn finish(self) -> Result<S, WriteError> {
        self.raw.finish()
    }
}
