use std::marker::PhantomData;

use crate::{RootArrayBuilder, RootObjectBuilder};
use crate::config::WriterOptions;
use crate::error::WriteError;
use crate::raw_writer::RawWriter;
use crate::sink::Sink;

/// Typestate marker: writer has not yet received its root value.
/// Exposes the `push_*` and `start_*` methods.
pub struct Empty;

/// Typestate marker: writer's root value is in place. Only
/// [`finish`](Writer::finish) is available.
pub struct Filled;

/// Builder-pattern writer for a single Kahon document.
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
    /// Total bytes emitted to the sink so far.
    pub fn bytes_written(&self) -> u64 {
        self.raw.bytes_written()
    }

    /// Bytes of unreferenced padding emitted under [`PageAlignment::Aligned`]
    /// (zero otherwise). Useful for quantifying the cost of disk-friendly
    /// layout.
    ///
    /// [`PageAlignment::Aligned`]: crate::PageAlignment::Aligned
    pub fn padding_bytes_written(&self) -> u64 {
        self.raw.padding_bytes_written()
    }

    /// Approximate live in-memory footprint of the writer's working buffers.
    pub fn buffered_bytes(&self) -> usize {
        self.raw.buffered_bytes()
    }
}

impl<S: Sink> Writer<S, Empty> {
    /// Create a writer with default options.
    ///
    /// I/O errors from the initial header write are deferred and
    /// surface on the first push or `finish`.
    pub fn new(sink: S) -> Self {
        Self {
            raw: RawWriter::new(sink),
            _state: PhantomData,
        }
    }

    /// Create a writer with caller-supplied [`WriterOptions`].
    ///
    /// Returns [`WriteError::InvalidOption`] if the options fail
    /// validation.
    pub fn with_options(sink: S, opts: WriterOptions) -> Result<Self, WriteError> {
        Ok(Self {
            raw: RawWriter::with_options(sink, opts)?,
            _state: PhantomData,
        })
    }

    /// Push a `null` as the document root.
    pub fn push_null(mut self) -> Result<Writer<S, Filled>, WriteError> {
        self.raw.push_null()?;
        Ok(self.into_filled())
    }

    /// Push a boolean as the document root.
    pub fn push_bool(mut self, v: bool) -> Result<Writer<S, Filled>, WriteError> {
        self.raw.push_bool(v)?;
        Ok(self.into_filled())
    }

    /// Push a signed 64-bit integer as the document root.
    pub fn push_i64(mut self, v: i64) -> Result<Writer<S, Filled>, WriteError> {
        self.raw.push_i64(v)?;
        Ok(self.into_filled())
    }

    /// Push an unsigned 64-bit integer as the document root.
    pub fn push_u64(mut self, v: u64) -> Result<Writer<S, Filled>, WriteError> {
        self.raw.push_u64(v)?;
        Ok(self.into_filled())
    }

    /// Push a 64-bit float as the document root.
    ///
    /// Returns [`WriteError::NaNOrInfinity`] for NaN or ±∞.
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

    pub(crate) fn into_raw(self) -> RawWriter<S> {
        self.raw
    }
}

impl<S: Sink> Writer<S, Filled> {
    pub(crate) fn from_raw(raw: RawWriter<S>) -> Self {
        Self {
            raw,
            _state: PhantomData,
        }
    }

    /// Finalize the document and return the underlying sink.
    pub fn finish(self) -> Result<S, WriteError> {
        self.raw.finish()
    }
}
