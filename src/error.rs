use std::fmt;
use std::io;

/// Errors returned by the writer.
///
/// Variants cover I/O failures, value-shape problems (NaN/±∞ floats),
/// and structural errors (empty document, multiple roots, invalid options).
/// API misuse (mismatched key/value pushes, double-finish) is prevented
/// by the builder borrow-checker and `finish(self)` consuming the writer,
/// so it does not appear here.
///
/// Once any operation returns an error, the writer becomes *poisoned*
/// and subsequent calls fail with [`WriteError::Poisoned`] instead of
/// producing a malformed document.
#[derive(Debug)]
pub enum WriteError {
    /// The underlying [`Sink`](crate::Sink) returned an `io::Error`.
    Io(io::Error),
    /// `f64` was NaN or ±∞, neither of which is representable.
    NaNOrInfinity,
    /// [`Writer::finish`](crate::Writer::finish) was called before any
    /// root value was pushed.
    EmptyDocument,
    /// More than one root value was pushed at the writer level.
    MultipleRootValues,
    /// A previous error left the writer in an unrecoverable state.
    Poisoned,
    /// [`WriterOptions`](crate::WriterOptions) failed validation.
    InvalidOption(&'static str),
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteError::Io(e) => write!(f, "I/O error: {}", e),
            WriteError::NaNOrInfinity => write!(f, "NaN or Infinity float is not representable"),
            WriteError::EmptyDocument => write!(f, "finish() called with no root value"),
            WriteError::MultipleRootValues => write!(f, "more than one root value pushed"),
            WriteError::Poisoned => write!(f, "writer poisoned by prior error"),
            WriteError::InvalidOption(s) => write!(f, "invalid writer option: {}", s),
        }
    }
}

impl std::error::Error for WriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WriteError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for WriteError {
    fn from(e: io::Error) -> Self {
        WriteError::Io(e)
    }
}
