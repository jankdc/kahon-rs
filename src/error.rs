use std::fmt;
use std::io;

/// Errors returned by the writer.
///
/// After any error mid-document the writer is *poisoned*: subsequent
/// calls fail with [`WriteError::Poisoned`] instead of producing a
/// malformed document.
#[derive(Debug)]
pub enum WriteError {
    /// The underlying [`Sink`](crate::Sink) returned an `io::Error`.
    Io(io::Error),
    /// `f64` was NaN or ±∞.
    NaNOrInfinity,
    /// [`Writer::finish`](crate::Writer::finish) was called with no root
    /// value.
    EmptyDocument,
    /// More than one root value was pushed.
    MultipleRootValues,
    /// A previous error left the writer in an unrecoverable state.
    Poisoned,
    /// [`WriterOptions`](crate::WriterOptions) failed validation.
    InvalidOption(&'static str),
    /// `RawWriter::end_array` / `end_object` was called with a
    /// mismatched or absent top frame.
    FrameMismatch,
    /// `RawWriter::push_key` was called outside an object frame.
    KeyOutsideObject,
    /// An extension header was written but no payload value followed
    /// before the enclosing frame closed (or `finish`/`snapshot_trailer`
    /// was called).
    ExtensionWithoutPayload,
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
            WriteError::FrameMismatch => {
                write!(f, "raw writer close did not match the open frame")
            }
            WriteError::KeyOutsideObject => {
                write!(f, "raw writer key push outside an object frame")
            }
            WriteError::ExtensionWithoutPayload => {
                write!(
                    f,
                    "extension header written without a payload value following"
                )
            }
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
