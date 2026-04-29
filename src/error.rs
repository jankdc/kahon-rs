use std::fmt;
use std::io;

/// Errors returned by the writer.
///
/// Most variants describe API misuse (out-of-order pushes) or
/// value-shape problems (NaN floats, out-of-range integers).
/// I/O failures from the underlying sink are wrapped in
/// [`WriteError::Io`].
///
/// Once any operation returns an error, the writer becomes *poisoned*
/// and subsequent calls fail with [`WriteError::Poisoned`] instead of
/// producing a malformed document.
#[derive(Debug)]
pub enum WriteError {
    /// The underlying [`Sink`](crate::Sink) returned an `io::Error`.
    Io(io::Error),
    /// Integer fell outside the representable range `[-2^63, 2^64 - 1]`.
    IntegerOutOfRange,
    /// `f64` was NaN or ±∞, neither of which is representable.
    NaNOrInfinity,
    /// A float could not be encoded losslessly in the chosen tag.
    FloatPrecisionLoss,
    /// [`Writer::finish`](crate::Writer::finish) was called before any
    /// root value was pushed.
    EmptyDocument,
    /// More than one root value was pushed at the writer level.
    MultipleRootValues,
    /// A value was pushed into an object without a preceding key.
    MisuseObjectValue,
    /// A key was pushed while a previous key still awaited its value.
    MisuseObjectKey,
    /// A previous error left the writer in an unrecoverable state.
    Poisoned,
    /// [`Writer::finish`](crate::Writer::finish) was called twice.
    AlreadyFinished,
    /// [`WriterOptions`](crate::WriterOptions) failed validation.
    InvalidOption(&'static str),
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteError::Io(e) => write!(f, "I/O error: {}", e),
            WriteError::IntegerOutOfRange => write!(f, "integer out of [-2^63, 2^64-1] range"),
            WriteError::NaNOrInfinity => write!(f, "NaN or Infinity float is not representable"),
            WriteError::FloatPrecisionLoss => write!(f, "float cannot be represented losslessly"),
            WriteError::EmptyDocument => write!(f, "finish() called with no root value"),
            WriteError::MultipleRootValues => write!(f, "more than one root value pushed"),
            WriteError::MisuseObjectValue => write!(f, "value pushed into object without a key"),
            WriteError::MisuseObjectKey => write!(f, "key pushed but previous key has no value"),
            WriteError::Poisoned => write!(f, "writer poisoned by prior error"),
            WriteError::AlreadyFinished => write!(f, "writer already finished"),
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
