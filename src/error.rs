use std::fmt;
use std::io;

#[derive(Debug)]
pub enum WriteError {
    Io(io::Error),
    IntegerOutOfRange,
    NaNOrInfinity,
    FloatPrecisionLoss,
    DuplicateKey,
    EmptyDocument,
    MultipleRootValues,
    MisuseObjectValue,
    MisuseObjectKey,
    Poisoned,
    AlreadyFinished,
    InvalidOption(&'static str),
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteError::Io(e) => write!(f, "I/O error: {}", e),
            WriteError::IntegerOutOfRange => write!(f, "integer out of [-2^63, 2^64-1] range"),
            WriteError::NaNOrInfinity => write!(f, "NaN or Infinity float is not representable"),
            WriteError::FloatPrecisionLoss => write!(f, "float cannot be represented losslessly"),
            WriteError::DuplicateKey => write!(f, "duplicate key within an object run"),
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
