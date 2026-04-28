mod align;
mod bplus;
mod builder;
mod encode;
mod error;
mod sink;
mod types;
mod writer;

pub use builder::{ArrayBuilder, ObjectBuilder};
pub use error::WriteError;
pub use sink::Sink;
pub use writer::{BuildPolicy, NodeSizing, PageAlignment, Writer, WriterOptions};

pub type Result<T> = std::result::Result<T, WriteError>;
