use std::io;

/// An append-only byte sink. Every `Write` implementor is a `Sink` via the
/// blanket impl below; a `Vec<u8>` also qualifies since `Vec<u8>: Write`.
pub trait Sink {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()>;
}

impl<W: io::Write + ?Sized> Sink for W {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        io::Write::write_all(self, buf)
    }
}

/// Bundle of state threaded through every encoder helper: the sink to write to,
/// a running byte position (so callers can capture node offsets), a reusable
/// scratch buffer to stage encoded bytes, and a counter for unreferenced
/// padding bytes emitted by page-alignment policy.
pub(crate) struct WriteCtx<'a, S: Sink> {
    pub sink: &'a mut S,
    pub pos: &'a mut u64,
    pub scratch: &'a mut Vec<u8>,
    pub padding_written: &'a mut u64,
}
