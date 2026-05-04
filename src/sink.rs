use std::io;
use std::io::{Seek, SeekFrom};

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

/// A [`Sink`] that supports discarding bytes written past a given length.
///
/// Required by the writer's checkpoint primitives ([`Writer::checkpoint`],
/// [`Writer::rollback`](crate::Writer::rollback)): rolling back a checkpoint
/// truncates the sink in place rather than buffering tentative writes in
/// memory.
///
/// Append-only sinks (sockets, pipes) intentionally do not implement this
/// trait. A buffered/staging mode that lifts that restriction is a planned
/// future addition; it will not change the [`Writer`](crate::Writer) API.
///
/// [`Writer::checkpoint`]: crate::Writer::checkpoint
pub trait RewindableSink: Sink {
    /// Discard bytes past `len` and reposition the write cursor at `len`.
    ///
    /// `len` MUST be `<= current_length`. Implementations are not required
    /// to grow the sink; growth-on-rollback is a programming error and
    /// callers (the `Writer`) ensure it never happens.
    fn rewind_to(&mut self, len: u64) -> io::Result<()>;
}

impl RewindableSink for std::fs::File {
    fn rewind_to(&mut self, len: u64) -> io::Result<()> {
        // `set_len` shrinks the file but does not move the OS-level write
        // cursor. If the cursor is left past EOF, the next write extends
        // the file with implicit zero-padding - corrupting the document.
        // Always reposition explicitly.
        self.set_len(len)?;
        self.seek(SeekFrom::Start(len))?;
        Ok(())
    }
}

impl RewindableSink for Vec<u8> {
    fn rewind_to(&mut self, len: u64) -> io::Result<()> {
        // `Vec<u8>: Write` appends; there is no separate cursor, so
        // truncate is sufficient.
        self.truncate(len as usize);
        Ok(())
    }
}

impl RewindableSink for std::io::Cursor<Vec<u8>> {
    fn rewind_to(&mut self, len: u64) -> io::Result<()> {
        self.get_mut().truncate(len as usize);
        self.set_position(len);
        Ok(())
    }
}

/// Forwarding impl so `&mut S: RewindableSink` works when `S` is rewindable
/// and writable. `Writer::new(&mut file)` is a common pattern; this keeps it
/// usable. The `io::Write` bound is needed because the `Sink` supertrait is
/// satisfied via the `io::Write` blanket impl.
impl<S: RewindableSink + io::Write + ?Sized> RewindableSink for &mut S {
    fn rewind_to(&mut self, len: u64) -> io::Result<()> {
        (**self).rewind_to(len)
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
