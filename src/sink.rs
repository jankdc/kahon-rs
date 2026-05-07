use std::io;
use std::io::{Seek, SeekFrom};

/// An append-only byte sink. Implemented for every [`io::Write`] via a
/// blanket impl, so `Vec<u8>`, `File`, `BufWriter`, etc. work directly.
pub trait Sink {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()>;
}

impl<W: io::Write + ?Sized> Sink for W {
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        io::Write::write_all(self, buf)
    }
}

/// A [`Sink`] that can discard bytes past a given length.
///
/// Required for checkpoint and `try_write` rollback. Append-only sinks
/// (sockets, pipes) intentionally do not implement this trait.
pub trait RewindableSink: Sink {
    /// Truncate the sink to `len` bytes and reposition the write cursor
    /// at `len`. `len` must be `<= current_length`.
    fn rewind_to(&mut self, len: u64) -> io::Result<()>;
}

impl RewindableSink for std::fs::File {
    fn rewind_to(&mut self, len: u64) -> io::Result<()> {
        // set_len does not move the OS write cursor; without the seek,
        // the next write would zero-pad past EOF and corrupt the file.
        self.set_len(len)?;
        self.seek(SeekFrom::Start(len))?;
        Ok(())
    }
}

impl RewindableSink for Vec<u8> {
    fn rewind_to(&mut self, len: u64) -> io::Result<()> {
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

/// Forwarding impl so `Writer::new(&mut file)` works.
impl<S: RewindableSink + io::Write + ?Sized> RewindableSink for &mut S {
    fn rewind_to(&mut self, len: u64) -> io::Result<()> {
        (**self).rewind_to(len)
    }
}

pub(crate) struct WriteCtx<'a, S: Sink> {
    pub sink: &'a mut S,
    pub pos: &'a mut u64,
    pub scratch: &'a mut Vec<u8>,
    pub padding_written: &'a mut u64,
}
