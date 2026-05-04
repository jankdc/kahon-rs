//! Page-alignment padding helpers shared by B+tree node emission and the
//! trailer writer. Padding bytes are unreferenced `Null` tags (`0x00`),
//! invisible to readers following offsets.

use crate::config::PageAlignment;
use crate::error::WriteError;
use crate::sink::{Sink, WriteCtx};

const PAD_CHUNK: [u8; 256] = [0u8; 256];

/// Append `n` Null tag bytes (`0x00`). These are valid scalar values that
/// nothing references, so a reader following offsets never sees them.
pub(crate) fn write_padding<S: Sink>(
    ctx: &mut WriteCtx<'_, S>,
    n: usize,
) -> Result<(), WriteError> {
    let mut remaining = n;
    while remaining > 0 {
        let take = remaining.min(PAD_CHUNK.len());
        ctx.sink.write_all(&PAD_CHUNK[..take])?;
        remaining -= take;
    }
    *ctx.pos += n as u64;
    *ctx.padding_written += n as u64;
    Ok(())
}

/// If `align` is enabled and a node of `node_size` bytes would straddle a
/// page boundary, emit padding so the node starts at the next boundary.
/// Nodes larger than `page_size` are emitted as-is (two pages is unavoidable).
pub(crate) fn pad_for_node<S: Sink>(
    ctx: &mut WriteCtx<'_, S>,
    node_size: usize,
    align: &PageAlignment,
) -> Result<(), WriteError> {
    let PageAlignment::Aligned { page_size } = align else {
        return Ok(());
    };
    let page_size = *page_size;
    if node_size == 0 || node_size > page_size {
        return Ok(());
    }
    let pos_in_page = (*ctx.pos as usize) % page_size;
    if pos_in_page + node_size <= page_size {
        return Ok(());
    }
    write_padding(ctx, page_size - pos_in_page)
}
