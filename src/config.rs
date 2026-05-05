//! Writer configuration: B+tree node sizing, page-alignment policy, and
//! the top-level [`WriterOptions`] bundle.

use crate::error::WriteError;

/// How the writer decides when to close a B+tree node and flush it to disk.
#[derive(Clone, Debug)]
pub enum NodeSizing {
    /// Fixed entry-count cap. Predictable, smallest files. Each leaf or
    /// internal node closes once it accumulates `n` entries.
    Fanout(usize),

    /// Target on-disk byte budget per node. The writer derives an
    /// effective fanout per flush from the offset width currently in
    /// use, sizing nodes to fill (but not exceed) the target. Recommended
    /// value for disk-resident files: 4096 (one OS page). See spec §13.3.
    ///
    /// If a single entry would exceed the target, the writer waits for a
    /// second entry rather than emitting an `m=1` node.
    TargetBytes(usize),
}

/// Whether to pad the body for page-cache friendliness on disk.
///
/// Padding bytes are `Null` tags (`0x00`) that no offset references; they
/// are invisible to a reader that traverses the document by following
/// offsets from the root.
#[derive(Clone, Debug)]
pub enum PageAlignment {
    /// No padding. Tightest file size; preferred for in-memory or
    /// network use where cache locality is not a concern.
    None,

    /// Pad so that:
    ///   1. No container node ≤ `page_size` straddles a page boundary.
    ///   2. The 12-byte trailer lands in the file's last `page_size`-aligned
    ///      page, i.e. `file_size % page_size == 0`.
    Aligned { page_size: usize },
}

/// Bundle of layout knobs threaded through B+tree construction.
///
/// Combine via [`BuildPolicy::compact`] (in-memory friendly) or
/// [`BuildPolicy::disk_aligned`] (file-backed), or build directly from
/// [`NodeSizing`] and [`PageAlignment`] for fine control.
#[derive(Clone, Debug)]
pub struct BuildPolicy {
    /// How node fanout is decided when flushing.
    pub sizing: NodeSizing,
    /// Whether to pad container layout for page-cache friendliness.
    pub align: PageAlignment,
}

impl BuildPolicy {
    /// Minimum sane `TargetBytes`. Below 64, internal nodes can't fit two
    /// entries at W=8 with header overhead, which would force violations
    /// of the `m >= 2` floor.
    pub const MIN_TARGET_BYTES: usize = 64;

    /// Tight, predictable layout: fixed fanout, no padding. Best for
    /// fixture tests and in-memory pipelines.
    pub fn compact(fanout: usize) -> Self {
        Self {
            sizing: NodeSizing::Fanout(fanout),
            align: PageAlignment::None,
        }
    }

    /// Disk-tuned layout: each node targets one page, trailer is
    /// page-aligned. Suitable default for files that will be `pread`-ed
    /// or memory-mapped.
    pub fn disk_aligned(page_size: usize) -> Self {
        Self {
            sizing: NodeSizing::TargetBytes(page_size),
            align: PageAlignment::Aligned { page_size },
        }
    }

    pub(crate) fn validate(&self) -> Result<(), WriteError> {
        match self.sizing {
            NodeSizing::Fanout(f) if f < 2 => {
                return Err(WriteError::InvalidOption(
                    "fanout must be >= 2 (spec §8 invariant 3)",
                ));
            }
            NodeSizing::TargetBytes(t) if t < Self::MIN_TARGET_BYTES => {
                return Err(WriteError::InvalidOption("target_node_bytes must be >= 64"));
            }
            _ => {}
        }
        if let PageAlignment::Aligned { page_size } = self.align {
            if !page_size.is_power_of_two() || page_size < 64 {
                return Err(WriteError::InvalidOption(
                    "page_size must be a power of two and >= 64",
                ));
            }
        }
        Ok(())
    }
}

impl Default for BuildPolicy {
    fn default() -> Self {
        // In-memory friendly out of the box: fixed fanout, no padding,
        // tightest output. Opt into `disk_aligned(page_size)` for files
        // that will be `pread`-ed or memory-mapped.
        Self::compact(128)
    }
}

/// Top-level writer configuration.
///
/// Combine with [`Writer::with_options`](crate::Writer::with_options) to
/// override the defaults. `WriterOptions::default()` gives an in-memory–friendly
/// setup (fixed fanout, no padding, tightest output).
#[derive(Clone, Debug)]
pub struct WriterOptions {
    /// Max entries buffered (and sorted) per object run before flushing.
    /// Larger values give modestly faster keyed lookups (fewer runs to walk
    /// at internal nodes) at proportional memory cost. Sweet spot is roughly
    /// the effective fanout squared; below that, lookups pay; above, you
    /// only burn memory.
    pub object_sort_window: usize,

    /// Node-sizing and page-alignment knobs. See [`BuildPolicy`].
    pub policy: BuildPolicy,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            object_sort_window: 16_384,
            policy: BuildPolicy::default(),
        }
    }
}
