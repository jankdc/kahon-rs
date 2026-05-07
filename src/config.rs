//! Writer configuration knobs.

use crate::error::WriteError;

/// Strategy for choosing B+tree node size.
#[derive(Clone, Debug)]
pub enum NodeSizing {
    /// Fixed number of entries per node. Predictable, tightest output.
    Fanout(usize),

    /// Target on-disk bytes per node. Use one OS page (e.g. 4096) for
    /// files that will be `pread`-ed or memory-mapped.
    TargetBytes(usize),
}

/// Whether to pad the body for page-cache friendliness on disk.
///
/// Padding is invisible to readers; it only affects file size.
#[derive(Clone, Debug)]
pub enum PageAlignment {
    /// No padding. Tightest file size; best for in-memory or network use.
    None,

    /// Pad so that no container node ≤ `page_size` straddles a page
    /// boundary and the file size is a multiple of `page_size`.
    Aligned { page_size: usize },
}

/// How container nodes are sized and whether the body is page-aligned.
///
/// Use the [`compact`](Self::compact) or
/// [`disk_aligned`](Self::disk_aligned) constructors for typical setups.
#[derive(Clone, Debug)]
pub struct BuildPolicy {
    pub sizing: NodeSizing,
    pub align: PageAlignment,
}

impl BuildPolicy {
    /// Minimum accepted value for [`NodeSizing::TargetBytes`].
    pub const MIN_TARGET_BYTES: usize = 64;

    /// Tight, predictable layout: fixed fanout, no padding. Best for
    /// in-memory or network use.
    pub fn compact(fanout: usize) -> Self {
        Self {
            sizing: NodeSizing::Fanout(fanout),
            align: PageAlignment::None,
        }
    }

    /// Disk-tuned layout: each node targets one page, file size is a
    /// multiple of `page_size`. Best for files that will be `pread`-ed
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
                return Err(WriteError::InvalidOption("fanout must be >= 2"));
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
        Self::compact(128)
    }
}

/// Top-level writer configuration. Pass to
/// [`Writer::with_options`](crate::Writer::with_options).
///
/// `WriterOptions::default()` is in-memory friendly (tight output, no
/// padding); switch [`policy`](Self::policy) to
/// [`BuildPolicy::disk_aligned`] for file-backed workloads.
#[derive(Clone, Debug)]
pub struct WriterOptions {
    /// Max entries buffered per object run before flushing. Larger
    /// values speed up keyed lookups in the resulting file at
    /// proportional memory cost.
    pub object_sort_window: usize,

    /// Node-sizing and page-alignment knobs.
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
