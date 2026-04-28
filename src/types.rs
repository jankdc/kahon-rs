//! Kahon binary format type codes and framing constants (spec §4–§7).

pub(crate) const MAGIC: [u8; 4] = *b"KAHN";

pub(crate) const NULL: u8 = 0x00;
pub(crate) const FLAGS: u8 = 0x00;
pub(crate) const FALSE: u8 = 0x01;
pub(crate) const TRUE: u8 = 0x02;
pub(crate) const VERSION: u8 = 0x01;
pub(crate) const TINY_NEG_INT: u8 = 0x02;
pub(crate) const TINY_UINT: u8 = 0x13;
pub(crate) const EMPTY_ARRAY: u8 = 0x33;
pub(crate) const EMPTY_OBJECT: u8 = 0x34;
pub(crate) const UINT8: u8 = 0x40;
pub(crate) const UINT16: u8 = 0x41;
pub(crate) const UINT32: u8 = 0x42;
pub(crate) const UINT64: u8 = 0x43;
pub(crate) const INT8: u8 = 0x44;
pub(crate) const INT16: u8 = 0x45;
pub(crate) const INT32: u8 = 0x46;
pub(crate) const INT64: u8 = 0x47;
pub(crate) const FLOAT32: u8 = 0x50;
pub(crate) const FLOAT64: u8 = 0x51;
pub(crate) const TINY_STRING: u8 = 0x60;
pub(crate) const STRING: u8 = 0x6F;
pub(crate) const ARRAY_LEAF_TAG: u8 = 0x70;
pub(crate) const ARRAY_INTERNAL_TAG: u8 = 0x74;
pub(crate) const OBJECT_LEAF_TAG: u8 = 0x80;
pub(crate) const OBJECT_INTERNAL_TAG: u8 = 0x84;
