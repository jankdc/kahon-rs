//! Shared test helpers. Keeping this under `tests/common/` (rather than
//! `tests/common.rs`) prevents Cargo from treating it as its own test binary.
//!
//! Each test binary (golden, property, ...) re-includes `mod common` and uses
//! a different subset of helpers, so per-target dead_code warnings are
//! expected. Suppress them globally for the helpers.
#![allow(dead_code)]

pub mod driver;
pub mod encode;
pub mod reader;
pub mod walker;
