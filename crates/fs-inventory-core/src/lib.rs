// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Core library for `fs-inventory`: the parallel walker, platform backends,
//! file-name encoding, and the inventory record model.
//!
//! File names are handled as raw bytes throughout; conversion to text happens
//! only at the output boundary, so no name is ever lost or altered.

#![warn(missing_docs)]

pub mod backend;
pub mod model;
pub mod names;
mod spill;
pub mod walker;

/// Version of the inventory output schema. Bump when columns are added, or
/// change name, type, or meaning. Version 3 added the symlink target columns.
pub const SCHEMA_VERSION: u32 = 3;

/// The oldest schema version this library's readers accept. Version 2 scans
/// differ only in lacking the symlink target columns.
pub const OLDEST_READABLE_SCHEMA: u32 = 2;

/// Whether scans written with `version` can be read (verified, rolled up,
/// or converted).
pub fn readable_schema(version: Option<u64>) -> bool {
    version.is_some_and(|v| {
        (u64::from(OLDEST_READABLE_SCHEMA)..=u64::from(SCHEMA_VERSION)).contains(&v)
    })
}

/// Version of this library, as recorded in output metadata.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
