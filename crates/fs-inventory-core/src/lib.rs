// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Core library for `fs-inventory`: the parallel walker, platform backends,
//! file-name encoding, and the inventory record model.
//!
//! File names are handled as raw bytes throughout; conversion to text happens
//! only at the output boundary, so no name is ever lost or altered.

#![warn(missing_docs)]

/// Version of the inventory output schema. Bump on any incompatible change
/// to column names, types, or semantics.
pub const SCHEMA_VERSION: u32 = 1;

/// Version of this library, as recorded in output metadata.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
