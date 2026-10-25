// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Process exit codes. These are a stable interface for scripts and schedulers.

use std::process::ExitCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// The crawl completed with no errors.
    Success = 0,
    /// The crawl completed, but some entries could not be read; see the errors table.
    CompletedWithErrors = 1,
    /// Invalid arguments or configuration.
    Usage = 2,
    /// The crawl was interrupted or abandoned stalled subtrees; it can be resumed.
    Incomplete = 3,
    /// A fatal error; no usable output was produced.
    Fatal = 4,
}

impl From<Exit> for ExitCode {
    fn from(exit: Exit) -> Self {
        ExitCode::from(exit as u8)
    }
}
