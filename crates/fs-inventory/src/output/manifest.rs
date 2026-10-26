// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! `manifest.json`: what a scan covered, how it was run, whether it finished,
//! and which part files hold its rows. Ingestion jobs read this first.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::parts::PartInfo;

/// Scan status, as recorded in the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    /// The scan is still running (or the process died).
    Running,
    /// Every directory was visited. Per-entry errors may still have occurred.
    Complete,
    /// Stopped early by a signal; queued directories were not read.
    Interrupted,
    /// Output could not be written; the parts listed are valid but partial.
    Failed,
    /// Finished, except for directories given up on after `--stall-timeout`
    /// (listed in `stalled`).
    Incomplete,
}

/// A directory the scan gave up on because reading it stopped making
/// progress.
#[derive(Debug, Clone, Serialize)]
pub struct StalledDir {
    pub path: String,
    pub path_b64: Option<String>,
    /// Seconds without progress when the scan gave up.
    pub seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Root {
    /// The root path as text.
    pub path: String,
    /// The exact bytes, base64-encoded, when `path` differs from them.
    pub path_b64: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Totals {
    pub entries: u64,
    /// Rows by kind (`file`, `dir`, ...).
    pub by_kind: BTreeMap<&'static str, u64>,
    pub dirs_read: u64,
    /// Sum of `size` over entries that are not directories.
    pub bytes: u64,
    pub errors: u64,
    /// Entries deleted between being listed and being read.
    pub vanished: u64,
    /// Directories not entered again because `--detect-cycles` had already
    /// seen them.
    pub revisits: u64,
    /// Directories that waited in the spill file because the queue was over
    /// its memory budget.
    pub spilled: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorSummary {
    pub op: &'static str,
    pub errno: Option<i32>,
    pub errno_name: Option<&'static str>,
    pub count: u64,
}

/// One resume of an interrupted scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resume {
    pub resumed_at: String,
    /// When the checkpoint it continued from was taken.
    pub checkpoint_at: String,
    pub tool_version: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Parts {
    pub entries: Vec<PartInfo>,
    pub errors: Vec<PartInfo>,
    /// With `--resolve-ids`, once the scan finishes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub users: Option<Vec<PartInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<PartInfo>>,
}

/// What `--resolve-ids` found.
#[derive(Debug, Clone, Serialize)]
pub struct IdSummary {
    pub users: usize,
    pub groups: usize,
    /// IDs with no account or group on the scanning host.
    pub users_unnamed: usize,
    pub groups_unnamed: usize,
    /// The command that looked them up: `getent` or `dscacheutil`.
    pub source: Option<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Manifest {
    pub tool: &'static str,
    pub tool_version: &'static str,
    pub schema_version: u32,
    pub scan_id: String,
    pub status: Status,
    pub host: String,
    pub roots: Vec<Root>,
    pub format: &'static str,
    pub compression: String,
    pub columns: Vec<&'static str>,
    pub error_columns: Vec<&'static str>,
    /// Options that shape the output.
    pub options: BTreeMap<String, serde_json::Value>,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub duration_seconds: Option<f64>,
    pub totals: Totals,
    pub errors_by_kind: Vec<ErrorSummary>,
    /// Directories given up on after `--stall-timeout`.
    pub stalled: Vec<StalledDir>,
    /// Each time the scan was resumed after being interrupted.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub resumes: Vec<Resume>,
    /// With `--resolve-ids`, once the scan finishes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids: Option<IdSummary>,
    pub parts: Parts,
}

impl Manifest {
    /// Writes `manifest.json` atomically.
    pub fn write(&self, scan_dir: &Path) -> io::Result<()> {
        write_value(
            scan_dir,
            &serde_json::to_value(self).map_err(io::Error::other)?,
        )
    }
}

/// Writes a manifest, possibly read back and amended, atomically.
pub fn write_value(scan_dir: &Path, manifest: &serde_json::Value) -> io::Result<()> {
    let mut json = serde_json::to_vec_pretty(manifest).map_err(io::Error::other)?;
    json.push(b'\n');
    let tmp = scan_dir.join("manifest.json.tmp");
    fs::write(&tmp, json)?;
    fs::File::open(&tmp)?.sync_all()?;
    fs::rename(tmp, scan_dir.join("manifest.json"))
}
