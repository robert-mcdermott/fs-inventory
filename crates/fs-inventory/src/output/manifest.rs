// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! `manifest.json`: what a scan covered, how it was run, whether it finished,
//! and which part files hold its rows. Ingestion jobs read this first.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;

use serde::Serialize;

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
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
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
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorSummary {
    pub op: &'static str,
    pub errno: Option<i32>,
    pub errno_name: Option<&'static str>,
    pub count: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Parts {
    pub entries: Vec<PartInfo>,
    pub errors: Vec<PartInfo>,
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
    pub options: BTreeMap<&'static str, serde_json::Value>,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub duration_seconds: Option<f64>,
    pub totals: Totals,
    pub errors_by_kind: Vec<ErrorSummary>,
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
