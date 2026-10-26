// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Checkpoint files: what `resume` needs to continue a scan.
//!
//! A scan with checkpoints keeps `checkpoint/` in its output directory:
//!
//! - `state`: the latest checkpoint. A JSON header (the scan's settings and
//!   every part committed so far) and the walker's state, zstd-compressed.
//!   Written atomically, so it's always the previous checkpoint or the new
//!   one.
//! - `spill` and `visited`: files the walker's state refers to.
//! - `lock`: locked by the process scanning, so two can't write one scan.
//!
//! The directory is removed when the scan finishes.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read as _, Write as _};
use std::path::{Path, PathBuf};

use fs_inventory_core::walker::WalkState;
use rustix::fs::{FlockOperation, flock};
use serde::{Deserialize, Serialize};

use crate::output::manifest::Resume;
use crate::output::parts::{PartInfo, sync_dir};
use crate::scan::{RunSettings, ScanSettings};

/// The checkpoint directory, inside a scan directory.
pub const DIR: &str = "checkpoint";
const STATE: &str = "state";
const LOCK: &str = "lock";
const MAGIC: &[u8; 8] = b"FSINVCK1";

/// Everything about a scan at a checkpoint except the walker's state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    pub scan_id: String,
    pub host: String,
    pub started_at: String,
    pub created_at: String,
    /// Seconds spent scanning up to this checkpoint, across runs.
    pub elapsed_seconds: f64,
    /// 0 for the original scan, then 1, 2, ... for each resume.
    pub run: u32,
    pub settings: ScanSettings,
    pub runtime: RunSettings,
    /// The manifest's `options`, as the original scan recorded them.
    pub options: BTreeMap<String, serde_json::Value>,
    pub resumes: Vec<Resume>,
    pub entry_parts: Vec<PartInfo>,
    pub error_parts: Vec<PartInfo>,
    /// Entry rows by kind, in `EntryKind::ALL` order.
    pub kinds: Vec<u64>,
    pub error_rows: u64,
}

/// The checkpoint directory of `scan_dir`.
pub fn dir(scan_dir: &Path) -> PathBuf {
    scan_dir.join(DIR)
}

/// Whether `scan_dir` has a checkpoint to resume from.
pub fn exists(scan_dir: &Path) -> bool {
    dir(scan_dir).join(STATE).is_file()
}

/// Writes a checkpoint atomically.
pub fn write(scan_dir: &Path, header: &Header, state: &WalkState) -> io::Result<()> {
    let dir = dir(scan_dir);
    let header = serde_json::to_vec(header).map_err(io::Error::other)?;
    let tmp = dir.join(format!("{STATE}.tmp"));
    let mut file = File::create(&tmp)?;
    file.write_all(MAGIC)?;
    file.write_all(&(header.len() as u64).to_le_bytes())?;
    file.write_all(&header)?;
    zstd::stream::copy_encode(&state.encode()[..], &mut file, 3)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, dir.join(STATE))?;
    sync_dir(&dir)
}

/// Reads the checkpoint of `scan_dir`.
pub fn read(scan_dir: &Path) -> io::Result<(Header, WalkState)> {
    let invalid = |what: &str| io::Error::new(io::ErrorKind::InvalidData, what.to_string());
    let mut bytes = Vec::new();
    File::open(dir(scan_dir).join(STATE))?.read_to_end(&mut bytes)?;
    let rest = bytes
        .strip_prefix(MAGIC)
        .ok_or_else(|| invalid("not an fs-inventory checkpoint"))?;
    let (len, rest) = rest
        .split_first_chunk::<8>()
        .ok_or_else(|| invalid("truncated checkpoint"))?;
    let len = usize::try_from(u64::from_le_bytes(*len)).map_err(io::Error::other)?;
    if len > rest.len() {
        return Err(invalid("truncated checkpoint"));
    }
    let (header, state) = rest.split_at(len);
    let header: Header = serde_json::from_slice(header).map_err(io::Error::other)?;
    let state = WalkState::decode(&zstd::stream::decode_all(state)?)?;
    Ok((header, state))
}

/// Creates the checkpoint directory of `scan_dir` and locks it, so no other
/// process can scan into it. Returns `None` if another process holds the
/// lock. The lock lasts as long as the returned file is open. On a file
/// system without locks, it locks nothing.
pub fn lock(scan_dir: &Path) -> io::Result<Option<File>> {
    let dir = dir(scan_dir);
    fs::create_dir_all(&dir)?;
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join(LOCK))?;
    match flock(&file, FlockOperation::NonBlockingLockExclusive) {
        Err(e) if e == rustix::io::Errno::WOULDBLOCK => Ok(None),
        // Locked, or locks aren't supported (some NFS mounts).
        _ => Ok(Some(file)),
    }
}

/// Removes the checkpoint directory of a finished scan.
pub fn remove(scan_dir: &Path) -> io::Result<()> {
    match fs::remove_dir_all(dir(scan_dir)) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}
