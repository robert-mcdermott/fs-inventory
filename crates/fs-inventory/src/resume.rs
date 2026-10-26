// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! The `resume` command: continue an interrupted scan from its checkpoint.
//!
//! Part files committed after the checkpoint are deleted, along with any
//! `.tmp` files a crash left: their rows belong to directories the
//! checkpoint still has queued, which the resumed scan reads again. Every
//! row ends up in the scan exactly once.

use std::fs;
use std::io;
use std::path::Path;

use serde_json::Value;

use crate::checkpoint;
use crate::cli::ResumeArgs;
use crate::exit::Exit;
use crate::output::Destination;
use crate::scan::{self, Origin, SetupError};

pub fn run(args: &ResumeArgs) -> Exit {
    scan::report(run_resume(args))
}

fn run_resume(args: &ResumeArgs) -> Result<Exit, SetupError> {
    let dir = std::path::absolute(&args.scan_dir)
        .map_err(|e| SetupError::usage(format!("invalid scan directory: {e}")))?;
    let shown = dir.display();
    // A scan still running holds the lock; say so before anything else.
    let lock = if checkpoint::dir(&dir).is_dir() {
        Some(
            checkpoint::lock(&dir)
                .map_err(|e| SetupError::fatal(format!("cannot lock {shown}: {e}")))?
                .ok_or_else(|| {
                    SetupError::usage(format!(
                        "{shown} is still being written by another fs-inventory process"
                    ))
                })?,
        )
    } else {
        None
    };
    let manifest: Value = fs::read_to_string(dir.join("manifest.json"))
        .map_err(|e| SetupError::usage(format!("cannot read {shown}/manifest.json: {e}")))
        .and_then(|text| {
            serde_json::from_str(&text)
                .map_err(|e| SetupError::usage(format!("cannot read {shown}/manifest.json: {e}")))
        })?;
    match manifest["status"].as_str() {
        Some("complete") => {
            println!("{shown}: the scan is complete; nothing to resume");
            return Ok(Exit::Success);
        }
        Some("incomplete") => {
            println!(
                "{shown}: the scan finished, except for directories given up on after --stall-timeout, which resume doesn't retry"
            );
            return Ok(Exit::Success);
        }
        _ => {}
    }
    if manifest["schema_version"].as_u64() != Some(u64::from(fs_inventory_core::SCHEMA_VERSION)) {
        return Err(SetupError::usage(format!(
            "{shown} was written with schema version {}; this fs-inventory resumes version {}",
            manifest["schema_version"],
            fs_inventory_core::SCHEMA_VERSION
        )));
    }
    let lock = match lock {
        Some(lock) if checkpoint::exists(&dir) => lock,
        _ => {
            return Err(SetupError::usage(format!(
                "{shown} has no checkpoint to resume from (it was scanned with --checkpoint-interval 0)"
            )));
        }
    };
    let (header, state) = checkpoint::read(&dir)
        .map_err(|e| SetupError::fatal(format!("cannot read the checkpoint in {shown}: {e}")))?;
    if manifest["scan_id"].as_str() != Some(header.scan_id.as_str()) {
        return Err(SetupError::fatal(format!(
            "the checkpoint in {shown} belongs to a different scan"
        )));
    }
    discard_uncheckpointed(&dir, &header)
        .map_err(|e| SetupError::fatal(format!("cannot clean up {shown}: {e}")))?;

    let zero_is_none = |d: std::time::Duration| (!d.is_zero()).then_some(d);
    let mut runtime = header.runtime.clone();
    if let Some(threads) = args.threads {
        runtime.threads = threads;
    }
    if let Some(writers) = args.writers {
        runtime.writers = usize::from(writers);
    }
    if let Some(max_memory) = args.max_memory {
        runtime.max_memory = Some(max_memory);
    }
    if let Some(timeout) = args.stall_timeout {
        runtime.stall_timeout = zero_is_none(timeout);
    }
    if let Some(interval) = args.checkpoint_interval {
        runtime.checkpoint_interval = zero_is_none(interval);
    }
    let settings = header.settings.clone();
    scan::execute(
        &settings,
        &runtime,
        &args.progress,
        Destination::Dir(dir),
        Origin::Resume {
            header: Box::new(header),
            state,
            lock,
        },
    )
}

/// Deletes every part the checkpoint doesn't list, any `.tmp` files, and a
/// rollup made from the partial scan.
fn discard_uncheckpointed(dir: &Path, header: &checkpoint::Header) -> io::Result<()> {
    let keep: std::collections::BTreeSet<&str> = header
        .entry_parts
        .iter()
        .chain(&header.error_parts)
        .map(|part| part.file.as_str())
        .collect();
    for table in ["entries", "errors"] {
        let table_dir = dir.join(table);
        let entries = match fs::read_dir(&table_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = entry?;
            let name = format!("{table}/{}", entry.file_name().to_string_lossy());
            if !keep.contains(name.as_str()) && entry.file_type()?.is_file() {
                fs::remove_file(entry.path())?;
            }
        }
    }
    for stale in ["rollup", "rollup_ages", ".rollup-staging"] {
        match fs::remove_dir_all(dir.join(stale)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
    }
    Ok(())
}
