// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! The `verify` command: checks that a scan directory is complete and
//! internally consistent before anything ingests it.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;

use serde_json::Value;

use crate::exit::Exit;
use crate::output::parquet;

/// Checks a scan directory. Exits 0 when it is complete and consistent, and
/// 1 (listing the problems) otherwise.
pub fn run(scan_dir: &Path) -> Exit {
    let report = check(scan_dir);
    for problem in &report.problems {
        eprintln!("fs-inventory: problem: {problem}");
    }
    if let Some(summary) = &report.summary {
        println!("{summary}");
    }
    if report.problems.is_empty() {
        println!("ok");
        Exit::Success
    } else {
        Exit::CompletedWithErrors
    }
}

#[derive(Debug, Default)]
pub struct Report {
    pub summary: Option<String>,
    pub problems: Vec<String>,
}

pub fn check(scan_dir: &Path) -> Report {
    let mut report = Report::default();
    let manifest_path = scan_dir.join("manifest.json");
    let manifest: Value = match fs::read_to_string(&manifest_path)
        .map_err(|e| e.to_string())
        .and_then(|text| serde_json::from_str(&text).map_err(|e| e.to_string()))
    {
        Ok(m) => m,
        Err(e) => {
            report
                .problems
                .push(format!("cannot read {}: {e}", manifest_path.display()));
            return report;
        }
    };

    let schema_version = manifest["schema_version"].as_u64();
    if schema_version != Some(u64::from(fs_inventory_core::SCHEMA_VERSION)) {
        report.problems.push(format!(
            "schema version is {}, but this fs-inventory reads version {}",
            manifest["schema_version"],
            fs_inventory_core::SCHEMA_VERSION
        ));
    }
    let status = manifest["status"].as_str().unwrap_or("missing");
    if status != "complete" {
        report
            .problems
            .push(format!("scan status is `{status}`, not `complete`"));
    }
    let format = manifest["format"].as_str().unwrap_or_default();
    let scan_id = manifest["scan_id"].as_str().unwrap_or_default();
    // With literal control characters, CSV records can span lines, so line
    // counts don't equal row counts.
    let count_text_rows =
        !(format == "csv" && manifest["options"]["csv_raw_controls"].as_bool() == Some(true));

    let mut table_rows = [0u64; 2];
    let mut part_counts = [0usize; 2];
    for (t, table) in ["entries", "errors"].into_iter().enumerate() {
        let parts = manifest["parts"][table]
            .as_array()
            .cloned()
            .unwrap_or_default();
        part_counts[t] = parts.len();
        let mut listed = BTreeSet::new();
        for part in &parts {
            let Some(file) = part["file"].as_str() else {
                report
                    .problems
                    .push(format!("a {table} part has no file name"));
                continue;
            };
            listed.insert(file.to_string());
            let rows = part["rows"].as_u64().unwrap_or_default();
            table_rows[t] += rows;
            let path = scan_dir.join(file);
            match fs::metadata(&path) {
                Err(e) => {
                    report.problems.push(format!("{file}: {e}"));
                    continue;
                }
                Ok(meta) if Some(meta.len()) != part["bytes"].as_u64() => {
                    report.problems.push(format!(
                        "{file}: {} bytes, but the manifest says {}",
                        meta.len(),
                        part["bytes"]
                    ));
                }
                Ok(_) => {}
            }
            let counted = match format {
                "parquet" => check_parquet(&path, scan_id, &mut report.problems, file),
                _ if count_text_rows => count_lines(&path).map(|lines| {
                    if format == "csv" {
                        lines.saturating_sub(1)
                    } else {
                        lines
                    }
                }),
                _ => Ok(rows),
            };
            match counted {
                Ok(n) if n == rows => {}
                Ok(n) => report
                    .problems
                    .push(format!("{file}: {n} rows, but the manifest says {rows}")),
                Err(e) => report.problems.push(format!("{file}: {e}")),
            }
        }
        // Anything else in the table directory is leftover or foreign.
        if let Ok(entries) = fs::read_dir(scan_dir.join(table)) {
            for entry in entries.flatten() {
                let name = format!("{table}/{}", entry.file_name().to_string_lossy());
                if !listed.contains(&name) {
                    report
                        .problems
                        .push(format!("{name} is not listed in the manifest"));
                }
            }
        }
    }

    let totals = &manifest["totals"];
    for (t, (name, key)) in [("entries", "entries"), ("errors", "errors")]
        .into_iter()
        .enumerate()
    {
        let expected = totals[key].as_u64().unwrap_or_default();
        if table_rows[t] != expected {
            report.problems.push(format!(
                "{name} parts hold {} rows, but the totals say {expected}",
                table_rows[t]
            ));
        }
    }

    report.summary = Some(format!(
        "scan {scan_id}: {status}, {} entries in {} parts, {} errors in {} parts ({format})",
        table_rows[0], part_counts[0], table_rows[1], part_counts[1]
    ));
    report
}

/// Reads a Parquet part's footer: its row count, and that it belongs to this scan.
fn check_parquet(
    path: &Path,
    scan_id: &str,
    problems: &mut Vec<String>,
    file: &str,
) -> io::Result<u64> {
    let footer_scan_id = parquet::footer_value(path, "fs_inventory.scan_id")?;
    if footer_scan_id.as_deref() != Some(scan_id) {
        problems.push(format!(
            "{file}: belongs to scan {}, not {scan_id}",
            footer_scan_id.as_deref().unwrap_or("(none)")
        ));
    }
    parquet::row_count(path)
}

/// Counts lines in a text part, decompressing it if needed.
fn count_lines(path: &Path) -> io::Result<u64> {
    let file = File::open(path)?;
    let name = path.to_string_lossy();
    let mut reader: Box<dyn Read> = if name.ends_with(".zst") {
        Box::new(zstd::Decoder::new(file)?)
    } else if name.ends_with(".gz") {
        Box::new(flate2::read::GzDecoder::new(file))
    } else {
        Box::new(file)
    };
    let mut buf = vec![0; 1 << 20];
    let mut lines = 0;
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            return Ok(lines);
        }
        lines += buf[..n].iter().filter(|&&b| b == b'\n').count() as u64;
    }
}
