// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! A scan of a tree that changes while it's read: files are deleted,
//! recreated, and renamed, and directories removed and recreated, the whole
//! time. The scan must finish with a valid output, no duplicate rows, the
//! untouched part of the tree exactly right, and the deletions it ran into
//! counted as vanished.

#![allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::Value;

const CHURN_DIRS: usize = 8;
const FILES: usize = 300;
const SUB_FILES: usize = 30;

fn churn_dir(root: &Path, i: usize) -> PathBuf {
    root.join("churn").join(format!("c{i}"))
}

/// Creates `sub` and its files in a churned directory.
fn make_sub(dir: &Path) {
    let sub = dir.join("sub");
    fs::create_dir(&sub).unwrap();
    for j in 0..SUB_FILES {
        fs::write(sub.join(format!("s{j}")), b"x").unwrap();
    }
}

fn make_tree(root: &Path) {
    for i in 0..20 {
        let dir = root.join("stable").join(format!("d{i}"));
        fs::create_dir_all(&dir).unwrap();
        for j in 0..25 {
            fs::write(dir.join(format!("f{j}")), vec![b'x'; i * j]).unwrap();
        }
    }
    for i in 0..CHURN_DIRS {
        let dir = churn_dir(root, i);
        fs::create_dir_all(&dir).unwrap();
        for j in 0..FILES {
            fs::write(dir.join(format!("f{j}")), b"x").unwrap();
        }
        make_sub(&dir);
    }
}

/// One pass of changes over every churned directory: a third of the files
/// are deleted until the next pass, a fifth are renamed until the next pass,
/// and `sub` is replaced. Errors are ignored, as when a file to rename was
/// just deleted.
fn churn(root: &Path, round: usize) {
    for i in 0..CHURN_DIRS {
        let dir = churn_dir(root, i);
        let file = |j: usize| dir.join(format!("f{j}"));
        let moved = |j: usize| dir.join(format!("moved-{j}"));
        for j in ((round + 2) % 3..FILES).step_by(3) {
            let _ = fs::write(file(j), b"xy");
        }
        for j in (round % 3..FILES).step_by(3) {
            let _ = fs::remove_file(file(j));
        }
        for j in ((round + 4) % 5..FILES).step_by(5) {
            let _ = fs::rename(moved(j), file(j));
        }
        for j in (round % 5..FILES).step_by(5) {
            let _ = fs::rename(file(j), moved(j));
        }
        let _ = fs::remove_dir_all(dir.join("sub"));
        make_sub(&dir);
    }
}

/// Every row of a JSON Lines table, keyed by exact path bytes.
fn read_jsonl(scan: &Path, table: &str) -> BTreeMap<Vec<u8>, Value> {
    let mut rows = BTreeMap::new();
    let Ok(parts) = fs::read_dir(scan.join(table)) else {
        return rows;
    };
    for part in parts {
        for line in fs::read_to_string(part.unwrap().path()).unwrap().lines() {
            let row: Value = serde_json::from_str(line).unwrap();
            let path = match row["path_b64"].as_str() {
                Some(b64) => BASE64.decode(b64).unwrap(),
                None => row["path"].as_str().unwrap().as_bytes().to_vec(),
            };
            assert!(rows.insert(path, row).is_none(), "duplicate row");
        }
    }
    rows
}

#[test]
fn scans_a_tree_that_changes_throughout() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap().join("tree");
    make_tree(&root);
    let stable: BTreeMap<Vec<u8>, fs::Metadata> = walk(&root.join("stable"));

    let entries = walk(&root).len();

    let stop = Arc::new(AtomicBool::new(false));
    let passes = Arc::new(AtomicUsize::new(0));
    let churner = {
        let (root, stop, passes) = (root.clone(), Arc::clone(&stop), Arc::clone(&passes));
        std::thread::spawn(move || {
            let mut round = 0;
            while !stop.load(Ordering::Relaxed) {
                churn(&root, round);
                round += 1;
                passes.store(round, Ordering::Relaxed);
            }
        })
    };
    // Start the scan once the tree is changing, and time a pass of changes.
    let started = Instant::now();
    while passes.load(Ordering::Relaxed) == 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
    let pass = started.elapsed();
    // Slow every stat (a hook in debug builds) so the scan lasts about eight
    // passes, however fast this machine changes the tree: each of the
    // crawl threads stats about `entries / THREADS` names. Directories then
    // stay open long enough for changes to land in the middle of reading
    // them.
    const THREADS: u128 = 4;
    let scan_time = (pass * 8).max(Duration::from_secs(1));
    let delay_us = scan_time.as_micros() * THREADS / entries as u128;
    let scan = tmp.path().join("scan");
    let before = passes.load(Ordering::Relaxed);
    let out = Command::new(env!("CARGO_BIN_EXE_fs-inventory"))
        .args(["scan", root.to_str().unwrap(), "-f", "jsonl"])
        .args(["-t", &THREADS.to_string()])
        .args(["-o", scan.to_str().unwrap(), "--progress", "none"])
        .env("FS_INVENTORY_DEBUG_STAT_DELAY_US", delay_us.to_string())
        .output()
        .unwrap();
    let during = passes.load(Ordering::Relaxed) - before;
    stop.store(true, Ordering::Relaxed);
    churner.join().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(matches!(out.status.code(), Some(0 | 1)), "{stderr}");
    assert!(
        during >= 2,
        "the tree changed during only {during} passes ({pass:?} each)"
    );

    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(scan.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["status"], "complete");
    let verify = Command::new(env!("CARGO_BIN_EXE_fs-inventory"))
        .args(["verify", scan.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(
        verify.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&verify.stderr)
    );

    let rows = read_jsonl(&scan, "entries");
    let totals = &manifest["totals"];
    assert_eq!(totals["entries"], rows.len());
    let vanished = totals["vanished"].as_u64().unwrap();
    assert!(vanished > 0, "no deletions were seen: {totals}");

    // The part of the tree nobody touched is exact.
    for (path, meta) in &stable {
        let row = rows.get(path).unwrap_or_else(|| panic!("missing {path:?}"));
        assert_eq!(row["ino"], meta.ino(), "{path:?}");
        assert_eq!(row["size"], meta.size(), "{path:?}");
        assert_eq!(row["mode"], meta.mode(), "{path:?}");
        assert_eq!(row["nlink"], meta.nlink(), "{path:?}");
    }
    let stable_prefix = root.join("stable");
    let stable_rows = rows
        .keys()
        .filter(|p| p.starts_with(stable_prefix.as_os_str().as_bytes()))
        .count();
    assert_eq!(stable_rows, stable.len());

    // Every churned row names something the churn creates, as its kind.
    let churn_prefix = root.join("churn");
    for (path, row) in &rows {
        let Ok(rest) = Path::new(std::ffi::OsStr::from_bytes(path)).strip_prefix(&churn_prefix)
        else {
            continue;
        };
        let name = rest.file_name().map(|n| n.to_string_lossy().into_owned());
        let expected = match (rest.components().count(), name.as_deref()) {
            (0 | 1, _) => "dir",
            (2, Some("sub")) => "dir",
            (2, Some(n)) if n.starts_with('f') || n.starts_with("moved-") => "file",
            (3, Some(n)) if n.starts_with('s') => "file",
            _ => panic!("unexpected row {path:?}"),
        };
        assert_eq!(row["kind"], expected, "{path:?}");
    }

    // A directory's totals count its children as they were listed. Only a
    // subdirectory removed before it was opened is counted without a row.
    let mut children: BTreeMap<&str, u64> = BTreeMap::new();
    for row in rows.values() {
        if let Some(parent) = row["parent_path"].as_str() {
            *children.entry(parent).or_default() += 1;
        }
    }
    let mut uncounted = 0;
    for row in rows.values().filter(|r| r["kind"] == "dir") {
        let Some(listed) = row["dir_entries"].as_u64() else {
            continue;
        };
        let written = children
            .get(row["path"].as_str().unwrap())
            .copied()
            .unwrap_or(0);
        assert!(written <= listed, "{row}");
        uncounted += listed - written;
    }
    assert!(uncounted <= vanished, "{uncounted} > {vanished}");

    // Directories replaced between being listed and opened are the only
    // errors.
    for error in read_jsonl(&scan, "errors").values() {
        assert_eq!(error["op"], "verify", "{error}");
    }
}

/// Every path under `root`, including `root`, from `std::fs`.
fn walk(root: &Path) -> BTreeMap<Vec<u8>, fs::Metadata> {
    let mut out = BTreeMap::new();
    out.insert(
        root.as_os_str().as_bytes().to_vec(),
        fs::symlink_metadata(root).unwrap(),
    );
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let meta = fs::symlink_metadata(&path).unwrap();
            if meta.is_dir() {
                stack.push(path.clone());
            }
            out.insert(path.as_os_str().as_bytes().to_vec(), meta);
        }
    }
    out
}
