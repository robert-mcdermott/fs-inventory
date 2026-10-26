// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Trees at the limits: 2,000 levels deep, and paths over 10,000 bytes,
//! past `PATH_MAX`, so their directories are opened one component at a
//! time. The million-entry directory test is ignored by default, since it
//! takes minutes on macOS; run it with `cargo test --test extreme --
//! --ignored`.

#![allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File};
use std::io::Write as _;
use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;
use std::process::{Command, Output};

use arrow_array::Array as _;
use arrow_array::RecordBatch;
use arrow_array::cast::AsArray as _;
use arrow_array::types::{Int32Type, Int64Type};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rustix::fs::{Mode, OFlags};
use serde_json::Value;

fn fs_inventory(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fs-inventory"))
        .args(args)
        .output()
        .unwrap()
}

fn ok(out: &Output) {
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Makes `levels` nested directories named `name` under `base`, with a
/// file named `leaf` at the bottom, working relative to open directories
/// since the full paths are too long for the OS. Returns the file's path.
fn chain(base: &Path, name: &str, levels: usize, leaf: &str) -> Vec<u8> {
    let dir_flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
    let mut fd = rustix::fs::open(base, dir_flags, Mode::empty()).unwrap();
    let mut path = base.as_os_str().as_bytes().to_vec();
    for _ in 0..levels {
        rustix::fs::mkdirat(&fd, name, Mode::from_raw_mode(0o755)).unwrap();
        fd = rustix::fs::openat(&fd, name, dir_flags, Mode::empty()).unwrap();
        path.push(b'/');
        path.extend_from_slice(name.as_bytes());
    }
    let file = rustix::fs::openat(
        &fd,
        leaf,
        OFlags::WRONLY | OFlags::CREATE | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o644),
    )
    .unwrap();
    File::from(file).write_all(b"bottom").unwrap();
    path.push(b'/');
    path.extend_from_slice(leaf.as_bytes());
    path
}

fn read_parquet(scan: &Path, table: &str) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    for part in fs::read_dir(scan.join(table)).unwrap() {
        let file = File::open(part.unwrap().path()).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        batches.extend(reader.map(Result::unwrap));
    }
    batches
}

#[test]
fn deep_trees_and_long_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap().join("tree");
    fs::create_dir_all(root.join("deep")).unwrap();
    fs::create_dir_all(root.join("long")).unwrap();
    // 2,000 levels: paths of about 8,000 bytes.
    let deep = chain(&root.join("deep"), "dir", 2000, "bottom");
    // 40 levels of the longest names allowed: paths of about 10,300 bytes.
    let long_name = "n".repeat(255);
    let long = chain(&root.join("long"), &long_name, 40, &"f".repeat(255));
    assert!(deep.len() > 8000 && long.len() > 10_000);
    // root, deep, long, 2,040 chained directories, and 2 files.
    let entries = 2045;

    let scan = tmp.path().join("scan");
    let scan_arg = scan.to_str().unwrap();
    ok(&fs_inventory(&[
        "scan",
        root.to_str().unwrap(),
        "-o",
        scan_arg,
        "--progress",
        "none",
    ]));
    ok(&fs_inventory(&["verify", scan_arg]));
    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(scan.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["totals"]["entries"], entries);
    assert_eq!(manifest["totals"]["errors"], 0);

    // Every row, through convert, with its exact path and parent link.
    let jsonl = tmp.path().join("jsonl");
    ok(&fs_inventory(&[
        "convert",
        scan_arg,
        "--to",
        "jsonl",
        "-o",
        jsonl.to_str().unwrap(),
    ]));
    let mut rows: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
    for part in fs::read_dir(jsonl.join("entries")).unwrap() {
        for line in fs::read_to_string(part.unwrap().path()).unwrap().lines() {
            let row: Value = serde_json::from_str(line).unwrap();
            let path = match row["path_b64"].as_str() {
                Some(b64) => BASE64.decode(b64).unwrap(),
                None => row["path"].as_str().unwrap().as_bytes().to_vec(),
            };
            assert!(rows.insert(path, row).is_none(), "duplicate row");
        }
    }
    assert_eq!(rows.len(), entries);
    assert_eq!(rows[&deep]["depth"], 2002);
    assert_eq!(rows[&deep]["size"], 6);
    assert_eq!(rows[&long]["depth"], 42);
    assert_eq!(rows[&long]["name"], "f".repeat(255));
    let dir_ids: BTreeSet<u64> = rows.values().filter_map(|r| r["dir_id"].as_u64()).collect();
    for (path, row) in &rows {
        let parent = row["parent_id"].as_u64().unwrap();
        assert!(
            parent == 0 || dir_ids.contains(&parent),
            "{:?} has no parent row",
            String::from_utf8_lossy(&path[..80])
        );
    }

    // Subtree totals reach the root from 2,000 levels down.
    ok(&fs_inventory(&["rollup", scan_arg]));
    let mut rollup_rows = 0;
    for batch in read_parquet(&scan, "rollup") {
        rollup_rows += batch.num_rows();
        let depth = batch
            .column_by_name("depth")
            .unwrap()
            .as_primitive::<Int32Type>();
        let col = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_primitive::<Int64Type>()
                .clone()
        };
        let (subtree, files) = (col("entries"), col("files"));
        for i in 0..batch.num_rows() {
            if depth.value(i) == 0 {
                assert_eq!(subtree.value(i), entries as i64 - 1);
                assert_eq!(files.value(i), 2);
            }
        }
    }
    assert_eq!(rollup_rows, entries - 2, "one row per directory");
    ok(&fs_inventory(&["verify", scan_arg]));
}

#[test]
#[ignore = "creates a million files"]
fn a_million_entries_in_one_directory() {
    const FILES: usize = 1_000_000;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap().join("huge");
    fs::create_dir(&root).unwrap();
    let dir = rustix::fs::open(
        &root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    for i in 0..FILES {
        rustix::fs::openat(
            &dir,
            format!("file-{i:07}"),
            OFlags::WRONLY | OFlags::CREATE | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o644),
        )
        .unwrap();
    }

    let scan = tmp.path().join("scan");
    let scan_arg = scan.to_str().unwrap();
    ok(&fs_inventory(&[
        "scan",
        root.to_str().unwrap(),
        "-o",
        scan_arg,
        "--progress",
        "none",
    ]));
    ok(&fs_inventory(&["verify", scan_arg]));

    let mut names = HashSet::with_capacity(FILES);
    let mut dir_entries = None;
    for batch in read_parquet(&scan, "entries") {
        let kind = batch.column_by_name("kind").unwrap().as_string::<i32>();
        let name = batch.column_by_name("name").unwrap().as_string::<i32>();
        let totals = batch
            .column_by_name("dir_entries")
            .unwrap()
            .as_primitive::<Int64Type>();
        for i in 0..batch.num_rows() {
            if kind.value(i) == "dir" {
                assert!(dir_entries.is_none());
                dir_entries = Some(totals.value(i));
            } else {
                assert!(!totals.is_valid(i));
                assert!(names.insert(name.value(i).to_string()), "duplicate");
            }
        }
    }
    assert_eq!(names.len(), FILES);
    assert_eq!(dir_entries, Some(FILES as i64));
}
