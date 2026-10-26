// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Checkpoints and `resume`: scans killed (`kill -9`) or interrupted at
//! various points, then resumed, must equal an uninterrupted scan, with
//! every row exactly once.
//!
//! The scans are slowed down with `FS_INVENTORY_DEBUG_STAT_DELAY_US`, a hook
//! in debug builds, so they can be stopped partway through a small tree.

#![allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

use arrow_array::Array as _;
use arrow_array::cast::AsArray as _;
use arrow_array::types::Int64Type;
use serde_json::Value;

/// Microseconds added to every `stat`: 2,449 entries take about 2.5 s with
/// two threads.
const DELAY_US: &str = "2000";

fn command(args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fs-inventory"));
    command
        .args(args)
        .env("FS_INVENTORY_DEBUG_STAT_DELAY_US", DELAY_US);
    command
}

fn run(args: &[&str]) -> Output {
    command(args).output().expect("failed to run fs-inventory")
}

fn start(args: &[&str]) -> Child {
    command(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start fs-inventory")
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// 40 directories of 60 files under 8 more: 2,449 entries.
fn tree() -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap().join("tree");
    for d in 0..40 {
        let dir = root.join(format!("d{}", d % 8)).join(format!("s{d}"));
        fs::create_dir_all(&dir).unwrap();
        for f in 0..60 {
            fs::write(dir.join(format!("f{f}")), vec![b'x'; f]).unwrap();
        }
    }
    (tmp, root)
}

/// What an uninterrupted scan writes for a path.
#[derive(Debug, PartialEq, Eq)]
struct Row {
    kind: String,
    size: Option<i64>,
    dir_entries: Option<i64>,
    dir_files: Option<i64>,
    dir_bytes: Option<i64>,
    error: Option<String>,
}

/// Every row of a Parquet scan by path, after checking that no path
/// appears twice and that every row's `parent_id` is its parent's `dir_id`.
fn rows(scan: &Path) -> BTreeMap<String, Row> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(scan.join("manifest.json")).unwrap()).unwrap();
    let mut rows = BTreeMap::new();
    let mut ids = BTreeMap::new();
    let mut links = Vec::new();
    for part in manifest["parts"]["entries"].as_array().unwrap() {
        let file = File::open(scan.join(part["file"].as_str().unwrap())).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        for batch in reader.map(Result::unwrap) {
            let string = |name: &str| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_string::<i32>()
                    .clone()
            };
            let int = |name: &str| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_primitive::<Int64Type>()
                    .clone()
            };
            let (path, parent_path, kind, error) = (
                string("path"),
                string("parent_path"),
                string("kind"),
                string("error"),
            );
            let (dir_id, parent_id, size) = (int("dir_id"), int("parent_id"), int("size"));
            let (dir_entries, dir_files, dir_bytes) =
                (int("dir_entries"), int("dir_files"), int("dir_bytes"));
            let opt = |a: &arrow_array::Int64Array, i| (!a.is_null(i)).then(|| a.value(i));
            for i in 0..batch.num_rows() {
                let row = Row {
                    kind: kind.value(i).to_string(),
                    size: opt(&size, i),
                    dir_entries: opt(&dir_entries, i),
                    dir_files: opt(&dir_files, i),
                    dir_bytes: opt(&dir_bytes, i),
                    error: (!error.is_null(i)).then(|| error.value(i).to_string()),
                };
                let path = path.value(i).to_string();
                if let Some(id) = opt(&dir_id, i) {
                    assert!(ids.insert(id, path.clone()).is_none(), "dir_id {id} twice");
                }
                if parent_id.value(i) != 0 {
                    links.push((parent_id.value(i), parent_path.value(i).to_string()));
                }
                assert!(rows.insert(path.clone(), row).is_none(), "{path} twice");
            }
        }
    }
    for (parent_id, parent_path) in links {
        assert_eq!(ids.get(&parent_id), Some(&parent_path));
    }
    rows
}

fn manifest(scan: &Path) -> Value {
    serde_json::from_str(&fs::read_to_string(scan.join("manifest.json")).unwrap()).unwrap()
}

/// A scan of `root` with no delay, to compare against.
fn reference(root: &Path, dir: &Path) -> BTreeMap<String, Row> {
    let out = Command::new(env!("CARGO_BIN_EXE_fs-inventory"))
        .args(["scan", root.to_str().unwrap(), "-o", dir.to_str().unwrap()])
        .args(["--progress", "none"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    rows(dir)
}

fn kill_after(mut child: Child, delay: Duration) {
    std::thread::sleep(delay);
    child.kill().unwrap();
    child.wait().unwrap();
}

/// Waits until `ready` holds. Timings start from there, not from spawning:
/// starting a freshly built binary can take a second on macOS.
fn wait_until(ready: impl Fn() -> bool) {
    for _ in 0..800 {
        if ready() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("timed out waiting for fs-inventory");
}

/// Whether a scan has written its first checkpoint, which it does before
/// reading any directory.
fn checkpointed(scan: &Path) -> bool {
    scan.join("checkpoint").join("state").exists()
}

/// How many times the manifest says the scan was resumed.
fn resumes(scan: &Path) -> usize {
    fs::read_to_string(scan.join("manifest.json"))
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
        .and_then(|m| m["resumes"].as_array().map(Vec::len))
        .unwrap_or(0)
}

#[test]
fn killed_scans_resume_to_the_same_inventory() {
    let (tmp, root) = tree();
    let expected = reference(&root, &tmp.path().join("reference"));
    assert_eq!(expected.len(), 2449);
    let scan = tmp.path().join("scan");
    let scan_arg = scan.to_str().unwrap();

    let first = start(&[
        "scan",
        root.to_str().unwrap(),
        "-o",
        scan_arg,
        "-t",
        "2",
        "--checkpoint-interval",
        "100ms",
    ]);
    wait_until(|| checkpointed(&scan));
    kill_after(first, Duration::from_millis(500));
    assert_eq!(manifest(&scan)["status"], "running");
    // Killed twice more while resuming, at different points.
    for (i, delay) in [400, 700].into_iter().enumerate() {
        let child = start(&["resume", scan_arg]);
        wait_until(|| resumes(&scan) > i);
        kill_after(child, Duration::from_millis(delay));
    }
    let out = run(&["resume", scan_arg]);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    assert!(text(&out).contains("resuming scan"), "{}", text(&out));

    assert_eq!(rows(&scan), expected);
    let manifest = manifest(&scan);
    assert_eq!(manifest["status"], "complete");
    assert_eq!(manifest["totals"]["entries"], 2449);
    // Every resume is recorded, including those killed before they took a
    // checkpoint.
    assert_eq!(manifest["resumes"].as_array().unwrap().len(), 3);
    assert!(!scan.join("checkpoint").exists());
    let out = run(&["verify", scan_arg]);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
}

#[test]
fn interrupted_scans_resume_without_redoing_work() {
    let (tmp, root) = tree();
    let expected = reference(&root, &tmp.path().join("reference"));
    let scan = tmp.path().join("scan");
    let scan_arg = scan.to_str().unwrap();

    let child = start(&["scan", root.to_str().unwrap(), "-o", scan_arg, "-t", "2"]);
    wait_until(|| checkpointed(&scan));
    std::thread::sleep(Duration::from_millis(600));
    // SIGINT, as Ctrl-C sends.
    let pid = rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::INT).unwrap();
    let out = child.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(3));
    let interrupted = manifest(&scan);
    assert_eq!(interrupted["status"], "interrupted");
    let written = interrupted["totals"]["entries"].as_u64().unwrap();
    assert!(written > 0 && written < 2449, "{written}");

    // The final checkpoint is exact: the resumed scan starts with every row
    // the interrupted one wrote.
    let out = run(&["resume", scan_arg, "--progress", "plain"]);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    assert!(
        text(&out).contains(&format!("{written} entries written")),
        "{}",
        text(&out)
    );
    assert_eq!(rows(&scan), expected);
}

#[test]
fn resume_refuses_a_scan_that_is_still_running() {
    let (tmp, root) = tree();
    let scan = tmp.path().join("scan");
    let scan_arg = scan.to_str().unwrap();
    let child = start(&["scan", root.to_str().unwrap(), "-o", scan_arg, "-t", "1"]);
    wait_until(|| checkpointed(&scan));
    let out = run(&["resume", scan_arg]);
    kill_after(child, Duration::ZERO);
    assert_eq!(out.status.code(), Some(2), "{}", text(&out));
    assert!(text(&out).contains("still being written"), "{}", text(&out));
}

#[test]
fn resume_has_nothing_to_do_for_finished_or_uncheckpointed_scans() {
    let (tmp, root) = tree();
    let scan = tmp.path().join("scan");
    reference(&root, &scan);
    let out = run(&["resume", scan.to_str().unwrap()]);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    assert!(text(&out).contains("nothing to resume"));

    let unprotected = tmp.path().join("unprotected");
    let child = start(&[
        "scan",
        root.to_str().unwrap(),
        "-o",
        unprotected.to_str().unwrap(),
        "-t",
        "1",
        "--checkpoint-interval",
        "0",
    ]);
    wait_until(|| unprotected.join("manifest.json").exists());
    kill_after(child, Duration::from_millis(300));
    assert_eq!(manifest(&unprotected)["status"], "running");
    assert!(!unprotected.join("checkpoint").exists());
    let out = run(&["resume", unprotected.to_str().unwrap()]);
    assert_eq!(out.status.code(), Some(2), "{}", text(&out));
    assert!(text(&out).contains("no checkpoint"), "{}", text(&out));
}

#[test]
fn resume_discards_parts_written_after_the_checkpoint() {
    let (tmp, root) = tree();
    let expected = reference(&root, &tmp.path().join("reference"));
    let scan = tmp.path().join("scan");
    let scan_arg = scan.to_str().unwrap();
    // Small buffers and parts, and only the first checkpoint (before any
    // row), so the kill leaves committed parts the checkpoint doesn't list.
    let child = start(&[
        "scan",
        root.to_str().unwrap(),
        "-o",
        scan_arg,
        "-t",
        "2",
        "--max-memory",
        "1MiB",
        "--part-max-rows",
        "50",
        "--checkpoint-interval",
        "1h",
    ]);
    let committed = || {
        fs::read_dir(scan.join("entries")).map_or(0, |entries| {
            entries
                .filter(|e| {
                    e.as_ref()
                        .unwrap()
                        .path()
                        .extension()
                        .is_some_and(|x| x == "parquet")
                })
                .count()
        })
    };
    wait_until(|| committed() >= 2);
    kill_after(child, Duration::ZERO);

    let out = run(&["resume", scan_arg]);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    assert_eq!(rows(&scan), expected);
    // Only parts the manifest lists remain, the resumed ones named for the
    // run that wrote them.
    let listed: BTreeSet<String> = manifest(&scan)["parts"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| {
            p["file"]
                .as_str()
                .unwrap()
                .trim_start_matches("entries/")
                .to_string()
        })
        .collect();
    let remaining: BTreeSet<String> = fs::read_dir(scan.join("entries"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(remaining, listed);
    assert!(
        listed.iter().all(|p| p.starts_with("part-r1-")),
        "{listed:?}"
    );
}

#[test]
fn text_scans_resume_too() {
    let (tmp, root) = tree();
    let scan = tmp.path().join("scan");
    let scan_arg = scan.to_str().unwrap();
    let child = start(&[
        "scan",
        root.to_str().unwrap(),
        "-o",
        scan_arg,
        "-f",
        "jsonl",
        "--compression",
        "zstd",
        "-t",
        "2",
        "--checkpoint-interval",
        "100ms",
    ]);
    wait_until(|| checkpointed(&scan));
    kill_after(child, Duration::from_millis(600));
    let out = run(&["resume", scan_arg]);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    let out = run(&["verify", scan_arg]);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    let mut paths = BTreeSet::new();
    for part in manifest(&scan)["parts"]["entries"].as_array().unwrap() {
        let file = File::open(scan.join(part["file"].as_str().unwrap())).unwrap();
        let text = String::from_utf8(zstd::stream::decode_all(file).unwrap()).unwrap();
        for line in text.lines() {
            let row: Value = serde_json::from_str(line).unwrap();
            assert!(paths.insert(row["path"].as_str().unwrap().to_string()));
        }
    }
    assert_eq!(paths.len(), 2449);
}
