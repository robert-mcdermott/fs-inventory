// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Scans of generated trees (`--synthetic`). A generated tree's inode
//! numbers are 2 to its entry count plus 1, each used once, so a scan wrote
//! every entry exactly once if and only if its inode numbers are exactly
//! that range, however it was interrupted and resumed.

#![allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]

use std::fs::{self, File};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray as _;
use arrow_array::types::UInt64Type;
use serde_json::Value;

fn command(args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fs-inventory"));
    command.args(args);
    command
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn manifest(scan: &Path) -> Value {
    serde_json::from_str(&fs::read_to_string(scan.join("manifest.json")).unwrap()).unwrap()
}

/// Checks that a Parquet scan of a generated tree has every entry exactly
/// once, and that `verify` agrees.
fn assert_every_entry_once(scan: &Path, entries: u64) {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut inodes = Vec::new();
    for part in manifest(scan)["parts"]["entries"].as_array().unwrap() {
        let file = File::open(scan.join(part["file"].as_str().unwrap())).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        for batch in reader.map(Result::unwrap) {
            let ino = batch.column_by_name("ino").unwrap();
            inodes.extend(ino.as_primitive::<UInt64Type>().values().iter().copied());
        }
    }
    inodes.sort_unstable();
    assert_eq!(inodes.len() as u64, entries);
    assert!(
        inodes.iter().copied().eq(2..entries + 2),
        "duplicate or missing entries"
    );
    let out = command(&["verify", scan.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
}

/// Waits for a child to exit, killing it after `limit`.
fn wait(child: &mut Child, limit: Duration) -> Option<i32> {
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status.code().or(Some(-1));
        }
        if started.elapsed() > limit {
            child.kill().unwrap();
            child.wait().unwrap();
            return None;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn generated_trees_are_scanned_completely() {
    let tmp = tempfile::tempdir().unwrap();
    for (i, (spec, entries)) in [
        ("1", 1),
        ("40k", 40_000),
        ("entries=60000,files=4,fanout=2,huge=5000,seed=3", 60_000),
    ]
    .into_iter()
    .enumerate()
    {
        let scan = tmp.path().join(format!("scan{i}"));
        let out = command(&["scan", "/generated", "--synthetic", spec, "-t", "7"])
            .args(["-o", scan.to_str().unwrap(), "--progress", "plain"])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(0), "{}", text(&out));
        assert!(
            text(&out).contains("scanning a generated tree"),
            "{}",
            text(&out)
        );
        let manifest = manifest(&scan);
        assert_eq!(manifest["totals"]["entries"], entries);
        assert!(
            manifest["options"]["synthetic"]
                .as_str()
                .unwrap()
                .starts_with("entries=")
        );
        assert_every_entry_once(&scan, entries);
    }
}

#[test]
fn memory_budgets_fit_writers_and_warn_when_too_small() {
    let tmp = tempfile::tempdir().unwrap();
    let scan = tmp.path().join("scan");
    // A budget fits about one Parquet writer per 160 MB.
    let out = command(&["scan", "/generated", "--synthetic", "1000", "-t", "4"])
        .args(["--max-memory", "320M", "-o", scan.to_str().unwrap()])
        .args(["--progress", "none"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    assert!(!text(&out).contains("need about"), "{}", text(&out));
    assert_eq!(manifest(&scan)["options"]["writers"], 2);

    // 256 threads need more than 256 MB, and the scan says so up front.
    let out = command(&["scan", "/generated", "--synthetic", "1000", "-t", "256"])
        .args([
            "--max-memory",
            "256M",
            "-o",
            "-",
            "-f",
            "csv",
            "--progress",
            "none",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("256 crawl threads need about")
            && stderr.contains("--max-memory 256.00 MiB"),
        "{stderr}"
    );
}

#[test]
fn killed_scans_of_generated_trees_resume_exactly() {
    let tmp = tempfile::tempdir().unwrap();
    let scan = tmp.path().join("scan");
    let scan_arg = scan.to_str().unwrap();
    // 30,000 entries at 100 µs per stat (a hook in debug builds) with two
    // threads: about 1.5 seconds.
    let mut child = command(&["scan", "/generated", "--synthetic", "30k", "-t", "2"])
        .args(["-o", scan_arg, "--checkpoint-interval", "100ms"])
        .env("FS_INVENTORY_DEBUG_STAT_DELAY_US", "100")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let started = Instant::now();
    while !scan.join("checkpoint").join("state").exists() {
        assert!(started.elapsed() < Duration::from_secs(20), "no checkpoint");
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(600));
    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(manifest(&scan)["status"], "running");

    // The checkpoint records the tree, so the resume scans the same one.
    let out = command(&["resume", scan_arg]).output().unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    let manifest = manifest(&scan);
    assert_eq!(manifest["status"], "complete");
    assert_eq!(manifest["resumes"].as_array().unwrap().len(), 1);
    assert_every_entry_once(&scan, 30_000);
}

/// Scans of every shape and thread count, some interrupted at random
/// moments: every one must exit promptly with the right code.
#[test]
fn every_scan_exits() {
    let tmp = tempfile::tempdir().unwrap();
    let cases: Vec<u64> = (0..120).collect();
    let failures = std::sync::Mutex::new(Vec::new());
    let (dir, failed) = (tmp.path(), &failures);
    std::thread::scope(|scope| {
        for chunk in cases.chunks(30) {
            scope.spawn(move || {
                for &case in chunk {
                    // A small linear congruential generator: enough variety,
                    // and the same cases every run.
                    let mut state = case.wrapping_mul(6_364_136_223_846_793_005) ^ 0x5eed;
                    let mut next = |n: u64| {
                        state = state
                            .wrapping_mul(6_364_136_223_846_793_005)
                            .wrapping_add(1_442_695_040_888_963_407);
                        (state >> 33) % n
                    };
                    let entries = [1, 2, 3, 50, 700, 9_000, 20_000][next(7) as usize];
                    let threads = [1, 2, 3, 7, 16, 64, 256][next(7) as usize];
                    let spec = format!(
                        "entries={entries},files={},fanout={},huge={},seed={case}",
                        [1, 4, 32][next(3) as usize],
                        [2, 3, 8][next(3) as usize],
                        [0, 1000][next(2) as usize],
                    );
                    let scan = dir.join(format!("scan{case}"));
                    let mut args = vec![
                        "scan".to_string(),
                        "/generated".into(),
                        "--synthetic".into(),
                        spec,
                        "-t".into(),
                        threads.to_string(),
                        "--progress".into(),
                        "none".into(),
                    ];
                    if next(2) == 0 {
                        args.extend(["-o".into(), "-".into(), "-f".into(), "csv".into()]);
                    } else {
                        let interval = ["0", "50ms", "10m"][next(3) as usize];
                        args.extend([
                            "-o".into(),
                            scan.to_str().unwrap().into(),
                            "--checkpoint-interval".into(),
                            interval.into(),
                        ]);
                    }
                    let interrupt = next(3) == 0;
                    let args: Vec<&str> = args.iter().map(String::as_str).collect();
                    let mut child = command(&args)
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .spawn()
                        .unwrap();
                    if interrupt {
                        std::thread::sleep(Duration::from_millis(next(200)));
                        let pid =
                            rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap())
                                .unwrap();
                        let _ = rustix::process::kill_process(pid, rustix::process::Signal::INT);
                    }
                    let code = wait(&mut child, Duration::from_secs(60));
                    // An interrupt that arrives before the handler is
                    // installed ends the process by the signal (-1 here).
                    let ok = match code {
                        None => false,
                        Some(code) if interrupt => [0, 3, -1].contains(&code),
                        Some(code) => code == 0,
                    };
                    if !ok {
                        failed.lock().unwrap().push(format!("{args:?}: {code:?}"));
                    }
                    let _ = fs::remove_dir_all(&scan);
                }
            });
        }
    });
    let failures = failures.into_inner().unwrap();
    assert!(failures.is_empty(), "{failures:#?}");
}
