// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! `--resolve-ids`: the users and groups tables hold every ID in the scan's
//! entries, with names looked up on this host, including after a scan is
//! killed and resumed.

#![allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use arrow_array::Array as _;
use arrow_array::cast::AsArray as _;
use arrow_array::types::Int64Type;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;

fn command(args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fs-inventory"));
    command.args(args);
    command
}

fn ok(out: &Output) -> String {
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(matches!(out.status.code(), Some(0 | 1)), "{text}");
    text
}

fn manifest(scan: &Path) -> Value {
    serde_json::from_str(&fs::read_to_string(scan.join("manifest.json")).unwrap()).unwrap()
}

/// Every value of an Int64 column, and of `name` when there is one, from a
/// Parquet table's parts.
fn column(scan: &Path, table: &str, name: &str) -> Vec<(i64, Option<String>)> {
    let mut values = Vec::new();
    for part in manifest(scan)["parts"][table].as_array().unwrap() {
        let file = File::open(scan.join(part["file"].as_str().unwrap())).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        for batch in reader.map(Result::unwrap) {
            let ids = batch
                .column_by_name(name)
                .unwrap()
                .as_primitive::<Int64Type>();
            let names = batch
                .column_by_name("name")
                .map(|c| c.as_string::<i32>().clone());
            for row in 0..batch.num_rows() {
                let label = names
                    .as_ref()
                    .filter(|n| !n.is_null(row))
                    .map(|n| n.value(row).to_string());
                values.push((ids.value(row), label));
            }
        }
    }
    values
}

/// Checks that the users and groups tables hold exactly the entries' IDs,
/// each once, and that the manifest counts them.
fn check_tables(scan: &Path) -> BTreeMap<i64, Option<String>> {
    let entry_uids: BTreeSet<i64> = column(scan, "entries", "uid")
        .into_iter()
        .map(|v| v.0)
        .collect();
    let entry_gids: BTreeSet<i64> = column(scan, "entries", "gid")
        .into_iter()
        .map(|v| v.0)
        .collect();
    let users = column(scan, "users", "uid");
    let groups = column(scan, "groups", "gid");
    assert_eq!(
        users.iter().map(|v| v.0).collect::<Vec<_>>(),
        entry_uids.into_iter().collect::<Vec<_>>()
    );
    assert_eq!(
        groups.iter().map(|v| v.0).collect::<Vec<_>>(),
        entry_gids.into_iter().collect::<Vec<_>>()
    );
    let ids = &manifest(scan)["ids"];
    assert_eq!(ids["users"], users.len());
    assert_eq!(ids["groups"], groups.len());
    let out = command(&["verify", scan.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    users.into_iter().collect()
}

#[test]
fn tables_cover_every_id_after_a_kill_and_resume() {
    let tmp = tempfile::tempdir().unwrap();
    let scan = tmp.path().join("scan");
    let scan_arg = scan.to_str().unwrap();
    // 20,000 generated entries at 100 µs per stat (a hook in debug builds)
    // with two threads: about a second.
    let mut child = command(&["scan", "/generated", "--synthetic", "20k", "-t", "2"])
        .args([
            "--resolve-ids",
            "-o",
            scan_arg,
            "--checkpoint-interval",
            "100ms",
        ])
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
    std::thread::sleep(Duration::from_millis(500));
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(manifest(&scan)["ids"].is_null());
    assert!(!scan.join("users").exists());

    // The IDs seen before the checkpoint come from the checkpoint.
    ok(&command(&["resume", scan_arg]).output().unwrap());
    let users = check_tables(&scan);
    // The generated root belongs to root, which every host has.
    assert_eq!(users[&0].as_deref(), Some("root"));
    assert!(users.len() > 20, "{} users", users.len());
}

#[test]
fn interrupted_scans_get_their_tables_when_resumed() {
    let tmp = tempfile::tempdir().unwrap();
    let scan = tmp.path().join("scan");
    let scan_arg = scan.to_str().unwrap();
    let child = command(&["scan", "/generated", "--synthetic", "20k", "-t", "2"])
        .args(["--resolve-ids", "-o", scan_arg])
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
    std::thread::sleep(Duration::from_millis(300));
    let pid = rustix::process::Pid::from_raw(i32::try_from(child.id()).unwrap()).unwrap();
    rustix::process::kill_process(pid, rustix::process::Signal::INT).unwrap();
    assert_eq!(child.wait_with_output().unwrap().status.code(), Some(3));
    let interrupted = manifest(&scan);
    assert_eq!(interrupted["status"], "interrupted");
    assert!(interrupted["ids"].is_null());
    ok(&command(&["resume", scan_arg]).output().unwrap());
    check_tables(&scan);
}

#[test]
fn text_tables_name_the_current_user() {
    let tmp = tempfile::tempdir().unwrap();
    let tree = tmp.path().join("tree");
    fs::create_dir_all(tree.join("sub")).unwrap();
    fs::write(tree.join("sub/file"), b"x").unwrap();
    let id = |flag: &str| {
        let out = Command::new("id").arg(flag).output().unwrap();
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    };
    let (uid, user) = (id("-u"), id("-un"));
    for format in ["csv", "jsonl"] {
        let scan = tmp.path().join(format);
        let text = ok(&command(&[
            "scan",
            tree.to_str().unwrap(),
            "--resolve-ids",
            "-f",
            format,
        ])
        .args(["-o", scan.to_str().unwrap(), "--progress", "plain"])
        .output()
        .unwrap());
        assert!(text.contains("looking up the names of 1 user ID"), "{text}");
        let table =
            fs::read_to_string(scan.join(format!("users/part-w00-000000.{format}"))).unwrap();
        let expected = match format {
            "csv" => format!("uid,name\n{uid},{user}\n"),
            _ => format!("{{\"uid\":{uid},\"name\":\"{user}\"}}\n"),
        };
        assert_eq!(table, expected);
        let out = command(&["verify", scan.to_str().unwrap()])
            .output()
            .unwrap();
        assert_eq!(
            out.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // convert carries the tables along.
    let parquet = tmp.path().join("parquet");
    let converted = tmp.path().join("converted");
    ok(&command(&["scan", tree.to_str().unwrap(), "--resolve-ids"])
        .args(["-o", parquet.to_str().unwrap(), "--progress", "none"])
        .output()
        .unwrap());
    ok(
        &command(&["convert", parquet.to_str().unwrap(), "--to", "csv"])
            .args(["-o", converted.to_str().unwrap()])
            .output()
            .unwrap(),
    );
    assert_eq!(
        fs::read_to_string(converted.join("users/part-w00-000000.csv")).unwrap(),
        format!("uid,name\n{uid},{user}\n")
    );
    assert_eq!(manifest(&converted)["ids"], manifest(&parquet)["ids"]);
}

#[test]
fn needs_an_output_directory() {
    let out = command(&[
        "scan",
        "/generated",
        "--synthetic",
        "10",
        "--resolve-ids",
        "-o",
        "-",
        "-f",
        "csv",
    ])
    .output()
    .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("needs an output directory"));
}
