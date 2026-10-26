// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! `convert`: a Parquet scan converted to CSV or JSON Lines must equal the
//! same scan written in that format directly. With one crawl thread, scans
//! of the same tree assign the same directory IDs, so rows compare exactly.

#![allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]

use std::fs;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fs-inventory"))
        .args(args)
        .output()
        .expect("failed to run fs-inventory")
}

fn ok(out: &Output) -> String {
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(out.status.code(), Some(0), "{text}");
    text
}

/// Every row of a table's parts, decompressed, without CSV headers, sorted.
fn rows(scan: &Path, table: &str) -> Vec<String> {
    let mut rows = Vec::new();
    let mut files: Vec<PathBuf> = fs::read_dir(scan.join(table))
        .map(|d| d.map(|e| e.unwrap().path()).collect())
        .unwrap_or_default();
    files.sort();
    for file in files {
        let bytes = fs::read(&file).unwrap();
        let text = if file.extension().is_some_and(|e| e == "zst") {
            String::from_utf8(zstd::decode_all(&bytes[..]).unwrap()).unwrap()
        } else {
            String::from_utf8(bytes).unwrap()
        };
        rows.extend(
            text.lines()
                .skip(usize::from(file.to_string_lossy().contains(".csv")))
                .map(String::from),
        );
    }
    rows.sort();
    rows
}

/// Checks that two sorted row lists are equal, showing the first difference.
fn same_rows(converted: &[String], direct: &[String], case: &str, format: &str) {
    if let Some((c, d)) = converted.iter().zip(direct).find(|(c, d)| c != d) {
        panic!("{case} {format}: first differing row\nconverted: {c}\ndirect:    {d}");
    }
    assert_eq!(converted.len(), direct.len(), "{case} {format}");
}

/// A scan's error rows without their times, which differ between scans.
fn error_rows(scan: &Path) -> Vec<String> {
    rows(scan, "errors")
        .into_iter()
        .map(|row| match row.rsplit_once(',') {
            Some((rest, _time)) if !row.starts_with('{') => rest.to_string(),
            _ => {
                let mut value: Value = serde_json::from_str(&row).unwrap();
                value["time"] = Value::Null;
                value.to_string()
            }
        })
        .collect()
}

/// Scans `root` with `options` into `out`, in one thread.
fn scan(root: &str, format: &str, options: &[&str], out: &Path) {
    let mut args = vec!["scan", root, "-t", "1", "--symlink-targets", "-f", format];
    args.extend_from_slice(options);
    args.extend(["-o", out.to_str().unwrap(), "--progress", "none"]);
    let out = run(&args);
    // Exit 1: some errors were recorded.
    assert!(matches!(out.status.code(), Some(0 | 1)), "{out:?}");
}

fn convert(parquet: &Path, format: &str, out: &Path, extra: &[&str]) {
    let mut args = vec![
        "convert",
        parquet.to_str().unwrap(),
        "-f",
        format,
        "-o",
        out.to_str().unwrap(),
    ];
    args.extend_from_slice(extra);
    let text = ok(&run(&args));
    assert!(text.contains("converted"), "{text}");
}

fn manifest(scan: &Path) -> Value {
    serde_json::from_str(&fs::read_to_string(scan.join("manifest.json")).unwrap()).unwrap()
}

/// Generated trees are the same every time, so whole rows compare; on a
/// real tree, reading directories changes their access times.
#[test]
fn converted_scans_equal_direct_text_scans() {
    let tmp = tempfile::tempdir().unwrap();
    let cases: [(&str, &[&str], &[&str]); 6] = [
        ("default", &[], &[]),
        (
            "ns-epoch",
            &["--time-unit", "ns", "--time-format", "epoch"],
            &[],
        ),
        ("raw", &["--raw-paths", "always", "--csv-raw-controls"], &[]),
        (
            "columns",
            &["--columns", "path,kind,size,mtime,symlink_target"],
            &[],
        ),
        ("fields", &["--fields", "-size,-atime"], &[]),
        ("compressed", &[], &["--compression", "zstd"]),
    ];
    for (name, options, convert_options) in cases {
        let base = tmp.path().join(name);
        let options: Vec<&str> = ["--synthetic", "20k"]
            .iter()
            .chain(options)
            .copied()
            .collect();
        let parquet = base.join("parquet");
        scan("/generated", "parquet", &options, &parquet);
        for format in ["csv", "jsonl"] {
            let direct = base.join(format!("direct-{format}"));
            let converted = base.join(format!("converted-{format}"));
            scan("/generated", format, &options, &direct);
            convert(&parquet, format, &converted, convert_options);
            same_rows(
                &rows(&converted, "entries"),
                &rows(&direct, "entries"),
                name,
                format,
            );
            let (converted_manifest, direct_manifest) = (manifest(&converted), manifest(&direct));
            assert_eq!(converted_manifest["format"], format);
            assert_eq!(converted_manifest["converted_from"]["format"], "parquet");
            assert_eq!(converted_manifest["columns"], direct_manifest["columns"]);
            assert_eq!(converted_manifest["totals"], direct_manifest["totals"]);
            let verify = run(&["verify", converted.to_str().unwrap()]);
            assert_eq!(verify.status.code(), Some(0), "{verify:?}");
        }
    }
}

#[test]
fn converted_errors_equal_direct_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("tree");
    fs::create_dir_all(root.join("locked/inside")).unwrap();
    fs::create_dir_all(root.join("open")).unwrap();
    fs::write(root.join("open/file"), b"data").unwrap();
    rustix::fs::chmod(root.join("locked"), rustix::fs::Mode::empty()).unwrap();
    let root_arg = root.to_str().unwrap();
    let parquet = tmp.path().join("parquet");
    scan(root_arg, "parquet", &[], &parquet);
    for format in ["csv", "jsonl"] {
        let direct = tmp.path().join(format!("direct-{format}"));
        let converted = tmp.path().join(format!("converted-{format}"));
        scan(root_arg, format, &[], &direct);
        convert(&parquet, format, &converted, &[]);
        assert_eq!(error_rows(&converted), error_rows(&direct), "{format}");
        assert_eq!(
            rows(&converted, "entries").len(),
            rows(&direct, "entries").len()
        );
    }
    rustix::fs::chmod(root.join("locked"), rustix::fs::Mode::from_raw_mode(0o755)).unwrap();
}

#[test]
fn converts_to_stdout() {
    let tmp = tempfile::tempdir().unwrap();
    let parquet = tmp.path().join("parquet");
    let direct = tmp.path().join("direct");
    for (format, out) in [("parquet", &parquet), ("csv", &direct)] {
        ok(&run(&[
            "scan",
            "/generated",
            "--synthetic",
            "5000",
            "-t",
            "1",
            "-f",
            format,
            "-o",
            out.to_str().unwrap(),
            "--progress",
            "none",
        ]));
    }
    let mut child = Command::new(env!("CARGO_BIN_EXE_fs-inventory"))
        .args(["convert", parquet.to_str().unwrap(), "-f", "csv", "-o", "-"])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut text = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut text)
        .unwrap();
    assert!(child.wait().unwrap().success());
    let mut lines: Vec<String> = text.lines().skip(1).map(String::from).collect();
    lines.sort();
    assert_eq!(lines, rows(&direct, "entries"));
}

#[test]
fn refuses_what_it_cannot_convert() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = tmp.path().join("csv");
    ok(&run(&[
        "scan",
        "/generated",
        "--synthetic",
        "100",
        "-f",
        "csv",
        "-o",
        csv.to_str().unwrap(),
        "--progress",
        "none",
    ]));
    // `--to`, the name before 1.0, still works.
    let out = run(&["convert", csv.to_str().unwrap(), "--to", "jsonl", "-o", "-"]);
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("convert reads Parquet scans"));

    let parquet = tmp.path().join("parquet");
    ok(&run(&[
        "scan",
        "/generated",
        "--synthetic",
        "100",
        "-o",
        parquet.to_str().unwrap(),
        "--progress",
        "none",
    ]));
    // The output directory must be new or empty, as for a scan.
    let out = run(&[
        "convert",
        parquet.to_str().unwrap(),
        "-f",
        "csv",
        "-o",
        csv.to_str().unwrap(),
    ]);
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("not empty"));
}
