// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! End-to-end scans of a "torture tree" of awkward names, file types, and
//! timestamps, checked against an independent `std::fs` walk.

#![allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File, FileTimes};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, SystemTime};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::Value;

fn fs_inventory(args: &[&OsStr]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fs-inventory"))
        .args(args)
        .output()
        .expect("failed to run fs-inventory")
}

fn scan(root: &Path, out: &Path, extra: &[&str]) -> Output {
    let mut args: Vec<&OsStr> = vec![
        OsStr::new("scan"),
        root.as_os_str(),
        OsStr::new("-o"),
        out.as_os_str(),
        OsStr::new("--progress"),
        OsStr::new("none"),
    ];
    args.extend(extra.iter().map(OsStr::new));
    fs_inventory(&args)
}

/// Names valid on every platform.
fn portable_names() -> Vec<Vec<u8>> {
    let mut names: Vec<Vec<u8>> = [
        "plain.txt",
        "with space.txt",
        " leading-space",
        "trailing-space ",
        "-rf",
        "comma,name.csv",
        "quote\"name",
        "new\nline",
        "tab\there",
        "esc\x1bape",
        "del\x7f",
        "日本語.txt",
        "caf\u{e9}-nfc.txt",
        "cafe\u{301}-nfd.txt",
        "emoji-\u{1f468}\u{200d}\u{1f469}\u{200d}\u{1f467}.txt",
        "bidi-\u{202e}txt.exe",
        "CON",
        "aux.c",
        "trailing.",
        "name:colon",
        "back\\slash",
        "UPPER.TXT",
        ".hidden",
        "archive.tar.gz",
        "123",
        "0001",
        "true",
    ]
    .iter()
    .map(|s| s.as_bytes().to_vec())
    .collect();
    names.push([vec![b'x'; 251], b".txt".to_vec()].concat());
    names
}

/// Names that are not valid UTF-8. APFS refuses these, so only Linux tests them.
fn invalid_names() -> Vec<Vec<u8>> {
    vec![
        b"latin1-caf\xe9".to_vec(),
        b"sjis-\x83e\x83X\x83g".to_vec(),
        b"lone-\x80".to_vec(),
        b"bad-\xff\xfe".to_vec(),
    ]
}

fn set_mtime(path: &Path, offset_from_epoch: i64) {
    let time = if offset_from_epoch >= 0 {
        SystemTime::UNIX_EPOCH + Duration::from_secs(offset_from_epoch.unsigned_abs())
    } else {
        SystemTime::UNIX_EPOCH - Duration::from_secs(offset_from_epoch.unsigned_abs())
    };
    let file = File::options().write(true).open(path).unwrap();
    file.set_times(FileTimes::new().set_modified(time).set_accessed(time))
        .unwrap();
}

fn make_torture_tree(root: &Path) -> Vec<Vec<u8>> {
    let mut names = portable_names();
    if cfg!(target_os = "linux") {
        names.extend(invalid_names());
    }
    let names_dir = root.join("names");
    fs::create_dir(&names_dir).unwrap();
    for name in &names {
        fs::write(names_dir.join(OsStr::from_bytes(name)), name).unwrap();
    }

    let deep = root.join("a/b/c/d");
    fs::create_dir_all(&deep).unwrap();
    fs::write(deep.join("leaf.txt"), "leaf").unwrap();
    fs::create_dir(root.join("empty")).unwrap();
    let many = root.join("many");
    fs::create_dir(&many).unwrap();
    for i in 0..2000 {
        fs::write(many.join(format!("f-{i:04}")), "").unwrap();
    }

    std::os::unix::fs::symlink("a", root.join("link-to-a")).unwrap();
    std::os::unix::fs::symlink("nowhere", root.join("dangling")).unwrap();
    fs::write(root.join("a/hard1"), "linked").unwrap();
    fs::hard_link(root.join("a/hard1"), root.join("a/hard2")).unwrap();
    let _ = Command::new("mkfifo").arg(root.join("pipe")).status();

    let sparse = File::create(root.join("sparse")).unwrap();
    sparse.set_len(1 << 30).unwrap();

    let times = root.join("times");
    fs::create_dir(&times).unwrap();
    for (name, secs) in [
        ("y1901", -2_147_483_648_i64),
        ("y1969", -1),
        ("y1970", 0),
        ("y2038", 2_147_483_648),
        ("y2106", 4_294_967_295),
        // APFS stores nanoseconds in 64 bits, so it can't go past 2262.
        ("y2200", 7_258_118_400),
    ] {
        fs::write(times.join(name), "t").unwrap();
        set_mtime(&times.join(name), secs);
    }
    names
}

/// Every path under `root`, from `std::fs`.
fn oracle(root: &Path) -> BTreeMap<Vec<u8>, fs::Metadata> {
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

/// Every row of a JSON Lines table, keyed by exact path bytes.
fn read_jsonl(dir: &Path, table: &str) -> BTreeMap<Vec<u8>, Value> {
    let mut rows = BTreeMap::new();
    let Ok(entries) = fs::read_dir(dir.join(table)) else {
        return rows;
    };
    for part in entries {
        let part = part.unwrap().path();
        assert!(
            !part.to_string_lossy().ends_with(".tmp"),
            "leftover {part:?}"
        );
        for line in fs::read_to_string(&part).unwrap().lines() {
            let row: Value = serde_json::from_str(line).unwrap();
            let path = exact_path(&row);
            assert!(rows.insert(path, row).is_none(), "duplicate row");
        }
    }
    rows
}

fn exact_path(row: &Value) -> Vec<u8> {
    match row["path_b64"].as_str() {
        Some(b64) => BASE64.decode(b64).unwrap(),
        None => row["path"].as_str().unwrap().as_bytes().to_vec(),
    }
}

fn tree() -> (tempfile::TempDir, PathBuf, Vec<Vec<u8>>) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap().join("tree");
    fs::create_dir(&root).unwrap();
    let names = make_torture_tree(&root);
    (tmp, root, names)
}

#[test]
fn jsonl_scan_matches_std_exactly() {
    let (tmp, root, names) = tree();
    let out_dir = tmp.path().join("scan");
    let out = scan(&root, &out_dir, &["-f", "jsonl", "-t", "8"]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let rows = read_jsonl(&out_dir, "entries");
    let expected = oracle(&root);
    let missing: Vec<_> = expected.keys().filter(|p| !rows.contains_key(*p)).collect();
    let extra: Vec<_> = rows.keys().filter(|p| !expected.contains_key(*p)).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "missing {missing:?}, extra {extra:?}"
    );

    for (path, meta) in &expected {
        let row = &rows[path];
        assert_eq!(row["ino"], meta.ino(), "{path:?}");
        assert_eq!(row["size"], meta.size(), "{path:?}");
        assert_eq!(row["mode"], meta.mode(), "{path:?}");
        assert_eq!(row["nlink"], meta.nlink(), "{path:?}");
        assert_eq!(row["uid"], meta.uid(), "{path:?}");
        // Text columns are always valid UTF-8 (serde_json would have failed).
        let flags = row["name_flags"].as_u64().unwrap();
        let name = Path::new(OsStr::from_bytes(path))
            .file_name()
            .unwrap()
            .as_bytes();
        if std::str::from_utf8(name).is_err() {
            assert_eq!(flags & 1, 1, "invalid UTF-8 not flagged: {path:?}");
            assert!(row["path_b64"].is_string());
        } else {
            assert!(row["path_b64"].is_null(), "{path:?}");
        }
    }

    let row = |rel: &str| &rows[root.join(rel).as_os_str().as_bytes()];
    assert_eq!(
        row("names/new\nline")["name_flags"].as_u64().unwrap() & 2,
        2
    );
    assert_eq!(
        row("names/cafe\u{301}-nfd.txt")["name_flags"]
            .as_u64()
            .unwrap()
            & 32,
        32
    );
    assert_eq!(row("names/caf\u{e9}-nfc.txt")["name_flags"], 0);
    assert_eq!(row("names/CON")["name_flags"].as_u64().unwrap() & 512, 512);
    assert_eq!(row("names/UPPER.TXT")["extension"], "txt");
    assert_eq!(row("names/.hidden")["extension"], Value::Null);
    assert_eq!(row("link-to-a")["kind"], "symlink");
    assert_eq!(row("dangling")["kind"], "symlink");
    assert_eq!(row("a/hard1")["ino"], row("a/hard2")["ino"]);
    assert_eq!(row("a/hard1")["nlink"], 2);
    assert!(row("sparse")["alloc_bytes"].as_u64().unwrap() < 1 << 30);
    if expected.contains_key(root.join("pipe").as_os_str().as_bytes()) {
        assert_eq!(row("pipe")["kind"], "fifo");
    }
    assert_eq!(row("times/y1901")["mtime"], "1901-12-13T20:45:52.000000Z");
    assert_eq!(row("times/y1969")["mtime"], "1969-12-31T23:59:59.000000Z");
    assert_eq!(row("times/y2106")["mtime"], "2106-02-07T06:28:15.000000Z");
    assert_eq!(row("times/y2200")["mtime"], "2200-01-01T00:00:00.000000Z");
    let many = row("many");
    assert_eq!(many["dir_entries"], 2000);
    assert_eq!(many["dir_files"], 2000);
    assert_eq!(row("empty")["dir_entries"], 0);
    assert_eq!(rows[root.as_os_str().as_bytes()]["depth"], 0);
    assert_eq!(row("a/b/c/d/leaf.txt")["depth"], 5);
    assert_eq!(
        names.len(),
        rows.keys()
            .filter(|p| p.starts_with(root.join("names/").as_os_str().as_bytes()))
            .count()
    );

    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(out_dir.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["status"], "complete");
    assert_eq!(manifest["totals"]["entries"], rows.len());
    let part_rows: u64 = manifest["parts"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["rows"].as_u64().unwrap())
        .sum();
    assert_eq!(part_rows, rows.len() as u64);
}

#[test]
fn csv_rows_stay_on_one_line_and_load_in_duckdb() {
    let (tmp, root, _) = tree();
    let out_dir = tmp.path().join("scan");
    let out = scan(&root, &out_dir, &["-f", "csv", "--compression", "zstd"]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(out_dir.join("manifest.json")).unwrap()).unwrap();
    let total = manifest["totals"]["entries"].as_u64().unwrap();
    assert_eq!(total as usize, oracle(&root).len());

    // Control characters are escaped, so every record is one physical line.
    let mut lines = 0;
    for part in manifest["parts"]["entries"].as_array().unwrap() {
        let file = File::open(out_dir.join(part["file"].as_str().unwrap())).unwrap();
        let text = std::io::read_to_string(zstd::Decoder::new(file).unwrap()).unwrap();
        lines += text.lines().count() - 1;
    }
    assert_eq!(lines as u64, total);

    let duckdb = Command::new("duckdb").arg("-version").output();
    if !duckdb.is_ok_and(|o| o.status.success()) {
        // CI installs DuckDB and sets this, so the check can't be skipped there.
        assert!(
            std::env::var_os("FS_INVENTORY_REQUIRE_DUCKDB").is_none(),
            "duckdb is required but not installed"
        );
        eprintln!("duckdb not found; skipping the DuckDB load check");
        return;
    }
    // Through SQL alone, recover the exact bytes of every path.
    let macros = concat!(env!("CARGO_MANIFEST_DIR"), "/../../sql/fs_inventory.sql");
    let query = format!(
        "SELECT to_base64(coalesce(path_raw, encode(path))) FROM fs_entries_csv('{}')",
        out_dir.display()
    );
    let output = Command::new("duckdb")
        .args(["-no-agent", "-csv", "-noheader"])
        .args(["-c", &format!(".read {macros}"), "-c", &query])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let recovered: std::collections::BTreeSet<String> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(String::from)
        .collect();
    let expected: std::collections::BTreeSet<String> =
        oracle(&root).keys().map(|p| BASE64.encode(p)).collect();
    assert_eq!(recovered, expected);
}

#[test]
fn stdout_streams_rows() {
    let (_tmp, root, _) = tree();
    let out = fs_inventory(&[
        OsStr::new("scan"),
        root.as_os_str(),
        OsStr::new("-o"),
        OsStr::new("-"),
        OsStr::new("-f"),
        OsStr::new("jsonl"),
        OsStr::new("--progress"),
        OsStr::new("none"),
    ]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rows = String::from_utf8(out.stdout).unwrap().lines().count();
    assert_eq!(rows, oracle(&root).len());
}

#[test]
fn unreadable_directories_are_reported() {
    if rustix::process::geteuid().is_root() {
        // Root reads everything; nothing to test.
        return;
    }
    let (tmp, root, _) = tree();
    let locked = root.join("locked");
    fs::create_dir(&locked).unwrap();
    fs::write(locked.join("secret"), "x").unwrap();
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
    let out_dir = tmp.path().join("scan");
    let out = scan(&root, &out_dir, &["-f", "jsonl"]);
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();

    assert_eq!(
        out.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let errors = read_jsonl(&out_dir, "errors");
    let error = &errors[locked.as_os_str().as_bytes()];
    assert_eq!(error["op"], "open");
    assert_eq!(error["errno_name"], "EACCES");
    let rows = read_jsonl(&out_dir, "entries");
    let row = &rows[locked.as_os_str().as_bytes()];
    assert_eq!(row["error"], "open: EACCES");
    assert!(row["dir_entries"].is_null());
    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(out_dir.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["status"], "complete");
    assert_eq!(manifest["errors_by_kind"][0]["errno_name"], "EACCES");
}

#[test]
fn output_inside_the_scanned_tree_is_skipped() {
    let (_tmp, root, _) = tree();
    let out_dir = root.join("scan-output");
    let out = scan(&root, &out_dir, &["-f", "jsonl"]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rows = read_jsonl(&out_dir, "entries");
    assert!(
        !rows
            .keys()
            .any(|p| p.starts_with(out_dir.as_os_str().as_bytes()))
    );
}

#[test]
fn output_directory_must_be_empty() {
    let (tmp, root, _) = tree();
    let out_dir = tmp.path().join("scan");
    fs::create_dir(&out_dir).unwrap();
    fs::write(out_dir.join("old"), "x").unwrap();
    let out = scan(&root, &out_dir, &["-f", "csv"]);
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("not empty"));
}

#[test]
fn parts_roll_and_never_split_rows() {
    let (tmp, root, _) = tree();
    // Enough rows for several of a worker's ~1 MiB chunks; parts roll
    // between chunks, never inside one.
    let big = root.join("big");
    fs::create_dir(&big).unwrap();
    for i in 0..8000 {
        fs::write(big.join(format!("file-with-a-longish-name-{i:05}.dat")), "").unwrap();
    }
    let out_dir = tmp.path().join("scan");
    let args = [
        "-f",
        "jsonl",
        "-t",
        "1",
        "--writers",
        "1",
        "--part-max-rows",
        "1",
    ];
    let out = scan(&root, &out_dir, &args);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(out_dir.join("manifest.json")).unwrap()).unwrap();
    let parts = manifest["parts"]["entries"].as_array().unwrap();
    assert!(parts.len() >= 2, "{}", parts.len());
    // Every part parses on its own, and the rows add up.
    assert_eq!(read_jsonl(&out_dir, "entries").len(), oracle(&root).len());
}

/// Every batch of a Parquet table, read with the parquet crate.
fn read_parquet(dir: &Path, table: &str) -> Vec<arrow_array::RecordBatch> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut batches = Vec::new();
    for part in fs::read_dir(dir.join(table)).unwrap() {
        let path = part.unwrap().path();
        assert_eq!(path.extension().unwrap(), "parquet", "unexpected {path:?}");
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        batches.extend(reader.map(Result::unwrap));
    }
    batches
}

/// Exact path bytes of every row: `path_raw` when present, else `path`.
fn parquet_paths(batches: &[arrow_array::RecordBatch]) -> Vec<Vec<u8>> {
    use arrow_array::Array as _;
    use arrow_array::cast::AsArray as _;
    let mut paths = Vec::new();
    for batch in batches {
        let text = batch.column_by_name("path").unwrap().as_string::<i32>();
        let raw = batch.column_by_name("path_raw").unwrap().as_binary::<i32>();
        for i in 0..batch.num_rows() {
            paths.push(if raw.is_null(i) {
                text.value(i).as_bytes().to_vec()
            } else {
                raw.value(i).to_vec()
            });
        }
    }
    paths
}

fn verify(dir: &Path) -> Output {
    fs_inventory(&[OsStr::new("verify"), dir.as_os_str()])
}

#[test]
fn parquet_is_the_default_and_matches_std_exactly() {
    let (tmp, root, _) = tree();
    let out_dir = tmp.path().join("scan");
    let out = scan(&root, &out_dir, &["-t", "8"]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let batches = read_parquet(&out_dir, "entries");
    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 33);
    assert_eq!(
        schema.field_with_name("mtime").unwrap().data_type(),
        &arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        schema.field_with_name("path_raw").unwrap().data_type(),
        &arrow_schema::DataType::Binary
    );
    let type_of = |name: &str| schema.field_with_name(name).unwrap().data_type().clone();
    assert_eq!(type_of("alloc_bytes"), arrow_schema::DataType::Int64);
    assert_eq!(type_of("uid"), arrow_schema::DataType::Int64);
    assert_eq!(type_of("mode"), arrow_schema::DataType::Int32);
    assert_eq!(type_of("ino"), arrow_schema::DataType::UInt64);

    let mut paths = parquet_paths(&batches);
    let total = paths.len();
    paths.sort();
    paths.dedup();
    assert_eq!(paths.len(), total, "duplicate rows");
    let expected: Vec<Vec<u8>> = oracle(&root).into_keys().collect();
    assert_eq!(paths, expected);

    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(out_dir.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["format"], "parquet");
    assert_eq!(manifest["compression"], "zstd:3");
    // A scan this small never backs up, so one writer writes one part.
    assert_eq!(manifest["parts"]["entries"].as_array().unwrap().len(), 1);
    let out = verify(&out_dir);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).ends_with("ok\n"));
}

#[test]
fn selected_columns_only() {
    let (tmp, root, _) = tree();
    let parquet_dir = tmp.path().join("parquet");
    let out = scan(&root, &parquet_dir, &["--columns", "size,path,mtime"]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let batches = read_parquet(&parquet_dir, "entries");
    let names: Vec<String> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, ["path", "size", "mtime"]);
    assert_eq!(verify(&parquet_dir).status.code(), Some(0));

    let csv_dir = tmp.path().join("csv");
    let out = scan(
        &root,
        &csv_dir,
        &["-f", "csv", "--columns", "path,path_b64,kind"],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let part = fs::read_dir(csv_dir.join("entries"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let text = fs::read_to_string(part).unwrap();
    assert_eq!(text.lines().next(), Some("path,path_b64,kind"));
    assert_eq!(verify(&csv_dir).status.code(), Some(0));
}

#[test]
fn verify_catches_damaged_scans() {
    let (tmp, root, _) = tree();
    let fresh = |name: &str| {
        let dir = tmp.path().join(name);
        let out = scan(
            &root,
            &dir,
            &["--part-max-rows", "1", "-t", "1", "--writers", "1"],
        );
        assert_eq!(
            out.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(verify(&dir).status.code(), Some(0));
        dir
    };
    let problems = |dir: &Path| {
        let out = verify(dir);
        assert_eq!(out.status.code(), Some(1));
        String::from_utf8_lossy(&out.stderr).into_owned()
    };

    let stray = fresh("stray");
    fs::write(stray.join("entries/part-w00-999999.parquet.tmp"), "junk").unwrap();
    assert!(problems(&stray).contains("not listed in the manifest"));

    let missing = fresh("missing");
    fs::remove_file(missing.join("entries/part-w00-000000.parquet")).unwrap();
    assert!(problems(&missing).contains("part-w00-000000.parquet"));

    let swapped = fresh("swapped");
    let other = fresh("other");
    fs::copy(
        other.join("entries/part-w00-000000.parquet"),
        swapped.join("entries/part-w00-000000.parquet"),
    )
    .unwrap();
    assert!(problems(&swapped).contains("belongs to scan"));

    let interrupted = fresh("interrupted");
    let manifest_path = interrupted.join("manifest.json");
    let manifest = fs::read_to_string(&manifest_path).unwrap();
    fs::write(
        &manifest_path,
        manifest.replace("\"complete\"", "\"interrupted\""),
    )
    .unwrap();
    assert!(problems(&interrupted).contains("interrupted"));
}

#[test]
fn rollup_totals_match_the_tree() {
    use arrow_array::cast::AsArray as _;
    use arrow_array::types::Int64Type;

    let (tmp, root, _) = tree();
    let out_dir = tmp.path().join("scan");
    let out = scan(&root, &out_dir, &[]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rollup = |extra: &[&str]| {
        let mut args = vec![OsStr::new("rollup"), out_dir.as_os_str()];
        args.extend(extra.iter().map(OsStr::new));
        fs_inventory(&args)
    };
    let out = rollup(&["--age-depth", "1"]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("directories"));

    // The root's totals cover everything below it.
    let expected = oracle(&root);
    let files: Vec<_> = expected.values().filter(|m| !m.is_dir()).collect();
    let dirs = expected.values().filter(|m| m.is_dir()).count() - 1;
    let bytes: i64 = files.iter().map(|m| m.size() as i64).sum();
    let batches = read_parquet(&out_dir, "rollup");
    let mut found_root = false;
    let mut rows = 0;
    for batch in &batches {
        rows += batch.num_rows();
        let depth = batch
            .column_by_name("depth")
            .unwrap()
            .as_primitive::<arrow_array::types::Int32Type>();
        let col = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_primitive::<Int64Type>()
                .clone()
        };
        let (entries, nfiles, ndirs, nbytes, incomplete) = (
            col("entries"),
            col("files"),
            col("dirs"),
            col("bytes"),
            col("incomplete_dirs"),
        );
        for i in 0..batch.num_rows() {
            if depth.value(i) == 0 {
                found_root = true;
                assert_eq!(entries.value(i) as usize, expected.len() - 1);
                assert_eq!(nfiles.value(i) as usize, files.len());
                assert_eq!(ndirs.value(i) as usize, dirs);
                assert_eq!(nbytes.value(i), bytes);
                assert_eq!(incomplete.value(i), 0);
            }
        }
    }
    assert!(found_root);
    assert_eq!(rows, dirs + 1, "one rollup row per directory");

    // Age rows exist for the root and its children, and add up to the files.
    let ages = read_parquet(&out_dir, "rollup_ages");
    let mut root_mtime_files = 0;
    for batch in &ages {
        let dir_id = batch
            .column_by_name("dir_id")
            .unwrap()
            .as_primitive::<Int64Type>();
        let time = batch.column_by_name("time").unwrap().as_string::<i32>();
        let count = batch
            .column_by_name("files")
            .unwrap()
            .as_primitive::<Int64Type>();
        for i in 0..batch.num_rows() {
            if dir_id.value(i) == 1 && time.value(i) == "mtime" {
                root_mtime_files += count.value(i);
            }
        }
    }
    assert_eq!(root_mtime_files as usize, files.len());

    // The manifest lists the rollup, verify checks it, and a rerun replaces it.
    let out = verify(&out_dir);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = rollup(&["--age-buckets", "7d,1y"]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(out_dir.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(
        manifest["rollup"]["age_buckets"],
        serde_json::json!(["future", "<7d", "7d-1y", ">=1y"])
    );
    assert_eq!(verify(&out_dir).status.code(), Some(0));
    assert!(!out_dir.join(".rollup-staging").exists());

    // Text scans are refused.
    let csv_dir = tmp.path().join("csv");
    assert_eq!(scan(&root, &csv_dir, &["-f", "csv"]).status.code(), Some(0));
    let out = fs_inventory(&[OsStr::new("rollup"), csv_dir.as_os_str()]);
    assert_eq!(out.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&out.stderr).contains("Parquet"));
}

#[test]
fn skipped_fields_are_null() {
    use arrow_array::Array as _;

    let (tmp, root, _) = tree();
    let out_dir = tmp.path().join("scan");
    let out = scan(&root, &out_dir, &["--fields", "-size,-blocks,-atime"]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let batches = read_parquet(&out_dir, "entries");
    for batch in &batches {
        for name in [
            "size",
            "alloc_bytes",
            "atime",
            "dir_bytes",
            "dir_alloc_bytes",
        ] {
            let column = batch.column_by_name(name).unwrap();
            assert_eq!(
                column.null_count(),
                batch.num_rows(),
                "{name} should be null"
            );
        }
        assert_eq!(batch.column_by_name("mtime").unwrap().null_count(), 0);
    }
    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(out_dir.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(
        manifest["options"]["fields"],
        serde_json::json!(["mtime", "ctime", "btime"])
    );
    // Rollup still works; sizes count as zero.
    let out = fs_inventory(&[OsStr::new("rollup"), out_dir.as_os_str()]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let csv_dir = tmp.path().join("csv");
    let out = scan(
        &root,
        &csv_dir,
        &[
            "-f",
            "csv",
            "--fields",
            "mtime",
            "--columns",
            "name,size,mtime,atime",
        ],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let part = fs::read_dir(csv_dir.join("entries"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    for line in fs::read_to_string(part).unwrap().lines().skip(1) {
        // Columns keep schema order (name, size, atime, mtime), and names may
        // contain commas, so split from the right.
        let fields: Vec<&str> = line.rsplitn(4, ',').collect();
        assert!(fields[0].ends_with('Z'), "mtime should be set: {line}");
        assert_eq!(fields[1], "", "atime should be empty: {line}");
        assert_eq!(fields[2], "", "size should be empty: {line}");
    }

    let out = scan(&root, &tmp.path().join("bad"), &["--fields", "sizes"]);
    assert_eq!(out.status.code(), Some(2));
}

/// GNU `find`, an independent walker, lists the same entries with the same
/// metadata, including names that aren't valid UTF-8.
#[cfg(target_os = "linux")]
#[test]
fn gnu_find_agrees_exactly() {
    let (tmp, root, _) = tree();
    let out_dir = tmp.path().join("scan");
    let out = scan(&root, &out_dir, &["-f", "jsonl", "--time-unit", "ns"]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // `%T+` is the mtime as `2026-10-25+01:30:00.1234567890`, in UTC.
    let find = Command::new("find")
        .arg(&root)
        .args(["-printf", "%p\\0%y %i %s %T+ %m %n %U %G\\0"])
        .env("TZ", "UTC")
        .output()
        .unwrap();
    assert!(find.status.success());

    type Fact = (String, u64, i64, String, u32, u64, u32, u32);
    let mut expected: BTreeMap<Vec<u8>, Fact> = BTreeMap::new();
    let mut fields = find.stdout.split(|&b| b == 0);
    while let (Some(path), Some(facts)) = (fields.next(), fields.next()) {
        let facts = std::str::from_utf8(facts).unwrap();
        let f: Vec<&str> = facts.split(' ').collect();
        let kind = match f[0] {
            "f" => "file",
            "d" => "dir",
            "l" => "symlink",
            "p" => "fifo",
            "s" => "socket",
            "c" => "char",
            "b" => "block",
            other => panic!("unexpected type {other}"),
        };
        let fact = (
            kind.to_string(),
            f[1].parse().unwrap(),
            f[2].parse().unwrap(),
            format!(
                "{}Z",
                f[3].replace('+', "T")
                    .trim_end_matches('0')
                    .trim_end_matches('.')
            ),
            u32::from_str_radix(f[4], 8).unwrap(),
            f[5].parse().unwrap(),
            f[6].parse().unwrap(),
            f[7].parse().unwrap(),
        );
        assert!(expected.insert(path.to_vec(), fact).is_none());
    }
    assert!(expected.len() > 2000, "{} entries", expected.len());

    let rows = read_jsonl(&out_dir, "entries");
    let actual: BTreeMap<Vec<u8>, Fact> = rows
        .iter()
        .map(|(path, row)| {
            let mode = u32::try_from(row["mode"].as_u64().unwrap()).unwrap();
            let fact = (
                row["kind"].as_str().unwrap().to_string(),
                row["ino"].as_u64().unwrap(),
                row["size"].as_i64().unwrap(),
                row["mtime"]
                    .as_str()
                    .unwrap()
                    .trim_end_matches('Z')
                    .trim_end_matches('0')
                    .trim_end_matches('.')
                    .to_string()
                    + "Z",
                mode & 0o7777,
                row["nlink"].as_u64().unwrap(),
                u32::try_from(row["uid"].as_u64().unwrap()).unwrap(),
                u32::try_from(row["gid"].as_u64().unwrap()).unwrap(),
            );
            (path.clone(), fact)
        })
        .collect();
    let missing: Vec<_> = expected
        .keys()
        .filter(|p| !actual.contains_key(*p))
        .collect();
    let extra: Vec<_> = actual
        .keys()
        .filter(|p| !expected.contains_key(*p))
        .collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "missing {missing:?}, extra {extra:?}"
    );
    for (path, fact) in &expected {
        assert_eq!(&actual[path], fact, "{:?}", String::from_utf8_lossy(path));
    }
}
