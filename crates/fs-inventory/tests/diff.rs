// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! The `fs_diff` DuckDB macro: two scans of a tree, with changes made in
//! between, must differ in exactly those changes. Needs the `duckdb`
//! command; skipped without it unless `FS_INVENTORY_REQUIRE_DUCKDB` is set,
//! as CI sets it.

#![allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;

const MACROS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../sql/fs_inventory.sql");

/// Runs SQL in DuckDB with the macros loaded, returning its CSV output, or
/// `None` when DuckDB isn't installed and isn't required.
fn duckdb(sql: &str) -> Option<String> {
    let found = Command::new("duckdb").arg("-version").output();
    if !found.is_ok_and(|o| o.status.success()) {
        assert!(
            std::env::var_os("FS_INVENTORY_REQUIRE_DUCKDB").is_none(),
            "duckdb is required but not installed"
        );
        eprintln!("duckdb not found; skipping");
        return None;
    }
    let out = Command::new("duckdb")
        .env_remove("AI_AGENT")
        .args([
            "-csv",
            "-noheader",
            "-c",
            &format!(".read {MACROS}"),
            "-c",
            sql,
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    Some(String::from_utf8(out.stdout).unwrap())
}

fn scan(root: &Path, out: &Path) {
    let status = Command::new(env!("CARGO_BIN_EXE_fs-inventory"))
        .args([
            "scan",
            root.to_str().unwrap(),
            "--symlink-targets",
            "-t",
            "2",
        ])
        .args(["-o", out.to_str().unwrap(), "--progress", "none"])
        .status()
        .unwrap();
    assert!(status.success());
}

/// `fs_diff`'s rows by entry name: the change and its changed columns.
fn diff(old: &Path, new: &Path, args: &str) -> Option<BTreeMap<String, (String, Vec<String>)>> {
    let sql = format!(
        "SELECT regexp_extract(path, '[^/]*$'), change, array_to_string(list_sort(changes), ' ') \
         FROM fs_diff('{}', '{}'{args})",
        old.display(),
        new.display()
    );
    let out = duckdb(&sql)?;
    Some(
        out.lines()
            .map(|line| {
                let mut fields = line.splitn(3, ',');
                let name = fields.next().unwrap().to_string();
                let change = fields.next().unwrap().to_string();
                let changes = fields
                    .next()
                    .unwrap_or_default()
                    .split_whitespace()
                    .map(String::from)
                    .collect();
                (name, (change, changes))
            })
            .collect(),
    )
}

#[test]
fn fs_diff_finds_exactly_the_changes() {
    let tmp = tempfile::tempdir().unwrap();
    let tree = tmp.path().join("tree");
    let sub = tree.join("sub");
    fs::create_dir_all(&sub).unwrap();
    for name in ["keep", "grow", "perm", "replace", "rename", "gone"] {
        fs::write(sub.join(name), b"data\n").unwrap();
    }
    std::os::unix::fs::symlink("sub/keep", tree.join("link")).unwrap();
    let (old, new) = (tmp.path().join("old"), tmp.path().join("new"));
    scan(&tree, &old);

    fs::write(sub.join("grow"), b"data\nmore\n").unwrap();
    rustix::fs::chmod(sub.join("perm"), rustix::fs::Mode::from_raw_mode(0o600)).unwrap();
    // Replaced the way editors save: written beside the old file and renamed
    // over it, so it can't get the old inode number back, as a file created
    // just after a delete can (ext4 reuses them at once).
    fs::write(sub.join("replace.new"), b"data\n").unwrap();
    fs::rename(sub.join("replace.new"), sub.join("replace")).unwrap();
    fs::rename(sub.join("rename"), sub.join("renamed")).unwrap();
    fs::remove_file(sub.join("gone")).unwrap();
    fs::write(sub.join("added"), b"new\n").unwrap();
    fs::remove_file(tree.join("link")).unwrap();
    std::os::unix::fs::symlink("sub/grow", tree.join("link")).unwrap();
    scan(&tree, &new);

    let Some(rows) = diff(&old, &new, "") else {
        return;
    };
    let has = |name: &str, column: &str| rows[name].1.iter().any(|c| c == column);
    for (name, change) in [
        ("added", "added"),
        ("renamed", "added"),
        ("gone", "removed"),
        ("rename", "removed"),
        ("grow", "changed"),
        ("perm", "changed"),
        ("replace", "changed"),
        ("link", "changed"),
        ("sub", "changed"),
    ] {
        assert_eq!(rows[name].0, change, "{name}: {rows:?}");
    }
    assert!(has("grow", "size"));
    assert!(has("perm", "mode") && !has("perm", "size"));
    assert!(has("replace", "ino"));
    assert!(has("link", "symlink_target"));
    assert!(has("sub", "dir_bytes"));
    assert!(!rows.contains_key("keep"), "{rows:?}");
    assert!(
        rows.values()
            .all(|(_, changes)| !changes.iter().any(|c| c == "atime"))
    );

    // Without ignoring anything, every change above still shows.
    let exact = diff(&old, &new, ", ignore := []").unwrap();
    assert!(rows.keys().all(|name| exact.contains_key(name)));

    // A scan has no differences from itself.
    assert!(diff(&new, &new, ", ignore := []").unwrap().is_empty());

    let summary = duckdb(&format!(
        "SELECT change, kind, entries, size_change FROM fs_diff_summary('{}', '{}') \
         WHERE kind = 'file' ORDER BY change",
        old.display(),
        new.display()
    ))
    .unwrap();
    // grow gained 5 bytes; added (4) and renamed (5) were added, and gone
    // (5) and rename (5) removed.
    assert_eq!(
        summary,
        "added,file,2,9\nchanged,file,3,5\nremoved,file,2,-10\n"
    );
}
