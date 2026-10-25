// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! End-to-end checks of the command-line contract: exit codes and messages.

#![allow(clippy::expect_used, reason = "test helpers may panic")]

use std::process::{Command, Output};

fn fs_inventory(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fs-inventory"))
        .args(args)
        .output()
        .expect("failed to run fs-inventory")
}

#[test]
fn version_prints_package_version() {
    let out = fs_inventory(&["--version"]);
    assert_eq!(out.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        stdout.trim(),
        format!("fs-inventory {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn help_lists_every_subcommand() {
    let out = fs_inventory(&["--help"]);
    assert_eq!(out.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&out.stdout);
    for sub in [
        "scan", "resume", "rollup", "convert", "verify", "schema", "probe",
    ] {
        assert!(stdout.contains(sub), "help is missing `{sub}`:\n{stdout}");
    }
}

#[test]
fn usage_errors_exit_2() {
    assert_eq!(fs_inventory(&[]).status.code(), Some(2));
    assert_eq!(fs_inventory(&["scan"]).status.code(), Some(2));
    assert_eq!(
        fs_inventory(&["scan", "/data", "-o", "-"]).status.code(),
        Some(2)
    );
    assert_eq!(fs_inventory(&["bogus"]).status.code(), Some(2));
}

#[test]
fn unimplemented_commands_exit_4() {
    let out = fs_inventory(&["verify", "/scans/x"]);
    assert_eq!(out.status.code(), Some(4));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("not implemented yet"), "{stderr}");
}

#[test]
fn unimplemented_scan_options_are_refused() {
    // Parquet (the default format) arrives in M2.
    let out = fs_inventory(&["scan", "/data", "-o", "out"]);
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("Parquet"));
    let out = fs_inventory(&[
        "scan",
        "/data",
        "-o",
        "out",
        "-f",
        "csv",
        "--symlink-targets",
    ]);
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("--symlink-targets"));
}
