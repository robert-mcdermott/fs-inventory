# Changelog

All notable changes to this project are documented in this file. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `scan` writes CSV or JSON Lines (Parquet arrives in M2), as rolling part files that are committed atomically, or as one stream to stdout. Optional zstd or gzip compression.
- Parallel work-stealing walker with a configurable thread count. On Linux it uses `getdents64` and `statx` relative to the open directory, falls back to `fstatat` on kernels without `statx`, avoids directory atime updates, and never triggers automounts.
- Lossless file names: text columns are always valid UTF-8, exact bytes go in `path_b64` when they differ, and `name_flags` marks invalid UTF-8, control characters, non-NFC names, Windows-reserved names, and long names or paths.
- Paths longer than `PATH_MAX` are opened one component at a time.
- Per-directory totals over immediate children, crawl-assigned directory IDs linking every row to its parent, and mount point detection.
- Errors table and per-errno summary. Per-entry failures, vanished entries, and internal errors never stop a scan.
- `manifest.json` with a scan ID (UUIDv7), status, roots, options, totals, and committed part files.
- Excludes (globs), `--max-depth`, `--cross-mounts`, snapshot-directory skipping, and automatic skipping of the scan's own output directory.
- Progress reports (plain or JSON), and graceful handling of SIGINT and SIGTERM (a second signal exits immediately).
- DuckDB macros in `sql/fs_inventory.sql`.
- Benchmark kit in `bench/`.
- Cargo workspace with the `fs-inventory-core` library and `fs-inventory` command-line crates.
- Command-line interface for `scan`, `resume`, `rollup`, `convert`, `verify`, `schema`, and `probe`, with argument validation and documented exit codes. The commands are not implemented yet.
- Apache-2.0 license and NOTICE.
- CI on Linux (x86_64 and aarch64) and macOS: formatting, Clippy, tests, the minimum supported Rust version, static musl builds, and cargo-deny license and advisory checks.
