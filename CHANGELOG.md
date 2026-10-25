# Changelog

All notable changes to this project are documented in this file. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `fs-inventory rollup SCAN_DIR` writes two Parquet tables into the scan: `rollup`, with totals over every directory's whole subtree (entries, files, directories, bytes, allocated bytes, oldest and newest times, and how many directories weren't fully read), and `rollup_ages`, with file counts and bytes by age for directories down to `--age-depth` (default 4), in buckets set by `--age-buckets` (default `30d,90d,1y,2y,3y,5y,10y`) and measured from the scan's start time. The manifest lists both, `verify` checks them, and running it again replaces them. DuckDB macros `fs_rollup` and `fs_rollup_ages`, and heat-map examples in `sql/examples.sql`.

- `--fields` chooses which optional metadata to read (size, blocks, atime, mtime, ctime, btime). Skipped fields are written as null, and on Linux they aren't requested from the kernel, so `--fields -size,-blocks` avoids a round trip per file to Lustre's storage servers. `size`, `alloc_bytes`, `atime`, `mtime`, and `ctime` are now nullable.
- `--detect-cycles` remembers every directory entered (by device and inode) and never enters one twice, guarding against bind-mount loops, hard-linked directories, and overlapping roots. A directory reached again gets a row with the error `revisit`, and the manifest counts them.

### Changed

- Writer threads start one at a time and are added, up to `--writers`, only when output backs up. Small scans now write one part file per table instead of one per CPU core.
- Schema version 2: integer columns are now signed (`INTEGER` or `BIGINT`), so scans load into databases without unsigned types and DuckDB functions such as `format_bytes()` work without casts. Only `dev`, `ino`, and `rdev` stay unsigned (`UBIGINT`). Values above the signed maximum are capped, not wrapped. `verify` reports scans written with schema version 1.

### Added

- Parquet output, now the default format: zstd level 3, row groups of up to 1,048,576 rows, page statistics, column encodings chosen by measurement (about 25 bytes per entry on real data), and scan ID, schema version, host, and roots in every file's footer.
- `--columns` writes a subset of entry columns in any format.
- `fs-inventory schema` describes the schema as DuckDB SQL, JSON, or Markdown. `docs/schema.md` is generated from it.
- `fs-inventory verify` checks a scan before ingestion: status, schema version, every part's size and row count, each Parquet part's scan ID, totals, and stray or leftover files.
- DuckDB macros `fs_entries` and `fs_errors` for Parquet scans, and example queries in `sql/examples.sql`.
- mimalloc as the memory allocator. The static musl build no longer slows down as crawl threads are added.
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
