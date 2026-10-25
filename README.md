# fs-inventory

A high-performance, crash-resilient file system inventory tool and Rust library.

`fs-inventory` crawls a POSIX file system in parallel and records the metadata of every file and directory, ready for analysis in tools like DuckDB. It is built for file systems with hundreds of millions to billions of entries, decades of history, and names in any language or legacy encoding.

> **Status:** milestone M2. Scans write Parquet (the default), CSV, or JSON Lines. See [PLAN.md](PLAN.md) for the design and roadmap.

## Features

- **Parallel crawling:** a work-stealing crawler with a configurable number of threads (`--threads`).
- **Compact Parquet output:** about 25 bytes per entry with zstd (measured on 1.2 million real entries with 126-byte average paths), so a billion-file scan takes about 25 GB. Every part carries its scan ID and schema version.
- **Lossless names:** every name is kept byte for byte. Text columns are always valid UTF-8; when a name isn't valid UTF-8, or contains control characters, its exact bytes are kept in `path_b64`. `name_flags` marks names that will cause trouble (legacy encodings, control characters, Windows-reserved names, and so on).
- **Never stops on errors:** an unreadable directory or vanished file is recorded in an errors table and the crawl continues.
- **Read-only:** never follows symlinks, never reads file contents, doesn't trigger automounts, and on Linux doesn't update directory access times when it has permission.
- **Paths of any length:** paths longer than the 4,096-byte system limit are opened one directory at a time.
- **Safe output:** part files appear only once complete and synced to disk, and Ctrl-C finishes the directories in progress and writes the manifest before exiting.

## Build

Requires Rust 1.88 or later. The toolchain in `rust-toolchain.toml` installs automatically through rustup.

```bash
cargo build --release
```

The binary is written to `target/release/fs-inventory`. For a static Linux binary that runs on older distributions:

```bash
cargo build --release --target x86_64-unknown-linux-musl
```

## Usage

Scan a tree into a new output directory (Parquet with zstd by default):

```bash
fs-inventory scan /data -o /scratch/inventory/data-2026-10-25
```

Check that a scan is complete and consistent before loading it anywhere:

```bash
fs-inventory verify /scratch/inventory/data-2026-10-25
```

Stream to stdout instead (errors go to stderr):

```bash
fs-inventory scan /data -o - -f jsonl | gzip > data.jsonl.gz
```

Useful options:

| Option | Effect |
|---|---|
| `-f, --format parquet\|csv\|jsonl` | Output format (default `parquet`) |
| `--columns LIST` | Write only these columns, such as `path,size,mtime,uid` |
| `-t, --threads N` | Crawl threads (default 32; network file systems often benefit from 64–256) |
| `--exclude GLOB` | Skip matching paths. Globs without `/` match names at any depth. |
| `--cross-mounts` | Descend into other mounted file systems. Mount points are always recorded. |
| `--max-depth N` | Don't read directories more than N levels down |
| `--progress plain\|json` | Periodic progress on stderr |

Run `fs-inventory scan --help` for every option.

### Output

```
data-2026-10-25/
  manifest.json                      scan ID, status, roots, options, totals, and the list of part files
  entries/part-w00-000000.parquet    one row per file or directory
  errors/part-w00-000000.parquet     one row per failure (only if there were any)
```

The columns are described in [docs/schema.md](docs/schema.md), or run `fs-inventory schema`. Large scans are split into many part files, and DuckDB reads them as one table:

```sql
SELECT kind, count(*), sum(size) FROM read_parquet('/scratch/inventory/data-2026-10-25/entries/*.parquet') GROUP BY kind;
```

[sql/fs_inventory.sql](sql/fs_inventory.sql) has macros for every format (`fs_entries`, `fs_entries_csv`, `fs_entries_jsonl`, and the `fs_errors` equivalents), and [sql/examples.sql](sql/examples.sql) has example queries: age profiles, top users, extensions, hard-link-aware usage, and problem names.

Only treat a scan as complete when `manifest.json` has `"status": "complete"`, or when `fs-inventory verify` exits 0.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Complete with no errors |
| 1 | Complete, but some entries couldn't be read (see `errors/`) |
| 2 | Invalid arguments or configuration |
| 3 | Interrupted; queued directories weren't read |
| 4 | Fatal error, such as output that couldn't be written |

## Benchmarking

[bench/](bench/README.md) has a synthetic tree generator and a runner that sweeps thread counts and compares against `find`, for use on real storage.

## Project layout

| Path | Contents |
|---|---|
| `crates/fs-inventory-core` | Library: walker, platform backends, name encoding, record model |
| `crates/fs-inventory` | Output writers and the `fs-inventory` command-line tool |
| `sql/` | DuckDB macros for reading scans |
| `bench/` | Benchmark kit |

## License

Copyright 2026 Robert McDermott.

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE).
