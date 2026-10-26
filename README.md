# fs-inventory

A high-performance, crash-resilient file system inventory tool and Rust library.

`fs-inventory` crawls a POSIX file system in parallel and records the metadata of every file and directory, ready for analysis in tools like DuckDB. It is built for file systems with hundreds of millions to billions of entries, decades of history, and names in any language or legacy encoding.

> **Status:** release 0.6.0. Scans write Parquet (the default), CSV, or JSON Lines, resume after interruptions, and have been tested on generated trees of up to a billion entries; `rollup` adds subtree totals and file-age histograms. Benchmarks on large real file systems are still to come. See the [changelog](CHANGELOG.md) for what each release contains and [PLAN.md](PLAN.md) for the roadmap.

## Features

- **Parallel crawling:** a work-stealing crawler with a configurable number of threads (`--threads`). Directories with millions of entries are stat'ed by several threads at once, and `probe` suggests a thread count for your storage.
- **Compact Parquet output:** about 25 bytes per entry with zstd (measured on 1.2 million real entries with 126-byte average paths), so a billion-file scan takes about 25 GB. Every part carries its scan ID and schema version.
- **Lossless names:** every name is kept byte for byte. Text columns are always valid UTF-8; when a name isn't valid UTF-8, or contains control characters, its exact bytes are kept as well. `name_flags` marks names that will cause trouble (legacy encodings, control characters, Windows-reserved names, and so on).
- **Never stops on errors:** an unreadable directory or vanished file is recorded in an errors table and the crawl continues. A thread stuck on a hung NFS server is reported, and with `--stall-timeout` the scan gives up on that directory and finishes the rest.
- **Bounded memory:** directories waiting to be read spill to a temporary file beyond a budget (`--max-memory`), so huge trees don't exhaust memory.
- **Any format, any time:** scans write Parquet, CSV, or JSON Lines, and `convert` turns a Parquet scan into either text format later, exactly as a text scan would have written it.
- **Owner names:** `--resolve-ids` adds tables of user and group names, looked up once per ID when the scan finishes, through LDAP and sssd too, so dashboards show names and deleted accounts stand out.
- **Heat-map ready:** `rollup` adds per-directory totals over whole subtrees and file counts by age, so dashboards read a small table instead of every file.
- **Read-only:** never follows symlinks (`--symlink-targets` records where they point), never reads file contents, doesn't trigger automounts, and on Linux doesn't update directory access times when it has permission.
- **Paths of any length:** paths longer than the 4,096-byte system limit are opened one directory at a time.
- **Safe output:** part files appear only once complete and synced to disk, and Ctrl-C finishes the directories in progress and writes the manifest before exiting.
- **Resumable:** a scan takes a checkpoint every 10 minutes. After Ctrl-C, `kill -9`, or a crash, `fs-inventory resume` continues it, and the result matches an uninterrupted scan, with every row exactly once.
- **Runs on any Linux:** the static build has no library dependencies and runs on any distribution with kernel 3.2 or later, including RHEL 7.

## Install

On Linux or macOS:

```bash
curl --proto '=https' --tlsv1.2 -fsSL https://raw.githubusercontent.com/robert-mcdermott/fs-inventory/main/install.sh | sh
```

This installs the latest release as `~/.local/bin/fs-inventory`. It checks the download's SHA-256 checksum and version, and replaces an existing copy only after both checks pass. It doesn't use `sudo` or change `PATH`. The Linux binaries are static, so they run on any distribution with kernel 3.2 or later, including RHEL 7.

To build from source instead (Rust 1.88 or later):

```bash
cargo build --release
```

The [user guide](docs/user-guide.md#1-install) covers installing a pinned version, installing for every user, and building static Linux binaries.

## Quick start

Scan a tree into a new output directory (Parquet with zstd by default):

```bash
fs-inventory scan /data/projects -o /scratch/inventory/projects-2026-10-25
```

Check that the scan is complete and consistent before using it:

```bash
fs-inventory verify /scratch/inventory/projects-2026-10-25
```

Add subtree totals and file-age histograms:

```bash
fs-inventory rollup /scratch/inventory/projects-2026-10-25
```

Query it in the DuckDB shell, using the macros in [sql/fs_inventory.sql](sql/fs_inventory.sql):

```sql
.read sql/fs_inventory.sql
SELECT kind, count(*) AS entries, sum(size) AS bytes
FROM fs_entries('/scratch/inventory/projects-2026-10-25') GROUP BY kind;
```

On a network file system, find a good thread count first (it writes nothing):

```bash
fs-inventory probe /nfs/projects --threads 16,32,64,128,256
```

If a scan is interrupted or killed, continue it from its last checkpoint:

```bash
fs-inventory resume /scratch/inventory/projects-2026-10-25
```

Only treat a scan as complete when its `manifest.json` has `"status": "complete"`, or when `fs-inventory verify` exits with code 0.

## Documentation

The documentation is also a website, with search: **<https://robert-mcdermott.github.io/fs-inventory/>**.

| Document | Contents |
|---|---|
| [User guide](docs/user-guide.md) | Installing; common scenarios (NFS, Lustre, scheduled scans, migrations); every command and option; the output formats, manifest, and exit codes; querying scans with DuckDB |
| [Schema](docs/schema.md) | Every column of every table (generated by `fs-inventory schema --format markdown`) |
| [Architecture](docs/architecture.md) | How fs-inventory works: the crawler, platform backends, output pipeline, and robustness |
| [sql/](sql/) | DuckDB macros (`fs_inventory.sql`) and example queries (`examples.sql`) |
| [Changelog](CHANGELOG.md) | What each release contains |
| [PLAN.md](PLAN.md) | Design history, decisions, and roadmap |

## Benchmarking

[bench/](bench/README.md) has a turnkey benchmark for a server's storage that writes a report to send back, scale tests on generated trees of any size, a tree generator, and a runner that sweeps thread counts and compares against `find`.

## Project layout

| Path | Contents |
|---|---|
| `crates/fs-inventory-core` | Library: walker, platform backends, name encoding, record model |
| `crates/fs-inventory` | Output writers and the `fs-inventory` command-line tool |
| `docs/` | User guide, schema reference, and architecture |
| `sql/` | DuckDB macros and example queries |
| `bench/` | Benchmark kit |

## License

Copyright 2026 Robert McDermott.

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE).
