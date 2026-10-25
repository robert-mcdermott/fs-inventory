# fs-inventory

A high-performance, crash-resilient file system inventory tool and Rust library.

`fs-inventory` crawls a POSIX file system in parallel and records the metadata of every file and directory as Parquet, CSV, or JSON Lines, ready for analysis in tools like DuckDB. It is built for file systems with hundreds of millions to billions of entries, decades of history, and names in any language or legacy encoding.

> **Status:** early development (milestone M0, project skeleton). The command-line interface is defined, but the subcommands are not implemented yet. See [PLAN.md](PLAN.md) for the design and roadmap.

## Features

These are planned; [PLAN.md](PLAN.md) has the details.

- A parallel, work-stealing crawler with a configurable thread count.
- Lossless file names: invalid UTF-8 and legacy encodings are preserved byte-for-byte, and every output stays valid for its format.
- Sharded Parquet output (zstd) that DuckDB reads with a single glob, plus CSV and JSON Lines.
- Never stops on per-entry errors, and resumes after interruption from periodic checkpoints.
- Read-only: never follows symlinks, never reads file contents, and doesn't update directory access times where the OS allows.

## Build

Requires Rust 1.88 or later. The pinned toolchain in `rust-toolchain.toml` installs automatically through rustup.

```bash
cargo build --release
```

The binary is written to `target/release/fs-inventory`.

For a fully static Linux binary that runs on older distributions:

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
```

## Usage

```bash
fs-inventory scan /data -o /scratch/inventory/data-2026-10-25
```

```bash
fs-inventory --help
```

## Project layout

| Path | Contents |
|---|---|
| `crates/fs-inventory-core` | Library: walker, platform backends, name encoding, record model |
| `crates/fs-inventory` | Output writers and the `fs-inventory` command-line tool |

## License

Copyright 2026 Robert McDermott.

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE).
