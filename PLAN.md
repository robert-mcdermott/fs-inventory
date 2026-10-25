# fs-inventory: Design and Implementation Plan

`fs-inventory` is a high-performance, crash-resilient file system inventory library and command-line tool. It crawls a POSIX file system in parallel and records the metadata of every file and directory as CSV, JSON Lines, or Parquet, ready for analytics in DuckDB.

License: Apache-2.0.

---

## 1. Goals and non-goals

### Goals

- **Scale:** inventory file systems of 10⁸ to 10⁹+ entries and petabytes of data in hours, not days.
- **Never crash mid-crawl:** every per-entry failure is recorded and counted, and the crawl continues.
- **Lossless names:** every file name is preserved byte-for-byte, including invalid UTF-8, legacy encodings (Latin-1, CP1252, Shift-JIS, GBK, Big5, EUC-KR, KOI8-R, MacRoman), control characters, and decomposed (NFD) Unicode. Every output stays valid for its format.
- **Analytics-ready output:** sharded Parquet that DuckDB reads with a single glob, plus CSV and JSON Lines for other tools.
- **Read-only and side-effect-free:** the crawl never modifies the file system, never follows symlinks, never reads file contents, and doesn't update directory access times where the OS allows that.
- **Resumable:** a crawl interrupted after 20 hours can resume and lose only a few minutes of work.
- **Library plus CLI:** the walker and data model are a reusable Rust library, and the CLI is a thin layer over it.

### Non-goals for v1

- Content hashing, deduplication, or reading file data.
- Modifying files (no chown, purge, or repair actions).
- Windows/NTFS. The backend abstraction leaves room for it later.
- A built-in reporting UI. DuckDB is the analytics engine, and we ship SQL views and example queries.

---

## 2. Language recommendation: Rust

Both Go and Rust can saturate a metadata server. On network and parallel file systems, the crawl is bound by RPC latency, not by the CPU. The choice comes down to correctness at the edges, the Parquet stack, and predictable behavior during multi-day runs at billion-row scale.

| Concern | Rust | Go |
|---|---|---|
| Parquet/Arrow writer | `parquet` / `arrow` crates (Apache arrow-rs, v60.x, Oct 2026). Mature and fast. Used by DataFusion, delta-rs, and InfluxDB 3. | `arrow-go` v18.8 (Apache). Much improved in 2025–26 (big writer speed and memory gains). Solid. |
| Raw-byte file names | The type system separates `[u8]`/`OsStr` from `str` (guaranteed UTF-8), so every bytes→text conversion is an explicit, reviewable decision. | `string` is an arbitrary byte bag. Invalid UTF-8 flows silently into "string" columns. |
| Memory over 10⁹ rows | No GC. Per-worker buffers are reused, and memory use is predictable. | GC. Manageable with `GOMEMLIMIT` and pooling, but uses more memory and has tail pauses. |
| Low-level syscalls | `rustix`: raw `statx`, `getdents64` into a caller-supplied buffer, `openat` with `O_NOATIME`, no libc dependency. | `x/sys/unix` has the same calls. The `os` package hides most of the control. |
| Concurrency | OS threads with crossbeam work-stealing deques. Explicit and tunable. | Goroutines are the easiest model. The runtime adds OS threads for blocking syscalls. |
| Static single binary | `x86_64/aarch64-unknown-linux-musl` (paired with mimalloc). Runs on old HPC distributions. | Trivial (`CGO_ENABLED=0`). |
| Failure containment | `Result` everywhere. `catch_unwind` at directory boundaries contains any bug. | Error values. `recover()` per goroutine. |
| Development speed and contributors | Slower to write, with a steeper learning curve. | Faster to write. Easier for sysadmins to contribute. |

**Recommendation: Rust.** Two factors decide it.

1. **The biggest correctness risk is file names.** I verified with DuckDB 1.5.1 on this machine that a Parquet column annotated as a string with *one* invalid UTF-8 value makes the whole file unreadable: `Invalid string encoding found in Parquet file … is not valid UTF8!`. A single legacy Shift-JIS file name among a billion entries would poison the dataset. Rust's `str`/`[u8]` split makes this class of bug a compile-time concern rather than a production surprise.
2. **Billion-row output with steady memory.** arrow-rs is a reference-quality Parquet writer, and having no GC keeps memory use flat across multi-day crawls.

Go is a reasonable alternative if long-term maintainers are Go-first. The architecture below is language-neutral. Both toolchains are already installed here (rustc 1.99, go 1.26.6), along with DuckDB 1.5.1.

---

## 3. Architecture

```
                    seed roots
                        │
             ┌──────────▼───────────┐
             │      Scheduler       │◄──────────────── child directories
             │ global injector +    │                  (pushed to the worker's
             │ per-worker LIFO deque│                   own deque: depth-first)
             └──────────┬───────────┘
             pop local / steal from peers (FIFO end = big subtrees)
       ┌────────────────┼────────────────┐
  ┌────▼────┐      ┌────▼────┐      ┌────▼────┐
  │ crawl 0 │      │ crawl 1 │ ...  │ crawl N │   openat(dir) → getdents64 → statx per entry
  └────┬────┘      └────┬────┘      └────┬────┘   (N = --threads, I/O bound, default 32)
       └───── directory-aligned record batches ──┘
                        │  bounded channel (backpressure)
             ┌──────────▼───────────┐
             │   Writer pool (M)    │   Arrow → Parquet (zstd) | CSV | JSONL
             │ each owns a rolling  │   part.tmp → fsync → rename = commit
             │ part file            │   (M = --writers, CPU bound, default min(cores, 8))
             └──────────┬───────────┘
                        ▼
  out/ manifest.json · entries/part-*.parquet · errors/part-*.parquet · checkpoints/
```

### 3.1 Scheduler: work stealing

- Each crawl worker owns a **LIFO** `crossbeam_deque::Worker`. New subdirectories go onto the local deque, so each worker runs depth-first. That keeps the queue small and helps the dentry cache.
- An idle worker first steals a batch from the global `Injector`, then steals from the **FIFO end** of a random peer's deque. The FIFO end holds directories nearest the root, which are the largest chunks of work. This is the same pattern used by HPC tools such as mpiFileUtils/libcircle, within a single process.
- **Termination:** an atomic `pending_dirs` counter goes up on push and down after a directory and all its child pushes are finished. When it reaches zero and all workers are idle, the crawl is done.
- **Threads are long-lived.** There is no thread-per-directory creation or teardown. The thread count is configurable from 1 to 1024.
- **Tuning by Little's law:** throughput ≈ concurrency ÷ per-op latency. With 0.5 ms NFS metadata RPCs, 64 in-flight stats can sustain about 128k stats/s until the server saturates. Network file systems want many more threads (64–256) than local disks. M3 adds an optional `probe` subcommand that samples throughput at several thread counts and suggests a setting.
- **Queue memory:** a task is about 150 bytes (path bytes, `dir_id`, depth, and the stat already gathered by the parent). A million queued directories is about 150 MB. Beyond a configurable budget, the queue spills to a temporary file (M3).

### 3.2 Processing one directory

1. **Open:** `openat(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC | O_NOATIME)`. If `O_NOATIME` returns `EPERM` (we don't own the directory and lack `CAP_FOWNER`), retry once without it and remember that for the rest of the run.
2. **Verify identity:** `fstat` the open fd and compare `(dev, ino)` with what the parent saw. If the directory was replaced between the parent's stat and this open, record "changed during crawl" and skip it. This prevents time-of-check/time-of-use (TOCTOU) races.
3. **Read entries:** call `getdents64` into a reusable per-worker buffer (default 256 KiB, configurable, versus glibc's ~32 KiB). Entries are processed as they stream in. A directory is never fully loaded into memory before its entries are processed.
4. **Stat each entry relative to the directory fd:** `statx(dirfd, name, AT_SYMLINK_NOFOLLOW | AT_NO_AUTOMOUNT, mask)`. Stat-ing relative to the directory fd avoids resolving the full path on every call. That matters a lot on NFS, Lustre, and GPFS. `AT_NO_AUTOMOUNT` keeps autofs trigger directories from mounting thousands of file systems.
   - **Fallback:** if `statx` returns `ENOSYS` (kernels older than 4.11, such as RHEL 7), use `fstatat`. Detect this once per run.
   - **Attribute mask:** configurable. On Lustre, leaving out size, blocks, and mtime skips per-stripe OST glimpse RPCs, which gives a fast namespace-only scan (`--fields -size`).
   - **`--statx-dont-sync`:** accept cached attributes on network file systems (`AT_STATX_DONT_SYNC`). Faster, but the values may be slightly stale.
5. **Classify and append:** write the row to the worker's local batch builder, and update directory aggregates (entry count, bytes, and allocated bytes of immediate children).
6. **Subdirectories:** push a task unless the directory is excluded (glob), is a snapshot directory, is past `--max-depth`, or is on another device (default; see §3.4).
7. **Symlinks:** recorded, never followed. `readlink` runs only with `--symlink-targets`.
8. **The directory's own row:** written *after* its entries, so it carries its aggregates. It uses the stat the parent already took. If the directory can't be opened (`EACCES`, etc.), it still gets a row, with `error` set.

### 3.3 Very large directories

A single directory can hold 10M+ entries. `getdents` is one sequential stream, but the stat calls don't have to be.

- **M1:** a large directory streams through one worker in batches. Memory stays bounded and other workers keep going.
- **M3:** above a threshold (default 100k entries), and when other workers are idle, the owning worker publishes **stat-chunk tasks**: a shared directory fd (`Arc<OwnedFd>`) plus a block of names. Idle workers run those stats in parallel. Aggregates are merged with atomics, and whichever chunk finishes last writes the directory's row.
- Rows from a chunked directory go to a dedicated `bigdir-<dir_id>` part that is committed only when the whole directory completes. That keeps resume semantics simple (§6).

### 3.4 Traversal policy defaults

| Policy | Default | Flag |
|---|---|---|
| Follow symlinks | Never | (none) |
| Cross mount points | **No.** Mount points are recorded with a flag but not entered. This avoids autofs storms and crawling NFS mounts by accident. | `--cross-mounts` |
| Snapshot directories (`.snapshot`, `~snapshot`, `.snapshots`, `.zfs`) | Skipped | `--include-snapshots` |
| Excludes | none | `--exclude <glob>` (repeatable), `--exclude-from <file>`. Uses `globset` (one compiled matcher) instead of a linear list. |
| Max depth | unlimited | `--max-depth N` |
| Directory cycles (bind mounts, legacy hard-linked directories) | No tracking (cycles are impossible without following symlinks and crossing mounts) | `--detect-cycles` tracks `(dev, ino)` of every directory |

### 3.5 Paths longer than PATH_MAX

Deep trees regularly exceed the 4096-byte `PATH_MAX`. Paths are kept as growable byte buffers with no fixed-size arrays. Opening by full path fails with `ENAMETOOLONG` past the limit. In that case the worker walks down component by component with `openat` from the deepest ancestor it can open. A unit test builds a path longer than 10,000 bytes.

### 3.6 Platform backends

The walker talks to a `FsBackend` trait. This also enables deterministic fault-injection tests (§9).

- **Linux (primary):** `statx`, `getdents64`, `O_NOATIME`, `AT_NO_AUTOMOUNT`, and statx attributes (compressed, immutable, append-only, encrypted, mount root). Built with `rustix` raw syscalls, so the static musl binary has no glibc version dependency.
- **macOS:** `fstatat` plus `st_birthtime` and `st_flags` in M1. M5 adds a `getattrlistbulk` backend (one call returns names and attributes for many entries), adopted only if benchmarks on APFS, SMB, and NFS justify it.
- **FreeBSD and other Unix:** generic `fstatat` backend, best effort.

---

## 4. Data model

### 4.1 File-name and path encoding

This is the core robustness feature.

- **Internally, names are bytes from start to finish.** A name becomes text only at the output boundary, in one well-tested module (`names.rs`).
- **`path` / `name` / `parent_path` (string columns) are always valid UTF-8:**
  - If the original bytes are valid UTF-8, they are emitted **unchanged**: no normalization, no replacement. NFD stays NFD.
  - Otherwise, each invalid byte is rendered as `\xNN`. The string is readable and searchable, but not guaranteed reversible.
- **`path_raw` (binary/BLOB) holds the exact original bytes.** By default it is filled only when `path` isn't byte-identical to the original. That is under 0.01% of rows on most systems, and null values cost almost nothing in Parquet. `--raw-paths always` fills it for every row. Exact bytes for any row: `coalesce(path_raw, encode(path))`.
- **In CSV and JSONL**, raw bytes are written as base64 (`path_b64`). Decode them in DuckDB with `from_base64()`.
- **`name_flags`** is a bitmask (UINT32) of name problems that matter for analytics and for migrations to SMB, Windows, or object stores. DuckDB views in `sql/` expose each bit as a boolean.

  | Bit | Meaning |
  |---|---|
  | `INVALID_UTF8` | Name isn't valid UTF-8 (a legacy encoding or corruption) |
  | `C0_CONTROL` / `C1_CONTROL` / `DEL` | Contains control characters (newline, tab, ESC, …) |
  | `BIDI_CONTROL` | Contains bidirectional overrides (U+202A–E, U+2066–9), which can spoof how a name displays |
  | `NOT_NFC` | Valid UTF-8 but not NFC-normalized (common from macOS HFS+) |
  | `LEADING_TRAILING_SPACE`, `LEADING_DASH`, `TRAILING_DOT` | Hazardous names |
  | `WINDOWS_RESERVED` | Contains `<>:"\|?*` or `\`, or is named `CON`, `PRN`, `AUX`, `NUL`, `COM1`, … |
  | `LONG_NAME` | Name longer than 255 bytes |
  | `LONG_PATH` | Path longer than 4096 bytes |
- **Optional `--guess-encoding` (M5):** for names that aren't valid UTF-8, feed *all* such names in the same directory to `chardetng` (Firefox's detector). Siblings usually share an encoding, and short names alone are poor signals. Output columns are `name_charset_guess` (for example `Shift_JIS`, `windows-1252`, `GBK`) and `name_decoded` (a best-effort UTF-8 rendering). This helps international teams find and fix legacy names.
- **Extension:** computed from the basename bytes. It is the text after the last `.`. A leading-dot name (`.bashrc`) or a trailing dot means no extension. ASCII is lowercased, and the result is capped at 32 bytes; anything longer is treated as "no extension".
- **CSV escaping:** RFC 4180 quoting. Control characters are C-escaped (`\n`, `\t`, `\x1b`) by default, so every record is exactly one physical line. `--csv-raw-controls` keeps them as quoted literals instead.

### 4.2 Timestamps

- Stored in Parquet as `TIMESTAMP(MICROS, UTC)`, which DuckDB reads as `TIMESTAMPTZ`. The range is ±290,000 years, so corrupted values from 30-year-old systems still fit: pre-1970 negative times, 0, 2038 and 2106 wraparounds. Out-of-range values saturate and set a `time_flags` bit, and never panic.
- `--time-unit ns` keeps nanosecond precision. Values outside 1677–2262 are clamped and flagged.
- CSV and JSON default to ISO-8601 UTC with fractional seconds. `--time-format epoch` writes `seconds.nanoseconds` instead.
- **`btime`** (birth/creation time) is nullable. It comes from `statx` `STATX_BTIME` on Linux when the file system supports it, and from `st_birthtime` on macOS and BSD. Documentation will point out that `ctime` is *change* time, not creation time, and that `ctime` is the trustworthy "recently written" signal because `cp -p`, `rsync -a`, and `tar` preserve `mtime`.

### 4.3 `entries` schema (v1)

| Column | Parquet type | Notes |
|---|---|---|
| `path` | STRING | Full path (UTF-8-safe, §4.1) |
| `parent_path` | STRING (dictionary) | Compresses to almost nothing because rows are grouped by directory |
| `name` | STRING | Basename |
| `extension` | STRING (dictionary), nullable | |
| `path_raw` | BINARY, nullable | Exact bytes (§4.1) |
| `name_flags` | UINT32 | Bitmask (§4.1) |
| `kind` | STRING (dictionary) | `file`, `dir`, `symlink`, `fifo`, `socket`, `char`, `block`, `unknown` |
| `depth` | UINT16 | 0 = root |
| `dir_id` | UINT64, nullable | Set for directories. Crawl-assigned and unique within a crawl |
| `parent_id` | UINT64 | `dir_id` of the containing directory (0 for the root) |
| `dev`, `ino` | UINT64 | Unsigned because hash-based inode numbers (Lustre, NFS) use the high bit |
| `nlink` | UINT64 | Count `(dev, ino)` once to avoid double-counting hard links |
| `uid`, `gid` | UINT32 | Names resolved only with `--resolve-ids` (cached) |
| `mode` | UINT32 | Full `st_mode`. The SQL views decode it to octal and `rwx` |
| `size` | INT64 | Logical size |
| `alloc_bytes` | INT64 | `st_blocks × 512`. Reveals sparse, compressed, and tiered/stubbed files |
| `atime`, `mtime`, `ctime` | TIMESTAMP(µs, UTC) | |
| `btime` | TIMESTAMP(µs, UTC), nullable | |
| `rdev` | UINT64, nullable | Device files only |
| `fs_attrs` | UINT64, nullable | statx attributes on Linux, `st_flags` on BSD/macOS |
| `symlink_target` / `symlink_target_raw` | STRING / BINARY, nullable | Only with `--symlink-targets` |
| `dir_entries`, `dir_files`, `dir_subdirs` | UINT64, nullable | Directories only: immediate children |
| `dir_bytes`, `dir_alloc_bytes` | INT64, nullable | Directories only: sum over immediate non-directory children |
| `is_mount_point` | BOOLEAN | `dev` differs from the parent's, or `STATX_ATTR_MOUNT_ROOT` |
| `time_flags` | UINT8 | Saturated or clamped timestamps |
| `error` | STRING, nullable | Example: `EACCES` on a directory that couldn't be opened (the row is present but the directory wasn't read) |

`--columns` and `--fields` select a subset to shrink the output. We'll measure in M2 whether storing `path` alongside `parent_path` and `name` is worth the extra bytes after zstd. If not, `path` becomes a view column.

### 4.4 `errors` schema

`path`, `path_raw`, `op` (`open`, `getdents`, `statx`, `readlink`, `verify`), `errno` (INT32), `message`, `time`, `worker`. Entries that vanish between `getdents` and `statx` (`ENOENT`) are counted separately as **`vanished`**, not as errors. Live file systems churn.

### 4.5 Recursive totals (`rollup`)

Each directory row carries totals for its *immediate* children. `fs-inventory rollup <scan-dir>` reads only the directory rows (about 5% of rows, 5 columns), rebuilds the tree from `dir_id`/`parent_id`, and writes `rollup/part-*.parquet`. Each output row has `dir_id`, `path`, `depth`, `total_entries`, `total_files`, `total_dirs`, `total_bytes`, `total_alloc_bytes`, `min_mtime`, `max_mtime`, and `max_ctime`. Running it after the crawl means it is always correct, including after a resume. For 50M directories it needs about 2 GB of RAM.

---

## 5. Output

### 5.1 Layout

```
scan-2026-10-25T0130Z/
  manifest.json            status running → complete | interrupted | incomplete; tool + schema version,
                           host, roots, options, start/end, totals by kind, errors by errno, throughput
  entries/part-w03-000017.parquet
  errors/part-w00-000002.parquet
  rollup/part-000000.parquet          (after `fs-inventory rollup`)
  checkpoints/ckpt-000042.bin.zst     (resume state; deleted on success unless --keep-checkpoints)
```

Repeated scans can sit side by side (`scans/scan_date=2026-10-25/…`), so DuckDB's hive partitioning gives you inventory-over-time analytics for free: growth, churn, new versus deleted files.

### 5.2 Parquet

- **Writer pool:** crawl workers hand over directory-aligned Arrow `RecordBatch`es through a bounded channel. Writers encode and compress in parallel. The number of output files follows `--writers`, not `--threads`, so 256 crawl threads don't produce 256 small files.
- **Defaults:** ZSTD level 3 (`--compression zstd[:N] | snappy | lz4 | none`), row groups of about 1M rows, 1 MiB data pages, column and offset indexes and min/max statistics enabled (so DuckDB can skip row groups on `mtime`, `size`, `uid`), and dictionary encoding on low-cardinality columns only.
- **Rolling parts:** a writer starts a new part at `--part-max-bytes` (default 1 GiB) or `--part-max-rows`.
- **Commit protocol:** write `*.parquet.tmp`, fsync, then rename. A finalized part is always a valid file. A crash leaves only `.tmp` files, which resume deletes.
- **Key-value metadata on every file:** `fs_inventory.schema_version`, `tool_version`, `scan_id`, `root`, `host`.

### 5.3 CSV and JSON Lines

- **Same directory layout and writer pool**, with optional compression (`.csv.zst`, `.jsonl.gz`).
- **`-o -` streams to stdout** for piping. Workers format rows into byte buffers in parallel, and a single writer thread calls `write_all` on whole buffers. There is no per-row locking.
- **JSON output is always JSON Lines** (one object per line). A single billion-element array can't be streamed or loaded.
- `fs-inventory convert <scan-dir> --to csv|jsonl` re-exports a Parquet scan without crawling again.

---

## 6. Robustness and long-crawl resilience

| Failure mode | Handling |
|---|---|
| `EACCES`, `EIO`, `ESTALE`, `ELOOP`, `ENAMETOOLONG` on any entry | Write a row to the errors table, increment a counter, continue. The directory row is still written, with `error` set. |
| Entry deleted between `getdents` and `statx` | Counted as `vanished`. Not an error. |
| Directory replaced between stat and open | Detected by the `(dev, ino)` check and recorded. |
| A bug or panic while processing a directory | `catch_unwind` at the directory boundary. The directory is logged as an internal error and the worker continues. Release builds set `panic = "unwind"`. |
| Hung NFS server (hard mount, thread stuck in D state) | A watchdog tracks each worker's current operation and start time. The progress line shows stalled paths. Other workers continue. With `--stall-timeout`, the crawl finalizes output, marks those subtrees incomplete in the manifest, and exits with code 3. |
| File descriptor exhaustion | Raise `RLIMIT_NOFILE` to the hard limit at startup. Each worker holds at most a few fds, and queued directories hold none. |
| Unbounded memory | Bounded channels, bounded batches, queue spill (M3), and `--max-memory` soft budget. |
| SIGINT / SIGTERM | Graceful: stop taking new directories, finish in-flight ones, commit parts, write a checkpoint and `manifest.status = interrupted`. A second signal exits immediately. Finalized parts stay valid either way. |
| Output disk full | Fatal for the run. Committed parts and the last checkpoint remain usable, and `resume` continues after space is freed. |
| Corrupt timestamps, sizes, or modes | Saturate and flag. Never panic on arithmetic. |
| Overly long paths or names | Growable buffers and an `openat` chain (§3.5). |
| autofs triggers | `AT_NO_AUTOMOUNT`, and no mount crossing by default. |

### 6.1 Checkpoint and resume (M4)

At 10,000 entries/s, a billion entries take about 28 hours. At 100,000/s, about 3 hours. Long crawls need resume. The invariants are designed in from M1, even though resume itself ships in M4.

1. **Rows are directory-atomic.** A directory's rows (including its own row) are released to a writer only when the directory completes. Huge directories go to their own `bigdir-*` part (§3.3).
2. **Periodic consistent checkpoints** (default every 10 minutes): the coordinator pauses workers at directory boundaries and drains the channel, and writers commit their current parts. It then writes the checkpoint atomically: the list of committed parts, the pending directory queue (paths, ids, and parent stat), the `dir_id` counter, running totals, and any huge directories still in progress along with the children they have already queued. The pause is bounded because huge directories don't block it; they are recorded as in progress.
3. **`fs-inventory resume <scan-dir>`:**
   1. Delete every part not listed in the last checkpoint, including leftover `.tmp` files.
   2. Reload the queue and counters.
   3. Re-crawl the in-progress huge directories. Their already-queued children are skipped so they don't appear twice.

   At most one checkpoint interval of work is redone, and no row is duplicated.

### 6.2 Privileges

- Run as root, or better, grant only the read-bypass capability: `setcap cap_dac_read_search+ep fs-inventory`. Never use setuid.
- NFS servers usually map root to an unprivileged user ("root squash"). For complete crawls, export file systems **read-only with `no_root_squash`** to a dedicated admin host.

---

## 7. CLI

```
fs-inventory scan [OPTIONS] <ROOT>...
  -o, --output <DIR|->            Output directory, or - for stdout (CSV or JSONL only)
  -f, --format parquet|csv|jsonl  Default: parquet
  -t, --threads <N>               Crawl threads (default 32; 64–256 for network file systems)
      --writers <N>               Encoder/writer threads (default min(cores, 8))
      --cross-mounts              Descend into other file systems
      --max-depth <N>
      --exclude <GLOB>            Repeatable
      --exclude-from <FILE>
      --include-snapshots
      --fields <SPEC>             Example: "default,+btime,-size" (namespace-only fast scan on Lustre)
      --columns <LIST>            Output column subset
      --statx-dont-sync           Accept cached attributes on network file systems
      --symlink-targets
      --resolve-ids               Add user and group name columns
      --raw-paths auto|always
      --guess-encoding
      --time-unit us|ns    --time-format iso|epoch   (CSV/JSONL)
      --compression zstd[:N]|snappy|lz4|gzip|none
      --part-max-bytes <SIZE>    --row-group-rows <N>
      --checkpoint-interval <DUR> (default 10m)
      --stall-timeout <DUR>
      --progress auto|plain|json|none    --progress-interval <DUR>
      --max-memory <SIZE>

fs-inventory resume  <SCAN_DIR>
fs-inventory rollup  <SCAN_DIR>
fs-inventory convert <SCAN_DIR> --to csv|jsonl
fs-inventory verify  <SCAN_DIR>     Check manifest, parts, and row counts
fs-inventory schema  [--format sql|json]
fs-inventory probe   <PATH>         Sample throughput at several thread counts (M3)
```

**Progress:** a stderr line every 10 seconds by default. It shows elapsed time, entries and entries/s (1-minute average), directories done and queued, bytes seen, errors, vanished entries, active and stalled workers, and output bytes. `--progress json` writes machine-readable lines for log collection.

**Exit codes:**

| Code | Meaning |
|---|---|
| 0 | Complete with no errors |
| 1 | Complete with per-entry errors (see the errors table) |
| 2 | Usage or configuration error |
| 3 | Interrupted or incomplete (resumable) |
| 4 | Fatal: output unwritable |

---

## 8. Project layout and dependencies

```
fs-inventory/
  Cargo.toml                # workspace; [profile.release] lto = "fat", codegen-units = 1, panic = "unwind"
  LICENSE  NOTICE  README.md  CHANGELOG.md  CONTRIBUTING.md  deny.toml  rust-toolchain.toml
  crates/
    fs-inventory-core/      # library: scheduler, walker, FsBackend (linux, macos, generic), names, model
    fs-inventory/           # library writers (parquet behind a default feature, csv, jsonl), manifest,
                            #   checkpoint, plus the CLI binary
  sql/                      # DuckDB views (decoded flags and modes) and example queries
  tests/fixtures/           # torture-tree generator
  benches/
  docs/
```

| Purpose | Crates (all Apache-2.0/MIT-compatible) |
|---|---|
| Syscalls | `rustix` |
| Scheduling | `crossbeam-deque`, `crossbeam-channel` |
| Columnar output | `arrow-array`, `arrow-schema`, `parquet` (feature `zstd`) |
| Text output | `csv`, `serde_json`, `base64` |
| Compression | `zstd`, `flate2` |
| CLI and runtime | `clap`, `globset`, `signal-hook`, `tracing`, `mimalloc` |
| Unicode and encodings | `unicode-normalization`, `chardetng` and `encoding_rs` (optional feature) |
| Testing | `proptest`, `tempfile`, `assert_cmd`, `criterion`, `cargo-fuzz` |

- **License hygiene:** every source file starts with `// SPDX-License-Identifier: Apache-2.0`. `cargo-deny` enforces an allowlist (Apache-2.0, MIT, BSD-2/3, ISC, Zlib, Unicode-3.0) and bans copyleft dependencies. A `NOTICE` file names the copyright holder.
- **Crate names:** `fs-inventory` and `fs-inventory-core` are both available on crates.io as of 2026-10-25.

---

## 9. Testing and benchmarking

### 9.1 Correctness

- **Property tests (`proptest`):** for *any* byte string, `path_raw`/`path` round-trips exactly, `path` is always valid UTF-8, valid UTF-8 input is never altered, and CSV and JSONL output re-parses to the same values.
- **Fuzzing (`cargo-fuzz`):** targets for the name encoder and the CSV and JSON escapers.
- **Torture-tree generator** (runs in a Linux container):
  - Names in every legacy encoding listed in §1, all C0 and C1 controls, DEL, bidi overrides, NFD, emoji with zero-width joiners, and 255-byte names.
  - Paths longer than 10,000 bytes, a 2,000-level chain, and a 1M-entry directory.
  - Dangling and looping symlinks, hard links, FIFOs, sockets, device nodes, and sparse files.
  - Unreadable directories and files (`chmod 000`).
  - Timestamps at 1901, -1, 0, 2038-01-19, 2106, 2262+, and 9999.

  > APFS rejects non-UTF-8 names (`EILSEQ`), which I verified on this Mac. Invalid-byte fixtures therefore must run on Linux (ext4, xfs, or tmpfs) via `podman` locally and in CI.
- **Oracle diff:** compare a scan against GNU `find -printf` NUL-delimited output on the same tree. The sets of `(path bytes, ino, size, mtime_ns, mode)` must match exactly.
- **Fault injection:** a mock `FsBackend` injects `EACCES`, `EIO`, `ENOENT`, `ESTALE`, slow operations, and hangs on chosen paths. It asserts that the crawl never aborts, that the errors table is right, and that the stall watchdog fires.
- **Churn test:** a concurrent creator and deleter runs during a scan, with no panics allowed and vanished entries counted.
- **Crash and resume test (M4):** `kill -9` at random points, then resume. The result must equal the oracle with zero duplicate rows.
- **DuckDB integration test:** `read_parquet('entries/*.parquet')` over the torture tree must succeed, with no UTF-8 errors and correct types.

### 9.2 Performance

- **Synthetic trees** of 10M and 100M entries (wide, deep, and mixed) on tmpfs and NVMe. Compare against `find`, `fd`, `du`, and `gdu`. Measure entries/s, CPU, peak memory, and output bytes per entry.
- **Real file systems** in your environment (NFS, GPFS/Spectrum Scale, Lustre, BeeGFS): a thread-count sweep to set defaults, plus `getdents` buffer-size and `--statx-dont-sync` experiments.
- **Targets to validate, not promises:**
  - CPU-bound with a warm cache, Parquet+zstd: ≥ 1M entries/s on a 16-core host.
  - Network file systems: throughput scales near-linearly with threads until the server saturates.
  - Memory: under 2 GB at default settings, independent of tree size (except queue growth on very wide trees).
- **CI (GitHub Actions):**
  - Platforms: Linux x86_64 and aarch64 (static musl binaries) and macOS arm64.
  - Checks: `clippy -D warnings`, `rustfmt`, `cargo-deny`, tests, and a minimum-supported-Rust-version (MSRV) check.

---

## 10. Roadmap

| Milestone | Scope | Exit criteria |
|---|---|---|
| **M0: Skeleton** | Workspace, LICENSE, NOTICE, CI, `cargo-deny`, `clap` skeleton, schema doc | CI green on Linux and macOS |
| **M1: Core walker + CSV/JSONL** | Work-stealing scheduler, Linux statx/getdents backend, generic backend (macOS), names module, directory aggregates, errors table, excludes, `--max-depth`, mount and snapshot policy, progress, graceful signals, stdout and directory output. Directory-atomic batching from day one. | Torture tree passes, the oracle diff is exact, there are zero panics under fault injection, and it beats `find` on 10M entries |
| **M2: Parquet** | Arrow builders, writer pool, rolling parts with the commit protocol, key-value metadata, manifest, `sql/` views and queries, `verify`, `schema` | A 100M-entry scan loads in DuckDB with no errors, and output size per entry is measured and documented |
| **M3: Scale** | Chunked stats for huge directories, stall watchdog, PATH_MAX chains, queue spill, `--max-memory`, `rollup`, `probe`, tuned defaults from real-file-system benchmarks | A 1B-entry synthetic or real scan completes, and memory stays flat |
| **M4: Resume** | Periodic checkpoints, `resume`, `kill -9` test suite | Resume produces output identical to an uninterrupted run |
| **M5: Platform and extras** | macOS `getattrlistbulk` backend (if benchmarks justify it), `--guess-encoding`, symlink targets, statx attributes, `--resolve-ids`, `convert`, release packaging (static binaries, .deb/.rpm, Homebrew), documentation site | Release 1.0 |

### Future ideas (post-1.0)

- **Multi-host crawls:** `--partition k/N` hashes top-level subtrees across N hosts. Sharded Parquet output merges in DuckDB with no extra step.
- **Native-scanner ingest adapters:** convert GPFS policy-engine `LIST` output and Lustre changelog or `lfs find` output into the same schema. These are typically much faster than a POSIX crawl on those systems.
- **`diff` subcommand:** new, deleted, and changed files between two scans.
- **Optional content hashing** for duplicate detection.
- **Windows backend** using `NtQueryDirectoryFile`, which returns names and metadata in bulk.

---

## 11. Decisions to confirm

| # | Decision | Recommendation |
|---|---|---|
| 1 | Language | Rust (§2) |
| 2 | Copyright holder in `NOTICE` and file headers | You personally, or your institution? |
| 3 | Cross mount points by default | **No.** Record mount points and require `--cross-mounts` to enter them |
| 4 | When to fill `path_raw` | Only when needed, with `--raw-paths always` available |
| 5 | Timestamp precision | Microseconds by default, `--time-unit ns` optional |
| 6 | v1 platforms | Linux (primary) and macOS. Windows later |
| 7 | Binary name | `fs-inventory` (optionally also install a short `fsinv` alias) |

---

## Appendix: example DuckDB queries

```sql
CREATE VIEW fs AS SELECT * FROM read_parquet('scan-*/entries/*.parquet');

-- Who used the most space in the last 24 hours? (ctime, because copies preserve mtime)
SELECT uid, count(*) AS files, sum(alloc_bytes) AS bytes
FROM fs WHERE kind = 'file' AND ctime > now() - INTERVAL 1 DAY
GROUP BY uid ORDER BY bytes DESC LIMIT 20;

-- Age profile by last modification year
SELECT year(mtime) AS yr, count(*) AS files, sum(size) AS bytes
FROM fs WHERE kind = 'file' GROUP BY yr ORDER BY yr;

-- Top extensions by bytes
SELECT extension, count(*) AS files, sum(size) AS bytes
FROM fs WHERE kind = 'file' GROUP BY extension ORDER BY bytes DESC LIMIT 50;

-- Hard-link-aware physical usage
SELECT sum(alloc_bytes) FROM (SELECT DISTINCT ON (dev, ino) alloc_bytes FROM fs WHERE kind = 'file');

-- Directories with more than 1M entries (application performance hazards)
SELECT path, dir_entries FROM fs WHERE kind = 'dir' AND dir_entries > 1000000 ORDER BY dir_entries DESC;

-- Names that will break a migration to SMB or object storage, with exact bytes
SELECT path, name_flags, coalesce(path_raw, encode(path)) AS exact_bytes
FROM fs WHERE name_flags <> 0;
```
