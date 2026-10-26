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

1. **The biggest correctness risk is file names.** I verified with DuckDB 1.5.1 and again with 2.0.0 on this machine that a Parquet column annotated as a string with *one* invalid UTF-8 value makes the whole file unreadable: `Invalid string encoding found in Parquet file … is not valid UTF8!`. A single legacy Shift-JIS file name among a billion entries would poison the dataset. Rust's `str`/`[u8]` split makes this class of bug a compile-time concern rather than a production surprise.
2. **Billion-row output with steady memory.** arrow-rs is a reference-quality Parquet writer, and having no GC keeps memory use flat across multi-day crawls.

Go is a reasonable alternative if long-term maintainers are Go-first. The architecture below is language-neutral. Both toolchains are already installed here (rustc 1.99, go 1.26.6), along with DuckDB 2.0.0.

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
- **Tuning by Little's law:** throughput ≈ concurrency ÷ per-op latency. With 0.5 ms NFS metadata RPCs, 64 in-flight stats can sustain about 128k stats/s until the server saturates. Network file systems want many more threads (64–256) than local disks. `fs-inventory probe` times short crawls at several thread counts and suggests the smallest one within 90% of the best rate.
- **Queue memory:** a task is about 150 bytes (path bytes, `dir_id`, depth, and the stat already gathered by the parent). A million queued directories is about 150 MB. Beyond a budget (1 GiB, or half of `--max-memory`), new tasks wait in a spill file next to the output, unlinked as soon as it's created, and are read back first in, first out once the queue drains.

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
- **M3:** past 10,000 entries, the owning worker publishes each further batch of names as a **stat-chunk task**: a shared handle to the open directory plus the batch. Idle workers stat those batches in parallel while the owner keeps reading names. Totals merge under a lock, and the owner helps with batches until all are done, then writes the directory's row.
- Rows from a chunk go to the part file of whichever worker stat'ed it. Checkpoints (§6.1) wait for a huge directory to finish, with every paused worker helping, so no checkpoint holds part of one.

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

- **Linux (primary):** `statx`, `getdents64`, `O_NOATIME`, and `AT_NO_AUTOMOUNT`. Built with `rustix` raw syscalls, so the static musl binary has no glibc version dependency.
- **macOS:** `fstatat` plus `st_birthtime` in M1. M5 adds a `getattrlistbulk` backend (one call returns names and attributes for many entries), adopted only if benchmarks on APFS, SMB, and NFS justify it.
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
- **No encoding guesses:** an option to guess the legacy character set of names that aren't valid UTF-8 (`--guess-encoding`, with `chardetng`) was dropped before 1.0 (decision 11). Detectors are unreliable on short names, and the exact bytes in `path_raw` let anyone decode names later with the right character set.
- **Extension:** computed from the basename bytes. It is the text after the last `.`. A leading-dot name (`.bashrc`) or a trailing dot means no extension. ASCII is lowercased, and the result is capped at 32 bytes; anything longer is treated as "no extension".
- **CSV escaping:** RFC 4180 quoting. Control characters are C-escaped (`\n`, `\t`, `\x1b`) by default, so every record is exactly one physical line. `--csv-raw-controls` keeps them as quoted literals instead.

### 4.2 Timestamps

- Stored in Parquet as `TIMESTAMP(MICROS, UTC)`, which DuckDB reads as `TIMESTAMPTZ`. The range is ±290,000 years, so corrupted values from 30-year-old systems still fit: pre-1970 negative times, 0, 2038 and 2106 wraparounds. Out-of-range values saturate and set a `time_flags` bit, and never panic.
- `--time-unit ns` keeps nanosecond precision. Values outside 1677–2262 are clamped and flagged.
- CSV and JSON default to ISO-8601 UTC with fractional seconds. `--time-format epoch` writes `seconds.nanoseconds` instead.
- **`btime`** (birth/creation time) is nullable. It comes from `statx` `STATX_BTIME` on Linux when the file system supports it, and from `st_birthtime` on macOS and BSD. Documentation will point out that `ctime` is *change* time, not creation time, and that `ctime` is the trustworthy "recently written" signal because `cp -p`, `rsync -a`, and `tar` preserve `mtime`.

### 4.3 Schema reference

The schema is defined once in code (`crates/fs-inventory/src/output/schema.rs`), and every format comes from it: the Parquet schema, CSV and JSON Lines columns, `fs-inventory schema --format sql|json|markdown`, and **[docs/schema.md](docs/schema.md)**, which is generated and checked by a test. Tests also check the DuckDB macros in `sql/` against it.

Main points:

- **`entries`** has 31 columns: path text, `path_raw` exact bytes, name flags, kind, depth, crawl IDs, the full `stat` (unsigned 64-bit `dev`/`ino`, timestamps in UTC), totals for directories, mount points, and errors.
- **`errors`** has one row per failed operation: path, exact bytes, operation, errno, message, and time.
- **Integer types:** integer columns are signed and wide enough for every value (`INTEGER` or `BIGINT`), so scans load into databases and tools without unsigned types, such as PostgreSQL and Spark, and DuckDB functions like `format_bytes()` work without casts. The exceptions are `dev`, `ino`, and `rdev`, which stay `UBIGINT`: they're opaque 64-bit identifiers, and inode numbers on Lustre and NFS really do use the top bit. Counts and byte totals are capped at the signed maximum (8 EiB), never wrapped. Schema version 2 made this change; version 1 had unsigned counts and sizes.
- **`--columns`** selects a subset of entry columns in every format.
- Entries that vanish between being listed and being read are counted as **vanished** in the manifest, not as errors. Live file systems churn.

### 4.5 Recursive totals (`rollup`)

Each directory row carries totals for its *immediate* children. `fs-inventory rollup <scan-dir>` reads only the directory rows (about 5% of rows, 5 columns), rebuilds the tree from `dir_id`/`parent_id`, and writes the `rollup` table: one row per directory with `entries`, `files`, `dirs`, `bytes`, `alloc_bytes`, the oldest and newest times, and `incomplete_dirs` (directories in the subtree that weren't fully read, so the totals are lower bounds). Running it after the crawl means it is always correct, including after a resume. It needs about 100 bytes of memory per directory: about 5 GB for 50M directories.

For file-age heat maps, it also writes **age histograms** to the `rollup_ages` table: file counts and bytes per age bucket (by default < 30 days, < 90 days, < 1 year, < 2, 3, 5, and 10 years, and older), for `mtime`, `ctime`, and `atime`, measured from the scan's start time, for directories down to `--age-depth` (default 4). To build them, `rollup` also reads a few columns of the file rows (`parent_id`, `size`, and the times), which is a fast column scan in Parquet. Buckets are configurable (`--age-buckets`), so they aren't frozen into the scan. The web app then reads a small directory × age-bucket table instead of aggregating billions of rows. Both tables are listed in the manifest and checked by `verify`; running `rollup` again replaces them.

---

## 5. Output

### 5.1 Layout

```
scan-2026-10-25T0130Z/
  manifest.json            status running → complete | interrupted | incomplete | failed; tool + schema version,
                           host, roots, options, start/end, totals by kind, errors by errno, throughput
  entries/part-w03-000017.parquet
  errors/part-w00-000002.parquet
  rollup/part-000000.parquet          (after `fs-inventory rollup`)
  rollup_ages/part-000000.parquet
  checkpoint/state                    (resume state: settings, parts, queued directories; removed when the scan finishes)
```

Every format is sharded the same way, so a billion-entry scan becomes many part files of bounded size rather than one huge file. DuckDB reads a glob of parts as a single table (`read_parquet('scan/entries/*.parquet')`, `read_csv('scan/entries/*.csv')`, `read_json('scan/entries/*.jsonl')`), and so can ingestion jobs that load parts in parallel.

Repeated scans can sit side by side (`scans/scan_date=2026-10-25/…`), so DuckDB's hive partitioning gives you inventory-over-time analytics for free: growth, churn, new versus deleted files.

### 5.2 Parquet

- **Pipeline:** crawl workers append rows to per-worker Arrow builders and hand directory-aligned record batches to the writers through a bounded channel. Writers encode and compress. The number of output files follows `--writers`, not `--threads`, so 256 crawl threads don't produce 256 small files.
- **Defaults:**
  - Compression: zstd level 3 (`--compression zstd[:N] | gzip[:N] | snappy | lz4 | none`).
  - Row groups: up to `--row-group-rows` (default 1,048,576) and 128 MiB in progress.
  - Page-level statistics and the page index, so DuckDB can skip row groups on `mtime`, `size`, or `uid`.
- **Encodings:** measured on 1.24 million real entries (average path 126 bytes):

  | Choice | Effect (bytes per entry) |
  |---|---|
  | `path` as `DELTA_BYTE_ARRAY` (each path stored as a suffix of the previous one) | 7.9 → 5.0 |
  | `ino` as `DELTA_BINARY_PACKED` | 1.6 → 1.3 |
  | No dictionary for high-cardinality columns (`path`, `name`, `path_raw`, `ino`, `dir_id`, timestamps) | Arrow's defaults are 36% larger overall; dictionary-encoded timestamps are about 70% larger |
  | Plain `name` and timestamps | Delta encoding made both larger |

- **Rolling parts:** a writer starts a new part at `--part-max-bytes` (default 1 GiB) or `--part-max-rows`.
- **Commit protocol:** write `*.parquet.tmp` (footer last), fsync, then rename. A finalized part is always a valid file, and a crash leaves only `.tmp` files.
- **Key-value metadata on every file:** `fs_inventory.schema_version`, `tool_version`, `scan_id`, `host`, and `roots`. `verify` checks each part's `scan_id` against the manifest.
- **Measured output size**, on the same 1.24 million entries:

  | Format | Bytes per entry | 1 billion entries |
  |---|---|---|
  | Parquet, zstd 3 (default) | 24.9 | about 25 GB |
  | Parquet, zstd 9 | 23.3 | about 23 GB |
  | Parquet, zstd 1 | 26.0 | about 26 GB |
  | CSV, zstd 3 | 36.0 | about 36 GB |
  | JSON Lines, zstd 3 | 41.0 | about 41 GB |
  | Parquet, Snappy | 41.0 | about 41 GB |
  | CSV, uncompressed | 446 | about 446 GB |

- **Allocator:** the binary uses mimalloc. Arrow builders allocate constantly, and musl's allocator (used by the static Linux build) serialized the crawl threads on a lock. A 500,000-file Parquet scan with 8 threads went from 722 ms to 332 ms. At 4 to 8 threads, Parquet now scans as fast as CSV.

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
| Hung NFS server (hard mount, thread stuck in D state) | A watchdog tracks each worker's last progress and the directory it's working on. The progress line shows stalled paths. Other workers continue. With `--stall-timeout`, the crawl finalizes output, marks those subtrees incomplete in the manifest, and exits with code 3. |
| File descriptor exhaustion | Raise `RLIMIT_NOFILE` to the hard limit at startup. Each worker holds at most a few fds, and queued directories hold none. |
| Unbounded memory | Bounded channels, bounded batches, queue spill (M3), and a `--max-memory` budget that counts crawl threads and writers. |
| SIGINT / SIGTERM | Graceful: stop taking new directories, finish in-flight ones, commit parts, write a checkpoint and `manifest.status = interrupted`. A second signal exits immediately. Finalized parts stay valid either way. |
| Output disk full | Fatal for the run. Committed parts and the last checkpoint remain usable, and `resume` continues after space is freed. |
| Corrupt timestamps, sizes, or modes | Saturate and flag. Never panic on arithmetic. |
| Overly long paths or names | Growable buffers and an `openat` chain (§3.5). |
| autofs triggers | `AT_NO_AUTOMOUNT`, and no mount crossing by default. |

### 6.1 Checkpoint and resume (M4) ✅

At 10,000 entries/s, a billion entries take about 28 hours. At 100,000/s, about 3 hours. Long crawls need resume.

As built (details in [docs/architecture.md](docs/architecture.md) §6.5):

1. **Consistent checkpoints by pausing at directory boundaries.** Every `--checkpoint-interval` (default 10 minutes), workers pause as they finish their current directory, helping with huge-directory batches meanwhile. With every worker paused, every row delivered to a sink belongs to a finished directory. The output sends every sink's rows, waits for the writers, and commits every open part. Then `checkpoint/state` is written atomically: the scan's settings, the committed parts, the queued directories, the spill file's position, the visited log's length, the `dir_id` counter, and the totals. This replaced the planned `bigdir-*` parts: a checkpoint never holds part of a directory.
2. **A stalled worker skips checkpoints** after 30 seconds instead of blocking the crawl. One abandoned by `--stall-timeout` is excluded, and its sink keeps only rows up to its last finished directory.
3. **`fs-inventory resume <scan-dir>`:**
   1. Locks the scan, so it can't run alongside the original process.
   2. Deletes every part the checkpoint doesn't list, including leftover `.tmp` files.
   3. Continues from the saved queue and counters with the saved settings, naming new parts `part-rN-wNN-...`.

   At most one checkpoint interval of work is redone, and no row is duplicated. Ctrl-C writes a final, exact checkpoint, so resuming after it redoes nothing.

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
      --resolve-ids               Write users and groups tables (ID to name)
      --raw-paths auto|always
      --time-unit us|ns    --time-format iso|epoch   (CSV/JSONL)
      --compression zstd[:N]|snappy|lz4|gzip|none
      --part-max-bytes <SIZE>    --row-group-rows <N>
      --checkpoint-interval <DUR> (default 10m)
      --stall-timeout <DUR>
      --progress auto|plain|json|none    --progress-interval <DUR>
      --max-memory <SIZE>

fs-inventory resume  <SCAN_DIR> [--threads N] [--checkpoint-interval DUR] ...
fs-inventory rollup  <SCAN_DIR> [--age-buckets LIST] [--age-depth N]
fs-inventory convert <SCAN_DIR> --to csv|jsonl
fs-inventory verify  <SCAN_DIR>     Check manifest, parts, and row counts
fs-inventory schema  [--format sql|json]
fs-inventory probe   <PATH> [--threads LIST] [--duration DUR] [--drop-caches]
                                    Time crawls at several thread counts and suggest one
```

**Progress:** a stderr line every 10 seconds by default. It shows elapsed time, entries and the rate since the last line, directories read, bytes seen, errors, directories queued, and stalled threads when there are any. `--progress json` writes the same values, plus vanished entries, as machine-readable lines for log collection.

**Exit codes:**

| Code | Meaning |
|---|---|
| 0 | Complete with no errors |
| 1 | Complete with per-entry errors (see the errors table) |
| 2 | Usage or configuration error |
| 3 | Interrupted or incomplete (resumable once `resume` exists, M4) |
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

Development happens on a macOS laptop, so tests are split by where they can run:

- **On the laptop:** unit, property, and integration tests, and small synthetic trees. Never quote laptop numbers as performance results.
- **In a Linux container (Podman):** the Linux `statx`/`getdents64` backend, invalid-byte file names (APFS rejects them), and the torture tree.
- **On a Linux server with large NFS mounts (later):** real performance. M1 ships a portable benchmark kit, `bench/`, with a synthetic tree generator and a runner that sweeps thread counts and compares against `find`. You copy the static binary and the kit to the server and run it there. `bench/server.py` runs the whole server benchmark in about 15 minutes and writes a report to send back.
- **Generated trees, anywhere:** `scan --synthetic` (hidden) scans a deterministic tree computed in memory, so scale tests need no storage. `bench/scale.py` records memory and throughput every second and can kill and resume the scan. Results on the laptop (M4, 10 cores), 2026-10-25:

  | Scan | Time | Peak memory | Checks |
  |---|---:|---:|---|
  | 100M entries, Parquet | 42 s | 1.55 GB | Complete, exact count, `verify` |
  | 100M, Parquet, `--max-memory 512M` | 52 s | 1.29 GB | Same |
  | 200M, Parquet, `kill -9` at 50% and resumed | 101 s | 1.58 GB | Every entry exactly once (by inode) |
  | 1B entries, CSV to `/dev/null` | 338 s | 238 MB | Complete, exact count |

  Memory levels off within the first third of each scan and stays within 3% of that to the end. The rates (2–3 million entries per second) are fs-inventory's own CPU ceiling on this machine, not storage performance.

- **Synthetic trees** of 10M and 100M entries (wide, deep, and mixed) on tmpfs and NVMe. Compare against `find`, `fd`, `du`, and `gdu`. Measure entries/s, CPU, peak memory, and output bytes per entry.
- **Real file systems** in your environment (NFS, GPFS/Spectrum Scale, Lustre, BeeGFS): a thread-count sweep to set defaults, plus `getdents` buffer-size and `--statx-dont-sync` experiments.
- **Targets to validate, not promises:**
  - CPU-bound with a warm cache, Parquet+zstd: ≥ 1M entries/s on a 16-core host.
  - Network file systems: throughput scales near-linearly with threads until the server saturates.
  - Memory: under 2 GB at default settings, independent of tree size (except queue growth on very wide trees). Independence from tree size is confirmed up to a billion entries. Memory does grow with crawl threads (allocator retention): on Linux with two Parquet writers, about 1.7 GB with 64 threads and 2.3 to 3.2 GB with 256. Defaults stay as they are for speed; `--max-memory` budgets threads and writers too, and returns freed memory at once (40 to 50% less memory for 5 to 8% more CPU time), so it holds where memory is limited.
- **CI (GitHub Actions):**
  - Platforms: Linux x86_64 and aarch64 (static musl binaries) and macOS arm64.
  - Checks: `clippy -D warnings`, `rustfmt`, `cargo-deny`, tests, and a minimum-supported-Rust-version (MSRV) check.

---

## 10. Roadmap

| Milestone | Scope | Exit criteria |
|---|---|---|
| **M0: Skeleton** ✅ | Workspace, LICENSE, NOTICE, CI, `cargo-deny`, `clap` skeleton | CI green on Linux and macOS |
| **M1: Core walker + CSV/JSONL** ✅ | Work-stealing scheduler, Linux statx/getdents backend, generic backend (macOS), names module, directory aggregates, errors table, excludes, `--max-depth`, mount and snapshot policy, progress, graceful signals, stdout and directory output with rolling parts and the commit protocol, manifest, benchmark kit. Directory-atomic batching from day one. | Torture tree passes in a Linux container, the oracle diff is exact, there are zero panics under fault injection, and DuckDB 2.0 loads the output |
| **M2: Parquet** ✅ | Arrow builders, Parquet writer pool, key-value metadata, `--columns`, `sql/` macros and example queries, `verify`, `schema` (also generates `docs/schema.md`), mimalloc | Output size per entry measured on 1.24M real entries (§5.2), and DuckDB 2.0 loads Parquet, CSV, and JSON Lines scans. A 100M-entry scan of a generated tree ran on the laptop (§9.2); scans of real storage at that scale happen on the Linux server. |
| **M3: Scale** (features done) | Chunked stats for huge directories, stall watchdog, PATH_MAX chains, queue spill, `--max-memory`, `rollup`, `probe`, `--fields`, `--detect-cycles`, tuned defaults from real-file-system benchmarks | A 1B-entry synthetic or real scan completes, and memory stays flat: met with a generated billion-entry tree on the laptop (§9.2). Default tuning from real storage happens on the Linux server. |
| **M4: Resume** ✅ | Periodic checkpoints, `resume`, `kill -9` test suite | Resume produces output identical to an uninterrupted run: tested by resuming from every checkpoint of a walk, and by `kill -9` at several points of real scans |
| **M5: Platform and extras** | `diff` between two scans (§11), `convert` ✅, `--symlink-targets` ✅ (schema version 3), `--resolve-ids` ✅ (decision 10), macOS `getattrlistbulk` backend (if benchmarks justify it), static release binaries and `install.sh` ✅, documentation site ✅, scale tests and the `--max-memory` budget ✅ (0.5.0), then hardening and a final review of the CLI and schema | Release 1.0 |

### Future ideas (post-1.0)

- **Multi-host crawls:** `--partition k/N` hashes top-level subtrees across N hosts. Sharded Parquet output merges in DuckDB with no extra step.
- **Native-scanner ingest adapters:** convert GPFS policy-engine `LIST` output and Lustre changelog or `lfs find` output into the same schema. These are typically much faster than a POSIX crawl on those systems.
- **Optional content hashing** for duplicate detection.
- **Windows backend** using `NtQueryDirectoryFile`, which returns names and metadata in bulk.
- **File attributes** (immutable, append-only, compressed, encrypted) from `statx` and macOS file flags. Parked (decision 12): `alloc_bytes` already shows compression and sparse files, and NFS reports few attributes.
- **Packages:** `.deb` and `.rpm`, and a Homebrew tap. Parked (decision 12): the static binaries and `install.sh` cover installation.

---

## 11. Downstream consumer: inventory web application

fs-inventory will feed a separate file inventory and analytics web application: finding data, file-age reports, and file-age heat maps. The tool stays stand-alone, but its output is a long-lived contract with that system's ingestion. That shapes several choices:

- **Schema stability:** `fs_inventory.schema_version` is in every part file and in the manifest. Adding a nullable column is a minor change; renaming, removing, or retyping one bumps the major schema version.
- **Ingest only complete scans:** `manifest.json` has a unique `scan_id`, the roots, start and end times, and a `status`. Ingestion takes a scan only when `status` is `complete` (or `incomplete`, if it accepts partial data) and records the `scan_id` it loaded.
- **Parallel loading:** sharded part files let ingestion load parts concurrently, and each part is self-describing.
- **Incremental updates:** the app's database is refreshed from successive scans. The stable identity of an entry across scans is its exact path bytes (`coalesce(path_raw, encode(path))`). `(dev, ino)` changes across NFS clients and remounts, so it is only a rename hint. A `diff` subcommand (moved up to M5) emits new, deleted, and changed entries between two scans so the app can apply deltas instead of reloading billions of rows.
- **Heat maps:** `rollup` (§4.5) produces per-directory recursive age histograms after each scan, so the app reads a small table instead of aggregating every file.
- **Orchestration:** documented exit codes and `--progress json` let a scheduler run scans and track them.

---

## 12. Decisions

| # | Decision | Status |
|---|---|---|
| 1 | Language | **Decided:** Rust (§2) |
| 2 | Copyright holder in `NOTICE` and file headers | **Decided:** Robert McDermott |
| 3 | Cross mount points by default | **No.** Record mount points and require `--cross-mounts` to enter them |
| 4 | When to fill `path_raw` | Only when needed, with `--raw-paths always` available |
| 5 | Timestamp precision | Microseconds by default, `--time-unit ns` optional |
| 6 | v1 platforms | Linux (primary) and macOS. Windows later |
| 7 | Binary name | `fs-inventory` (optionally also install a short `fsinv` alias) |
| 8 | Versioning | **Decided:** the tool version follows Semantic Versioning. Each milestone is released as `0.N.0` (M3 is 0.3.0), and so are feature releases between milestones (0.5.0 shipped scale tests and the `--max-memory` budget). Fixes in between are patch releases, and M5 is 1.0.0. The version changes only in a release commit, which is then tagged `vX.Y.Z`. Pushing the tag runs `.github/workflows/release.yml`, which builds the binaries and drafts the release with that version's changelog section; publishing the draft makes it the latest release, which `install.sh` installs. The schema version is separate (§11) and changes only when the output format breaks readers. |
| 9 | Default stall timeout | **Decided:** none. A stalled directory is reported after 60 seconds, and the scan waits for it; `--stall-timeout DURATION` opts in to giving up on it. An unattended scan can hang on a dead server, but never skips part of a tree unless asked to. |
| 10 | User and group names | **Decided:** opt-in `--resolve-ids`, off by default. Each crawl thread collects the IDs it sees, and when the scan ends each unique ID is looked up once with `getent`, which reaches LDAP and sssd even from the static binary. The names go in `users` and `groups` tables next to the entries; entry rows keep numeric IDs only. |
| 11 | Guessing legacy encodings | **Decided:** dropped (`--guess-encoding`). Guesses are unreliable on short names, and the exact bytes are kept. |
| 12 | Parked until after 1.0 | File attributes, `.deb` and `.rpm` packages, and a Homebrew tap (§10, future ideas). |

---

## Appendix: example DuckDB queries

Tested with DuckDB 2.0.0. DuckDB 2.0 disables the `x -> x * 2` lambda syntax by default, so SQL shipped with fs-inventory uses `lambda x: x * 2`.

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
