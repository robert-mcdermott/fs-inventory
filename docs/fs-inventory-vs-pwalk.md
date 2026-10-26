# Competitive analysis: fs-inventory vs. pwalk

- **Compared:** [pwalk](https://github.com/fizwit/filesystem-reporting-tools), built unmodified with `cc -O2`) and fs-inventory from the `scale-tests` branch after 0.4.0, with Parquet output, checkpoints, the stall watchdog, and the `--max-memory` budget.
- **Last updated:** 2026-10-26.

> **Caveat:** every number below comes from a Linux VM on a laptop with a warm metadata cache. That measures CPU efficiency, not storage. The comparison that matters is on the Linux server with large NFS mounts. 

## Summary

- **Correctness:** on the torture tree, pwalk silently lost a subtree, altered names, and produced CSV that DuckDB refuses to read. fs-inventory recorded every entry with exact bytes.
- **Speed (warm cache, Linux VM, 5 vCPUs):** fs-inventory was about 11× faster than pwalk and 7× faster than `find`, with CSV or Parquet output.
- **Memory:** fs-inventory uses far more than pwalk's 12 MB: on a 500,000-file tree, 30–310 MB with CSV and 110–440 MB with Parquet (1 to 32 threads), and on large trees about 1.7 GB with 64 threads and 2.3–3.2 GB with 256. Most of it is allocator retention per thread. It's bounded and doesn't grow with tree size, and `--max-memory` cuts it 40–50% when memory is limited.

## Correctness on the torture tree (Linux)

Tree: 10 awkward names (Latin-1, Shift-JIS, a stray byte, newline, tab, BEL, ESC, comma and quote, Japanese, a 1901 timestamp) plus a chain of 25 directories with 200-byte names (a path of over 5,000 bytes). Raw pwalk output is in `compare/torture/`. fs-inventory recorded every entry (paths up to 5,037 bytes) with 0 errors and `verify` passing, escaped and flagged the awkward names with exact bytes kept, kept the ESC as `\x1B`, and recorded the 1901 time correctly.

| Behavior | pwalk | fs-inventory |
|---|---|---|
| Entries recorded | 32 of 39. Every `lstat` past 4,096 bytes fails with `File name too long`, and that subtree is missing from the output. | All (opens long paths component by component) |
| Non-UTF-8 names (Latin-1, Shift-JIS, stray bytes) | Raw bytes written into the CSV | Readable text with `\xNN` escapes, plus exact bytes in `path_b64` |
| Loading in DuckDB 2.0 | `read_csv` fails on the whole file: "This file is not utf-8 encoded". With `ignore_errors`, 30 rows load. | Loads with the shipped macros, and SQL recovers the exact bytes of every path |
| Control characters | `\n`, `\t`, `\a` escaped; ESC silently **dropped** (`esc<ESC>x` became `escx`), so names can collide | Escaped in CSV (`\x1B`), flagged in `name_flags`, exact bytes kept |
| Error messages | Garbage thread IDs (uninitialized fields): `threadID=3472328296227680304,rdepth=808464432` | Errors table with path, operation, errno, and message. Exit code 1 when any occurred. |
| Timestamps | Whole seconds since epoch | ISO-8601 UTC, microseconds (nanoseconds optional), plus birth time where available |
| Interrupted or killed scans | Start over | Checkpoints every 10 minutes; `resume` continues with every entry exactly once (tested to 200M entries with `kill -9`) |
| Hung directories (dead NFS server) | The thread blocks forever | Reported after 60 s; `--stall-timeout` abandons the subtree and the scan finishes |
| Output | One CSV stream | Parquet (default, about 25 bytes per real entry), CSV, or JSON Lines in rolling parts, with a manifest and `verify` |

## Performance (warm cache)

Tree: 500,000 empty files in 5,000 directories (`bench/gen_tree.py --files 500000`), 3 runs each, median wall time. pwalk's thread count is fixed at 32 at compile time.

Linux VM (Linux 6.15 aarch64, 5 vCPUs, overlayfs), 2026-10-26 (`compare/results/linux-vm-20261026-current`):

| Tool | Threads | Median | Entries/s | Peak RSS |
|---|---|---|---|---|
| fs-inventory (CSV part files) | 1 | 0.82 s | 617k | 29 MB |
| fs-inventory (CSV part files) | 4 | 0.31 s | 1.64M | 69 MB |
| fs-inventory (CSV part files) | 8 | 0.27 s | 1.89M | 111 MB |
| fs-inventory (CSV part files) | 32 | 0.30 s | 1.71M | 307 MB |
| fs-inventory (CSV to stdout) | 4 | 0.26 s | 1.97M | 65 MB |
| fs-inventory (Parquet, the default) | 1 | 0.78 s | 649k | 113 MB |
| fs-inventory (Parquet) | 4 | 0.28 s | 1.78M | 150 MB |
| fs-inventory (Parquet) | 8 | 0.27 s | 1.90M | 286 MB |
| fs-inventory (Parquet) | 32 | 0.35 s | 1.43M | 436 MB |
| `find -printf` | 1 | 1.88 s | 269k | 12 MB |
| pwalk | 32 | 3.00 s | 168k | 12 MB |

With the cache warm the work is CPU-bound, so threads beyond the 5 vCPUs add nothing, and 32 threads cost memory without adding speed. On NFS, where each `stat` waits on the network, more threads should help until the server saturates.

## Memory

pwalk holds almost nothing: it prints each row under a global lock. fs-inventory buffers rows per thread and encodes them in writer threads, which is where its speed comes from, and it pays for that in memory:

- **This tree (500,000 files):** see the table above. Memory grows with threads, not with the tree.
- **Large trees (generated, Linux, 2 Parquet writers, no budget):** about 750 MB with 16 threads, 1.7 GB with 64, and 2.3–3.2 GB with 256, flat from early in the scan to the end. A billion-entry scan to CSV at 10 threads peaked at 238 MB.
- **Why:** mimalloc (chosen because musl's allocator serialized threads) keeps freed memory per thread and returns it a second late. The live heap is much smaller: 160 MB with 1 crawl thread, 270 MB with 64.
- **`--max-memory`:** returns memory at once (40–50% less, for 5–8% more CPU when CPU-bound) and budgets threads and writers. 256 threads under a 2 GiB budget held 1.0–1.15 GB across 300 million entries.

For a server inventory this is acceptable. Where it isn't, as in a memory-capped batch job, `--max-memory` is the answer, and pwalk's 12 MB is its one real advantage.

## Why fs-inventory is faster

| | pwalk | fs-inventory |
|---|---|---|
| Work distribution | A thread per directory up to 32; past that, the thread recurses into its subtree, so a deep subtree pins one thread | Fixed worker pool with work stealing; a huge directory's stats are shared among idle workers in batches |
| Output | One global mutex around every `printf` | Each worker encodes into its own buffer; a pool of writer threads encodes Parquet or text and writes directory-aligned parts |
| Metadata calls | `lstat` on the full path (the kernel resolves every component each time) | `statx` (or `fstatat`) relative to the open directory fd |
| Directory reads | `readdir` with the libc 32 KiB buffer | `getdents64` with a 256 KiB buffer |


