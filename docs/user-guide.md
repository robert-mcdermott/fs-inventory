# fs-inventory user guide

This guide covers fs-inventory 0.4.0: how to install it, how to run it in the situations it was built for, what it writes, and how to query the results with DuckDB. The examples scan a small demo tree at `/data/projects` on Linux. Every query was run against those scans, and the output shown is what was printed, except where an example is marked as an illustration.

Related documents:

- [Schema](schema.md): every column of every table.
- [Architecture](architecture.md): how fs-inventory works inside.
- [Changelog](../CHANGELOG.md): what changed in each release.

## Contents

1. [Install](#1-install)
2. [Quick start](#2-quick-start)
3. [Concepts](#3-concepts)
4. [Common scenarios](#4-common-scenarios)
5. [Command reference](#5-command-reference)
6. [Output reference](#6-output-reference)
7. [Querying scans with DuckDB](#7-querying-scans-with-duckdb)
8. [Performance tuning](#8-performance-tuning)
9. [Troubleshooting](#9-troubleshooting)
10. [Not yet available](#10-not-yet-available)

---

## 1. Install

fs-inventory is a single executable with no runtime dependencies. Each release has binaries for Linux (x86_64 and ARM) and macOS (Apple silicon and Intel). The Linux binaries are static: they run on any distribution with kernel 3.2 or later, including RHEL 7.

### Install a release

```bash
curl --proto '=https' --tlsv1.2 -fsSL https://raw.githubusercontent.com/robert-mcdermott/fs-inventory/main/install.sh | sh
```

The installer downloads the binary for your system and the release's `SHA256SUMS` file. It checks the binary's SHA-256 checksum, runs it to check its version, and only then installs it as `~/.local/bin/fs-inventory`, replacing an existing copy in one step. It doesn't use `sudo` or change `PATH`. If `~/.local/bin` isn't on your `PATH`, it says so and prints the full path to run.

Pass options to the installer after `sh -s --`:

| Option | Environment variable | Effect |
|---|---|---|
| `--version VERSION` | `FS_INVENTORY_VERSION` | Install this release, such as `v0.4.0`, instead of the latest |
| `--install-dir DIRECTORY` | `FS_INVENTORY_INSTALL_DIR` | Install here instead of `~/.local/bin` |

For example, to install a pinned version for every user of a server:

```bash
curl --proto '=https' --tlsv1.2 -fsSL https://raw.githubusercontent.com/robert-mcdermott/fs-inventory/main/install.sh | sudo sh -s -- --version v0.4.0 --install-dir /usr/local/bin
```

To install without the script, download the binary and `SHA256SUMS` from the [releases page](https://github.com/robert-mcdermott/fs-inventory/releases), check the checksum, and make the binary executable:

```bash
grep ' fs-inventory-linux-amd64$' SHA256SUMS | sha256sum --check
```

```bash
chmod +x fs-inventory-linux-amd64
```

On macOS, use `shasum -a 256 --check` in place of `sha256sum --check`. A binary downloaded with a web browser is quarantined by macOS; remove that with `xattr -d com.apple.quarantine fs-inventory-darwin-arm64`, or use the installer, which downloads with `curl`.

To uninstall, delete the binary.

### Build from source

Build with Rust 1.88 or later; the toolchain named in `rust-toolchain.toml` installs automatically through rustup.

```bash
cargo build --release
```

The binary is `target/release/fs-inventory`. Copy it anywhere on your `PATH`.

A normal Linux build links against the system's glibc and runs only on systems with that glibc version or newer. To build a static binary like the released ones, build on a Linux machine (or in a Linux container) with the musl C tools installed (`musl-tools` on Debian and Ubuntu):

```bash
rustup target add x86_64-unknown-linux-musl
```

```bash
cargo build --release --target x86_64-unknown-linux-musl
```

The binary is `target/x86_64-unknown-linux-musl/release/fs-inventory`. Use `aarch64-unknown-linux-musl` for ARM servers. The static build crawls as fast as the glibc build.

### Check the installation

```bash
fs-inventory --version
```

```
fs-inventory 0.4.0
```

On kernels older than 4.11 (such as RHEL 7), fs-inventory reads metadata with `fstatat` instead of `statx`, and `btime` (creation time) is always null.

---

## 2. Quick start

Scan a directory tree into a new output directory:

```bash
fs-inventory scan /data/projects -o /scratch/inventory/projects-2026-10-25
```

```
fs-inventory: complete: 285 entries in 0:00:00 (33.3k/s), 35 dirs, 44.02 GiB, 1 error, 0 vanished; output: /scratch/inventory/projects-2026-10-25
```

Check that the scan is complete and consistent:

```bash
fs-inventory verify /scratch/inventory/projects-2026-10-25
```

```
scan 01a1758c-51da-709d-bafb-c8feaa458bf3: complete, 285 entries in 1 part, 1 error in 1 part (parquet)
ok
```

Query it with DuckDB:

```bash
duckdb -c "SELECT kind, count(*) AS entries, sum(size) AS bytes
           FROM read_parquet('/scratch/inventory/projects-2026-10-25/entries/*.parquet')
           GROUP BY kind ORDER BY entries DESC"
```

```
┌─────────┬─────────┬─────────────┐
│  kind   │ entries │    bytes    │
│ varchar │  int64  │   int128    │
├─────────┼─────────┼─────────────┤
│ file    │     248 │ 47262948352 │
│ dir     │      36 │       34749 │
│ symlink │       1 │          12 │
└─────────┴─────────┴─────────────┘
```

The `1 error` above is a directory the scanning user wasn't allowed to read. It's recorded in the `errors` table, and the scan still completed. That is why `scan` exited with code 1 rather than 0 (see [exit codes](#exit-codes)).

---

## 3. Concepts

**A scan is one output directory.** Each run of `fs-inventory scan` writes a new directory holding `manifest.json` and one subdirectory per table:

```
/scratch/inventory/projects-2026-10-25/
  manifest.json                        scan ID, status, roots, options, totals, list of part files
  entries/part-w00-000000.parquet      one row per file, directory, symlink, ...
  errors/part-w00-000000.parquet       one row per failed operation (only if there were any)
  rollup/part-000000.parquet           subtree totals per directory (after `fs-inventory rollup`)
  rollup_ages/part-000000.parquet      file counts and bytes by age (after `fs-inventory rollup`)
```

**Tables are split into part files.** A large scan writes many parts per table, so no single file grows without bound. DuckDB, Spark, and most Parquet readers read a glob such as `entries/*.parquet` as one table.

**Every entry gets a row.** Files, directories, symlinks, devices, pipes, and sockets each get one row in `entries`, with their full path, owner, permissions, size, allocated space, and timestamps. A directory's row comes after its contents, and carries totals over its *immediate* children: `dir_entries`, `dir_files`, `dir_subdirs`, `dir_bytes`, and `dir_alloc_bytes`. `fs-inventory rollup` adds totals over each directory's *whole subtree*.

**Rows link to their parent directory.** Each directory gets a `dir_id`, unique within the scan, and every row's `parent_id` is its parent's `dir_id` (0 for a root). Joining `parent_id` to `dir_id` walks the tree.

**Names are kept exactly.** File names are bytes, not text, and some aren't valid UTF-8 (names from old systems, or corrupted ones). `path` is always valid text. When it can't show the name exactly, the exact bytes are kept in `path_raw` (or `path_b64` in CSV and JSON Lines). `name_flags` marks names that cause trouble for other systems.

**Failures don't stop a scan.** An unreadable directory gets a row with `error` set and a row in the `errors` table, and the scan continues. Entries deleted while the scan runs are counted as *vanished*, not as errors.

**Long scans can be resumed.** Every 10 minutes, a scan takes a checkpoint: a record of everything written so far and every directory still to read, in `checkpoint/` inside the output directory. If the scan is interrupted or killed, or its machine goes down, `fs-inventory resume` continues from the last checkpoint. The finished scan is the same as one that was never interrupted.

**Check the status before trusting a scan.** `manifest.json` has a `status`: `complete`, `interrupted`, `incomplete`, `failed`, or `running` (still in progress, or the process was killed). Treat a scan as finished only when the status is `complete`, or when `fs-inventory verify` exits with code 0.

---

## 4. Common scenarios

### 4.1 Inventory a directory tree

```bash
fs-inventory scan /data/projects -o /scratch/inventory/projects-2026-10-25
fs-inventory verify /scratch/inventory/projects-2026-10-25
fs-inventory rollup /scratch/inventory/projects-2026-10-25
```

- **Use a new output directory for each scan.** `scan` refuses to write into a directory that already contains files:

  ```
  fs-inventory: output directory /scratch/inventory/projects-2026-10-25 is not empty; choose a new directory for each scan
  ```

- **Name scan directories by tree and date,** such as `projects-2026-10-25`. Scans then sort by date, and several can be queried together ([7.5](#75-recipes)).
- **The output directory can sit inside the scanned tree.** fs-inventory skips its own output, so it never inventories itself.
- **`rollup` is optional.** It adds subtree totals and age histograms ([4.9](#49-subtree-totals-and-file-age-heat-maps)). It reads the finished scan, so it can run at any time later.

### 4.2 Scan a large network file system

NFS and other network file systems answer each metadata request over the network, so a single thread spends most of its time waiting. More threads keep more requests in flight. Find a good thread count with `probe` before the first big scan:

```bash
fs-inventory probe /nfs/projects --threads 16,32,64,128,256 --duration 30s
```

It crawls the tree for up to 30 seconds at each thread count, writes nothing, and suggests the smallest count that reaches 90% of the best rate ([5.5](#55-probe)). Then scan with that count, and with the options a long unattended scan needs:

```bash
fs-inventory scan /nfs/projects \
    -o /scratch/inventory/projects-2026-10-25 \
    --threads 128 \
    --stall-timeout 10m \
    --progress json --progress-interval 60s \
    2>> /scratch/inventory/projects-2026-10-25.log
```

- **`--threads 128`:** network file systems usually do best between 64 and 256. The default is 32.
- **`--stall-timeout 10m`:** if a crawl thread is stuck for 10 minutes in one call (a hung NFS server, for example), the scan gives up on that directory, finishes everything else, and ends with status `incomplete` and exit code 3 instead of hanging forever ([4.12](#412-interrupted-crashed-and-stalled-scans)).
- **`--progress json`:** writes one JSON progress line per interval to stderr, ready for log collection ([progress output](#progress-output)).
- **Write the output to local or scratch storage,** not to the file system being scanned, so writes don't compete with the crawl.
- **Checkpoints are on by default.** If the scan is killed or the machine goes down, `fs-inventory resume /scratch/inventory/projects-2026-10-25` continues it, losing at most the last 10 minutes of work ([4.12](#412-interrupted-crashed-and-stalled-scans)).

Two more options are worth knowing on network file systems:

- **`--statx-dont-sync`** (Linux) lets the client answer from its attribute cache instead of asking the server, which is faster. Sizes and times may be slightly out of date for files being written during the scan.
- **`--max-memory 4GiB`** caps the memory used for directories waiting to be read and for output buffers. Directories beyond the cap wait in a temporary file next to the output ([8.3](#83-memory)).

Run long scans so they survive a logout: under `nohup`, `tmux`, `screen`, a systemd unit, or a batch scheduler such as Slurm.

### 4.3 Fast namespace-only scans on Lustre

On Lustre, a file's size and allocated blocks live on the object storage servers, so reading them costs an extra round trip per file. When you only need names, owners, and times, skip them:

```bash
fs-inventory scan /lustre/projects -o /scratch/inventory/lustre-names-2026-10-25 --fields -size,-blocks
```

Skipped fields are written as null (`size` and `alloc_bytes` here), and on Linux they aren't requested from the kernel at all. `--fields` accepts:

| Value | Reads |
|---|---|
| (not given) | Everything: `size,blocks,atime,mtime,ctime,btime` |
| `-size,-blocks` | Everything except size and blocks |
| `mtime,ctime` | Only these optional fields |
| `times` | `atime,mtime,ctime,btime` |

Names, types, owners, permissions, inode numbers, and link counts are always read.

### 4.4 Several roots, bind mounts, and loops

Scan several trees into one inventory by listing them all:

```bash
fs-inventory scan /data/projects /data/archive /home -o /scratch/inventory/all-2026-10-25
```

Each root gets `depth` 0 and `parent_id` 0. Without symlink following (fs-inventory never follows symlinks), a directory can be reached twice only through a bind mount, a hard-linked directory (some old file systems allow them), or roots that overlap (`/data` and `/data/projects`). If any of those are possible, add `--detect-cycles`:

```bash
fs-inventory scan /data /data/projects -o /scratch/inventory/data-2026-10-25 --detect-cycles
```

It remembers every directory entered, by device and inode, and never enters one twice. A directory reached again gets a row with `error` set to `revisit: already visited through another path`, and the manifest counts revisits. It costs about 50 bytes of memory per directory.

### 4.5 Excluding paths

```bash
fs-inventory scan /data/projects -o /scratch/inventory/projects-2026-10-25 \
    --exclude node_modules --exclude '*.tmp' --exclude '/data/projects/*/scratch'
```

- **A glob without `/`** matches a name at any depth: `node_modules`, `*.tmp`, `.git`.
- **A glob with `/`** matches whole paths: `/data/projects/*/scratch`.
- **`--exclude-from FILE`** reads globs from a file, one per line. Both options can be repeated.
- **Quote globs** so the shell doesn't expand them.

Excluded entries get no row, aren't counted in their parent directory's totals, and excluded directories aren't entered. In the demo tree, `--exclude node_modules` dropped the scan from 285 entries to 196.

Two kinds of directory are skipped without any option:

- **Snapshot directories** (`.snapshot`, `~snapshot`, `.snapshots`, `.zfs`), which would otherwise multiply the inventory by the number of snapshots. Add `--include-snapshots` to scan them.
- **The scan's own output directory.**

### 4.6 Depth limits and mount points

`--max-depth N` lists entries down to N levels below each root. Directories at depth N get a row but aren't read, so their `dir_*` totals are null:

```bash
fs-inventory scan /data/projects -o - -f csv --columns path,kind,depth,dir_entries --max-depth 1
```

```
path,kind,depth,dir_entries
/data/projects/genomics,dir,1,
/data/projects/imaging,dir,1,
/data/projects/webapp,dir,1,
/data/projects/shared,dir,1,
/data/projects/restricted,dir,1,
/data/projects,dir,0,5
```

**Mount points** are directories on a different file system from their parent. They're always recorded, with `is_mount_point` true, but not entered unless you add `--cross-mounts`. fs-inventory never triggers automounts.

### 4.7 Scheduled scans for a database or web application

For regular scans that feed a database or an application, run scan, verify, and rollup from a script, and load only scans that verify. This one runs nightly from cron:

```bash
#!/bin/bash
# /usr/local/bin/inventory-projects: nightly inventory of /data/projects.
set -u
root=/data/projects
dir=/scratch/inventory/projects-$(date +%F)

fs-inventory scan "$root" -o "$dir" --threads 64 --stall-timeout 30m \
    --progress json --progress-interval 5m 2>> "$dir.log"
status=$?
# 0: complete; 1: complete, but some entries couldn't be read (see errors/).
if [ "$status" -gt 1 ]; then
    echo "scan of $root failed with exit code $status; see $dir.log" >&2
    exit "$status"
fi
fs-inventory verify "$dir" || exit 1
fs-inventory rollup "$dir" || exit 1
echo "$dir is ready to load"
```

If a scan is interrupted, such as by a reboot (exit code 3, or a manifest left at status `running`), `fs-inventory resume "$dir"` finishes it.

```
# m h dom mon dow  command
30 1 * * *  /usr/local/bin/inventory-projects
```

When loading scans into another system:

- **Load only scans whose `status` is `complete`.** Accept `incomplete` too only if partial data is useful to you; the `stalled` list in the manifest says which directories are missing.
- **Record the `scan_id`** (a UUIDv7, unique per scan) to avoid loading a scan twice. Every Parquet part carries the same ID in its metadata, so a part can't be mixed up with another scan's.
- **Check `schema_version`.** It's 2 in this release, and changes only when the output format changes in a way that would break readers. `tool_version` records the fs-inventory version.
- **Keep old scans side by side** to see change over time: growth, deleted files, and new files ([7.5](#75-recipes)).

### 4.8 Streaming to another program

With `-o -`, CSV or JSON Lines rows go to stdout, and error rows go to stderr as text lines:

```bash
fs-inventory scan /data/projects -o - -f jsonl | gzip > projects.jsonl.gz
```

```bash
fs-inventory scan /data/projects/shared -o - -f csv --columns path,kind,size,mtime,uid,name_flags,path_b64 | head -5
```

```
path,path_b64,name_flags,kind,uid,size,mtime
/data/projects/shared/Résumé.pdf,,0,file,1004,1024,2023-06-01T12:00:00.000000Z
/data/projects/shared/数据.csv,,0,file,1004,1024,2023-06-01T12:00:00.000000Z
/data/projects/shared/naïve notes.txt,,0,file,1004,1024,2023-06-01T12:00:00.000000Z
/data/projects/shared/CON,,512,file,1004,1024,2023-06-01T12:00:00.000000Z
```

Errors on stderr look like this:

```
fs-inventory: open /data/projects/restricted: Permission denied (os error 13)
```

- **Columns come out in schema order,** whatever order `--columns` lists them in.
- **Parquet can't be streamed to stdout.** Use an output directory for Parquet.
- **If the reading program exits early** (such as `head`), the scan stops and exits with code 3.

### 4.9 Subtree totals and file-age heat maps

Directory rows count only their immediate children. To answer "how big is this project, including everything below it?", run `rollup` on the finished scan:

```bash
fs-inventory rollup /scratch/inventory/projects-2026-10-25
```

```
rollup: 36 directories, 149 age rows (depth <= 4); wrote rollup/ and rollup_ages/ in /scratch/inventory/projects-2026-10-25
note: 1 directory wasn't read completely, so some totals are lower bounds (see incomplete_dirs)
```

It writes two tables into the scan:

- **`rollup`:** one row per directory with totals over its whole subtree: entries, files, directories, bytes, allocated bytes, the oldest and newest modification times, and `incomplete_dirs`. That last one counts directories in the subtree that weren't fully read (unreadable, not entered, or past `--max-depth`); when it isn't 0, the totals are lower bounds.
- **`rollup_ages`:** for each directory down to `--age-depth` levels (default 4), file counts and bytes by age bucket, measured from the scan's start time, for each of `mtime`, `ctime`, and `atime`. This is the data behind a file-age heat map.

Choose different buckets with `--age-buckets`, which takes ascending ages:

```bash
fs-inventory rollup /scratch/inventory/projects-2026-10-25 --age-buckets 7d,30d,90d,1y,3y --age-depth 2
```

Running `rollup` again replaces both tables. It works on Parquet scans only. Queries are in [7.5](#75-recipes).

### 4.10 Finding names that will break a migration

Before moving data to Windows, SMB shares, object storage, or a new file system, find names that won't survive the move:

```sql
SELECT path, fs_name_flags(name_flags) AS problems, coalesce(path_raw, encode(path)) AS exact_bytes
FROM fs_entries('/scratch/inventory/projects-2026-10-25') WHERE name_flags <> 0 ORDER BY path;
```

```
┌──────────────────────────────────────────┬──────────────────────────┬──────────────────────────────────────────┐
│                   path                   │         problems         │               exact_bytes                │
│                 varchar                  │        varchar[]         │                   blob                   │
├──────────────────────────────────────────┼──────────────────────────┼──────────────────────────────────────────┤
│ /data/projects/shared/ leading space.txt │ [leading_trailing_space] │ /data/projects/shared/ leading space.txt │
│ /data/projects/shared/-rf                │ [leading_dash]           │ /data/projects/shared/-rf                │
│ /data/projects/shared/CON                │ [windows_reserved]       │ /data/projects/shared/CON                │
│ /data/projects/shared/caf\xE9.txt        │ [invalid_utf8]           │ /data/projects/shared/caf\xE9.txt        │
│ /data/projects/shared/café.txt           │ [not_nfc]                │ /data/projects/shared/cafe\xCC\x81.txt   │
│ /data/projects/shared/line\nbreak.txt    │ [c0_control]             │ /data/projects/shared/line\x0Abreak.txt  │
│ /data/projects/shared/notes.             │ [trailing_dot]           │ /data/projects/shared/notes.             │
└──────────────────────────────────────────┴──────────────────────────┴──────────────────────────────────────────┘
```

`caf\xE9.txt` is a Latin-1 name from an old system; the byte `0xE9` isn't valid UTF-8. `café.txt` *looks* normal but is spelled with a separate combining accent, as macOS writes it, so it won't match the same name typed on Linux or Windows. Every flag is listed in [schema.md](schema.md#name-flags). See [7.2](#72-the-macros) for loading `fs_entries` and `fs_name_flags`.

### 4.11 Text output for spreadsheets and other tools

Parquet is the default and the best choice for DuckDB and data tools. For tools that only read text, write CSV or JSON Lines instead:

```bash
fs-inventory scan /data/projects -o /scratch/inventory/projects-csv -f csv --compression zstd
```

```
/scratch/inventory/projects-csv/
  manifest.json
  entries/part-w00-000000.csv.zst
  errors/part-w00-000000.csv.zst
```

- **Text compression:** CSV and JSON Lines can be compressed with `zstd` or `gzip`, or left uncompressed (the default).
- **Timestamps:** written as ISO-8601 UTC text by default; `--time-format epoch` writes seconds since 1970 instead.
- **Fewer columns:** `--columns` keeps only the columns you list, in any format.
- **Loading into DuckDB:** the `fs_entries_csv` and `fs_entries_jsonl` macros read these scans with the right column types ([7.2](#72-the-macros)).

Text output is several times larger than Parquet and slower to query. Parquet with zstd takes about 25 bytes per entry; a billion-entry scan is about 25 GB.

### 4.12 Interrupted, crashed, and stalled scans

**Every scan into a directory takes checkpoints,** every 10 minutes by default (`--checkpoint-interval`). A checkpoint pauses the crawl for a moment, makes every row written so far durable, and records the directories still to read, in `checkpoint/` inside the scan directory. That directory is removed when the scan finishes.

**Ctrl-C (SIGINT) or SIGTERM** stops a scan gracefully. The threads finish the directories they're reading, the part files are completed, a final checkpoint is written, and the manifest gets status `interrupted`. The exit code is 3. A second signal exits at once, with code 130, leaving the manifest at status `running` and the last periodic checkpoint in place.

**To continue a scan** that was interrupted or killed, ran out of memory, or was on a machine that went down, run `resume` on its directory:

```bash
fs-inventory resume /scratch/inventory/projects-2026-10-25
```

For the demo scan, stopped partway with SIGTERM:

```
fs-inventory: resuming scan 01a175ef-2926-79c2-a1b3-79c9be43dd89 from its checkpoint of 2026-10-26T01:59:32.285064Z: 156 entries written, 4 directories left
fs-inventory: complete: 285 entries in 0:00:00 (14.5k/s), 35 dirs, 44.02 GiB, 1 error, 0 vanished; output: /scratch/inventory/projects-2026-10-25
```

- **After Ctrl-C,** the final checkpoint is exact, so nothing is read twice.
- **After a crash or `kill -9`,** the scan continues from the last checkpoint. Part files written after it are deleted and the directories they covered are read again, so at most one checkpoint interval of work is redone, and no row appears twice.
- **The finished scan is the same as an uninterrupted one:** the same rows, checked the same way by `verify`. The manifest's `resumes` list records each resume.
- **Resume as often as needed.** A resumed scan takes checkpoints too.
- **Resume uses the original settings** for everything that shapes the output. It can change how the scan runs: `--threads`, `--writers`, `--max-memory`, `--stall-timeout`, `--checkpoint-interval`, and the progress options ([5.6](#56-resume)).
- **Resume on the same host,** without remounting the scanned file systems in between. Device numbers (`dev`) can change when a file system is remounted, which would make rows before and after the resume disagree, and `--detect-cycles` relies on them.

Without checkpoints (`--checkpoint-interval 0`, or a scan streamed with `-o -`), an interrupted scan's part files are valid but it's missing every directory not yet read, and it can only be run again from the start.

**A hung file system call,** such as a stat on a hard-mounted NFS export whose server is down, blocks its thread indefinitely. fs-inventory watches every crawl thread:

- After 60 seconds without progress, the progress line reports the stalled thread and its directory. An illustration:

  ```
  fs-inventory: 0:12:00  48.2M entries (61.3k/s)  2.10M dirs  1.20 PiB  12 errors  840 queued  1 stalled (longest 0:03:10: /nfs/projects/lab7/run42)
  ```

- With `--stall-timeout`, once a thread has made no progress for that long, the scan gives up on its directory. That directory gets a row with `error` set to `stall: ...` and an `errors` row; the other threads finish the rest of the tree. The scan ends with status `incomplete` and exit code 3, and the manifest's `stalled` list names each directory given up on.
- Without `--stall-timeout`, the scan waits for the call to return, however long that takes.
- While a thread is stalled, checkpoints can't complete: after waiting 30 seconds, the scan skips the checkpoint and says so on stderr. Once `--stall-timeout` gives up on the stuck directory, checkpoints continue.
- `resume` doesn't retry directories given up on after `--stall-timeout`.

The rows of every directory the stuck thread finished before it got stuck are still written. One limitation: if the thread was partway through a directory with tens of thousands of entries, some of that directory's rows may already have been written before it got stuck.

### 4.13 Permissions and side effects

- **Run as a user who can read the whole tree** (often root) for a complete inventory. Directories the user can't read are recorded with `error` set to `open: EACCES` and an `errors` row, and everything else is still scanned.
- **Read-only:** fs-inventory never opens files, only directories, so file access times don't change. On Linux it opens directories with `O_NOATIME` when allowed (the owner, or root), so directory access times don't change either.
- **No symlinks are followed.** A symlink gets its own row; its `size` is the length of the target path.
- **No automounts are triggered,** and other file systems aren't entered without `--cross-mounts`.

---

## 5. Command reference

```
fs-inventory scan     ROOT... -o DIR|-   Crawl directory trees and write an inventory
fs-inventory verify   SCAN_DIR           Check that a scan is complete and consistent
fs-inventory rollup   SCAN_DIR           Add subtree totals and age histograms to a scan
fs-inventory schema                      Print the inventory schema
fs-inventory probe    PATH               Time crawls at several thread counts
fs-inventory resume   SCAN_DIR           Continue an interrupted scan from its checkpoint
```

`fs-inventory COMMAND --help` lists every option. Sizes accept `K`, `M`, `G`, `T` (or `KiB`, `MiB`, `GiB`, `TiB`) for powers of 1024 and `KB`, `MB`, `GB`, `TB` for powers of 1000, such as `512MiB`. Durations accept forms such as `30s`, `10m`, `1h 30m`, `7d`, and `1y`.

### 5.1 scan

```
fs-inventory scan [OPTIONS] --output <DIR|-> <ROOT>...
```

Roots are made absolute (relative paths are taken from the current directory) and otherwise kept as given. Each root that's a directory is crawled, including a root that's a symlink to a directory; a root that's a file gets one row. Below the roots, symlinks are never followed.

**Output**

| Option | Default | Effect |
|---|---|---|
| `-o, --output DIR\|-` | (required) | New output directory, or `-` to stream CSV or JSON Lines to stdout |
| `-f, --format` | `parquet` | `parquet`, `csv`, or `jsonl` |
| `--compression CODEC` | `zstd:3` for Parquet, `none` for text | `zstd`, `zstd:LEVEL` (1–22), `gzip`, `gzip:LEVEL` (1–9), `snappy`, `lz4`, or `none`. Text formats take only `zstd`, `gzip`, or `none`. |
| `--columns LIST` | all | Write only these entry columns, such as `path,size,mtime,uid` |
| `--part-max-bytes SIZE` | `1GiB` | Start a new part file after this many bytes |
| `--part-max-rows N` | none | Start a new part file after this many rows |
| `--row-group-rows N` | `1048576` | Rows per Parquet row group (at most; groups also end at 128 MiB) |

**Traversal**

| Option | Default | Effect |
|---|---|---|
| `-t, --threads N` | `32` | Crawl threads (1–1024). Network file systems often do best with 64–256. |
| `--writers N` | CPU cores, at most 8 | Most threads that encode and write output. One starts; more join only when output backs up. |
| `--cross-mounts` | off | Enter other mounted file systems. Mount points are always recorded. |
| `--max-depth N` | none | List entries at most N levels below each root; directories at depth N aren't read |
| `--exclude GLOB` | none | Skip matching paths (repeatable); see [4.5](#45-excluding-paths) |
| `--exclude-from FILE` | none | Read exclude globs from a file, one per line (repeatable) |
| `--include-snapshots` | off | Enter `.snapshot`, `~snapshot`, `.snapshots`, and `.zfs` directories |
| `--detect-cycles` | off | Never enter a directory twice; see [4.4](#44-several-roots-bind-mounts-and-loops) |

**Metadata**

| Option | Default | Effect |
|---|---|---|
| `--fields LIST` | all | Optional metadata to read; see [4.3](#43-fast-namespace-only-scans-on-lustre) |
| `--statx-dont-sync` | off | Linux: accept cached attributes on network file systems (faster, possibly stale) |

**Encoding**

| Option | Default | Effect |
|---|---|---|
| `--raw-paths auto\|always` | `auto` | Fill `path_raw` only when `path` differs from the exact bytes, or for every row |
| `--csv-raw-controls` | off | In CSV, write control characters in names as they are (inside quotes) instead of as `\n`, `\t`, `\xNN` |
| `--time-unit us\|ns` | `us` | Timestamp precision. Microseconds cover any date; nanoseconds cover 1677–2262, and dates outside that range are clamped and flagged in `time_flags`. |
| `--time-format iso\|epoch` | `iso` | CSV and JSON Lines: ISO-8601 UTC text, or seconds since 1970 with fractional seconds |

**Robustness**

| Option | Default | Effect |
|---|---|---|
| `--checkpoint-interval DURATION` | `10m` | Take a checkpoint this often, so the scan can be resumed (`0` disables checkpoints); see [4.12](#412-interrupted-crashed-and-stalled-scans). Scans streamed with `-o -` take none. |
| `--stall-timeout DURATION` | none | Give up on directories whose thread has made no progress for this long; see [4.12](#412-interrupted-crashed-and-stalled-scans) |
| `--max-memory SIZE` | queue up to 1 GiB | Soft memory budget; see [8.3](#83-memory) |

**Progress**

| Option | Default | Effect |
|---|---|---|
| `--progress auto\|plain\|json\|none` | `auto` | Progress on stderr. `auto` is `plain` when stderr is a terminal, and otherwise prints only the final summary. `none` prints nothing. |
| `--progress-interval DURATION` | `10s` | Time between progress lines |

### 5.2 verify

```bash
fs-inventory verify /scratch/inventory/projects-2026-10-25
```

Checks a scan before it's used or loaded anywhere:

- **Manifest:** it can be read; its schema version is the one this fs-inventory writes; its status is `complete`.
- **Part files:** every part the manifest lists exists with the recorded size and row count, and each Parquet part carries the scan's ID.
- **Totals:** row counts match the manifest's totals.
- **Stray files:** none are left over, such as `.tmp` files from a crash.
- **Rollup:** its tables are checked the same way, if present.

It prints a summary and `ok`, and exits with code 0. Otherwise it prints each problem to stderr and exits with code 1:

```
fs-inventory: problem: scan status is `interrupted`, not `complete`
```

### 5.3 rollup

```
fs-inventory rollup [--age-buckets LIST] [--age-depth N] SCAN_DIR
```

| Option | Default | Effect |
|---|---|---|
| `--age-buckets LIST` | `30d,90d,1y,2y,3y,5y,10y` | Age bucket edges, ascending, measured back from the scan's start time |
| `--age-depth N` | `4` | Compute age histograms for directories down to this depth (0 is the root). Each costs about 0.5 KB of memory. |

With the default buckets, the labels are `future` (newer than the scan's start), `<30d`, `30d-90d`, `90d-1y`, `1y-2y`, `2y-3y`, `3y-5y`, `5y-10y`, and `>=10y`. A year is 365.25 days. The manifest gains a `rollup` section recording the buckets, depth, and part files. See [4.9](#49-subtree-totals-and-file-age-heat-maps).

### 5.4 schema

```bash
fs-inventory schema                      # DuckDB CREATE TABLE statements
fs-inventory schema --format json        # a JSON description of every column
fs-inventory schema --format markdown    # the reference in docs/schema.md
```

### 5.5 probe

```
fs-inventory probe [--threads LIST] [--duration DURATION] [--drop-caches] PATH
```

Crawls `PATH` at each thread count for up to `--duration` (default 30 s each), writes nothing, and suggests the smallest thread count within 90% of the best rate. More threads past that point mostly add load on the file server.

```bash
fs-inventory probe /data/projects --threads 1,2,4 --duration 2s
```

```
Probing /data/projects for up to 0:00:02 per thread count.
Without --drop-caches (Linux, root), later runs read metadata cached by earlier ones.
 threads        entries       time      entries/s
       1            285    0:00:00         211.2k  (read the whole tree)
       2            285    0:00:00         332.7k  (read the whole tree)
       4            285    0:00:00         183.4k  (read the whole tree)
Suggested: --threads 2
```

- **Caching skews later runs.** Each run after the first finds metadata the earlier runs left in the kernel's cache, so it looks faster than a real scan would be. On Linux, run as root with `--drop-caches` to clear the caches before every run.
- **Small trees give misleading results.** When a run reads the whole tree before its time is up, it says so; probe a tree large enough to keep every run busy for the full duration.

### 5.6 resume

```
fs-inventory resume [OPTIONS] SCAN_DIR
```

Continues a scan from its last checkpoint; see [4.12](#412-interrupted-crashed-and-stalled-scans). Everything that shapes the output comes from the original scan. These options change how it runs; each defaults to how the scan ran:

| Option | Effect |
|---|---|
| `-t, --threads N` | Crawl threads |
| `--writers N` | Most encoder and writer threads |
| `--checkpoint-interval DURATION` | Interval between checkpoints (`0` disables them) |
| `--stall-timeout DURATION` | Give up on stalled directories after this long (`0` waits forever) |
| `--max-memory SIZE` | Soft memory budget |
| `--progress`, `--progress-interval` | As for `scan` (these don't carry over) |

`resume` exits like `scan` ([exit codes](#exit-codes)). A scan that's already complete, or finished except for stalled directories, has nothing to resume: `resume` says so and exits with code 0. It exits with code 2 if the scan has no checkpoint (it was run with `--checkpoint-interval 0`), or if another fs-inventory process is still writing the scan.

---

## 6. Output reference

### Part files

- **Naming:** each table directory holds parts named `part-wNN-NNNNNN.EXT`, where `wNN` is the writer thread and the second number counts that writer's parts. Parts written after a resume are `part-rN-wNN-NNNNNN.EXT`, where `rN` counts the resumes. Rollup parts are `part-NNNNNN.parquet`.
- **Extensions:** `.parquet`; `.csv` or `.jsonl`, plus `.zst` or `.gz` when compressed.
- **Rolling:** a part is closed and a new one started at `--part-max-bytes` (1 GiB by default) or `--part-max-rows`.
- **Atomic commits:** a part is written as `NAME.tmp`, flushed to disk, and only then renamed. A part without `.tmp` is always complete, and after a crash only `.tmp` files are left to clean up.
- **One file per table for small scans:** writer threads start only as output backs up. Each checkpoint also commits the open parts, so a scan running for hours has more, smaller parts.

### The checkpoint directory

While a scan runs, and after it's interrupted, its directory also holds `checkpoint/`:

| File | Contents |
|---|---|
| `state` | The latest checkpoint: the scan's settings, every committed part, and the directories still to read |
| `spill` | Directories waiting to be read that didn't fit in memory ([8.3](#83-memory)) |
| `visited` | With `--detect-cycles`, the directories entered so far |
| `lock` | Locked by the process writing the scan, so two can't write one scan at once |

It's removed when the scan finishes. A scan that still has it isn't finished; check the manifest's `status`.

### Formats

| | Parquet (default) | CSV | JSON Lines |
|---|---|---|---|
| Best for | DuckDB, Spark, pandas, and other data tools | Spreadsheets and tools that read only CSV | Log pipelines and document stores |
| Size | About 25 bytes per entry | Several times larger | Larger still |
| Types | Stored in the file | Text; use the `_csv` macros | Text; use the `_jsonl` macros |
| Exact name bytes | `path_raw` (binary) | `path_b64` (base64 text) | `path_b64` (base64 text) |
| Timestamps | `TIMESTAMP WITH TIME ZONE`, UTC | ISO-8601 UTC text or epoch seconds | ISO-8601 UTC text or epoch seconds |
| Compression | zstd level 3; any codec | none, zstd, or gzip | none, zstd, or gzip |
| Stream to stdout | No | Yes | Yes |

Parquet files use zstd compression, row groups of up to 1,048,576 rows or 128 MiB, and page statistics, so DuckDB can skip row groups when filtering on `mtime`, `size`, or `uid`. Each file's metadata records the scan ID, schema version, tool version, host, and roots.

### Names and exact bytes

`path`, `parent_path`, and `name` are always valid UTF-8. How a name is written depends on the format:

| Name bytes | Parquet | JSON Lines | CSV |
|---|---|---|---|
| Valid UTF-8 | As is | As is | As is |
| Control characters (newline, tab, ...) | As is | JSON escapes (`\n`) | `\n`, `\t`, `\r`, or `\xNN`; as is with `--csv-raw-controls` |
| Not valid UTF-8 | Each bad byte as `\xNN` | Each bad byte as `\xNN` | Each bad byte as `\xNN` |

Whenever the text differs from the exact bytes, those bytes are also stored: in `path_raw` in Parquet, and base64-encoded in `path_b64` in CSV and JSON Lines. In every format, `coalesce(path_raw, encode(path))` is the exact path (the text macros decode `path_b64` into `path_raw` for you). With `--raw-paths always`, every row gets the exact bytes. For example, a name with a newline in CSV:

```
path,path_b64
/data/projects/shared/line\nbreak.txt,L2RhdGEvcHJvamVjdHMvc2hhcmVkL2xpbmUKYnJlYWsudHh0
```

and in JSON Lines, where JSON's own escape keeps it exact:

```
{"path":"/data/projects/shared/line\nbreak.txt","path_b64":null,"name_flags":2}
```

`name_flags` is a bitmask of name problems: invalid UTF-8, control characters, names not in Unicode NFC form, leading or trailing spaces, a leading `-`, a trailing `.`, Windows-reserved names and characters, names over 255 bytes, and paths of 4,096 bytes or more. [schema.md](schema.md#name-flags) lists the bits, and the `fs_name_flags()` macro decodes them.

### Timestamps

- **Stored in UTC,** with microsecond precision by default (`--time-unit ns` for nanoseconds).
- **`ctime` is not creation time.** It's the last change to the contents *or* metadata, and users can't set it, so it's the reliable signal of recent activity.
- **`mtime` can mislead.** Copies, restores, `tar`, and `rsync -t` preserve it, so restored data can look years old.
- **`btime` is creation time,** when the platform and file system record it: on Linux with kernel 4.11 or later and a file system that stores it, and on macOS.
- **`time_flags`** marks timestamps that couldn't be represented and were clamped.

### Nulls

| Column | Null when |
|---|---|
| `size`, `alloc_bytes`, `atime`, `mtime`, `ctime`, `btime` | Skipped with `--fields` |
| `btime` | Not available (see above) |
| `dir_id` | The entry isn't a directory |
| `dir_entries` and the other `dir_*` totals | The entry isn't a directory, or the directory wasn't read (unreadable, past `--max-depth`, an unentered mount point) |
| `extension` | No extension, or a directory |
| `path_raw` / `path_b64` | `path` is already exact |
| `rdev` | Not a device |
| `error` | Nothing went wrong |

### Errors and vanished entries

The `errors` table has one row per failed operation: `open`, `readdir`, `stat`, `verify` (the directory was replaced during the scan), `stall` (given up on after `--stall-timeout`), or `internal` (a bug in fs-inventory, contained to one entry). Each row has the path, the OS error number and name (such as `EACCES`), a message, and the time. The table exists only if there were errors.

Entries deleted between being listed and being read are counted as *vanished* in the manifest and the progress output. They aren't errors: live file systems change during a scan.

### The manifest

`manifest.json` describes the scan. From the demo scan, with lists shortened:

```json
{
  "columns": ["path", "parent_path", "name", "...", "error"],
  "compression": "zstd:3",
  "duration_seconds": 0.008556169,
  "error_columns": ["path", "path_raw", "op", "errno", "errno_name", "message", "time"],
  "errors_by_kind": [
    { "count": 1, "errno": 13, "errno_name": "EACCES", "op": "open" }
  ],
  "finished_at": "2026-10-26T00:11:32.707136Z",
  "format": "parquet",
  "host": "bd65838ce5c7",
  "options": { "threads": 32, "writers": 5, "exclude": [], "max_depth": null, "...": "..." },
  "parts": {
    "entries": [ { "bytes": 16233, "file": "entries/part-w00-000000.parquet", "rows": 285 } ],
    "errors": [ { "bytes": 2553, "file": "errors/part-w00-000000.parquet", "rows": 1 } ]
  },
  "rollup": { "age_buckets": ["future", "<30d", "...", ">=10y"], "age_depth": 4, "...": "..." },
  "roots": [ { "path": "/data/projects", "path_b64": null } ],
  "scan_id": "01a1758c-51da-709d-bafb-c8feaa458bf3",
  "schema_version": 2,
  "stalled": [],
  "started_at": "2026-10-26T00:11:32.698582Z",
  "status": "complete",
  "tool": "fs-inventory",
  "tool_version": "0.4.0",
  "totals": {
    "by_kind": { "dir": 36, "file": 248, "symlink": 1 },
    "bytes": 47262948364,
    "dirs_read": 35,
    "entries": 285,
    "errors": 1,
    "revisits": 0,
    "spilled": 0,
    "vanished": 0
  }
}
```

| Field | Meaning |
|---|---|
| `scan_id` | UUIDv7, unique per scan; also in every Parquet part's metadata |
| `status` | See below |
| `schema_version`, `tool_version` | Output format version, and the fs-inventory version that wrote it |
| `roots` | The scanned roots (`path_b64` holds exact bytes when needed) |
| `started_at`, `finished_at`, `duration_seconds` | When the scan ran, in UTC |
| `options` | The options the scan ran with |
| `columns`, `error_columns` | The columns written |
| `parts` | Every committed part file, with its size and row count |
| `totals` | Entries by kind; bytes (the sum of `size` over everything but directories); directories read; errors; vanished entries; revisits (`--detect-cycles`); directories that waited in the spill file |
| `errors_by_kind` | Error counts by operation and error number |
| `stalled` | Directories given up on with `--stall-timeout` |
| `rollup` | Added by `rollup`: buckets, depth, reference time, and its part files |
| `resumes` | Each resume: when it started, when the checkpoint it continued from was taken, and the fs-inventory version. Absent if the scan was never resumed. |

| Status | Meaning | Exit code |
|---|---|---|
| `running` | The scan is in progress, or the process was killed before it could finish; `resume` continues it from the last checkpoint | — |
| `complete` | Every directory was read (some may have failed; see `errors`) | 0 or 1 |
| `interrupted` | Stopped by a signal or a closed stdout; directories not yet read are missing, and `resume` continues it | 3 |
| `incomplete` | Finished, but gave up on stalled directories (`--stall-timeout`); `resume` doesn't retry them | 3 |
| `failed` | Output couldn't be written, such as a full disk | 4 |

### Progress output

With `--progress plain`, a line every `--progress-interval`:

```
fs-inventory: 0:00:02  284.2k entries (140.0k/s)  151 dirs  0 B  0 errors  40 queued
```

That is: elapsed time, entries so far (and the rate since the last line), directories read, bytes seen (the sum of `size` over everything but directories), errors, and directories waiting to be read. When threads are stalled, the line ends with how many and the longest-stalled directory.

With `--progress json`, one object per line, with the same values:

```json
{"bytes":0,"dirs":202,"elapsed_seconds":2.001,"entries":382202,"entries_per_second":188013.0,"errors":0,"event":"progress","queued":29,"stalled":[],"vanished":0}
```

Every scan ends with a summary, unless `--progress none`:

```
fs-inventory: complete: 285 entries in 0:00:00 (33.3k/s), 35 dirs, 44.02 GiB, 1 error, 0 vanished; output: /scratch/inventory/projects-2026-10-25
```

With `--progress json`, the summary is an object with `"event": "done"`, the status, the totals, and the output location.

### Exit codes

| Code | `scan` and `resume` | `verify` | Other commands |
|---|---|---|---|
| 0 | Complete with no errors (or, for `resume`, nothing to resume) | The scan is good | Success |
| 1 | Complete, but some entries couldn't be read (see `errors`) | Problems found | — |
| 2 | Invalid arguments or configuration | Invalid arguments | Invalid arguments |
| 3 | Interrupted, or gave up on stalled directories | — | — |
| 4 | Fatal: output couldn't be written | — | Failed (such as `rollup` on an unreadable scan) |
| 130 | A second Ctrl-C or SIGTERM | — | — |

---

## 7. Querying scans with DuckDB

[DuckDB](https://duckdb.org) is an analytical database that runs as a single program and queries Parquet, CSV, and JSON files in place. It's the quickest way to answer questions about a scan, and it handles billions of rows on a laptop. The examples use DuckDB 2.0.

### 7.1 Reading a scan directly

No setup is needed for Parquet scans:

```sql
SELECT count(*) FROM read_parquet('/scratch/inventory/projects-2026-10-25/entries/*.parquet');
```

### 7.2 The macros

[`sql/fs_inventory.sql`](../sql/fs_inventory.sql) defines table macros, which are named queries that take the scan directory as an argument. They save typing the glob, and for text scans they apply the right column types.

| Macro | Reads |
|---|---|
| `fs_entries(scan_dir)` | The `entries` table of a Parquet scan |
| `fs_errors(scan_dir)` | The `errors` table of a Parquet scan. It fails with "No files found" if the scan had no errors. |
| `fs_rollup(scan_dir)` | Subtree totals, after `fs-inventory rollup` |
| `fs_rollup_ages(scan_dir)` | Age histograms, after `fs-inventory rollup` |
| `fs_entries_csv(scan_dir)`, `fs_errors_csv(scan_dir)` | CSV scans, compressed or not |
| `fs_entries_jsonl(scan_dir)`, `fs_errors_jsonl(scan_dir)` | JSON Lines scans, compressed or not |
| `fs_name_flags(flags)` | Not a table: turns a `name_flags` value into a list of flag names |

The CSV and JSON Lines macros give every column's type explicitly, instead of letting DuckDB guess from a sample of rows. A guess can go wrong; a column of numeric-looking file names would be read as numbers, for example. They also add `path_raw`, decoded from `path_b64`, so queries work the same in every format. They need every column, so for text scans written with `--columns`, use `read_csv` or `read_json` with your own column list.

Load the macros in any of three ways:

**In the DuckDB shell**, for interactive work:

```
$ duckdb
D .read sql/fs_inventory.sql
D SELECT count(*) FROM fs_entries('/scratch/inventory/projects-2026-10-25');
```

**For one command**, with `-cmd`:

```bash
duckdb -cmd ".read sql/fs_inventory.sql" -c "SELECT count(*) FROM fs_entries('/scratch/inventory/projects-2026-10-25')"
```

**Once, into a database file.** Macros are stored in the database, so every later session has them:

```bash
duckdb inventory.duckdb < sql/fs_inventory.sql
```

```bash
duckdb inventory.duckdb -c "SELECT count(*) FROM fs_entries('/scratch/inventory/projects-2026-10-25')"
```

### 7.3 Settings and pitfalls

**Use UTC.** Timestamps are stored in UTC, but DuckDB shows them, and groups them with `year()` or `date_trunc()`, in the session's time zone. A file modified at 2009-02-01 12:00 UTC shows as 04:00 in Los Angeles. Near midnight on New Year's Day, that can move a file into a different year. Set UTC at the start of a session:

```sql
SET TimeZone = 'UTC';
```

**Cast sums before `format_bytes`.** `sum()` of a `BIGINT` column returns a 128-bit `HUGEINT` in DuckDB, which `format_bytes()` doesn't accept:

```
Binder Error: No function matches the given name and argument types 'format_bytes(HUGEINT)'
```

Cast the sum: `format_bytes(sum(size)::BIGINT)`. A single column such as `size` works without a cast.

**Sort on numbers, not formatted text.** In `SELECT format_bytes(size) AS size ... ORDER BY size`, the `ORDER BY` uses the formatted text, so `8.0 KiB` sorts above `8.0 GiB`. Give the table an alias and sort on its column, as the recipes below do: `FROM fs_entries(...) e ... ORDER BY e.size DESC`.

**Name the scan once.** The recipes keep the scan directory in a variable:

```sql
.read sql/fs_inventory.sql
SET TimeZone = 'UTC';
SET VARIABLE scan = '/scratch/inventory/projects-2026-10-25';
```

### 7.4 What's in a row

Every column is described in the [schema reference](schema.md). The ones most queries use:

| Column | Use |
|---|---|
| `path`, `parent_path`, `name`, `extension` | Finding things by location or name |
| `kind` | `file`, `dir`, `symlink`, `fifo`, `socket`, `char`, `block`, or `unknown` |
| `size`, `alloc_bytes` | Logical size, and space actually allocated on disk |
| `uid`, `gid`, `mode` | Owner, group, and permissions (`mode & 4095` is the permission bits) |
| `mtime`, `ctime`, `atime`, `btime` | Timestamps (see [Timestamps](#timestamps)) |
| `depth`, `dir_id`, `parent_id` | Tree structure |
| `dev`, `ino`, `nlink` | Identity; equal `(dev, ino)` means hard links to one file |
| `dir_entries`, `dir_files`, `dir_bytes`, ... | A directory's immediate contents |
| `error` | Why a directory wasn't fully read |

### 7.5 Recipes

Every recipe assumes the setup from [7.3](#73-settings-and-pitfalls). The outputs are from the demo tree used throughout this guide.

**Totals by kind**

```sql
SELECT kind, count(*) AS entries,
       format_bytes(sum(size)::BIGINT) AS size, format_bytes(sum(alloc_bytes)::BIGINT) AS on_disk
FROM fs_entries(getvariable('scan')) GROUP BY kind ORDER BY entries DESC;
```

```
┌─────────┬─────────┬──────────┬──────────┐
│  kind   │ entries │   size   │ on_disk  │
│ varchar │  int64  │ varchar  │ varchar  │
├─────────┼─────────┼──────────┼──────────┤
│ file    │     248 │ 44.0 GiB │ 17.7 MiB │
│ dir     │      36 │ 33.9 KiB │ 32.0 KiB │
│ symlink │       1 │ 12 bytes │ 0 bytes  │
└─────────┴─────────┴──────────┴──────────┘
```

`size` is what programs see, and `on_disk` is the space allocated. Here they differ hugely because the demo tree's large files are sparse. On real data, compression, deduplication, and tiering to tape or object storage also make `on_disk` smaller than `size`.

**Largest files**

```sql
SELECT path, format_bytes(size) AS size, mtime
FROM fs_entries(getvariable('scan')) e WHERE kind = 'file' ORDER BY e.size DESC LIMIT 5;
```

```
┌────────────────────────────────────────────────┬─────────┬──────────────────────────┐
│                      path                      │  size   │          mtime           │
│                    varchar                     │ varchar │ timestamp with time zone │
├────────────────────────────────────────────────┼─────────┼──────────────────────────┤
│ /data/projects/imaging/processed/archive.tar   │ 8.0 GiB │ 2021-11-02 08:00:00+00   │
│ /data/projects/genomics/runs/2017/run2/big.bam │ 3.0 GiB │ 2017-02-20 10:00:00+00   │
│ /data/projects/genomics/runs/2017/run1/big.bam │ 3.0 GiB │ 2017-01-20 10:00:00+00   │
│ /data/projects/genomics/runs/2019/run3/big.bam │ 3.0 GiB │ 2019-03-20 10:00:00+00   │
│ /data/projects/genomics/runs/2019/run2/big.bam │ 3.0 GiB │ 2019-02-20 10:00:00+00   │
└────────────────────────────────────────────────┴─────────┴──────────────────────────┘
```

**Finding files** by extension, name pattern, and age:

```sql
SELECT path, format_bytes(size) AS size, mtime FROM fs_entries(getvariable('scan'))
WHERE kind = 'file' AND extension = 'bam' AND mtime < TIMESTAMPTZ '2020-01-01' ORDER BY path;

SELECT path FROM fs_entries(getvariable('scan'))
WHERE regexp_matches(name, '^scan[0-9]+\.tif$') AND mtime < now() - INTERVAL 9 YEAR ORDER BY path;
```

`extension` is lowercase, without the dot, so `extension = 'bam'` also finds `.BAM` files.

**Age profile:** files and bytes by year of last modification.

```sql
SELECT year(mtime) AS year, count(*) AS files, format_bytes(sum(size)::BIGINT) AS size
FROM fs_entries(getvariable('scan')) WHERE kind = 'file' GROUP BY year ORDER BY year;
```

```
┌───────┬───────┬───────────┐
│ year  │ files │   size    │
│ int64 │ int64 │  varchar  │
├───────┼───────┼───────────┤
│  2009 │     1 │ 1.0 KiB   │
│  2015 │     1 │ 1.0 KiB   │
│  2016 │    13 │ 1.9 MiB   │
│  2017 │    29 │ 9.0 GiB   │
│  2018 │    13 │ 1.9 MiB   │
│  2019 │    15 │ 9.0 GiB   │
│  2021 │     1 │ 8.0 GiB   │
│  2022 │    15 │ 9.0 GiB   │
│  2023 │     9 │ 9.0 KiB   │
│  2024 │    80 │ 160.0 KiB │
│  2025 │    15 │ 9.0 GiB   │
│  2026 │    56 │ 2.0 MiB   │
└───────┴───────┴───────────┘
```

**Usage by owner, with user names.** Scans record numeric user IDs. To show names, save the system's user list on a machine that sees the same accounts, and join it:

```bash
getent passwd > /scratch/inventory/passwd.txt
```

```sql
SELECT coalesce(u.user, e.uid::VARCHAR) AS owner, count(*) AS files,
       format_bytes(sum(e.size)::BIGINT) AS size, format_bytes(sum(e.alloc_bytes)::BIGINT) AS on_disk
FROM fs_entries(getvariable('scan')) e
LEFT JOIN read_csv('/scratch/inventory/passwd.txt', delim = ':', header = false, quote = '',
    columns = {'user': 'VARCHAR', 'password': 'VARCHAR', 'uid': 'BIGINT', 'gid': 'BIGINT',
               'gecos': 'VARCHAR', 'home': 'VARCHAR', 'shell': 'VARCHAR'}) u USING (uid)
WHERE e.kind = 'file' GROUP BY ALL ORDER BY sum(e.size) DESC;
```

```
┌─────────┬───────┬───────────┬───────────┐
│  owner  │ files │   size    │  on_disk  │
│ varchar │ int64 │  varchar  │  varchar  │
├─────────┼───────┼───────────┼───────────┤
│ asmith  │    90 │ 36.0 GiB  │ 9.9 MiB   │
│ bjones  │    66 │ 8.0 GiB   │ 7.4 MiB   │
│ cwu     │    81 │ 168.0 KiB │ 328.0 KiB │
│ dlee    │    11 │ 11.0 KiB  │ 44.0 KiB  │
└─────────┴───────┴───────────┴───────────┘
```

Users whose IDs aren't in the list (deleted accounts, for example) show as their number. Groups work the same way with `getent group`.

**Who changed the most data recently.** Use `ctime`, because copies and restores preserve `mtime` but can't set `ctime`:

```sql
SELECT uid, count(*) AS files, format_bytes(sum(alloc_bytes)::BIGINT) AS on_disk
FROM fs_entries(getvariable('scan'))
WHERE kind = 'file' AND ctime > now() - INTERVAL 7 DAY
GROUP BY uid ORDER BY sum(alloc_bytes) DESC LIMIT 20;
```

**Top extensions by size**

```sql
SELECT extension, count(*) AS files, format_bytes(sum(size)::BIGINT) AS size
FROM fs_entries(getvariable('scan')) WHERE kind = 'file'
GROUP BY extension ORDER BY sum(size) DESC LIMIT 5;
```

```
┌───────────┬───────┬──────────┐
│ extension │ files │   size   │
│  varchar  │ int64 │ varchar  │
├───────────┼───────┼──────────┤
│ bam       │    12 │ 36.0 GiB │
│ tar       │     1 │ 8.0 GiB  │
│ gz        │    48 │ 9.3 MiB  │
│ tif       │    40 │ 5.8 MiB  │
│ png       │    25 │ 1.4 MiB  │
└───────────┴───────┴──────────┘
```

**Counting hard-linked files once.** A file with several hard links has a row for each link, all with the same `(dev, ino)`:

```sql
SELECT count(*) AS files, format_bytes(sum(alloc_bytes)::BIGINT) AS on_disk
FROM (SELECT DISTINCT ON (dev, ino) alloc_bytes FROM fs_entries(getvariable('scan')) WHERE kind = 'file');
```

**Sparse, compressed, or tiered files:** much less allocated than their size.

```sql
SELECT path, format_bytes(size) AS size, format_bytes(alloc_bytes) AS on_disk
FROM fs_entries(getvariable('scan')) e
WHERE kind = 'file' AND alloc_bytes < size / 10 ORDER BY e.size DESC LIMIT 5;
```

```
┌────────────────────────────────────────────────┬─────────┬─────────┐
│                      path                      │  size   │ on_disk │
│                    varchar                     │ varchar │ varchar │
├────────────────────────────────────────────────┼─────────┼─────────┤
│ /data/projects/imaging/processed/archive.tar   │ 8.0 GiB │ 0 bytes │
│ /data/projects/genomics/runs/2017/run2/big.bam │ 3.0 GiB │ 0 bytes │
│ /data/projects/genomics/runs/2017/run1/big.bam │ 3.0 GiB │ 0 bytes │
│ /data/projects/genomics/runs/2019/run3/big.bam │ 3.0 GiB │ 0 bytes │
│ /data/projects/genomics/runs/2019/run2/big.bam │ 3.0 GiB │ 0 bytes │
└────────────────────────────────────────────────┴─────────┴─────────┘
```

**Directories with the most files directly inside.** Very large directories slow down every tool that lists them:

```sql
SELECT path, dir_files, format_bytes(dir_bytes) AS size
FROM fs_entries(getvariable('scan')) WHERE kind = 'dir' ORDER BY dir_files DESC LIMIT 5;
```

**Directories that couldn't be read, and why**

```sql
SELECT path, error FROM fs_entries(getvariable('scan')) WHERE error IS NOT NULL;
SELECT path, op, errno_name, message FROM fs_errors(getvariable('scan'));
```

```
┌───────────────────────────┬──────────────┐
│           path            │    error     │
│          varchar          │   varchar    │
├───────────────────────────┼──────────────┤
│ /data/projects/restricted │ open: EACCES │
└───────────────────────────┴──────────────┘
┌───────────────────────────┬─────────┬────────────┬─────────────────────────────────┐
│           path            │   op    │ errno_name │             message             │
│          varchar          │ varchar │  varchar   │             varchar             │
├───────────────────────────┼─────────┼────────────┼─────────────────────────────────┤
│ /data/projects/restricted │ open    │ EACCES     │ Permission denied (os error 13) │
└───────────────────────────┴─────────┴────────────┴─────────────────────────────────┘
```

**Walking the tree.** Join a row's `parent_id` to its parent's `dir_id`:

```sql
SELECT c.path, p.path AS parent
FROM fs_entries(getvariable('scan')) c
JOIN fs_entries(getvariable('scan')) p ON c.parent_id = p.dir_id
WHERE c.name = 'app.js';
```

**Largest projects, counting everything below them** (after `rollup`):

```sql
SELECT path, files, format_bytes(bytes) AS size, newest_mtime, incomplete_dirs
FROM fs_rollup(getvariable('scan')) WHERE depth = 1 ORDER BY bytes DESC;
```

```
┌───────────────────────────┬───────┬───────────┬───────────────────────────────┬─────────────────┐
│           path            │ files │   size    │         newest_mtime          │ incomplete_dirs │
│          varchar          │ int64 │  varchar  │   timestamp with time zone    │      int64      │
├───────────────────────────┼───────┼───────────┼───────────────────────────────┼─────────────────┤
│ /data/projects/genomics   │    90 │ 36.0 GiB  │ 2026-10-09 12:00:00+00        │               0 │
│ /data/projects/imaging    │    66 │ 8.0 GiB   │ 2026-09-15 15:00:00+00        │               0 │
│ /data/projects/webapp     │    81 │ 168.0 KiB │ 2026-10-20 16:00:00+00        │               0 │
│ /data/projects/shared     │    12 │ 11.0 KiB  │ 2026-10-26 00:11:32.679417+00 │               0 │
│ /data/projects/restricted │     0 │ 0 bytes   │ NULL                          │               1 │
└───────────────────────────┴───────┴───────────┴───────────────────────────────┴─────────────────┘
```

`restricted` couldn't be read, so its `incomplete_dirs` is 1 and its totals (0 files) are lower bounds.

**Archive candidates:** large directories where nothing has been modified for three years.

```sql
SELECT path, files, format_bytes(bytes) AS size, newest_mtime, newest_ctime
FROM fs_rollup(getvariable('scan'))
WHERE newest_mtime < now() - INTERVAL 3 YEAR ORDER BY bytes DESC LIMIT 5;
```

```
┌────────────────────────────────────────┬───────┬─────────┬──────────────────────────┬───────────────────────────────┐
│                  path                  │ files │  size   │       newest_mtime       │         newest_ctime          │
│                varchar                 │ int64 │ varchar │ timestamp with time zone │   timestamp with time zone    │
├────────────────────────────────────────┼───────┼─────────┼──────────────────────────┼───────────────────────────────┤
│ /data/projects/genomics/runs/2017      │    15 │ 9.0 GiB │ 2017-03-20 10:00:00+00   │ 2026-10-26 00:11:32.046414+00 │
│ /data/projects/genomics/runs/2019      │    15 │ 9.0 GiB │ 2019-03-20 10:00:00+00   │ 2026-10-26 00:11:32.046414+00 │
│ /data/projects/genomics/runs/2022      │    15 │ 9.0 GiB │ 2022-03-20 10:00:00+00   │ 2026-10-26 00:11:32.046414+00 │
│ /data/projects/genomics/runs/2017/run2 │     5 │ 3.0 GiB │ 2017-02-20 10:00:00+00   │ 2026-10-26 00:11:32.046414+00 │
│ /data/projects/genomics/runs/2017/run1 │     5 │ 3.0 GiB │ 2017-01-20 10:00:00+00   │ 2026-10-26 00:11:32.046414+00 │
└────────────────────────────────────────┴───────┴─────────┴──────────────────────────┴───────────────────────────────┘
```

These runs were copied into place recently with their original modification times, so `newest_ctime` is recent while `newest_mtime` is years old. Filter on `newest_ctime` instead to find data that hasn't been touched in any way, including being copied or having its permissions changed.

**File-age heat map** (after `rollup`): bytes by age of last modification for each project. In long form, one row per project and age bucket, ready for a charting tool:

```sql
SELECT r.path, a.label, a.bucket, a.files, a.bytes
FROM fs_rollup_ages(getvariable('scan')) a
JOIN fs_rollup(getvariable('scan')) r USING (dir_id)
WHERE r.depth = 1 AND a.time = 'mtime'
ORDER BY r.path, a.bucket;
```

As a grid, with `PIVOT`. This one counts files; use `sum(bytes)` for bytes:

```sql
PIVOT (
    SELECT r.path, a.label, a.files
    FROM fs_rollup_ages(getvariable('scan')) a
    JOIN fs_rollup(getvariable('scan')) r USING (dir_id)
    WHERE r.depth = 1 AND a.time = 'mtime'
) ON label IN ('<30d', '30d-90d', '90d-1y', '1y-2y', '2y-3y', '3y-5y', '5y-10y', '>=10y')
USING sum(files) ORDER BY path;
```

```
┌─────────────────────────┬────────┬─────────┬────────┬────────┬────────┬────────┬────────┬────────┐
│          path           │  <30d  │ 30d-90d │ 90d-1y │ 1y-2y  │ 2y-3y  │ 3y-5y  │ 5y-10y │ >=10y  │
│         varchar         │ int128 │ int128  │ int128 │ int128 │ int128 │ int128 │ int128 │ int128 │
├─────────────────────────┼────────┼─────────┼────────┼────────┼────────┼────────┼────────┼────────┤
│ /data/projects/genomics │     30 │    NULL │   NULL │     15 │   NULL │     15 │     30 │   NULL │
│ /data/projects/imaging  │   NULL │       5 │     20 │   NULL │   NULL │      1 │     27 │     13 │
│ /data/projects/shared   │      1 │    NULL │   NULL │   NULL │   NULL │      9 │   NULL │      2 │
│ /data/projects/webapp   │      1 │    NULL │   NULL │   NULL │     80 │   NULL │   NULL │   NULL │
└─────────────────────────┴────────┴─────────┴────────┴────────┴────────┴────────┴────────┴────────┘
```

Listing the labels in `IN (...)` keeps the columns in age order. Only buckets with files have rows, so empty cells are `NULL`. `a.time` can also be `ctime` or `atime`.

**Comparing two scans:** files added, deleted, and changed between two scans of the same tree.

```sql
CREATE TEMP VIEW old AS SELECT * FROM fs_entries('/scratch/inventory/projects-2026-10-25') WHERE kind = 'file';
CREATE TEMP VIEW new AS SELECT * FROM fs_entries('/scratch/inventory/projects-2026-11-01') WHERE kind = 'file';

SELECT CASE WHEN old.path IS NULL THEN 'added' WHEN new.path IS NULL THEN 'deleted' ELSE 'changed' END AS change,
       count(*) AS files,
       format_bytes(sum(coalesce(new.size, 0) - coalesce(old.size, 0))::BIGINT) AS size_change
FROM old FULL JOIN new USING (path)
WHERE old.path IS NULL OR new.path IS NULL OR old.size <> new.size OR old.mtime <> new.mtime
GROUP BY ALL ORDER BY change;
```

```
┌─────────┬───────┬─────────────┐
│ change  │ files │ size_change │
│ varchar │ int64 │   varchar   │
├─────────┼───────┼─────────────┤
│ added   │     2 │ 5.0 GiB     │
│ changed │     1 │ 30.0 KiB    │
│ deleted │    15 │ -9.0 GiB    │
└─────────┴───────┴─────────────┘
```

Replace the `SELECT` with `SELECT path, old.size, new.size, old.mtime, new.mtime` to list the files themselves. Joining on `path` treats a renamed file as one deletion plus one addition.

**Several scans as one table.** A glob can cover many scans; `filename = true` adds each row's file name, which identifies its scan:

```sql
SELECT parse_path(filename)[-3] AS scan, count(*) AS entries, format_bytes(sum(size)::BIGINT) AS size
FROM read_parquet('/scratch/inventory/projects-2026-*/entries/*.parquet', filename = true)
GROUP BY scan ORDER BY scan;
```

```
┌─────────────────────┬─────────┬──────────┐
│        scan         │ entries │   size   │
│       varchar       │  int64  │ varchar  │
├─────────────────────┼─────────┼──────────┤
│ projects-2026-10-25 │     285 │ 44.0 GiB │
│ projects-2026-11-01 │     270 │ 40.0 GiB │
└─────────────────────┴─────────┴──────────┘
```

If scan directories are named `scan_date=2026-10-25` and so on, `read_parquet(..., hive_partitioning = true)` adds a `scan_date` column instead.

**Text-format scans** work the same way through their macros:

```sql
SELECT count(*) AS entries, count(path_raw) AS with_raw_bytes FROM fs_entries_csv('/scratch/inventory/projects-csv');
SELECT path, op, errno_name FROM fs_errors_csv('/scratch/inventory/projects-csv');
```

**Exporting results** for a spreadsheet or another tool:

```sql
COPY (SELECT path, size, mtime FROM fs_entries(getvariable('scan')) WHERE kind = 'file' AND size > 1e9)
TO 'big-files.csv' (HEADER);
```

**Saving a scan into a database.** Queries on Parquet files are fast, but for repeated work on one scan, copying it into a DuckDB database file is faster still:

```bash
duckdb inventory.duckdb
```

```sql
.read sql/fs_inventory.sql
CREATE TABLE projects_2026_10_25 AS SELECT * FROM fs_entries('/scratch/inventory/projects-2026-10-25');
```

More queries are in [`sql/examples.sql`](../sql/examples.sql).

---

## 8. Performance tuning

### 8.1 Threads

The crawl is limited by how fast the file system answers metadata requests, not by CPU. On a local disk with metadata cached, a few threads are enough. On network file systems each request waits on the network, so throughput grows with the number of requests in flight. With 0.5 ms per request, 64 threads can sustain about 128,000 entries per second until the server is the bottleneck. Use `probe` ([5.5](#55-probe)) to measure, and keep in mind that the file server is shared: past the suggested count, more threads add load for little gain.

A single huge directory doesn't hold the crawl back. After the first 10,000 entries, the thread reading it hands batches of names to idle threads, which read their metadata in parallel.

### 8.2 Fields and caching

- **`--fields`** reads less metadata per file. On Lustre, `--fields -size,-blocks` avoids a round trip per file to the storage servers.
- **`--statx-dont-sync`** lets a network file system client answer from cached attributes. It's faster, but may be slightly stale.

### 8.3 Memory

Memory is bounded by default:

- **Directories waiting to be read:** up to 1 GiB in memory. Beyond that, they wait in a spill file on disk, read back once memory drains. With checkpoints (the default), it's `checkpoint/spill` in the scan directory, removed when the scan finishes; otherwise it's created next to the output and deleted from the directory at once, so it never lingers. Either way, it uses disk space while the scan runs. The manifest's `totals.spilled` counts directories that waited there.
- **Output buffers:** 64 MiB shared by all crawl threads, plus up to 128 MiB per Parquet row group being built.

`--max-memory SIZE` sets one soft budget for all of it: half for waiting directories, an eighth for the crawl threads' buffers (at most 64 MiB), and a quarter for Parquet row groups. `--detect-cycles` adds about 50 bytes per directory, and `rollup --age-depth` about 0.5 KB per directory within that depth.

### 8.4 Checkpoints

Each checkpoint pauses the crawl while every thread finishes the directory it's reading (helping with very large directories meanwhile), and every open part file is committed and synced to disk. That's typically under a second, and longer while a very large directory is finished. At the default interval of 10 minutes, the cost is well under 1%; much shorter intervals slow a scan noticeably. Each checkpoint writes the directories waiting in memory to `checkpoint/state`, compressed: a few megabytes on most trees, more on very broad ones.

### 8.5 Output

- **Parquet with zstd level 3** is the best balance of size and speed. Raise the level (`--compression zstd:9`) for smaller files at the cost of more CPU.
- **Writers** are added automatically, up to `--writers`, only when output backs up.
- **Write output to a different file system** from the one being scanned.

---

## 9. Troubleshooting

**`output directory ... is not empty`**: each scan needs a new directory. Include the date in the name.

**`No files found that match the pattern ".../errors/*.parquet"`** from `fs_errors`: the scan had no errors, so there is no `errors` table. Check `totals.errors` in the manifest first.

**`format_bytes(HUGEINT)`**: cast the sum: `format_bytes(sum(size)::BIGINT)` ([7.3](#73-settings-and-pitfalls)).

**Dates are off by some hours, or files land in the wrong year**: set `SET TimeZone = 'UTC';` in DuckDB.

**`size` is null**: the scan was run with `--fields` leaving size out. The manifest's `options.fields` lists what was read.

**`verify` says `schema version is 1`**: the scan was written by a build before schema version 2 (which made integer columns signed). Scan again with this release.

**`scan status is running`**: the scan is still going, or the process was killed (`kill -9`, out of memory, power loss). In the second case, run `fs-inventory resume` on it ([4.12](#412-interrupted-crashed-and-stalled-scans)).

**`... is still being written by another fs-inventory process`** from `resume` or `scan`: a scan or resume is still running on that directory. Let it finish, or stop it first.

**`... has no checkpoint to resume from`**: the scan was run with `--checkpoint-interval 0`. Scan again into a new directory.

**`skipped a checkpoint: a crawl thread has made no progress`**: a thread is stuck, typically on a hung NFS server. See [4.12](#412-interrupted-crashed-and-stalled-scans).

**The scan stops making progress**: look for `stalled` in the progress output. A hung NFS server blocks its thread until the server returns. Use `--stall-timeout` so the scan gives up on that directory and finishes ([4.12](#412-interrupted-crashed-and-stalled-scans)).

**`--resolve-ids is not implemented yet`** (or `--symlink-targets`, `--guess-encoding`): these options are planned; see below.

---

## 10. Not yet available

These appear in `--help` but aren't implemented in 0.4.0. Using them prints `not implemented yet` and exits with code 4 (commands) or 2 (options).

| Feature | What it will do | Planned |
|---|---|---|
| `fs-inventory convert` | Re-export a Parquet scan as CSV or JSON Lines | Milestone M5 |
| `--symlink-targets` | Record where each symlink points | Milestone M5 |
| `--resolve-ids` | Add user and group name columns | Milestone M5 |
| `--guess-encoding` | Guess the legacy character set of names that aren't valid UTF-8 | Milestone M5 |

Until `--resolve-ids` arrives, join user names in DuckDB ([7.5](#75-recipes)). Until `convert` arrives, DuckDB can export any scan: `COPY (SELECT * FROM fs_entries('...')) TO 'entries.csv' (HEADER)`.
