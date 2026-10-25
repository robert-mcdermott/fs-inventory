# Benchmark kit

Two scripts for measuring fs-inventory on real storage. Both use only the Python standard library, so they run on any server with Python 3.8 or later.

| Script | Purpose |
|---|---|
| `gen_tree.py` | Creates a synthetic tree with a chosen number of files, fan-out, and files per directory |
| `run.py` | Scans a tree at several thread counts, optionally alongside `find` and other tools, and records time, entries per second, and peak memory in `results.csv` |

## Running on a Linux server

Build a static binary on any Linux machine (or download one from CI), then copy it and this directory to the server:

```bash
cargo build --release --target x86_64-unknown-linux-musl
```

```bash
scp target/x86_64-unknown-linux-musl/release/fs-inventory bench/*.py server:fs-inventory-bench/
```

Create a test tree on the file system you want to measure. Creating files is much slower than scanning them, so start with a million:

```bash
python3 -I gen_tree.py /nfs/scratch/fsinv-bench-1m --files 1000000
```

Sweep thread counts. `--drop-caches` (root only) clears the client's caches before each run, so each run goes to the server rather than reading cached metadata:

```bash
sudo python3 -I run.py /nfs/scratch/fsinv-bench-1m --bin ./fs-inventory --threads 8,16,32,64,128,256 --drop-caches --find
```

To measure a real tree, point `run.py` at it directly. Scans are read-only.

## Reading the results

- **Warm versus cold:** without `--drop-caches`, every run after the first reads cached metadata and measures CPU speed, not storage.
- **Network file systems:** throughput should rise with threads until the server or network saturates. The knee of that curve is the right `--threads` value for that system.
- **Laptop results:** a laptop SSD with a warm cache says little about NFS, GPFS, or Lustre. Use it only to catch regressions.
