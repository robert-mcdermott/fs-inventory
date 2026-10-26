# Benchmark kit

Scripts for measuring fs-inventory, on any machine or on real storage. They use only the Python standard library, so they run on any server with Python 3.6 or later.

| Script | Purpose |
|---|---|
| `server.py` | Turnkey benchmark of a server's storage: everything below in one run, with a report to send back |
| `scale.py` | Scale test: scans a generated tree of any size and records memory and throughput every second, optionally killing and resuming the scan |
| `gen_tree.py` | Creates a real directory tree with a chosen number of files, fan-out, and files per directory |
| `run.py` | Scans a tree at several thread counts, optionally alongside `find` and other tools, and records time, entries per second, and peak memory in `results.csv` |

`server.py` and `scale.py` use `scan --synthetic`, a hidden option that scans a tree generated in memory instead of the file system ([architecture, section 3.4](../docs/architecture.md#34-generated-trees)). The tree costs no storage and no I/O, so any size fits on any machine.

## Benchmarking a server

Copy the binary and this directory to the server. Build a static Linux binary on any Linux machine, or take one from a release:

```bash
cargo build --release --target x86_64-unknown-linux-musl
```

```bash
scp target/x86_64-unknown-linux-musl/release/fs-inventory bench/*.py server:fs-inventory-bench/
```

On the server, from a directory on local disk, point `server.py` at the tree to measure. Run it as root so it can drop the client's caches before each step:

```bash
sudo python3 -I server.py /nfs/projects --bin ./fs-inventory
```

It takes about 15 minutes, whatever the size of the tree, because every scan is time-boxed (`--duration`, 60 seconds by default). It measures:

1. **The system:** CPUs, memory, kernel, and the tree's mount type and options.
2. **The CPU ceiling:** a scan of a 20-million-entry generated tree. It never waits on storage, so it's the most this machine can crawl and write.
3. **`find`:** a single-threaded baseline.
4. **`probe`:** crawls at 8 to 256 threads, writing no output, and the suggested `--threads`.
5. **Scans:** Parquet scans at the suggested thread count, half it, and twice it: rate, CPU, peak memory, output size, errors, and stalls.
6. **Kill and resume,** with `--resume-root DIR`: a complete scan of a smaller tree that takes a few minutes, then the same scan killed with SIGKILL halfway and resumed, which must find the same entries.

On NFS, every step also records the RPCs it caused, from `/proc/self/mountstats`: calls per entry and average round-trip time for each operation. The results go to `report.md` and `results.json` in a work directory, bundled as a `.tar.gz` to send back. Scans never write to the tree being measured. Their output goes to the work directory and is deleted after each step unless `--keep` is given.

## Scale tests

`scale.py` scans a generated tree of any size and samples the scan's memory, rate, and queued directories every second into `timeline.csv`. A scan whose memory levels off and whose rate holds steady to the end behaves the same on a tree of any size. At the end it checks that the scan completed, that the manifest has exactly the tree's entries, and that `verify` passes. With DuckDB installed, it also checks that every entry was written exactly once.

```bash
python3 -I bench/scale.py 100M
```

```bash
python3 -I bench/scale.py 200M --kill-at 0.5
```

```bash
python3 -I bench/scale.py 1G --stdout
```

- **`--kill-at 0.5`** kills the scan with SIGKILL halfway through, then resumes it. It sets `--checkpoint-interval 10s` unless you pass another.
- **`--stdout`** writes CSV to `/dev/null`, which needs no disk space. Parquet output takes about 47 bytes per generated entry (random timestamps compress worse than real ones), so a 100-million-entry scan needs about 5 GB while it runs.
- **Scan options** go after `--`, such as `-- --max-memory 512M --writers 2`.

The rate is fs-inventory's own ceiling on that machine. Real storage is slower, so the rate says nothing about it.

## Real trees

`gen_tree.py` creates a real tree on the file system you want to measure. Creating files is much slower than scanning them, so start with a million:

```bash
python3 -I gen_tree.py /nfs/scratch/fsinv-bench-1m --files 1000000
```

`run.py` sweeps thread counts over a tree. `--drop-caches` (root only) clears the client's caches before each run, so each run goes to the server rather than reading cached metadata:

```bash
sudo python3 -I run.py /nfs/scratch/fsinv-bench-1m --bin ./fs-inventory --threads 8,16,32,64,128,256 --drop-caches --find
```

To measure a real tree, point `run.py` at it directly. Scans are read-only.

## Reading the results

- **Warm versus cold:** without dropping caches, every run after the first reads cached metadata and measures CPU speed, not storage.
- **Network file systems:** throughput should rise with threads until the server or network saturates. The knee of that curve is the right `--threads` value for that system. `probe` suggests the smallest thread count within 90% of the best rate.
- **CPU ceiling versus storage:** if a scan's rate approaches the generated tree's rate, the machine's CPUs are the limit, not the storage.
- **Laptop results:** a laptop SSD with a warm cache says little about NFS, GPFS, or Lustre. Use laptop numbers only to catch regressions.
