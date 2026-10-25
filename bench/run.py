#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Robert McDermott
"""Benchmark fs-inventory on one directory tree.

Runs `fs-inventory scan` at several thread counts, optionally alongside
`find` and other tools, and records wall time, entries per second, and peak
memory for every run in results.csv.

Examples:
    # Thread sweep on an NFS tree, dropping caches before each run (root):
    sudo python3 -I bench/run.py /nfs/projects --threads 16,32,64,128,256 --drop-caches

    # Compare with find and another tool; {root} and {threads} are substituted,
    # and the command's stdout is written to a file in the work directory:
    python3 -I bench/run.py /data/tree --find --tool 'other=/opt/other/scan {root}'

Notes:
  * Without --drop-caches, repeated runs read cached metadata and measure the
    CPU path, not the storage. On NFS the client cache matters as much as the
    server's, so drop caches on the client to measure the server.
  * Uses only the Python standard library.
"""

import argparse
import csv
import json
import os
import shlex
import shutil
import subprocess
import sys
import time

DROP_CACHES = "/proc/sys/vm/drop_caches"


def drop_caches():
    """Drops the page, dentry, and inode caches (Linux, root only)."""
    os.sync()
    with open(DROP_CACHES, "w") as f:
        f.write("3\n")


def run(command, stdout_path=None):
    """Runs a command and returns (seconds, max RSS in KiB, exit status)."""
    stdout = open(stdout_path, "wb") if stdout_path else subprocess.DEVNULL
    try:
        started = time.monotonic()
        process = subprocess.Popen(command, stdout=stdout, stderr=subprocess.PIPE)
        _, status, usage = os.wait4(process.pid, 0)
        seconds = time.monotonic() - started
        stderr = process.stderr.read().decode(errors="replace")
    finally:
        if stdout_path:
            stdout.close()
    # ru_maxrss is KiB on Linux and bytes on macOS.
    rss_kib = usage.ru_maxrss // 1024 if sys.platform == "darwin" else usage.ru_maxrss
    code = os.waitstatus_to_exitcode(status)
    if code not in (0, 1):
        print(f"  warning: exit {code}: {stderr.strip()[:500]}", file=sys.stderr)
    return seconds, rss_kib, code


def count_lines(path):
    with open(path, "rb") as f:
        return sum(1 for _ in f)


def find_command(root):
    """A find invocation that reads every entry's metadata, like a scan does."""
    probe = subprocess.run(["find", "--version"], capture_output=True)
    if probe.returncode == 0 and b"GNU" in probe.stdout:
        return ["find", root, "-printf", "%i %s %T@ %p\\n"]
    return ["find", root, "-ls"]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("root", help="directory tree to scan")
    parser.add_argument("--bin", default=None, help="fs-inventory binary (default: on PATH, else target/release)")
    parser.add_argument("--threads", default="8,16,32,64,128", help="comma-separated thread counts")
    parser.add_argument("--format", choices=["csv", "jsonl"], default="csv")
    parser.add_argument("--repeat", type=int, default=3, help="runs per configuration (default 3)")
    parser.add_argument("--work-dir", default=None, help="where scan output and results go")
    parser.add_argument("--drop-caches", action="store_true", help="drop Linux caches before each run (root)")
    parser.add_argument("--find", action="store_true", help="also time find as a baseline")
    parser.add_argument("--tool", action="append", default=[], metavar="NAME=COMMAND",
                        help="also time COMMAND; {root} and {threads} are substituted (repeatable)")
    parser.add_argument("--keep", action="store_true", help="keep scan output")
    args = parser.parse_args()

    binary = args.bin or shutil.which("fs-inventory") or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "target", "release", "fs-inventory")
    if not os.access(binary, os.X_OK):
        parser.error(f"fs-inventory binary not found: {binary} (build with `cargo build --release`)")
    if args.drop_caches and not os.access(DROP_CACHES, os.W_OK):
        parser.error("--drop-caches needs Linux and root")
    root = os.path.abspath(args.root)
    work = os.path.abspath(args.work_dir or f"bench-results-{time.strftime('%Y%m%dT%H%M%S')}")
    os.makedirs(work, exist_ok=True)
    threads = [int(t) for t in args.threads.split(",")]

    configs = [("fs-inventory", t, None) for t in threads]
    if args.find:
        configs.append(("find", 1, find_command(root)))
    for tool in args.tool:
        name, _, command = tool.partition("=")
        if not command:
            parser.error(f"--tool needs NAME=COMMAND: {tool}")
        for t in threads if "{threads}" in command else [None]:
            argv = [a.format(root=root, threads=t) for a in shlex.split(command)]
            configs.append((name, t, argv))

    results_path = os.path.join(work, "results.csv")
    with open(results_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["tool", "threads", "run", "seconds", "entries", "entries_per_second", "max_rss_kib", "exit"])
        for name, t, command in configs:
            for rep in range(1, args.repeat + 1):
                if args.drop_caches:
                    drop_caches()
                label = f"{name}-t{t}-r{rep}" if t else f"{name}-r{rep}"
                out = os.path.join(work, label)
                if command is None:
                    argv = [binary, "scan", root, "-o", out, "-f", args.format,
                            "-t", str(t), "--progress", "none"]
                    seconds, rss, code = run(argv)
                    with open(os.path.join(out, "manifest.json")) as m:
                        entries = json.load(m)["totals"]["entries"]
                    if not args.keep:
                        shutil.rmtree(out)
                else:
                    seconds, rss, code = run(command, stdout_path=out + ".out")
                    entries = count_lines(out + ".out")
                    if not args.keep:
                        os.remove(out + ".out")
                rate = entries / seconds if seconds > 0 else 0
                writer.writerow([name, t or "", rep, f"{seconds:.3f}", entries, f"{rate:.0f}", rss, code])
                f.flush()
                print(f"{label:28} {seconds:9.2f}s {entries:>14,} entries {rate:>12,.0f}/s {rss / 1024:9.1f} MiB")

    print(f"results: {results_path}")


if __name__ == "__main__":
    main()
