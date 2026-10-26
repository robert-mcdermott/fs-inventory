#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Robert McDermott
"""Scale test: scan a generated tree and record memory and throughput.

Runs `fs-inventory scan --synthetic`, which lists and stats a tree computed
in memory instead of the file system, so a tree of any size fits on any
machine. Every second it records the scan's resident memory, entries, rate,
and queued directories in timeline.csv. A scan whose memory levels off and
whose rate holds to the end will behave the same on a tree of any size.

The generated tree costs no I/O, so the rate is fs-inventory's own ceiling
on this machine: what it can crawl, encode, and write when the file system
answers instantly.

Examples:
    python3 -I bench/scale.py 100M
    python3 -I bench/scale.py 300M --kill-at 0.5          # kill -9 halfway, then resume
    python3 -I bench/scale.py 1G --stdout                 # CSV to /dev/null: no disk
    python3 -I bench/scale.py 100M -- --max-memory 512M   # scan options after --

The tree spec is `100M`, or `entries=100M,files=32,fanout=8,huge=1M,seed=0`.
Scan output is deleted afterwards unless --keep is given; at about 25 bytes
per entry, a 100M-entry scan needs 2.5 GB of free space while it runs.

Uses only the Python standard library.
"""

import argparse
import csv
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import threading
import time


def log(*args):
    print(*args, flush=True)


def rss_kib(pid):
    """Resident memory of a running process, in KiB."""
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        pass
    try:
        out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], stdout=subprocess.PIPE,
                             universal_newlines=True)
        return int(out.stdout.strip() or 0)
    except (OSError, ValueError):
        return 0


def dir_bytes(path):
    total = 0
    for parent, _, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(parent, name)).st_size
            except OSError:
                pass
    return total


class Phase:
    """One fs-inventory process (the scan, or a resume), sampled every second."""

    def __init__(self, name, argv, timeline, timeline_file, offset, kill_at_entries=None):
        self.name = name
        self.samples = []
        self.progress = {}
        self.summary = None
        self.notes = []
        self.killed = False
        started = time.monotonic()
        process = subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                   universal_newlines=True)
        reader = threading.Thread(target=self.read_progress, args=(process.stderr,), daemon=True)
        reader.start()
        ticks = 0
        while True:
            time.sleep(0.1)
            pid, status, usage = os.wait4(process.pid, os.WNOHANG)
            if pid:
                break
            ticks += 1
            if ticks % 10:
                continue
            # Progress counts include entries written before a resume.
            entries = self.progress.get("entries", 0)
            row = {
                "phase": name,
                "seconds": round(offset + time.monotonic() - started, 1),
                "entries": entries,
                "entries_per_second": int(self.progress.get("entries_per_second", 0)),
                "queued": self.progress.get("queued", 0),
                "rss_mib": round(rss_kib(process.pid) / 1024, 1),
            }
            self.samples.append(row)
            timeline.writerow(row)
            timeline_file.flush()
            if kill_at_entries is not None and entries >= kill_at_entries and not self.killed:
                process.send_signal(signal.SIGKILL)
                self.killed = True
        self.seconds = time.monotonic() - started
        process.returncode = -os.WTERMSIG(status) if os.WIFSIGNALED(status) else os.WEXITSTATUS(status)
        reader.join(timeout=5)
        self.exit = process.returncode
        self.cpu_seconds = usage.ru_utime + usage.ru_stime
        # ru_maxrss is KiB on Linux and bytes on macOS.
        self.peak_rss_kib = usage.ru_maxrss // 1024 if sys.platform == "darwin" else usage.ru_maxrss

    def read_progress(self, stream):
        for line in stream:
            if line.startswith("{"):
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if event.get("event") == "done":
                    self.summary = event
                else:
                    self.progress = event
            else:
                self.notes.append(line.rstrip())
                log(f"  {line.rstrip()}")


def report(label, phases, entries, args, output_bytes):
    samples = [s for p in phases for s in p.samples]
    seconds = sum(p.seconds for p in phases)
    cpu = sum(p.cpu_seconds or 0 for p in phases)
    peak = max(p.peak_rss_kib for p in phases) / 1024
    log(f"\n{label}")
    log(f"{'progress':>9} {'elapsed':>9} {'entries':>15} {'rate/s':>12} {'queued':>9} {'memory':>10}")
    shown = set()
    for s in samples:
        tenth = min(10, int(10 * s["entries"] / max(entries, 1)))
        if tenth in shown:
            continue
        shown.add(tenth)
        log(f"{tenth * 10:>8}% {s['seconds']:>8.0f}s {s['entries']:>15,} {s['entries_per_second']:>12,} "
              f"{s['queued']:>9,} {s['rss_mib']:>7.0f} MiB")
    rate = entries / seconds if seconds else 0
    log(f"  {entries:,} entries in {seconds:,.1f}s: {rate:,.0f} entries/s, "
          f"{cpu / max(entries, 1) * 1e6:.2f} CPU µs per entry ({cpu / max(seconds, 1e-9):.1f} cores busy)")
    log(f"  peak memory {peak:,.0f} MiB" + (f", output {output_bytes / 1e9:.2f} GB "
                                              f"({output_bytes / max(entries, 1):.1f} bytes per entry)"
                                              if output_bytes else ""))
    # Memory over the second half: flat if the last quarter's peak is close
    # to the second quarter's.
    half = [s["rss_mib"] for s in samples if s["entries"] >= entries / 4]
    if len(half) >= 4:
        q2 = max(half[: len(half) // 3])
        q4 = max(half[-len(half) // 3:])
        log(f"  memory after the first quarter: {q2:,.0f} MiB early, {q4:,.0f} MiB late")
    return {
        "label": label,
        "entries": entries,
        "seconds": round(seconds, 2),
        "entries_per_second": round(rate),
        "cpu_seconds": round(cpu, 2),
        "peak_rss_mib": round(peak, 1),
        "output_bytes": output_bytes,
        "phases": [{"name": p.name, "seconds": round(p.seconds, 2), "exit": p.exit, "killed": p.killed,
                    "peak_rss_mib": round(p.peak_rss_kib / 1024, 1)} for p in phases],
    }


def main():
    argv = sys.argv[1:]
    extra = []
    if "--" in argv:
        split = argv.index("--")
        argv, extra = argv[:split], argv[split + 1:]
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("tree", help="tree spec, such as 100M or entries=100M,files=32,fanout=8,huge=1M,seed=0")
    parser.add_argument("--bin", default=None, help="fs-inventory binary (default: target/release, else PATH)")
    parser.add_argument("--threads", type=int, default=os.cpu_count(),
                        help="crawl threads (default: CPU count, since a generated tree never waits on I/O)")
    parser.add_argument("--format", choices=["parquet", "csv", "jsonl"], default="parquet")
    parser.add_argument("--stdout", action="store_true",
                        help="write CSV to /dev/null instead of a directory: no disk space needed")
    parser.add_argument("--kill-at", type=float, metavar="FRACTION",
                        help="kill -9 the scan this far through (0-1), then resume it and check the total")
    parser.add_argument("--work-dir", default=None, help="where scan output and results go")
    parser.add_argument("--keep", action="store_true", help="keep scan output")
    args = parser.parse_args(argv)
    if args.stdout and args.kill_at is not None:
        parser.error("--kill-at needs an output directory, so it can't be used with --stdout")

    here = os.path.dirname(os.path.abspath(__file__))
    binary = args.bin or next(
        (b for b in [os.path.join(here, "..", "target", "release", "fs-inventory"), shutil.which("fs-inventory")]
         if b and os.access(b, os.X_OK)), None)
    if not binary:
        parser.error("fs-inventory binary not found (build with `cargo build --release`, or pass --bin)")
    version = subprocess.run([binary, "--version"], stdout=subprocess.PIPE,
                             universal_newlines=True).stdout.strip()
    work = os.path.abspath(args.work_dir or f"scale-results-{time.strftime('%Y%m%dT%H%M%S')}")
    os.makedirs(work, exist_ok=True)
    out = os.path.join(work, "scan")

    # Learn the exact entry count from the tree spec, as fs-inventory parses
    # it: a scan that lists only the root reports the spec in full.
    probe = subprocess.run([binary, "scan", "/synthetic", "--synthetic", args.tree, "--max-depth", "0",
                            "-o", "-", "-f", "csv", "--progress", "plain"],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    spec = re.search(r"generated tree \((entries=(\d+)[^)]*)\)", probe.stderr)
    if probe.returncode != 0 or not spec:
        parser.error(probe.stderr.strip() or "invalid tree spec")
    entries = int(spec.group(2))

    scan = [binary, "scan", "/synthetic", "--synthetic", args.tree, "-t", str(args.threads),
            "--progress", "json", "--progress-interval", "1s"]
    if args.stdout:
        scan += ["-o", "-", "-f", "csv"]
        label = f"{args.tree}: CSV to /dev/null"
    else:
        scan += ["-o", out, "-f", args.format]
        label = f"{args.tree}: {args.format}"
    if args.kill_at is not None:
        if not any(a.startswith("--checkpoint-interval") for a in extra):
            scan += ["--checkpoint-interval", "10s"]
        label += f", killed at {args.kill_at:.0%} and resumed"
    scan += extra

    log(f"{version} on {platform.system()} {platform.machine()}, {os.cpu_count()} CPUs; tree {spec.group(1)}")
    log(f"work dir {work}")
    log(" ".join(scan))
    timeline_path = os.path.join(work, "timeline.csv")
    phases = []
    with open(timeline_path, "w", newline="") as f:
        timeline = csv.DictWriter(f, ["phase", "seconds", "entries", "entries_per_second", "queued", "rss_mib"])
        timeline.writeheader()
        kill_at = int(entries * args.kill_at) if args.kill_at is not None else None
        phases.append(Phase("scan", scan, timeline, f, 0, kill_at))
        if phases[-1].killed:
            with open(os.path.join(out, "manifest.json")) as m:
                log(f"  killed; manifest status: {json.load(m)['status']}")
            resume = [binary, "resume", out, "--progress", "json", "--progress-interval", "1s"]
            log(" ".join(resume))
            phases.append(Phase("resume", resume, timeline, f, phases[-1].seconds))

    failures = []
    checks = ["complete", "exact entry count"]
    if args.kill_at is not None and not phases[0].killed:
        failures.append("the scan finished before it could be killed: use a larger tree")
    for p in phases:
        if p.exit not in (0, None) and not p.killed:
            failures.append(f"{p.name} exited with {p.exit}")
    output_bytes = 0
    if not args.stdout:
        with open(os.path.join(out, "manifest.json")) as m:
            manifest = json.load(m)
        if manifest["status"] != "complete":
            failures.append(f"manifest status is {manifest['status']}")
        if manifest["totals"]["entries"] != entries:
            failures.append(f"manifest has {manifest['totals']['entries']:,} entries, expected {entries:,}")
        verify = subprocess.run([binary, "verify", out], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True)
        if verify.returncode != 0:
            failures.append(f"verify failed: {verify.stdout.strip()} {verify.stderr.strip()}")
        output_bytes = dir_bytes(os.path.join(out, "entries"))
        if args.format == "parquet" and shutil.which("duckdb"):
            # Generated inode numbers are 2 to entries + 1, each used once, so
            # this proves every entry was written exactly once.
            query = (f"select count(*), count(distinct ino), min(ino), max(ino) "
                     f"from read_parquet('{out}/entries/*.parquet')")
            check = subprocess.run(["duckdb", "-csv", "-noheader", "-c", query], stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, universal_newlines=True)
            counts = check.stdout.strip().splitlines()[-1] if check.returncode == 0 else check.stderr.strip()
            if counts != f"{entries},{entries},2,{entries + 1}":
                failures.append(f"inode check: expected {entries},{entries},2,{entries + 1}, got {counts}")
            else:
                checks.append("every entry exactly once")
    else:
        done = phases[-1].summary or {}
        if done.get("entries") != entries:
            failures.append(f"scan reported {done.get('entries')} entries, expected {entries:,}")
    if not args.stdout and not any(f.startswith("verify") for f in failures):
        checks.insert(2, "verify ok")

    result = report(label, phases, entries, args, output_bytes)
    result.update({"version": version, "system": f"{platform.system()} {platform.machine()}",
                   "cpus": os.cpu_count(), "command": scan, "failures": failures})
    with open(os.path.join(work, "summary.json"), "w") as f:
        json.dump(result, f, indent=2)
    if not args.keep and os.path.isdir(out):
        shutil.rmtree(out)
    log(f"  {'FAILED: ' + '; '.join(failures) if failures else 'checks passed: ' + ', '.join(checks)}")
    log(f"timeline: {timeline_path}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
