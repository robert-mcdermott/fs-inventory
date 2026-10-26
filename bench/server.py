#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Robert McDermott
"""Turnkey benchmark of fs-inventory on a server's real storage.

Runs every measurement in one go and writes report.md and results.json to
send back. Nothing it does writes to the storage being measured: scans are
read-only, and their output goes to the work directory, which should be on
local disk. Each scan is time-boxed, so the whole run takes about
15 minutes with the default --duration of 60 seconds, however large ROOT is.

  1. System: CPUs, memory, kernel, and ROOT's mount type and options.
  2. CPU ceiling: a scan of a generated tree, which never waits on storage,
     so its rate is the most this machine can crawl and write.
  3. find: a single-threaded baseline over the same tree.
  4. Probe: crawls at several thread counts, writing no output.
  5. Scans: time-boxed scans with Parquet output at the suggested thread
     count, at half it, and at twice it.
  6. Resume (with --resume-root): a complete scan of a smaller tree, then
     the same scan killed with SIGKILL partway and resumed. The resumed
     scan must find exactly the same entries.

On NFS, each step also records the RPCs it caused (from
/proc/self/mountstats): calls per entry and average round-trip time for
GETATTR, LOOKUP, READDIRPLUS, and the rest.

Run it as root to drop the page, dentry, and inode caches before every
step, so each one reads from the server rather than from what earlier steps
cached. Without root, later steps run partly from cache, and the report
says so.

Examples:
    sudo python3 -I server.py /nfs/projects --bin ./fs-inventory
    sudo python3 -I server.py /nfs/projects --resume-root /nfs/projects/lab1 --duration 120
    python3 -I server.py /gpfs/scratch --threads 16,64,256

Uses only the Python standard library (3.6 or later).
"""

import argparse
import json
import os
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import threading
import time

DROP_CACHES = "/proc/sys/vm/drop_caches"


def log(message):
    print(message, flush=True)


def run_text(argv):
    """Runs a command and returns (exit code, stdout, stderr)."""
    try:
        p = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        return p.returncode, p.stdout, p.stderr
    except OSError as e:
        return None, "", str(e)


def exit_code(status):
    if os.WIFSIGNALED(status):
        return -os.WTERMSIG(status)
    return os.WEXITSTATUS(status)


def drop_caches(enabled):
    if enabled:
        os.sync()
        with open(DROP_CACHES, "w") as f:
            f.write("3\n")


# ---------------------------------------------------------------- system


def read_file(path):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return ""


def mount_of(path):
    """The mount holding `path`: (mount point, fs type, source, options)."""
    path = os.path.realpath(path)
    best = None
    for line in read_file("/proc/self/mountinfo").splitlines():
        fields = line.split()
        if "-" not in fields:
            continue
        sep = fields.index("-")
        point = fields[4].replace("\\040", " ")
        if path == point or path.startswith(point.rstrip("/") + "/") or point == "/":
            if best is None or len(point) > len(best[0]):
                options = fields[5] + "," + (fields[sep + 3] if len(fields) > sep + 3 else "")
                best = (point, fields[sep + 1], fields[sep + 2], options)
    if best is None:
        # macOS and the BSDs: `df` names the mount point, and `mount` prints
        # "SOURCE on POINT (TYPE, OPTIONS)".
        code, out, _ = run_text(["df", "-P", path])
        point = out.splitlines()[-1].split()[-1] if code == 0 and out.strip() else "/"
        best = (point, "", "", "")
        for line in run_text(["mount"])[1].splitlines():
            m = re.match(r"(.+?) on (.+?) \(([^,)]+),? ?(.*)\)$", line)
            if m and m.group(2) == point:
                best = (point, m.group(3), m.group(1), m.group(4))
    return best


def system_info(root, binary):
    cpu_model = ""
    for line in read_file("/proc/cpuinfo").splitlines():
        if line.lower().startswith(("model name", "cpu model")):
            cpu_model = line.split(":", 1)[1].strip()
            break
    if not cpu_model:
        for line in run_text(["lscpu"])[1].splitlines():
            if line.startswith("Model name:"):
                cpu_model = line.split(":", 1)[1].strip()
                break
    if not cpu_model and sys.platform == "darwin":
        cpu_model = run_text(["sysctl", "-n", "machdep.cpu.brand_string"])[1].strip()
    mem = re.search(r"MemTotal:\s+(\d+) kB", read_file("/proc/meminfo"))
    memory_kib = int(mem.group(1)) if mem else None
    if memory_kib is None and sys.platform == "darwin":
        memsize = run_text(["sysctl", "-n", "hw.memsize"])[1].strip()
        memory_kib = int(memsize) // 1024 if memsize.isdigit() else None
    point, fstype, source, options = mount_of(root)
    return {
        "host": socket.gethostname(),
        "kernel": " ".join(platform.uname()[0:3:2]),
        "machine": platform.machine(),
        "cpu_model": cpu_model,
        "cpus": os.cpu_count(),
        "memory_gib": round(memory_kib / 1048576, 1) if memory_kib else None,
        "fs_inventory": run_text([binary, "--version"])[1].strip(),
        "python": platform.python_version(),
        "root": os.path.realpath(root),
        "mount_point": point,
        "fs_type": fstype,
        "source": source,
        "mount_options": options,
        "root_user": os.geteuid() == 0,
    }


# ---------------------------------------------------------------- NFS stats


def mountstats(point):
    """Per-operation NFS counters for one mount: {op: [ops, rtt_ms]}."""
    text = read_file("/proc/self/mountstats")
    stanza = None
    for block in re.split(r"\n(?=device )", text):
        m = re.match(r"device (\S+) mounted on (\S+) with fstype (nfs\S*)", block)
        if m and m.group(2).replace("\\040", " ") == point:
            stanza = block
    if stanza is None or "per-op statistics" not in stanza:
        return None
    ops = {}
    for line in stanza.split("per-op statistics", 1)[1].splitlines():
        m = re.match(r"\s*([A-Z0-9_]+):\s+(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)", line)
        if m:
            ops[m.group(1)] = [int(m.group(2)), int(m.group(3))]
    return ops


def rpc_delta(before, after, entries):
    """Calls per entry and average round trip, for operations that ran."""
    if before is None or after is None:
        return None
    rows = []
    for op, (count, rtt) in sorted(after.items()):
        calls = count - before.get(op, [0, 0])[0]
        if calls > 0:
            total_rtt = rtt - before.get(op, [0, 0])[1]
            rows.append({
                "op": op,
                "calls": calls,
                "per_entry": round(calls / entries, 4) if entries else None,
                "avg_rtt_ms": round(total_rtt / calls, 3),
            })
    rows.sort(key=lambda r: -r["calls"])
    return rows


# ---------------------------------------------------------------- runs


class Run:
    """Runs fs-inventory (or find) with a time limit, and records its rate,
    resource use, and progress events."""

    def __init__(self, argv, seconds=None, stop_signal=signal.SIGINT, kill_at_entries=None,
                 count_stdout=False, events_path=None):
        self.events = []
        self.stderr = []
        self.done = None
        self.lines = 0
        started = time.monotonic()
        stdout = subprocess.PIPE if count_stdout else subprocess.DEVNULL
        self.process = subprocess.Popen(argv, stdout=stdout, stderr=subprocess.PIPE)
        readers = [threading.Thread(target=self.read_stderr, daemon=True)]
        if count_stdout:
            readers.append(threading.Thread(target=self.count_lines, daemon=True))
        for r in readers:
            r.start()
        self.stopped = False
        self.killed = False
        while True:
            time.sleep(0.2)
            pid, status, usage = os.wait4(self.process.pid, os.WNOHANG)
            if pid:
                break
            elapsed = time.monotonic() - started
            if seconds is not None and elapsed >= seconds and not self.stopped:
                self.process.send_signal(stop_signal)
                self.stopped = True
            entries = self.events[-1].get("entries", 0) if self.events else 0
            if kill_at_entries is not None and entries >= kill_at_entries and not self.killed:
                self.process.send_signal(signal.SIGKILL)
                self.killed = True
        self.seconds = time.monotonic() - started
        self.process.returncode = exit_code(status)
        for r in readers:
            r.join(timeout=10)
        self.exit = self.process.returncode
        self.cpu_seconds = usage.ru_utime + usage.ru_stime
        self.peak_rss_mib = (usage.ru_maxrss / 1048576 if sys.platform == "darwin" else usage.ru_maxrss / 1024)
        if events_path:
            with open(events_path, "w") as f:
                for e in self.events:
                    f.write(json.dumps(e) + "\n")
                for line in self.stderr:
                    f.write(json.dumps({"stderr": line}) + "\n")

    def read_stderr(self):
        for raw in self.process.stderr:
            line = raw.decode("utf-8", "replace").rstrip("\n")
            if line.startswith("{"):
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if event.get("event") == "done":
                    self.done = event
                else:
                    self.events.append(event)
            elif line:
                self.stderr.append(line)

    def count_lines(self):
        for chunk in iter(lambda: self.process.stdout.read(1 << 20), b""):
            self.lines += chunk.count(b"\n")

    @property
    def entries(self):
        if self.done:
            return self.done.get("entries", 0)
        return self.events[-1].get("entries", 0) if self.events else self.lines

    def summary(self):
        entries = self.entries
        elapsed = self.done.get("elapsed_seconds", self.seconds) if self.done else self.seconds
        return {
            "entries": entries,
            "seconds": round(elapsed, 2),
            "entries_per_second": round(entries / elapsed) if elapsed else 0,
            "cpu_seconds": round(self.cpu_seconds, 2),
            "cores_busy": round(self.cpu_seconds / elapsed, 2) if elapsed else 0,
            "peak_rss_mib": round(self.peak_rss_mib, 1),
            "exit": self.exit,
            "errors": self.done.get("errors") if self.done else None,
            "status": self.done.get("status") if self.done else None,
            "stalled": [line for line in self.stderr if "stall" in line or "no progress" in line],
        }


def scan_argv(binary, root, threads, out, extra=(), progress="5s"):
    return [binary, "scan", root, "-t", str(threads), "-o", out, "--progress", "json",
            "--progress-interval", progress] + list(extra)


def output_bytes(out):
    total = 0
    for parent, _, files in os.walk(os.path.join(out, "entries")):
        for name in files:
            total += os.path.getsize(os.path.join(parent, name))
    return total


# ---------------------------------------------------------------- steps


def step_ceiling(r, binary, work, args):
    out = os.path.join(work, "ceiling")
    threads = os.cpu_count() or 8
    argv = scan_argv(binary, "/synthetic", threads, out, ["--synthetic", args.ceiling_entries])
    log(f"[ceiling] scanning a generated tree of {args.ceiling_entries} entries with {threads} threads")
    run = Run(argv, events_path=os.path.join(work, "ceiling.events.jsonl"))
    if any("unexpected argument '--synthetic'" in line for line in run.stderr):
        r["ceiling"] = {"skipped": "this fs-inventory has no --synthetic option (needs 0.5.0 or later)"}
        log("  skipped: " + r["ceiling"]["skipped"])
        return
    s = run.summary()
    if os.path.isdir(out):
        s["output_bytes_per_entry"] = round(output_bytes(out) / max(s["entries"], 1), 1)
        shutil.rmtree(out)
    s["threads"] = threads
    r["ceiling"] = s
    log(f"  {s['entries']:,} entries in {s['seconds']}s: {s['entries_per_second']:,}/s, "
        f"{s['cores_busy']} cores busy, peak memory {s['peak_rss_mib']:,.0f} MiB")


def step_find(r, root, work, args):
    code, out, _ = run_text(["find", "--version"])
    gnu = code == 0 and "GNU" in out
    argv = ["find", root, "-printf", "%i %s %T@ %p\\n"] if gnu else ["find", root, "-ls"]
    log(f"[find] {' '.join(argv)} for {args.duration}s")
    drop_caches(args.drop_caches)
    before = mountstats(r["system"]["mount_point"])
    run = Run(argv, seconds=args.duration, stop_signal=signal.SIGTERM, count_stdout=True)
    s = run.summary()
    s["command"] = " ".join(argv)
    s["rpc"] = rpc_delta(before, mountstats(r["system"]["mount_point"]), s["entries"])
    r["find"] = s
    log(f"  {s['entries']:,} entries in {s['seconds']}s: {s['entries_per_second']:,}/s")


def step_probe(r, binary, root, args):
    argv = [binary, "probe", root, "--threads", args.threads, "--duration", f"{args.duration}s"]
    if args.drop_caches:
        argv.append("--drop-caches")
    log(f"[probe] {' '.join(argv)}")
    code, out, err = run_text(argv)
    rows = []
    for line in out.splitlines():
        m = re.match(r"\s*(\d+)\s+(\d+)\s+(\S+)\s+(\S+)", line)
        if m:
            rows.append({"threads": int(m.group(1)), "entries": int(m.group(2)), "time": m.group(3),
                         "entries_per_second": m.group(4)})
            log(f"  {line.strip()}")
    suggested = re.search(r"Suggested: --threads (\d+)", out)
    r["probe"] = {"exit": code, "rows": rows, "suggested": int(suggested.group(1)) if suggested else None,
                  "output": out, "stderr": err}
    log(f"  suggested: {r['probe']['suggested']}")


def step_scans(r, binary, root, work, args):
    suggested = (r.get("probe") or {}).get("suggested") or 32
    counts = sorted({max(1, suggested // 2), suggested, min(1024, suggested * 2)})
    if args.scan_threads:
        counts = [int(t) for t in args.scan_threads.split(",")]
    r["scans"] = []
    for threads in counts:
        out = os.path.join(work, f"scan-t{threads}")
        log(f"[scan] {threads} threads, Parquet output, for {args.duration}s")
        drop_caches(args.drop_caches)
        before = mountstats(r["system"]["mount_point"])
        run = Run(scan_argv(binary, root, threads, out), seconds=args.duration,
                  events_path=os.path.join(work, f"scan-t{threads}.events.jsonl"))
        s = run.summary()
        s["threads"] = threads
        s["rpc"] = rpc_delta(before, mountstats(r["system"]["mount_point"]), s["entries"])
        if os.path.isdir(out):
            s["output_bytes_per_entry"] = round(output_bytes(out) / max(s["entries"], 1), 1)
            if not args.keep:
                shutil.rmtree(out)
        r["scans"].append(s)
        log(f"  {s['entries']:,} entries in {s['seconds']}s: {s['entries_per_second']:,}/s, "
            f"{s['cores_busy']} cores busy, peak memory {s['peak_rss_mib']:,.0f} MiB, {s['errors']} errors")


def step_resume(r, binary, work, args):
    root = args.resume_root
    threads = (r.get("probe") or {}).get("suggested") or 32
    reference = os.path.join(work, "resume-reference")
    interrupted = os.path.join(work, "resume-test")
    log(f"[resume] complete scan of {root} with {threads} threads, for reference")
    drop_caches(args.drop_caches)
    ref = Run(scan_argv(binary, root, threads, reference))
    ref_s = ref.summary()
    log(f"  {ref_s['entries']:,} entries in {ref_s['seconds']}s")
    result = {"root": root, "threads": threads, "reference": ref_s}
    r["resume"] = result
    if ref_s["status"] != "complete":
        result["outcome"] = "skipped: the reference scan did not complete"
        log("  " + result["outcome"])
        return
    kill_at = ref_s["entries"] // 2
    log(f"[resume] same scan, checkpoints every 10s, killed with SIGKILL at about {kill_at:,} entries")
    drop_caches(args.drop_caches)
    killed = Run(scan_argv(binary, root, threads, interrupted, ["--checkpoint-interval", "10s"], "1s"),
                 kill_at_entries=kill_at)
    result["killed"] = killed.summary()
    result["killed"]["killed"] = killed.killed
    if not killed.killed and killed.exit not in (0, None):
        result["outcome"] = f"failed: the scan exited with {killed.exit}: {' '.join(killed.stderr)[-500:]}"
        log("  " + result["outcome"])
        return
    if not killed.killed:
        result["outcome"] = "skipped: the scan finished before it could be killed (use a larger --resume-root)"
        log("  " + result["outcome"])
        return
    resumed = Run([binary, "resume", interrupted, "--progress", "json", "--progress-interval", "5s"])
    result["resumed"] = resumed.summary()
    with open(os.path.join(reference, "manifest.json")) as f:
        want = json.load(f)["totals"]
    with open(os.path.join(interrupted, "manifest.json")) as f:
        manifest = json.load(f)
    got = manifest["totals"]
    verify = run_text([binary, "verify", interrupted])
    # Totals that a changing tree can't explain: the tree may change between
    # runs, so a difference is reported, not judged.
    result["reference_totals"] = want
    result["resumed_totals"] = got
    result["resumes"] = len(manifest.get("resumes", []))
    result["verify"] = {"exit": verify[0], "output": verify[1].strip() + verify[2].strip()}
    same = {k: want.get(k) == got.get(k) for k in ("entries", "by_kind", "bytes", "dirs_read")}
    result["matches"] = same
    result["outcome"] = ("passed: the resumed scan matches the reference exactly" if all(same.values())
                         and verify[0] == 0 else "check: the resumed scan differs from the reference "
                         "(expected only if files changed between the runs)")
    log(f"  {result['outcome']}")
    if not args.keep:
        shutil.rmtree(reference, ignore_errors=True)
        shutil.rmtree(interrupted, ignore_errors=True)


# ---------------------------------------------------------------- report


def rpc_table(rpc):
    if not rpc:
        return []
    lines = ["", "| NFS operation | Calls | Per entry | Avg round trip |", "|---|---:|---:|---:|"]
    for row in rpc[:8]:
        lines.append(f"| {row['op']} | {row['calls']:,} | {row['per_entry']} | {row['avg_rtt_ms']} ms |")
    return lines


def write_report(r, path):
    s = r["system"]
    lines = [
        f"# fs-inventory benchmark: {s['host']}",
        "",
        f"- **When:** {r['started']} ({r['minutes']} minutes)",
        f"- **fs-inventory:** {s['fs_inventory']}",
        f"- **System:** {s['cpus']} CPUs ({s['cpu_model'] or 'unknown model'}), {s['memory_gib']} GiB, "
        f"{s['kernel']} {s['machine']}",
        f"- **Root:** `{s['root']}` on `{s['mount_point']}`: {s['fs_type']} from `{s['source']}`",
        f"- **Mount options:** `{s['mount_options']}`",
        f"- **Caches:** " + ("dropped before every step" if r["drop_caches"]
                             else "not dropped (that needs root): later steps ran partly from cache"),
        f"- **Time box per run:** {r['duration']}s",
    ]
    c = r.get("ceiling") or {}
    lines += ["", "## CPU ceiling (generated tree, no storage)", ""]
    if "skipped" in c:
        lines.append(c["skipped"])
    elif c:
        lines.append(f"{c['entries']:,} entries in {c['seconds']}s with {c['threads']} threads: "
                     f"**{c['entries_per_second']:,} entries/s**, {c['cores_busy']} cores busy, "
                     f"peak memory {c['peak_rss_mib']:,.0f} MiB, {c.get('output_bytes_per_entry')} bytes per entry")
    f = r.get("find")
    if f:
        lines += ["", "## find (single-threaded baseline)", "",
                  f"`{f['command']}`: {f['entries']:,} entries in {f['seconds']}s, "
                  f"**{f['entries_per_second']:,} entries/s**"]
        lines += rpc_table(f.get("rpc"))
    p = r.get("probe")
    if p:
        lines += ["", "## Probe (crawl only, no output)", "", "| Threads | Entries | Time | Entries/s |",
                  "|---:|---:|---:|---:|"]
        for row in p["rows"]:
            lines.append(f"| {row['threads']} | {row['entries']:,} | {row['time']} | {row['entries_per_second']} |")
        lines += ["", f"Suggested: `--threads {p['suggested']}`"]
    if r.get("scans"):
        lines += ["", "## Scans (Parquet output)", "",
                  "| Threads | Entries | Seconds | Entries/s | Cores busy | Peak memory | Bytes/entry | Errors | Stalls |",
                  "|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
        for x in r["scans"]:
            lines.append(f"| {x['threads']} | {x['entries']:,} | {x['seconds']} | {x['entries_per_second']:,} | "
                         f"{x['cores_busy']} | {x['peak_rss_mib']:,.0f} MiB | {x.get('output_bytes_per_entry')} | "
                         f"{x['errors']} | {len(x['stalled'])} |")
        best = max(r["scans"], key=lambda x: x["entries_per_second"])
        if best.get("rpc"):
            lines += ["", f"NFS calls at {best['threads']} threads:"] + rpc_table(best["rpc"])
    res = r.get("resume")
    if res:
        lines += ["", "## Kill and resume", "", f"Root `{res['root']}`, {res['threads']} threads: {res.get('outcome')}"]
        if "resumed_totals" in res:
            lines += ["", "| | Reference | Killed and resumed |", "|---|---:|---:|"]
            for key in ("entries", "dirs_read", "bytes", "errors"):
                lines.append(f"| {key} | {res['reference_totals'].get(key, ''):,} | {res['resumed_totals'].get(key, ''):,} |")
            lines += ["", f"verify: {res['verify']['output']}"]
    if r.get("failures"):
        lines += ["", "## Problems", ""] + [f"- {x}" for x in r["failures"]]
    with open(path, "w") as out:
        out.write("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("root", help="directory tree to measure (read only)")
    parser.add_argument("--bin", default=None, help="fs-inventory binary (default: on PATH, else ./fs-inventory)")
    parser.add_argument("--work-dir", default=None,
                        help="where output, logs, and the report go; use local disk (default: ./fs-inventory-bench-HOST-TIME)")
    parser.add_argument("--duration", type=int, default=60, help="seconds per time-boxed run (default 60)")
    parser.add_argument("--threads", default="8,16,32,64,128,256", help="thread counts for the probe")
    parser.add_argument("--scan-threads", default=None,
                        help="thread counts for the scans (default: the probe's suggestion, half it, and twice it)")
    parser.add_argument("--resume-root", default=None,
                        help="a smaller tree, which a scan covers in a few minutes, for the kill-and-resume test")
    parser.add_argument("--ceiling-entries", default="20M", help="size of the generated tree (default 20M)")
    parser.add_argument("--skip", default="", help="steps to skip, comma-separated: ceiling,find,probe,scans,resume")
    parser.add_argument("--keep", action="store_true", help="keep scan output")
    args = parser.parse_args()

    binary = args.bin or shutil.which("fs-inventory") or os.path.abspath("fs-inventory")
    if not os.access(binary, os.X_OK):
        parser.error(f"fs-inventory binary not found: {binary} (pass --bin)")
    binary = os.path.abspath(binary)
    if not os.path.isdir(args.root):
        parser.error(f"{args.root} is not a directory")
    if args.resume_root and not os.path.isdir(args.resume_root):
        parser.error(f"{args.resume_root} is not a directory")
    args.drop_caches = os.geteuid() == 0 and os.access(DROP_CACHES, os.W_OK)
    skip = {s.strip() for s in args.skip.split(",") if s.strip()}
    root = os.path.realpath(args.root)
    work = os.path.abspath(args.work_dir or
                           f"fs-inventory-bench-{socket.gethostname().split('.')[0]}-{time.strftime('%Y%m%dT%H%M%S')}")
    os.makedirs(work, exist_ok=True)
    if mount_of(work)[0] == mount_of(root)[0] and mount_of(root)[1].startswith(("nfs", "gpfs", "lustre")):
        log(f"warning: the work directory is on the storage being measured ({mount_of(root)[0]}); "
            "scan output will compete with the scans")

    started = time.monotonic()
    r = {"started": time.strftime("%Y-%m-%d %H:%M:%S %Z"), "duration": args.duration,
         "drop_caches": args.drop_caches, "failures": [], "system": system_info(root, binary)}
    s = r["system"]
    log(f"{s['fs_inventory']} on {s['host']}: {s['cpus']} CPUs, {s['memory_gib']} GiB, {s['kernel']}")
    log(f"root {root}: {s['fs_type']} {s['source']} ({s['mount_options']})")
    log("caches: " + ("dropped before every step" if args.drop_caches
                      else f"NOT dropped (run as root, with {DROP_CACHES} writable, to drop them)"))
    log(f"work dir {work}")
    steps = [
        ("ceiling", lambda: step_ceiling(r, binary, work, args)),
        ("find", lambda: step_find(r, root, work, args)),
        ("probe", lambda: step_probe(r, binary, root, args)),
        ("scans", lambda: step_scans(r, binary, root, work, args)),
        ("resume", lambda: step_resume(r, binary, work, args) if args.resume_root else None),
    ]
    for name, step in steps:
        if name in skip:
            continue
        try:
            step()
        except Exception as e:  # Keep going: a partial report is still useful.
            r["failures"].append(f"{name}: {type(e).__name__}: {e}")
            log(f"  {name} failed: {e}")
        with open(os.path.join(work, "results.json"), "w") as f:
            json.dump(r, f, indent=2)

    r["minutes"] = round((time.monotonic() - started) / 60, 1)
    with open(os.path.join(work, "results.json"), "w") as f:
        json.dump(r, f, indent=2)
    report = os.path.join(work, "report.md")
    write_report(r, report)
    bundle = work.rstrip("/") + ".tar.gz"
    with tarfile.open(bundle, "w:gz") as tar:
        for name in sorted(os.listdir(work)):
            full = os.path.join(work, name)
            if os.path.isfile(full):
                tar.add(full, arcname=os.path.join(os.path.basename(work), name))
    log(f"\nreport: {report}\nsend back: {bundle}")


if __name__ == "__main__":
    main()
