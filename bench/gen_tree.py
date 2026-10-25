#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Robert McDermott
"""Generate a synthetic directory tree for benchmarking fs-inventory.

The tree has a fixed fan-out of subdirectories per directory and a fixed
number of files per directory, filled breadth-first until the requested
number of files exists. Files are empty unless --file-size is given, so the
tree is cheap to create and the benchmark measures metadata operations.

Examples:
    python3 -I bench/gen_tree.py /scratch/bench/1m --files 1000000
    python3 -I bench/gen_tree.py /nfs/bench/10m --files 10000000 --fanout 20 --files-per-dir 200
    python3 -I bench/gen_tree.py /tmp/names --files 50000 --names mixed

Uses only the Python standard library.
"""

import argparse
import collections
import os
import random
import sys
import time

# Name stems for --names mixed: multi-byte UTF-8 in several scripts, NFD
# accents, spaces, and punctuation. On Linux, a few names are also invalid
# UTF-8 (Latin-1 and Shift-JIS bytes), as written by legacy systems.
MIXED_STEMS = [
    "report", "données", "日本語のファイル", "数据", "данные", "παράδειγμα",
    "café", "with space", "comma,name", "-dash", "UPPER", "résumé",
]
LEGACY_STEMS = [b"caf\xe9", b"\x83e\x83X\x83g", b"na\xefve"]
EXTENSIONS = ["txt", "csv", "dat", "h5", "fastq", "bam", "tif", "py", "log", "gz"]


def file_name(index, style, rng):
    if style == "ascii":
        return f"f{index:08d}.{EXTENSIONS[index % len(EXTENSIONS)]}".encode()
    ext = EXTENSIONS[index % len(EXTENSIONS)]
    if sys.platform.startswith("linux") and rng.random() < 0.01:
        stem = rng.choice(LEGACY_STEMS)
    else:
        stem = rng.choice(MIXED_STEMS).encode()
    return stem + f"-{index:08d}.{ext}".encode()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("root", help="directory to create (must not exist)")
    parser.add_argument("--files", type=int, default=1_000_000, help="total files (default 1,000,000)")
    parser.add_argument("--fanout", type=int, default=10, help="subdirectories per directory (default 10)")
    parser.add_argument("--files-per-dir", type=int, default=100, help="files per directory (default 100)")
    parser.add_argument("--file-size", type=int, default=0, help="bytes of data per file (default 0)")
    parser.add_argument("--names", choices=["ascii", "mixed"], default="ascii",
                        help="ascii names, or mixed international and legacy names")
    parser.add_argument("--seed", type=int, default=1, help="random seed for --names mixed")
    args = parser.parse_args()

    if os.path.exists(args.root):
        parser.error(f"{args.root} already exists")
    rng = random.Random(args.seed)
    payload = b"x" * args.file_size
    started = time.monotonic()
    root = os.fsencode(args.root)
    os.makedirs(root)
    # Directories are created only when they are filled, so the tree has no
    # empty leaves.
    queue = collections.deque([root])
    files = dirs = 0
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY

    while files < args.files:
        directory = queue.popleft()
        if directory != root:
            os.mkdir(directory)
            dirs += 1
        for _ in range(min(args.files_per_dir, args.files - files)):
            fd = os.open(os.path.join(directory, file_name(files, args.names, rng)), flags, 0o644)
            if payload:
                os.write(fd, payload)
            os.close(fd)
            files += 1
        queue.extend(os.path.join(directory, f"d{d:03d}".encode()) for d in range(args.fanout))
        if files % 100_000 < args.files_per_dir:
            rate = files / max(time.monotonic() - started, 1e-9)
            print(f"\r{files:,} files, {dirs:,} directories ({rate:,.0f} files/s)", end="", file=sys.stderr)

    elapsed = time.monotonic() - started
    print(f"\rcreated {files:,} files and {dirs + 1:,} directories under {args.root} in {elapsed:.1f}s",
          file=sys.stderr)


if __name__ == "__main__":
    main()
