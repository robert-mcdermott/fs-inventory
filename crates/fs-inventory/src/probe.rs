// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! The `probe` command: timed crawls at several thread counts, writing no
//! output, to choose `--threads` for a file system.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use fs_inventory_core::backend::{BackendOptions, NativeBackend};
use fs_inventory_core::model::{Entry, ErrorRecord};
use fs_inventory_core::walker::{self, Counters, Sink, WalkOptions};

use crate::cli::ProbeArgs;
use crate::exit::Exit;
use crate::progress::{format_count, format_duration};

/// The suggestion is the smallest thread count within this share of the
/// best rate: past it, more threads mostly add load on the server.
const GOOD_ENOUGH: f64 = 0.9;

/// Discards rows; the walker counts them.
struct Discard;

impl Sink for Discard {
    fn entry(&mut self, _: &Entry<'_>) {}
    fn error(&mut self, _: &ErrorRecord<'_>) {}
    fn end_dir(&mut self) {}
    fn finish(&mut self) {}
}

/// One timed crawl.
#[derive(Debug, Clone, Copy)]
struct Sample {
    threads: u16,
    entries: u64,
    elapsed: Duration,
    finished: bool,
}

impl Sample {
    fn rate(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs > 0.0 {
            self.entries as f64 / secs
        } else {
            0.0
        }
    }
}

pub fn run(args: &ProbeArgs) -> Exit {
    let root = match std::path::absolute(&args.path) {
        Ok(path) => path,
        Err(e) => {
            eprintln!("fs-inventory: invalid path {}: {e}", args.path.display());
            return Exit::Usage;
        }
    };
    if !root.is_dir() {
        eprintln!("fs-inventory: {} is not a directory", root.display());
        return Exit::Usage;
    }
    if args.drop_caches && !cfg!(target_os = "linux") {
        eprintln!("fs-inventory: --drop-caches works only on Linux");
        return Exit::Usage;
    }
    let root: std::path::PathBuf = root.components().collect();
    let roots = vec![std::os::unix::ffi::OsStringExt::into_vec(
        root.into_os_string(),
    )];
    // Fail before printing anything when caches can't be dropped.
    if args.drop_caches
        && let Err(e) = drop_caches()
    {
        eprintln!("fs-inventory: cannot drop caches (needs root): {e}");
        return Exit::Fatal;
    }

    println!(
        "Probing {} for up to {} per thread count.",
        args.path.display(),
        format_duration(args.duration)
    );
    if !args.drop_caches {
        println!(
            "Without --drop-caches (Linux, root), later runs read metadata cached by earlier ones."
        );
    }
    println!(
        "{:>8} {:>14} {:>10} {:>14}",
        "threads", "entries", "time", "entries/s"
    );

    let mut samples = Vec::new();
    for (i, &threads) in args.threads.iter().enumerate() {
        if args.drop_caches
            && i > 0
            && let Err(e) = drop_caches()
        {
            eprintln!("fs-inventory: cannot drop caches (needs root): {e}");
            return Exit::Fatal;
        }
        let sample = sample(&roots, threads, args.duration);
        println!(
            "{:>8} {:>14} {:>10} {:>14}{}",
            sample.threads,
            sample.entries,
            format_duration(sample.elapsed),
            format_count(sample.rate()),
            if sample.finished {
                "  (read the whole tree)"
            } else {
                ""
            }
        );
        samples.push(sample);
    }
    match suggest(&samples) {
        Some(threads) => println!("Suggested: --threads {threads}"),
        None => println!("No suggestion: nothing was read."),
    }
    Exit::Success
}

/// Crawls for at most `duration` with `threads` workers.
fn sample(roots: &[Vec<u8>], threads: u16, duration: Duration) -> Sample {
    let counters = Arc::new(Counters::default());
    let stop = Arc::new(AtomicBool::new(false));
    let timer = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let deadline = Instant::now() + duration;
            while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
            stop.store(true, Ordering::Relaxed);
        })
    };
    let started = Instant::now();
    let result = walker::walk(
        roots,
        WalkOptions {
            threads: usize::from(threads.max(1)),
            ..WalkOptions::default()
        },
        Arc::new(NativeBackend::new(BackendOptions::default())),
        |_| Discard,
        Arc::clone(&counters),
        Arc::clone(&stop),
    );
    let elapsed = started.elapsed();
    stop.store(true, Ordering::Relaxed);
    let _ = timer.join();
    Sample {
        threads,
        entries: counters.entries.load(Ordering::Relaxed),
        elapsed,
        finished: result.complete,
    }
}

/// The smallest thread count within [`GOOD_ENOUGH`] of the best rate.
fn suggest(samples: &[Sample]) -> Option<u16> {
    let best = samples.iter().map(Sample::rate).fold(0.0, f64::max);
    if best <= 0.0 {
        return None;
    }
    samples
        .iter()
        .filter(|s| s.rate() >= best * GOOD_ENOUGH)
        .map(|s| s.threads)
        .min()
}

/// Drops the page, dentry, and inode caches (Linux, root).
fn drop_caches() -> std::io::Result<()> {
    rustix::fs::sync();
    std::fs::write("/proc/sys/vm/drop_caches", "3\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(threads: u16, entries: u64) -> Sample {
        Sample {
            threads,
            entries,
            elapsed: Duration::from_secs(1),
            finished: false,
        }
    }

    #[test]
    fn suggests_the_knee_of_the_curve() {
        assert_eq!(
            suggest(&[s(8, 1000), s(16, 1900), s(32, 2000), s(64, 2050)]),
            Some(16)
        );
        assert_eq!(suggest(&[s(8, 5000), s(16, 3000)]), Some(8));
        assert_eq!(suggest(&[s(8, 0)]), None);
        assert_eq!(suggest(&[]), None);
    }
}
