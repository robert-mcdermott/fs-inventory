// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Periodic progress reports on stderr.

use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{RecvTimeoutError, Sender};
use fs_inventory_core::walker::Counters;

/// How progress is reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Style {
    Plain,
    Json,
    None,
}

/// A snapshot of the crawl counters.
#[derive(Debug, Clone, Copy, Default)]
pub struct Snapshot {
    pub elapsed: Duration,
    pub entries: u64,
    pub dirs: u64,
    pub bytes: u64,
    pub errors: u64,
    pub vanished: u64,
    pub queued: u64,
}

impl Snapshot {
    pub fn take(counters: &Counters, started: Instant) -> Self {
        Self {
            elapsed: started.elapsed(),
            entries: counters.entries.load(Ordering::Relaxed),
            dirs: counters.dirs.load(Ordering::Relaxed),
            bytes: counters.bytes.load(Ordering::Relaxed),
            errors: counters.errors.load(Ordering::Relaxed),
            vanished: counters.vanished.load(Ordering::Relaxed),
            queued: counters.pending.load(Ordering::Relaxed),
        }
    }

    fn rate(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs > 0.0 {
            self.entries as f64 / secs
        } else {
            0.0
        }
    }
}

/// A running progress reporter.
pub struct Reporter {
    stop: Option<Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl Reporter {
    /// Starts reporting every `interval`. With [`Style::None`], reports nothing.
    pub fn start(
        style: Style,
        interval: Duration,
        counters: Arc<Counters>,
        started: Instant,
    ) -> Self {
        if style == Style::None {
            return Self {
                stop: None,
                handle: None,
            };
        }
        let (tx, rx) = crossbeam_channel::bounded::<()>(0);
        let handle = std::thread::Builder::new()
            .name("fs-inventory-progress".into())
            .spawn(move || {
                let mut last = Snapshot::default();
                loop {
                    match rx.recv_timeout(interval) {
                        Err(RecvTimeoutError::Timeout) => {}
                        _ => return,
                    }
                    let now = Snapshot::take(&counters, started);
                    report(style, &now, &last);
                    last = now;
                }
            })
            .ok();
        Self {
            stop: Some(tx),
            handle,
        }
    }

    /// Stops reporting.
    pub fn stop(mut self) {
        drop(self.stop.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn report(style: Style, now: &Snapshot, last: &Snapshot) {
    let window = now.elapsed.saturating_sub(last.elapsed).as_secs_f64();
    let recent_rate = if window > 0.0 {
        now.entries.saturating_sub(last.entries) as f64 / window
    } else {
        0.0
    };
    let line = match style {
        Style::Plain => format!(
            "fs-inventory: {}  {} entries ({}/s)  {} dirs  {}  {} errors  {} queued\n",
            format_duration(now.elapsed),
            format_count(now.entries as f64),
            format_count(recent_rate),
            format_count(now.dirs as f64),
            format_bytes(now.bytes),
            now.errors,
            now.queued,
        ),
        Style::Json => format!(
            "{}\n",
            serde_json::json!({
                "event": "progress",
                "elapsed_seconds": round3(now.elapsed.as_secs_f64()),
                "entries": now.entries,
                "entries_per_second": recent_rate.round(),
                "dirs": now.dirs,
                "bytes": now.bytes,
                "errors": now.errors,
                "vanished": now.vanished,
                "queued": now.queued,
            })
        ),
        Style::None => return,
    };
    let _ = std::io::stderr().write_all(line.as_bytes());
}

/// Prints the end-of-scan summary.
pub fn summary(style: Style, status: &str, snapshot: &Snapshot, output: &str) {
    let line = match style {
        Style::None => return,
        Style::Plain => format!(
            "fs-inventory: {status}: {} entries in {} ({}/s), {} dirs, {}, {} errors, {} vanished; output: {output}\n",
            snapshot.entries,
            format_duration(snapshot.elapsed),
            format_count(snapshot.rate()),
            snapshot.dirs,
            format_bytes(snapshot.bytes),
            snapshot.errors,
            snapshot.vanished,
        ),
        Style::Json => format!(
            "{}\n",
            serde_json::json!({
                "event": "done",
                "status": status,
                "elapsed_seconds": round3(snapshot.elapsed.as_secs_f64()),
                "entries": snapshot.entries,
                "entries_per_second": snapshot.rate().round(),
                "dirs": snapshot.dirs,
                "bytes": snapshot.bytes,
                "errors": snapshot.errors,
                "vanished": snapshot.vanished,
                "output": output,
            })
        ),
    };
    let _ = std::io::stderr().write_all(line.as_bytes());
}

fn round3(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
}

/// Formats a count compactly: `950`, `12.3k`, `4.56M`, `1.20B`.
pub fn format_count(n: f64) -> String {
    match n {
        n if n < 1e3 => format!("{n:.0}"),
        n if n < 1e6 => format!("{:.1}k", n / 1e3),
        n if n < 1e9 => format!("{:.2}M", n / 1e6),
        n => format!("{:.2}B", n / 1e9),
    }
}

/// Formats bytes with binary units: `512 B`, `1.5 KiB`, `3.20 TiB`.
pub fn format_bytes(n: u64) -> String {
    const UNITS: [&str; 6] = ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    if n < 1024 {
        return format!("{n} B");
    }
    let mut value = n as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.2} {}", UNITS[unit])
}

/// Formats a duration as `H:MM:SS`.
pub fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    format!("{}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formatting() {
        assert_eq!(format_count(950.0), "950");
        assert_eq!(format_count(12_345.0), "12.3k");
        assert_eq!(format_count(4_560_000.0), "4.56M");
        assert_eq!(format_count(1_200_000_000.0), "1.20B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1536), "1.50 KiB");
        assert_eq!(format_bytes(3 << 40), "3.00 TiB");
        assert_eq!(format_bytes(u64::MAX), "16.00 EiB");
        assert_eq!(format_duration(Duration::from_secs(3_725)), "1:02:05");
    }
}
