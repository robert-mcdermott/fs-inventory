// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Output: per-worker sinks that encode rows, and a pool of writer threads
//! that turn them into part files or a stdout stream.
//!
//! Sinks hand over directory-aligned chunks through a bounded channel, so a
//! slow disk slows the crawl down instead of growing memory.

pub mod encode;
pub mod manifest;
pub mod parts;
pub mod timefmt;

use std::io::{self, BufWriter, Write};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::SystemTime;

use crossbeam_channel::{Receiver, Sender};
use flate2::write::GzEncoder;
use fs_inventory_core::model::{Entry, EntryKind, ErrorRecord, Timestamp};
use fs_inventory_core::names::{self, Controls};
use fs_inventory_core::walker::Sink;

use self::encode::{ENTRY_COLUMNS, ERROR_COLUMNS, Encoder};
use self::parts::{PartInfo, PartWriter, RollLimits, compression_suffix};
use crate::parse::Compression;

/// Memory shared by every worker's row buffer. Each worker sends its rows
/// once a directory ends past its share, so memory doesn't grow with the
/// thread count.
const SINK_BUDGET_BYTES: usize = 64 << 20;
/// Bounds on one worker's share.
const MIN_FLUSH_BYTES: usize = 64 << 10;
const MAX_FLUSH_BYTES: usize = 1 << 20;
/// A worker sends mid-directory once its buffer reaches this multiple of its
/// share, so very large directories don't hold unbounded memory.
const MAX_BUFFER_FACTOR: usize = 4;
/// Send buffered error rows once a directory ends past this size.
const ERROR_FLUSH_BYTES: usize = 64 << 10;

/// Where rows go.
#[derive(Debug, Clone)]
pub enum Destination {
    /// Part files under a scan directory.
    Dir(PathBuf),
    /// One stream on stdout; error rows go to stderr as text.
    Stdout,
}

#[derive(Debug, Clone)]
pub struct OutputConfig {
    pub destination: Destination,
    pub encoder: Encoder,
    pub compression: Compression,
    pub writers: usize,
    /// Crawl threads, each with its own sink.
    pub crawl_threads: usize,
    pub limits: RollLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Table {
    Entries,
    Errors,
}

struct Chunk {
    table: Table,
    bytes: Vec<u8>,
    rows: u64,
}

/// State shared by every sink.
struct Shared {
    encoder: Encoder,
    /// Each sink sends its rows at a directory boundary past this size.
    flush_bytes: usize,
    errors_to_stderr: bool,
    kinds: [AtomicU64; EntryKind::ALL.len()],
    error_rows: AtomicU64,
    stop: Arc<AtomicBool>,
}

/// What the writers produced.
#[derive(Debug, Default)]
pub struct OutputReport {
    pub entry_parts: Vec<PartInfo>,
    pub error_parts: Vec<PartInfo>,
    /// Rows written, by kind, in [`EntryKind::ALL`] order.
    pub kinds: [u64; EntryKind::ALL.len()],
    pub error_rows: u64,
    /// The first write failure, if any. Output after it was discarded.
    pub failure: Option<io::Error>,
}

/// A running output pipeline.
pub struct Output {
    shared: Arc<Shared>,
    tx: Sender<Chunk>,
    writers: Vec<JoinHandle<WriterResult>>,
}

type WriterResult = (Vec<PartInfo>, Vec<PartInfo>, Option<io::Error>);

impl Output {
    /// Starts the writer threads. `stop` is set if output fails, so the
    /// crawl ends early instead of producing rows nobody can store.
    pub fn start(config: OutputConfig, stop: Arc<AtomicBool>) -> io::Result<Self> {
        let writers = match config.destination {
            Destination::Dir(_) => config.writers.max(1),
            Destination::Stdout => 1,
        };
        let (tx, rx) = crossbeam_channel::bounded::<Chunk>(writers * 4);
        let flush_bytes = (SINK_BUDGET_BYTES / config.crawl_threads.max(1))
            .clamp(MIN_FLUSH_BYTES, MAX_FLUSH_BYTES);
        let shared = Arc::new(Shared {
            encoder: config.encoder,
            flush_bytes,
            errors_to_stderr: matches!(config.destination, Destination::Stdout),
            kinds: Default::default(),
            error_rows: AtomicU64::new(0),
            stop: Arc::clone(&stop),
        });
        let mut handles = Vec::with_capacity(writers);
        for index in 0..writers {
            let rx = rx.clone();
            let config = config.clone();
            let stop = Arc::clone(&stop);
            let handle = std::thread::Builder::new()
                .name(format!("fs-inventory-write-{index}"))
                .spawn(move || match &config.destination {
                    Destination::Dir(dir) => write_parts(rx, &config, dir.clone(), index, &stop),
                    Destination::Stdout => write_stdout(rx, &config, &stop),
                })?;
            handles.push(handle);
        }
        Ok(Self {
            shared,
            tx,
            writers: handles,
        })
    }

    /// A sink for one crawl worker.
    pub fn sink(&self) -> TextSink {
        TextSink {
            tx: self.tx.clone(),
            shared: Arc::clone(&self.shared),
            entries: Vec::new(),
            entry_rows: 0,
            errors: Vec::new(),
            error_rows: 0,
            kinds: [0; EntryKind::ALL.len()],
        }
    }

    /// Waits for every chunk to be written and commits the open parts. Call
    /// after every sink has finished.
    pub fn finish(self) -> OutputReport {
        drop(self.tx);
        let mut report = OutputReport::default();
        for handle in self.writers {
            match handle.join() {
                Ok((entries, errors, failure)) => {
                    report.entry_parts.extend(entries);
                    report.error_parts.extend(errors);
                    if report.failure.is_none() {
                        report.failure = failure;
                    }
                }
                Err(_) => {
                    report.failure.get_or_insert_with(|| {
                        io::Error::other("an output writer thread panicked")
                    });
                }
            }
        }
        for (count, kind) in report.kinds.iter_mut().zip(&self.shared.kinds) {
            *count = kind.load(Ordering::Relaxed);
        }
        report.error_rows = self.shared.error_rows.load(Ordering::Relaxed);
        report
    }
}

/// Receives every chunk; after a failure, keeps draining (and discarding) so
/// sinks never block on a full channel.
fn drain_after_failure(rx: &Receiver<Chunk>) {
    for _ in rx.iter() {}
}

fn write_parts(
    rx: Receiver<Chunk>,
    config: &OutputConfig,
    dir: PathBuf,
    index: usize,
    stop: &AtomicBool,
) -> WriterResult {
    let suffix = compression_suffix(config.compression);
    let extension = format!("{}{suffix}", config.encoder.format().extension());
    let prefix = format!("part-w{index:02}");
    let new_writer = |table, columns: &[&str]| {
        PartWriter::new(
            &dir,
            table,
            prefix.clone(),
            extension.clone(),
            config.encoder.header(columns),
            config.compression,
            config.limits,
        )
    };
    let mut entries = new_writer("entries", &ENTRY_COLUMNS);
    let mut errors = new_writer("errors", &ERROR_COLUMNS);

    let mut failure = None;
    for chunk in rx.iter() {
        let writer = match chunk.table {
            Table::Entries => &mut entries,
            Table::Errors => &mut errors,
        };
        if let Err(e) = writer.write(&chunk.bytes, chunk.rows) {
            failure = Some(e);
            stop.store(true, Ordering::Relaxed);
            drain_after_failure(&rx);
            break;
        }
    }
    let mut finished = |writer: PartWriter| match writer.finish() {
        Ok(parts) => parts,
        Err(e) => {
            failure.get_or_insert(e);
            Vec::new()
        }
    };
    let entry_parts = finished(entries);
    let error_parts = finished(errors);
    (entry_parts, error_parts, failure)
}

/// A compressed or plain stream over stdout.
enum StdoutStream {
    Plain(BufWriter<io::Stdout>),
    Zstd(zstd::Encoder<'static, BufWriter<io::Stdout>>),
    Gzip(GzEncoder<BufWriter<io::Stdout>>),
}

impl StdoutStream {
    fn new(compression: Compression) -> io::Result<Self> {
        let out = BufWriter::with_capacity(1 << 20, io::stdout());
        Ok(match compression {
            Compression::Zstd(level) => Self::Zstd(zstd::Encoder::new(out, level)?),
            Compression::Gzip(level) => {
                Self::Gzip(GzEncoder::new(out, flate2::Compression::new(level)))
            }
            Compression::None | Compression::Snappy | Compression::Lz4 => Self::Plain(out),
        })
    }

    fn writer(&mut self) -> &mut dyn Write {
        match self {
            Self::Plain(w) => w,
            Self::Zstd(w) => w,
            Self::Gzip(w) => w,
        }
    }

    fn finish(self) -> io::Result<()> {
        let mut out = match self {
            Self::Plain(w) => w,
            Self::Zstd(w) => w.finish()?,
            Self::Gzip(w) => w.finish()?,
        };
        out.flush()
    }
}

fn write_stdout(rx: Receiver<Chunk>, config: &OutputConfig, stop: &AtomicBool) -> WriterResult {
    let result = (|| -> io::Result<()> {
        let mut stream = StdoutStream::new(config.compression)?;
        stream
            .writer()
            .write_all(&config.encoder.header(&ENTRY_COLUMNS))?;
        let stderr = io::stderr();
        for chunk in rx.iter() {
            match chunk.table {
                Table::Entries => stream.writer().write_all(&chunk.bytes)?,
                Table::Errors => stderr.lock().write_all(&chunk.bytes)?,
            }
        }
        stream.finish()
    })();
    let failure = result.err();
    if failure.is_some() {
        stop.store(true, Ordering::Relaxed);
        drain_after_failure(&rx);
    }
    (Vec::new(), Vec::new(), failure)
}

/// Encodes one crawl worker's rows and hands them to the writers.
pub struct TextSink {
    tx: Sender<Chunk>,
    shared: Arc<Shared>,
    entries: Vec<u8>,
    entry_rows: u64,
    errors: Vec<u8>,
    error_rows: u64,
    kinds: [u64; EntryKind::ALL.len()],
}

impl TextSink {
    fn send(&mut self, table: Table) {
        let (buf, rows) = match table {
            Table::Entries => (&mut self.entries, &mut self.entry_rows),
            Table::Errors => (&mut self.errors, &mut self.error_rows),
        };
        if buf.is_empty() {
            return;
        }
        // Size the next buffer to the share, not to whatever this one grew
        // to, so one large directory doesn't keep its memory forever.
        let capacity = match table {
            Table::Entries => self.shared.flush_bytes + self.shared.flush_bytes / 4,
            Table::Errors => 0,
        };
        let chunk = Chunk {
            table,
            bytes: std::mem::replace(buf, Vec::with_capacity(capacity)),
            rows: std::mem::take(rows),
        };
        if self.tx.send(chunk).is_err() {
            // Every writer has stopped; the failure is reported by `finish`.
            self.shared.stop.store(true, Ordering::Relaxed);
        }
    }
}

impl Sink for TextSink {
    fn entry(&mut self, entry: &Entry<'_>) {
        // If encoding ever panics, drop the partial row before the walker
        // records the failure, so no part file gets a truncated line.
        let mark = self.entries.len();
        let encoder = self.shared.encoder;
        let entries = &mut self.entries;
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| encoder.entry(entry, entries))) {
            self.entries.truncate(mark);
            resume_unwind(panic);
        }
        self.entry_rows += 1;
        if let Some(i) = EntryKind::ALL.iter().position(|k| *k == entry.kind()) {
            self.kinds[i] += 1;
        }
        if self.entries.len() >= self.shared.flush_bytes * MAX_BUFFER_FACTOR {
            self.send(Table::Entries);
        }
    }

    fn error(&mut self, error: &ErrorRecord<'_>) {
        if self.shared.errors_to_stderr {
            let path = names::to_text(error.path, Controls::Escape);
            let line = format!(
                "fs-inventory: {} {path}: {}\n",
                error.info.op.as_str(),
                error.info.message
            );
            self.errors.extend_from_slice(line.as_bytes());
        } else {
            self.shared.encoder.error(error, now(), &mut self.errors);
        }
        self.error_rows += 1;
        self.shared.error_rows.fetch_add(1, Ordering::Relaxed);
    }

    fn end_dir(&mut self) {
        if self.entries.len() >= self.shared.flush_bytes {
            self.send(Table::Entries);
        }
        if self.errors.len() >= ERROR_FLUSH_BYTES {
            self.send(Table::Errors);
        }
    }

    fn finish(&mut self) {
        self.send(Table::Entries);
        self.send(Table::Errors);
        for (shared, local) in self.shared.kinds.iter().zip(&mut self.kinds) {
            shared.fetch_add(std::mem::take(local), Ordering::Relaxed);
        }
    }
}

/// The current time.
pub fn now() -> Timestamp {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => Timestamp {
            sec: i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
            nsec: d.subsec_nanos(),
        },
        Err(e) => {
            let d = e.duration();
            Timestamp::new(
                -i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
                -i64::from(d.subsec_nanos()),
            )
        }
    }
}
