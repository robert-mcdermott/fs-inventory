// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Output: per-worker sinks that buffer rows, and a pool of writer threads
//! that turn them into part files or a stdout stream.
//!
//! Sinks hand over directory-aligned chunks through a bounded channel, so a
//! slow disk slows the crawl down instead of growing memory. Sinks only
//! buffer rows (text, or Arrow columns); writers do the expensive Parquet
//! encoding and compression, so `--writers` sets the CPU spent on it.

pub mod arrow;
pub mod encode;
pub mod manifest;
pub mod parquet;
pub mod parts;
pub mod schema;
pub mod timefmt;

use std::io::{self, BufWriter, Write};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::SystemTime;

use arrow_array::RecordBatch;
use crossbeam_channel::{Receiver, Sender};
use flate2::write::GzEncoder;
use fs_inventory_core::model::{Entry, EntryKind, ErrorRecord, Timestamp};
use fs_inventory_core::names::{self, Controls};
use fs_inventory_core::walker::Sink;

use self::arrow::{ArrowOptions, EntryBatchBuilder, ErrorBatchBuilder, entry_schema};
use self::encode::Encoder;
use self::parquet::{ParquetPartWriter, ParquetSettings};
use self::parts::{PartInfo, PartWriter, RollLimits, compression_suffix};
use self::schema::{ERROR_COLUMNS, Selection, arrow_schema};
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
/// Send buffered error rows once a directory ends past this many.
const ERROR_FLUSH_ROWS: usize = 256;
/// Chunks queued per active writer before another writer starts.
const BACKLOG_PER_WRITER: usize = 4;

/// Where rows go.
#[derive(Debug, Clone)]
pub enum Destination {
    /// Part files under a scan directory.
    Dir(PathBuf),
    /// One stream on stdout (text formats only); error rows go to stderr.
    Stdout,
}

/// How rows are encoded.
#[derive(Debug, Clone)]
pub enum Format {
    /// CSV or JSON Lines.
    Text(Encoder),
    Parquet {
        arrow: ArrowOptions,
        settings: ParquetSettings,
    },
}

#[derive(Debug, Clone)]
pub struct OutputConfig {
    pub destination: Destination,
    pub format: Format,
    pub selection: Selection,
    /// Compression for text output; Parquet takes it from its settings.
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

enum Payload {
    Text(Vec<u8>),
    Batch(RecordBatch),
}

struct Chunk {
    table: Table,
    payload: Payload,
    rows: u64,
}

/// State shared by every sink.
struct Shared {
    format: Format,
    selection: Selection,
    /// Each sink sends its rows at a directory boundary past this size.
    flush_bytes: usize,
    errors_to_stderr: bool,
    kinds: [AtomicU64; EntryKind::ALL.len()],
    error_rows: AtomicU64,
    stop: Arc<AtomicBool>,
    /// A failure inside a sink, reported with the writers' failures.
    failure: Mutex<Option<io::Error>>,
    /// Writers start one at a time as the backlog grows, up to `max_writers`.
    writers: Mutex<Vec<JoinHandle<WriterResult>>>,
    active_writers: AtomicUsize,
    max_writers: usize,
    rx: Receiver<Chunk>,
    config: OutputConfig,
}

impl Shared {
    /// Starts another writer if there is room for one.
    fn add_writer(&self) -> io::Result<()> {
        let Ok(mut writers) = self.writers.lock() else {
            return Ok(());
        };
        let index = writers.len();
        if index >= self.max_writers {
            return Ok(());
        }
        let rx = self.rx.clone();
        let config = self.config.clone();
        let stop = Arc::clone(&self.stop);
        let handle = std::thread::Builder::new()
            .name(format!("fs-inventory-write-{index}"))
            .spawn(move || match &config.destination {
                Destination::Dir(dir) => write_parts(rx, &config, dir.clone(), index, &stop),
                Destination::Stdout => write_stdout(rx, &config, &stop),
            })?;
        writers.push(handle);
        self.active_writers.store(writers.len(), Ordering::Relaxed);
        Ok(())
    }

    fn fail(&self, error: io::Error) {
        self.stop.store(true, Ordering::Relaxed);
        if let Ok(mut failure) = self.failure.lock() {
            failure.get_or_insert(error);
        }
    }
}

/// What the writers produced.
#[derive(Debug, Default)]
pub struct OutputReport {
    pub entry_parts: Vec<PartInfo>,
    pub error_parts: Vec<PartInfo>,
    /// Rows written, by kind, in [`EntryKind::ALL`] order.
    pub kinds: [u64; EntryKind::ALL.len()],
    pub error_rows: u64,
    /// The first failure, if any. Output after it was discarded.
    pub failure: Option<io::Error>,
}

/// A running output pipeline.
pub struct Output {
    shared: Arc<Shared>,
    tx: Sender<Chunk>,
}

type WriterResult = (Vec<PartInfo>, Vec<PartInfo>, Option<io::Error>);

impl Output {
    /// Starts the writer threads. `stop` is set if output fails, so the
    /// crawl ends early instead of producing rows nobody can store.
    /// Writers start one at a time, up to `config.writers`, as chunks back
    /// up, so a small scan writes one file per table.
    pub fn start(config: OutputConfig, stop: Arc<AtomicBool>) -> io::Result<Self> {
        let max_writers = match config.destination {
            Destination::Dir(_) => config.writers.max(1),
            Destination::Stdout => 1,
        };
        let (tx, rx) = crossbeam_channel::bounded::<Chunk>(max_writers * BACKLOG_PER_WRITER);
        let flush_bytes = (SINK_BUDGET_BYTES / config.crawl_threads.max(1))
            .clamp(MIN_FLUSH_BYTES, MAX_FLUSH_BYTES);
        let shared = Arc::new(Shared {
            format: config.format.clone(),
            selection: config.selection,
            flush_bytes,
            errors_to_stderr: matches!(config.destination, Destination::Stdout),
            kinds: Default::default(),
            error_rows: AtomicU64::new(0),
            stop: Arc::clone(&stop),
            failure: Mutex::new(None),
            writers: Mutex::new(Vec::with_capacity(max_writers)),
            active_writers: AtomicUsize::new(0),
            max_writers,
            rx,
            config,
        });
        shared.add_writer()?;
        Ok(Self { shared, tx })
    }

    /// A sink for one crawl worker.
    pub fn sink(&self) -> RowSink {
        let (entries, errors) = match &self.shared.format {
            Format::Text(_) => (EntryBuffer::Text(Vec::new()), ErrorBuffer::Text(Vec::new())),
            Format::Parquet { arrow, .. } => (
                EntryBuffer::Arrow(Box::new(EntryBatchBuilder::new(
                    *arrow,
                    self.shared.selection,
                ))),
                ErrorBuffer::Arrow(Box::new(ErrorBatchBuilder::new(*arrow))),
            ),
        };
        RowSink {
            tx: self.tx.clone(),
            shared: Arc::clone(&self.shared),
            entries,
            entry_rows: 0,
            errors,
            error_rows: 0,
            kinds: [0; EntryKind::ALL.len()],
        }
    }

    /// Waits for every chunk to be written and commits the open parts. Call
    /// after every sink has finished.
    pub fn finish(self) -> OutputReport {
        drop(self.tx);
        let handles = match self.shared.writers.lock() {
            Ok(mut writers) => std::mem::take(&mut *writers),
            Err(_) => Vec::new(),
        };
        let mut report = OutputReport::default();
        for handle in handles {
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
        if let Ok(mut failure) = self.shared.failure.lock()
            && let Some(e) = failure.take()
        {
            report.failure.get_or_insert(e);
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

/// Writes one table in whichever format the scan uses.
#[allow(clippy::large_enum_variant, reason = "two per writer thread")]
enum TableWriter {
    Text(PartWriter),
    Parquet(ParquetPartWriter),
}

impl TableWriter {
    fn write(&mut self, chunk: &Chunk) -> io::Result<()> {
        match (self, &chunk.payload) {
            (Self::Text(w), Payload::Text(bytes)) => w.write(bytes, chunk.rows),
            (Self::Parquet(w), Payload::Batch(batch)) => w.write(batch),
            _ => Err(io::Error::other(
                "output chunk does not match the output format",
            )),
        }
    }

    fn finish(self) -> io::Result<Vec<PartInfo>> {
        match self {
            Self::Text(w) => w.finish(),
            Self::Parquet(w) => w.finish(),
        }
    }
}

fn table_writers(
    config: &OutputConfig,
    dir: &std::path::Path,
    index: usize,
) -> io::Result<(TableWriter, TableWriter)> {
    let prefix = format!("part-w{index:02}");
    Ok(match &config.format {
        Format::Text(encoder) => {
            let suffix = compression_suffix(config.compression);
            let extension = format!("{}{suffix}", encoder.format().extension());
            let writer = |table, columns: Vec<&str>| {
                TableWriter::Text(PartWriter::new(
                    dir,
                    table,
                    prefix.clone(),
                    extension.clone(),
                    encoder.header(&columns),
                    config.compression,
                    config.limits,
                ))
            };
            (
                writer("entries", encoder.entry_columns()),
                writer("errors", encoder.error_columns()),
            )
        }
        Format::Parquet { arrow, settings } => {
            let entries = ParquetPartWriter::new(
                dir,
                "entries",
                prefix.clone(),
                entry_schema(*arrow, config.selection),
                settings,
                config.limits,
            )?;
            let errors = ParquetPartWriter::new(
                dir,
                "errors",
                prefix,
                arrow_schema(&ERROR_COLUMNS, arrow.unit),
                settings,
                config.limits,
            )?;
            (TableWriter::Parquet(entries), TableWriter::Parquet(errors))
        }
    })
}

fn write_parts(
    rx: Receiver<Chunk>,
    config: &OutputConfig,
    dir: PathBuf,
    index: usize,
    stop: &AtomicBool,
) -> WriterResult {
    let (mut entries, mut errors) = match table_writers(config, &dir, index) {
        Ok(writers) => writers,
        Err(e) => {
            stop.store(true, Ordering::Relaxed);
            drain_after_failure(&rx);
            return (Vec::new(), Vec::new(), Some(e));
        }
    };

    let mut failure = None;
    for chunk in rx.iter() {
        let writer = match chunk.table {
            Table::Entries => &mut entries,
            Table::Errors => &mut errors,
        };
        if let Err(e) = writer.write(&chunk) {
            failure = Some(e);
            stop.store(true, Ordering::Relaxed);
            drain_after_failure(&rx);
            break;
        }
    }
    let mut finished = |writer: TableWriter| match writer.finish() {
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
        let Format::Text(encoder) = &config.format else {
            return Err(io::Error::other("stdout output must be CSV or JSON Lines"));
        };
        let mut stream = StdoutStream::new(config.compression)?;
        stream
            .writer()
            .write_all(&encoder.header(&encoder.entry_columns()))?;
        let stderr = io::stderr();
        for chunk in rx.iter() {
            let Payload::Text(bytes) = &chunk.payload else {
                return Err(io::Error::other("stdout output must be CSV or JSON Lines"));
            };
            match chunk.table {
                Table::Entries => stream.writer().write_all(bytes)?,
                Table::Errors => stderr.lock().write_all(bytes)?,
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

/// Buffered entry rows.
enum EntryBuffer {
    Text(Vec<u8>),
    Arrow(Box<EntryBatchBuilder>),
}

/// Buffered error rows.
enum ErrorBuffer {
    Text(Vec<u8>),
    Arrow(Box<ErrorBatchBuilder>),
}

/// Buffers one crawl worker's rows and hands them to the writers.
pub struct RowSink {
    tx: Sender<Chunk>,
    shared: Arc<Shared>,
    entries: EntryBuffer,
    entry_rows: u64,
    errors: ErrorBuffer,
    error_rows: u64,
    kinds: [u64; EntryKind::ALL.len()],
}

impl RowSink {
    fn entry_bytes(&self) -> usize {
        match &self.entries {
            EntryBuffer::Text(buf) => buf.len(),
            EntryBuffer::Arrow(builder) => builder.bytes(),
        }
    }

    fn send_entries(&mut self) {
        if self.entry_rows == 0 {
            return;
        }
        let payload = match &mut self.entries {
            EntryBuffer::Text(buf) => {
                // Size the next buffer to the share, not to whatever this one
                // grew to, so one large directory doesn't keep its memory.
                let capacity = self.shared.flush_bytes + self.shared.flush_bytes / 4;
                Payload::Text(std::mem::replace(buf, Vec::with_capacity(capacity)))
            }
            EntryBuffer::Arrow(builder) => match builder.finish() {
                Ok(batch) => Payload::Batch(batch),
                Err(e) => {
                    self.entry_rows = 0;
                    self.shared.fail(io::Error::other(e));
                    return;
                }
            },
        };
        let rows = std::mem::take(&mut self.entry_rows);
        self.send(Table::Entries, payload, rows);
    }

    fn send_errors(&mut self) {
        if self.error_rows == 0 {
            return;
        }
        let payload = match &mut self.errors {
            ErrorBuffer::Text(buf) => Payload::Text(std::mem::take(buf)),
            ErrorBuffer::Arrow(builder) => match builder.finish() {
                Ok(batch) => Payload::Batch(batch),
                Err(e) => {
                    self.error_rows = 0;
                    self.shared.fail(io::Error::other(e));
                    return;
                }
            },
        };
        let rows = std::mem::take(&mut self.error_rows);
        self.send(Table::Errors, payload, rows);
    }

    fn send(&self, table: Table, payload: Payload, rows: u64) {
        let active = self.shared.active_writers.load(Ordering::Relaxed);
        if active < self.shared.max_writers && self.tx.len() >= active * BACKLOG_PER_WRITER {
            // The writers are falling behind; failure to start one only
            // means the existing writers keep going alone.
            let _ = self.shared.add_writer();
        }
        if self
            .tx
            .send(Chunk {
                table,
                payload,
                rows,
            })
            .is_err()
        {
            // Every writer has stopped; the failure is reported by `finish`.
            self.shared.stop.store(true, Ordering::Relaxed);
        }
    }
}

impl Sink for RowSink {
    fn entry(&mut self, entry: &Entry<'_>) {
        match &mut self.entries {
            EntryBuffer::Text(buf) => {
                // If encoding ever panics, drop the partial row before the
                // walker records the failure, so no part gets a truncated line.
                let mark = buf.len();
                let Format::Text(encoder) = &self.shared.format else {
                    return;
                };
                if let Err(panic) = catch_unwind(AssertUnwindSafe(|| encoder.entry(entry, buf))) {
                    buf.truncate(mark);
                    resume_unwind(panic);
                }
            }
            // Computes every value before appending any, so a panic leaves
            // the columns consistent.
            EntryBuffer::Arrow(builder) => builder.append(entry),
        }
        self.entry_rows += 1;
        if let Some(i) = EntryKind::ALL.iter().position(|k| *k == entry.kind()) {
            self.kinds[i] += 1;
        }
        if self.entry_bytes() >= self.shared.flush_bytes * MAX_BUFFER_FACTOR {
            self.send_entries();
        }
    }

    fn error(&mut self, error: &ErrorRecord<'_>) {
        match (&mut self.errors, &self.shared.format) {
            (ErrorBuffer::Text(buf), _) if self.shared.errors_to_stderr => {
                let path = names::to_text(error.path, Controls::Escape);
                let line = format!(
                    "fs-inventory: {} {path}: {}\n",
                    error.info.op.as_str(),
                    error.info.message
                );
                buf.extend_from_slice(line.as_bytes());
            }
            (ErrorBuffer::Text(buf), Format::Text(encoder)) => encoder.error(error, now(), buf),
            (ErrorBuffer::Arrow(builder), _) => builder.append(error, now()),
            (ErrorBuffer::Text(_), Format::Parquet { .. }) => return,
        }
        self.error_rows += 1;
        self.shared.error_rows.fetch_add(1, Ordering::Relaxed);
    }

    fn end_dir(&mut self) {
        if self.entry_bytes() >= self.shared.flush_bytes {
            self.send_entries();
        }
        if self.error_rows as usize >= ERROR_FLUSH_ROWS {
            self.send_errors();
        }
    }

    fn finish(&mut self) {
        self.send_entries();
        self.send_errors();
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
