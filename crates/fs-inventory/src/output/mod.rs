// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Output: per-worker sinks that buffer rows, and a pool of writer threads
//! that turn them into part files or a stdout stream.
//!
//! Sinks hand over directory-aligned chunks through a bounded channel, so a
//! slow disk slows the crawl down instead of growing memory. Sinks only
//! buffer rows (text, or Arrow columns); writers do the expensive Parquet
//! encoding and compression, so `--writers` sets the CPU spent on it.
//!
//! Each sink's buffer is shared with the output, so if the walker abandons a
//! stuck worker, [`Output::finish`] still writes the rows of every directory
//! that worker finished, and drops only the one it was stuck in.
//!
//! For a checkpoint, [`Output::checkpoint`] sends every sink's rows, waits
//! for the writers to write them, and has each writer commit its open parts,
//! so every row delivered so far is in a committed part.

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
use std::sync::{Arc, Mutex, TryLockError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use arrow_array::RecordBatch;
use crossbeam_channel::{Receiver, Sender};
use flate2::write::GzEncoder;
use fs_inventory_core::model::{Entry, EntryKind, ErrorRecord, Timestamp};
use fs_inventory_core::names::{self, Controls};
use fs_inventory_core::walker::Sink;

use self::arrow::{ArrowOptions, EntryBatchBuilder, ErrorBatchBuilder, entry_schema};
use self::encode::Encoder;
use self::parquet::{ParquetPartWriter, ParquetSettings};
use self::parts::{PartInfo, PartWriter, RollLimits, compression_suffix, sync_dir};
use self::schema::{ERROR_COLUMNS, Selection, arrow_schema};
use crate::parse::Compression;

/// Default memory shared by every worker's row buffer. Each worker sends its
/// rows once a directory ends past its share, so memory doesn't grow with
/// the thread count.
pub const SINK_BUDGET_BYTES: usize = 64 << 20;
/// Bounds on one worker's share.
pub const MIN_FLUSH_BYTES: usize = 64 << 10;
const MAX_FLUSH_BYTES: usize = 1 << 20;
/// A worker sends mid-directory once its buffer reaches this multiple of its
/// share, so very large directories don't hold unbounded memory.
const MAX_BUFFER_FACTOR: usize = 4;
/// Send buffered error rows once a directory ends past this many.
const ERROR_FLUSH_ROWS: u64 = 256;
/// Chunks queued per active writer before another writer starts.
const BACKLOG_PER_WRITER: usize = 4;
/// How long [`Output::finish`] waits to salvage a stuck worker's rows.
const SALVAGE_WAIT: Duration = Duration::from_secs(5);

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
    /// Memory shared by the sinks' row buffers.
    pub sink_budget: usize,
    pub limits: RollLimits,
    /// Resumed runs number their parts `part-rN-wNN-...`, so they never
    /// collide with earlier runs' parts.
    pub run: u32,
    /// What earlier runs of a resumed scan committed.
    pub previous: OutputState,
}

/// Every committed part, and the rows in them.
#[derive(Debug, Clone, Default)]
pub struct OutputState {
    pub entry_parts: Vec<PartInfo>,
    pub error_parts: Vec<PartInfo>,
    /// Entry rows by kind, in [`EntryKind::ALL`] order.
    pub kinds: [u64; EntryKind::ALL.len()],
    pub error_rows: u64,
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

/// A request to one writer.
enum Control {
    /// Commit the open parts and reply with every part committed so far.
    Commit(Sender<io::Result<WriterParts>>),
}

/// A writer's committed entry and error parts.
type WriterParts = (Vec<PartInfo>, Vec<PartInfo>);

/// State shared by every sink and writer.
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
    /// One per writer, in the same order.
    controls: Mutex<Vec<Sender<Control>>>,
    /// Chunks sent and not yet written.
    in_flight: Arc<AtomicU64>,
    /// Sinks salvaged at a checkpoint, before the output finished.
    salvaged: AtomicUsize,
    active_writers: AtomicUsize,
    max_writers: usize,
    /// Taken when the output finishes, so no new writer starts.
    rx: Mutex<Option<Receiver<Chunk>>>,
    /// Dropped to tell writers to drain the channel and stop, even while a
    /// stuck worker still holds a sender.
    shutdown: Mutex<Option<Sender<()>>>,
    shutdown_rx: Receiver<()>,
    /// Every sink's buffer, for salvage.
    sinks: Mutex<Vec<Arc<Mutex<SinkState>>>>,
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
        let Some(rx) = self.rx.lock().ok().and_then(|rx| rx.clone()) else {
            return Ok(());
        };
        let shutdown = self.shutdown_rx.clone();
        let config = self.config.clone();
        let stop = Arc::clone(&self.stop);
        let (control_tx, control) = crossbeam_channel::bounded(1);
        let in_flight = Arc::clone(&self.in_flight);
        let handle = std::thread::Builder::new()
            .name(format!("fs-inventory-write-{index}"))
            .spawn(move || {
                let input = Input {
                    rx,
                    shutdown,
                    control,
                    in_flight,
                };
                match &config.destination {
                    Destination::Dir(dir) => {
                        write_parts(&input, &config, dir.clone(), index, &stop)
                    }
                    Destination::Stdout => write_stdout(&input, &config, &stop),
                }
            })?;
        writers.push(handle);
        if let Ok(mut controls) = self.controls.lock() {
            controls.push(control_tx);
        }
        self.active_writers.store(writers.len(), Ordering::Relaxed);
        Ok(())
    }

    fn fail(&self, error: io::Error) {
        self.stop.store(true, Ordering::Relaxed);
        if let Ok(mut failure) = self.failure.lock() {
            failure.get_or_insert(error);
        }
    }

    fn add_kinds(&self, kinds: &mut [u64; EntryKind::ALL.len()]) {
        for (shared, local) in self.kinds.iter().zip(kinds) {
            shared.fetch_add(std::mem::take(local), Ordering::Relaxed);
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
    /// Sinks whose rows were salvaged after their worker was abandoned.
    pub salvaged: usize,
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
        let (shutdown, shutdown_rx) = crossbeam_channel::bounded::<()>(0);
        let flush_bytes = (config.sink_budget / config.crawl_threads.max(1))
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
            controls: Mutex::new(Vec::with_capacity(max_writers)),
            in_flight: Arc::new(AtomicU64::new(0)),
            salvaged: AtomicUsize::new(0),
            active_writers: AtomicUsize::new(0),
            max_writers,
            rx: Mutex::new(Some(rx)),
            shutdown: Mutex::new(Some(shutdown)),
            shutdown_rx,
            sinks: Mutex::new(Vec::new()),
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
        let state = Arc::new(Mutex::new(SinkState {
            tx: Some(self.tx.clone()),
            entries,
            entry_rows: 0,
            errors,
            error_rows: 0,
            mark_rows: 0,
            mark_bytes: 0,
            kinds: [0; EntryKind::ALL.len()],
            pending_kinds: [0; EntryKind::ALL.len()],
            closed: false,
            salvaged: None,
        }));
        if let Ok(mut sinks) = self.shared.sinks.lock() {
            sinks.push(Arc::clone(&state));
        }
        RowSink {
            shared: Arc::clone(&self.shared),
            state,
        }
    }

    /// Writes every chunk, salvages the rows of sinks whose worker never
    /// finished (it was abandoned while stuck), and commits the open parts.
    /// Call after the walk returns.
    pub fn finish(self) -> OutputReport {
        drop(self.tx);
        let sinks = match self.shared.sinks.lock() {
            Ok(mut sinks) => std::mem::take(&mut *sinks),
            Err(_) => Vec::new(),
        };
        let mut salvaged = self.shared.salvaged.load(Ordering::Relaxed);
        for state in sinks {
            if salvage(&state, &self.shared) {
                salvaged += 1;
            }
        }
        // Writers drain what's queued and stop, even if a stuck worker still
        // holds a sender.
        drop(self.shared.shutdown.lock().ok().and_then(|mut s| s.take()));
        drop(self.shared.rx.lock().ok().and_then(|mut rx| rx.take()));
        let handles = match self.shared.writers.lock() {
            Ok(mut writers) => std::mem::take(&mut *writers),
            Err(_) => Vec::new(),
        };
        let previous = &self.shared.config.previous;
        let mut report = OutputReport {
            entry_parts: previous.entry_parts.clone(),
            error_parts: previous.error_parts.clone(),
            salvaged,
            ..OutputReport::default()
        };
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
        for ((count, kind), before) in report
            .kinds
            .iter_mut()
            .zip(&self.shared.kinds)
            .zip(previous.kinds)
        {
            *count = before + kind.load(Ordering::Relaxed);
        }
        report.error_rows = previous.error_rows + self.shared.error_rows.load(Ordering::Relaxed);
        report
    }

    /// Makes every row the sinks hold durable, for a checkpoint: sends each
    /// sink's rows, salvaging the sinks of `abandoned` workers (by sink
    /// index), waits until the writers have written them all, and has each
    /// writer commit its open parts. Every other sink must be at a directory
    /// boundary, with its worker paused. Returns every committed part.
    pub fn checkpoint(&self, abandoned: &[usize]) -> io::Result<OutputState> {
        let shared = &self.shared;
        let sinks = shared
            .sinks
            .lock()
            .map(|sinks| sinks.clone())
            .map_err(|_| io::Error::other("output sinks poisoned"))?;
        let mut kinds = [0u64; EntryKind::ALL.len()];
        for (index, state) in sinks.iter().enumerate() {
            if abandoned.contains(&index) {
                if salvage(state, shared) {
                    shared.salvaged.fetch_add(1, Ordering::Relaxed);
                }
                continue;
            }
            let Ok(mut state) = state.lock() else {
                continue;
            };
            if state.closed {
                continue;
            }
            state.commit_kinds();
            state.send_entries(shared);
            state.send_errors(shared);
            for (total, kind) in kinds.iter_mut().zip(state.kinds) {
                *total += kind;
            }
        }
        // Writers may still be writing what was just sent.
        while shared.in_flight.load(Ordering::Acquire) > 0 {
            let stopped = shared
                .writers
                .lock()
                .is_ok_and(|w| w.iter().any(JoinHandle::is_finished));
            if stopped {
                return Err(io::Error::other("an output writer stopped"));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let previous = &shared.config.previous;
        let mut state = OutputState {
            entry_parts: previous.entry_parts.clone(),
            error_parts: previous.error_parts.clone(),
            kinds,
            error_rows: previous.error_rows + shared.error_rows.load(Ordering::Relaxed),
        };
        for ((total, kind), before) in state
            .kinds
            .iter_mut()
            .zip(&shared.kinds)
            .zip(previous.kinds)
        {
            *total += before + kind.load(Ordering::Relaxed);
        }
        let controls = shared
            .controls
            .lock()
            .map(|c| c.clone())
            .map_err(|_| io::Error::other("output writers poisoned"))?;
        for control in controls {
            let (reply, replies) = crossbeam_channel::bounded(1);
            let stopped = || io::Error::other("an output writer stopped");
            control
                .send(Control::Commit(reply))
                .map_err(|_| stopped())?;
            let (entries, errors) = replies.recv().map_err(|_| stopped())??;
            state.entry_parts.extend(entries);
            state.error_parts.extend(errors);
        }
        if let Destination::Dir(dir) = &shared.config.destination {
            for table in ["entries", "errors"] {
                let table = dir.join(table);
                if table.exists() {
                    sync_dir(&table)?;
                }
            }
        }
        Ok(state)
    }
}

/// Sends the rows of a sink whose worker never finished, up to the last
/// directory it completed. Returns whether there was anything to salvage.
fn salvage(state: &Mutex<SinkState>, shared: &Shared) -> bool {
    // The stuck worker only holds the lock while buffering a row, never
    // during file system calls, so this rarely waits.
    let deadline = Instant::now() + SALVAGE_WAIT;
    let mut guard = loop {
        match state.try_lock() {
            Ok(guard) => break guard,
            Err(TryLockError::Poisoned(poisoned)) => break poisoned.into_inner(),
            Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(TryLockError::WouldBlock) => return false,
        }
    };
    let state = &mut *guard;
    if state.closed {
        return false;
    }
    // Drop the rows of the directory the worker was stuck in.
    let keep = state.mark_rows;
    match &mut state.entries {
        EntryBuffer::Text(buf) => buf.truncate(state.mark_bytes),
        EntryBuffer::Arrow(builder) => match builder.finish() {
            Ok(batch) => {
                let length = usize::try_from(keep)
                    .unwrap_or(usize::MAX)
                    .min(batch.num_rows());
                state.salvaged = Some(batch.slice(0, length));
            }
            Err(e) => shared.fail(io::Error::other(e)),
        },
    }
    state.entry_rows = keep;
    state.pending_kinds = [0; EntryKind::ALL.len()];
    state.closed = true;
    state.send_entries(shared);
    state.send_errors(shared);
    shared.add_kinds(&mut state.kinds);
    state.tx = None;
    true
}

/// Writer input: the chunk channel, this writer's requests, and the
/// shutdown signal.
struct Input {
    rx: Receiver<Chunk>,
    shutdown: Receiver<()>,
    control: Receiver<Control>,
    /// Chunks sent and not yet written; decremented by [`Self::done`].
    in_flight: Arc<AtomicU64>,
}

enum Event {
    Chunk(Chunk),
    Control(Control),
}

impl Input {
    /// The next chunk or request, or `None` once the channel is empty after
    /// shutdown (or every sender is gone).
    fn next(&self) -> Option<Event> {
        crossbeam_channel::select! {
            recv(self.rx) -> chunk => chunk.ok().map(Event::Chunk),
            recv(self.control) -> control => match control {
                Ok(control) => Some(Event::Control(control)),
                // The output outlives its writers, so this doesn't happen;
                // carry on with chunks.
                Err(_) => self.rx.recv().ok().map(Event::Chunk),
            },
            recv(self.shutdown) -> _ => self.rx.try_recv().ok().map(Event::Chunk),
        }
    }

    /// A chunk has been written (or discarded).
    fn done(&self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }

    /// Discards everything until shutdown, so sinks never block on a full
    /// channel after a writer fails, and refuses commits.
    fn drain(&self) {
        while let Some(event) = self.next() {
            match event {
                Event::Chunk(_) => self.done(),
                Event::Control(Control::Commit(reply)) => {
                    let _ = reply.send(Err(io::Error::other("output failed earlier")));
                }
            }
        }
    }
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

    fn checkpoint(&mut self) -> io::Result<Vec<PartInfo>> {
        match self {
            Self::Text(w) => w.checkpoint(),
            Self::Parquet(w) => w.checkpoint(),
        }
    }
}

fn table_writers(
    config: &OutputConfig,
    dir: &std::path::Path,
    index: usize,
) -> io::Result<(TableWriter, TableWriter)> {
    let prefix = match config.run {
        0 => format!("part-w{index:02}"),
        run => format!("part-r{run}-w{index:02}"),
    };
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
    input: &Input,
    config: &OutputConfig,
    dir: PathBuf,
    index: usize,
    stop: &AtomicBool,
) -> WriterResult {
    let (mut entries, mut errors) = match table_writers(config, &dir, index) {
        Ok(writers) => writers,
        Err(e) => {
            stop.store(true, Ordering::Relaxed);
            input.drain();
            return (Vec::new(), Vec::new(), Some(e));
        }
    };

    let mut failure = None;
    while let Some(event) = input.next() {
        let result = match event {
            Event::Chunk(chunk) => {
                let writer = match chunk.table {
                    Table::Entries => &mut entries,
                    Table::Errors => &mut errors,
                };
                let result = writer.write(&chunk);
                input.done();
                result
            }
            Event::Control(Control::Commit(reply)) => {
                let parts = entries
                    .checkpoint()
                    .and_then(|e| errors.checkpoint().map(|r| (e, r)));
                let reply_with = match &parts {
                    Ok(parts) => Ok(parts.clone()),
                    Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
                };
                let _ = reply.send(reply_with);
                parts.map(|_| ())
            }
        };
        if let Err(e) = result {
            failure = Some(e);
            stop.store(true, Ordering::Relaxed);
            input.drain();
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

fn write_stdout(input: &Input, config: &OutputConfig, stop: &AtomicBool) -> WriterResult {
    let result = (|| -> io::Result<()> {
        let Format::Text(encoder) = &config.format else {
            return Err(io::Error::other("stdout output must be CSV or JSON Lines"));
        };
        let mut stream = StdoutStream::new(config.compression)?;
        stream
            .writer()
            .write_all(&encoder.header(&encoder.entry_columns()))?;
        let stderr = io::stderr();
        while let Some(event) = input.next() {
            let chunk = match event {
                Event::Chunk(chunk) => chunk,
                // Streams take no checkpoints; there are no parts.
                Event::Control(Control::Commit(reply)) => {
                    let _ = reply.send(Ok((Vec::new(), Vec::new())));
                    continue;
                }
            };
            input.done();
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
        input.drain();
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

/// One sink's buffers, shared with the output for salvage.
struct SinkState {
    /// `None` once the sink finished or was salvaged.
    tx: Option<Sender<Chunk>>,
    entries: EntryBuffer,
    entry_rows: u64,
    errors: ErrorBuffer,
    error_rows: u64,
    /// Rows (and text bytes) in `entries` as of the last directory boundary.
    mark_rows: u64,
    mark_bytes: usize,
    /// Rows by kind, sent or up to the mark.
    kinds: [u64; EntryKind::ALL.len()],
    /// Rows by kind since the mark.
    pending_kinds: [u64; EntryKind::ALL.len()],
    /// Set by `finish` or salvage; later rows are dropped.
    closed: bool,
    /// The salvaged Arrow rows, already sliced to the mark.
    salvaged: Option<RecordBatch>,
}

impl SinkState {
    fn entry_bytes(&self) -> usize {
        match &self.entries {
            EntryBuffer::Text(buf) => buf.len(),
            EntryBuffer::Arrow(builder) => builder.bytes(),
        }
    }

    /// Everything since the mark becomes committed (it's being sent).
    fn commit_kinds(&mut self) {
        for (kind, pending) in self.kinds.iter_mut().zip(&mut self.pending_kinds) {
            *kind += std::mem::take(pending);
        }
    }

    fn send_entries(&mut self, shared: &Shared) {
        if self.entry_rows == 0 {
            return;
        }
        let payload = match (self.salvaged.take(), &mut self.entries) {
            (Some(batch), _) => Payload::Batch(batch),
            (None, EntryBuffer::Text(buf)) => {
                // Size the next buffer to the share, not to whatever this one
                // grew to, so one large directory doesn't keep its memory.
                let capacity = shared.flush_bytes + shared.flush_bytes / 4;
                Payload::Text(std::mem::replace(buf, Vec::with_capacity(capacity)))
            }
            (None, EntryBuffer::Arrow(builder)) => match builder.finish() {
                Ok(batch) => Payload::Batch(batch),
                Err(e) => {
                    self.entry_rows = 0;
                    shared.fail(io::Error::other(e));
                    return;
                }
            },
        };
        let rows = std::mem::take(&mut self.entry_rows);
        self.mark_rows = 0;
        self.mark_bytes = 0;
        self.commit_kinds();
        self.send(shared, Table::Entries, payload, rows);
    }

    fn send_errors(&mut self, shared: &Shared) {
        if self.error_rows == 0 {
            return;
        }
        let payload = match &mut self.errors {
            ErrorBuffer::Text(buf) => Payload::Text(std::mem::take(buf)),
            ErrorBuffer::Arrow(builder) => match builder.finish() {
                Ok(batch) => Payload::Batch(batch),
                Err(e) => {
                    self.error_rows = 0;
                    shared.fail(io::Error::other(e));
                    return;
                }
            },
        };
        let rows = std::mem::take(&mut self.error_rows);
        self.send(shared, Table::Errors, payload, rows);
    }

    fn send(&self, shared: &Shared, table: Table, payload: Payload, rows: u64) {
        let Some(tx) = &self.tx else { return };
        let active = shared.active_writers.load(Ordering::Relaxed);
        // A closing sink's last rows arrive in a burst with every other
        // worker's, which doesn't mean the writers are falling behind.
        if !self.closed && active < shared.max_writers && tx.len() >= active * BACKLOG_PER_WRITER {
            // The writers are falling behind; failure to start one only
            // means the existing writers keep going alone.
            let _ = shared.add_writer();
        }
        shared.in_flight.fetch_add(1, Ordering::AcqRel);
        if tx
            .send(Chunk {
                table,
                payload,
                rows,
            })
            .is_err()
        {
            // Every writer has stopped; the failure is reported by `finish`.
            shared.in_flight.fetch_sub(1, Ordering::AcqRel);
            shared.stop.store(true, Ordering::Relaxed);
        }
    }
}

/// Buffers one crawl worker's rows and hands them to the writers.
pub struct RowSink {
    shared: Arc<Shared>,
    state: Arc<Mutex<SinkState>>,
}

impl Sink for RowSink {
    fn entry(&mut self, entry: &Entry<'_>) {
        let Ok(mut guard) = self.state.lock() else {
            return;
        };
        let state = &mut *guard;
        if state.closed {
            return;
        }
        let appended = match &mut state.entries {
            EntryBuffer::Text(buf) => {
                // If encoding ever panics, drop the partial row before the
                // walker records the failure, so no part gets a truncated line.
                let mark = buf.len();
                let Format::Text(encoder) = &self.shared.format else {
                    return;
                };
                let result = catch_unwind(AssertUnwindSafe(|| encoder.entry(entry, buf)));
                if result.is_err() {
                    buf.truncate(mark);
                }
                result
            }
            // Computes every value before appending any, so a panic leaves
            // the columns consistent.
            EntryBuffer::Arrow(builder) => catch_unwind(AssertUnwindSafe(|| builder.append(entry))),
        };
        if let Err(panic) = appended {
            // Release the lock before the walker handles the panic.
            drop(guard);
            resume_unwind(panic);
        }
        state.entry_rows += 1;
        if let Some(i) = EntryKind::ALL.iter().position(|k| *k == entry.kind()) {
            state.pending_kinds[i] += 1;
        }
        if state.entry_bytes() >= self.shared.flush_bytes * MAX_BUFFER_FACTOR {
            state.send_entries(&self.shared);
        }
    }

    fn error(&mut self, error: &ErrorRecord<'_>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.closed {
            return;
        }
        match (&mut state.errors, &self.shared.format) {
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
        state.error_rows += 1;
        self.shared.error_rows.fetch_add(1, Ordering::Relaxed);
    }

    fn end_dir(&mut self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.closed {
            return;
        }
        state.mark_rows = state.entry_rows;
        state.mark_bytes = match &state.entries {
            EntryBuffer::Text(buf) => buf.len(),
            EntryBuffer::Arrow(_) => 0,
        };
        state.commit_kinds();
        if state.entry_bytes() >= self.shared.flush_bytes {
            state.send_entries(&self.shared);
        }
        if state.error_rows >= ERROR_FLUSH_ROWS {
            state.send_errors(&self.shared);
        }
    }

    fn finish(&mut self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.closed {
            return;
        }
        state.closed = true;
        state.send_entries(&self.shared);
        state.send_errors(&self.shared);
        state.commit_kinds();
        self.shared.add_kinds(&mut state.kinds);
        state.tx = None;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::encode::{EncodeOptions, TextFormat, TimeStyle};
    use crate::output::timefmt::Precision;
    use fs_inventory_core::model::{Fields, Stat};

    fn config(dir: &std::path::Path) -> OutputConfig {
        OutputConfig {
            destination: Destination::Dir(dir.to_path_buf()),
            format: Format::Text(Encoder::new(EncodeOptions {
                format: TextFormat::Jsonl,
                raw_paths_always: false,
                csv_raw_controls: false,
                precision: Precision::Micros,
                time_style: TimeStyle::Iso,
                selection: Selection::ALL,
                fields: Fields::ALL,
            })),
            selection: Selection::ALL,
            compression: Compression::None,
            writers: 2,
            crawl_threads: 2,
            sink_budget: SINK_BUDGET_BYTES,
            limits: RollLimits {
                max_bytes: u64::MAX,
                max_rows: None,
            },
            run: 0,
            previous: OutputState::default(),
        }
    }

    fn row(sink: &mut RowSink, path: &[u8]) {
        let stat = Stat {
            mode: 0o100644,
            ..Stat::default()
        };
        sink.entry(&Entry {
            path,
            parent_path: b"/d",
            name: &path[3..],
            depth: 1,
            dir_id: None,
            parent_id: 1,
            stat: &stat,
            is_mount_point: false,
            totals: None,
            error: None,
        });
    }

    #[test]
    fn abandoned_sinks_keep_completed_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let output = Output::start(config(tmp.path()), Arc::new(AtomicBool::new(false))).unwrap();

        // A healthy worker.
        let mut healthy = output.sink();
        row(&mut healthy, b"/d/a");
        healthy.end_dir();
        healthy.finish();

        // A worker that finished one directory, then got stuck in the next.
        let mut stuck = output.sink();
        row(&mut stuck, b"/d/done-1");
        row(&mut stuck, b"/d/done-2");
        stuck.end_dir();
        row(&mut stuck, b"/d/half-read");

        let report = output.finish();
        assert_eq!(report.salvaged, 1);
        assert!(report.failure.is_none());
        let mut paths = Vec::new();
        for part in &report.entry_parts {
            let text = std::fs::read_to_string(tmp.path().join(&part.file)).unwrap();
            for line in text.lines() {
                let value: serde_json::Value = serde_json::from_str(line).unwrap();
                paths.push(value["path"].as_str().unwrap().to_string());
            }
        }
        paths.sort();
        assert_eq!(paths, ["/d/a", "/d/done-1", "/d/done-2"]);
        assert_eq!(report.kinds.iter().sum::<u64>(), 3);

        // Rows the stuck worker writes later are dropped.
        row(&mut stuck, b"/d/too-late");
        stuck.end_dir();
    }

    fn paths_in(dir: &std::path::Path, parts: &[PartInfo]) -> Vec<String> {
        let mut paths = Vec::new();
        for part in parts {
            let text = std::fs::read_to_string(dir.join(&part.file)).unwrap();
            for line in text.lines() {
                let value: serde_json::Value = serde_json::from_str(line).unwrap();
                paths.push(value["path"].as_str().unwrap().to_string());
            }
        }
        paths.sort();
        paths
    }

    #[test]
    fn checkpoints_commit_every_delivered_row() {
        let tmp = tempfile::tempdir().unwrap();
        let output = Output::start(config(tmp.path()), Arc::new(AtomicBool::new(false))).unwrap();
        let mut first = output.sink();
        let mut second = output.sink();
        let mut stuck = output.sink();
        row(&mut first, b"/d/a");
        first.end_dir();
        row(&mut second, b"/d/b");
        second.end_dir();
        row(&mut stuck, b"/d/done");
        stuck.end_dir();
        row(&mut stuck, b"/d/half-read");

        // The third sink's worker was abandoned mid-directory.
        let state = output.checkpoint(&[2]).unwrap();
        assert_eq!(state.kinds.iter().sum::<u64>(), 3);
        // Committed and complete on disk, with nothing left in progress.
        assert_eq!(
            paths_in(tmp.path(), &state.entry_parts),
            ["/d/a", "/d/b", "/d/done"]
        );
        let leftovers = std::fs::read_dir(tmp.path().join("entries"))
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "tmp")
            })
            .count();
        assert_eq!(leftovers, 0);

        // Later rows go to new parts.
        row(&mut first, b"/d/c");
        first.end_dir();
        first.finish();
        second.finish();
        let report = output.finish();
        assert!(report.failure.is_none());
        assert_eq!(report.salvaged, 1);
        assert_eq!(
            paths_in(tmp.path(), &report.entry_parts),
            ["/d/a", "/d/b", "/d/c", "/d/done"]
        );
        assert!(report.entry_parts.len() > state.entry_parts.len());
        assert_eq!(report.kinds.iter().sum::<u64>(), 4);
    }

    #[test]
    fn resumed_runs_keep_earlier_parts() {
        let tmp = tempfile::tempdir().unwrap();
        let earlier = PartInfo {
            file: "entries/part-w00-000000.jsonl".into(),
            rows: 7,
            bytes: 100,
        };
        let mut kinds = [0; EntryKind::ALL.len()];
        kinds[0] = 7;
        let config = OutputConfig {
            run: 2,
            previous: OutputState {
                entry_parts: vec![earlier.clone()],
                error_parts: Vec::new(),
                kinds,
                error_rows: 1,
            },
            ..config(tmp.path())
        };
        let output = Output::start(config, Arc::new(AtomicBool::new(false))).unwrap();
        let mut sink = output.sink();
        row(&mut sink, b"/d/new");
        sink.end_dir();
        sink.finish();
        let report = output.finish();
        assert_eq!(report.entry_parts[0], earlier);
        assert_eq!(
            report.entry_parts[1].file,
            "entries/part-r2-w00-000000.jsonl"
        );
        assert_eq!(report.kinds.iter().sum::<u64>(), 8);
        assert_eq!(report.error_rows, 1);
    }
}
