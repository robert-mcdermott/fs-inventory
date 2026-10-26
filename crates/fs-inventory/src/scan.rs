// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! The `scan` command: set up output, crawl, and report.

use std::collections::BTreeMap;
use std::collections::hash_map::RandomState;
use std::fs::{self, File};
use std::hash::{BuildHasher as _, Hasher as _};
use std::io::{self, IsTerminal as _};
use std::os::unix::ffi::OsStringExt as _;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use fs_inventory_core::backend::synthetic::{SyntheticBackend, SyntheticOptions};
use fs_inventory_core::backend::{Backend, BackendOptions, NativeBackend};
use fs_inventory_core::model::{EntryKind, Fields, errno_name};
use fs_inventory_core::names::{self, Controls};
use fs_inventory_core::walker::{
    self, Checkpointer, Counters, Start, WalkOptions, WalkResult, WalkState,
};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::checkpoint;
use crate::cli::{Format, Progress, ProgressOptions, RawPaths, ScanArgs, TimeFormat, TimeUnit};
use crate::exit::Exit;
use crate::output::arrow::ArrowOptions;
use crate::output::encode::{EncodeOptions, Encoder, TextFormat, TimeStyle};
use crate::output::manifest::{
    ErrorSummary, Manifest, Parts, Resume as ResumeRecord, Root, StalledDir, Status, Totals,
};
use crate::output::parquet::ParquetSettings;
use crate::output::parts::{RollLimits, sync_dir};
use crate::output::schema::{ENTRY_COLUMNS, ERROR_COLUMNS, Selection};
use crate::output::timefmt::{self, Precision};
use crate::output::{
    Destination, Format as OutputFormat, Output, OutputConfig, OutputReport, OutputState, now,
};
use crate::parse::{Compression, parse_compression};
use crate::progress::{self, Reporter, Snapshot, Style};

/// What a scan holds besides its budgeted buffers, measured on Linux with
/// generated trees and the allocator returning freed memory at once, as it
/// does under `--max-memory`: the process itself, and each Parquet writer's
/// encoders.
const BASE_MEMORY: usize = 64 << 20;
const WRITER_MEMORY: usize = 32 << 20;
/// The smallest in-progress row group, and queue of waiting directories.
const MIN_ROW_GROUP: usize = 8 << 20;
const MIN_QUEUE: usize = 16 << 20;
/// The share of a budget planned for: peak memory varied by up to a tenth
/// between identical scans.
const PLANNED: f64 = 0.875;

/// Memory the crawl threads hold, given each one's row buffer share:
/// batches on their way to the writers, and what the allocator keeps.
/// Measured with 16 to 256 threads and shares of 64 KiB to 1 MiB, it grows
/// with the square root of the thread count, since each thread holds less
/// when more of them share the work.
fn thread_memory(threads: usize, share: usize) -> usize {
    let per_root = (40 << 20) as f64 + 48.0 * share as f64;
    (per_root * (threads as f64).sqrt()) as usize
}

/// How `--max-memory` is divided.
struct MemoryBudgets {
    /// Directories waiting to be read; more wait in a spill file.
    queue: usize,
    /// The crawl threads' row buffers, together.
    sinks: usize,
    /// In-progress Parquet row groups, across all writers.
    row_groups: usize,
    /// With a budget too small for the crawl threads and writers, about
    /// the budget they need.
    shortfall: Option<usize>,
}

impl MemoryBudgets {
    /// Without a limit, the defaults. With one, what the threads and writers
    /// hold regardless comes first. Of the rest, half is for queued
    /// directories, a quarter for the crawl threads' row buffers (at what
    /// they really cost), and a quarter for Parquet row groups.
    fn new(max_memory: Option<u64>, threads: usize, parquet_writers: usize) -> Self {
        let Some(m) = max_memory.map(|m| usize::try_from(m).unwrap_or(usize::MAX)) else {
            return Self {
                queue: 1 << 30,
                sinks: crate::output::SINK_BUDGET_BYTES,
                row_groups: usize::MAX,
                shortfall: None,
            };
        };
        let planned = (m as f64 * PLANNED) as usize;
        let fixed = BASE_MEMORY + parquet_writers * WRITER_MEMORY + thread_memory(threads, 0);
        let rest = planned.saturating_sub(fixed);
        // The row buffer share that costs a quarter of the rest.
        let per_byte = thread_memory(threads, 1 << 20) - thread_memory(threads, 0);
        let share = ((rest / 4) as f64 / per_byte as f64 * f64::from(1 << 20)) as usize;
        let least = BASE_MEMORY
            + parquet_writers * (WRITER_MEMORY + MIN_ROW_GROUP)
            + thread_memory(threads, crate::output::MIN_FLUSH_BYTES)
            + MIN_QUEUE;
        Self {
            queue: (rest / 2).max(MIN_QUEUE),
            sinks: share
                .saturating_mul(threads)
                .min(crate::output::SINK_BUDGET_BYTES),
            row_groups: rest / 4,
            shortfall: (least > planned).then(|| (least as f64 / PLANNED) as usize),
        }
    }

    /// The most Parquet writers whose fixed memory fits in a quarter of
    /// `max_memory`.
    fn writers_within(max_memory: u64) -> usize {
        let quarter = usize::try_from(max_memory / 4).unwrap_or(usize::MAX);
        (quarter / (WRITER_MEMORY + MIN_ROW_GROUP)).max(1)
    }

    /// The row group limit for each of `writers` writers.
    fn row_group(&self, writers: usize) -> usize {
        (self.row_groups / writers.max(1))
            .clamp(8 << 20, crate::output::parquet::MAX_ROW_GROUP_BYTES)
    }
}

/// Report a crawl thread as stalled after this long without progress.
const STALL_REPORT: Duration = Duration::from_secs(60);

/// Default interval between checkpoints.
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(600);

/// A problem found before the crawl starts.
pub struct SetupError {
    pub exit: Exit,
    pub message: String,
}

impl SetupError {
    pub fn usage(message: impl Into<String>) -> Self {
        Self {
            exit: Exit::Usage,
            message: message.into(),
        }
    }

    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            exit: Exit::Fatal,
            message: message.into(),
        }
    }
}

/// Reports a setup error and turns the outcome into an exit code.
pub fn report(outcome: Result<Exit, SetupError>) -> Exit {
    match outcome {
        Ok(exit) => exit,
        Err(e) => {
            eprintln!("fs-inventory: {}", e.message);
            e.exit
        }
    }
}

/// Everything that shapes a scan's rows and files. Checkpoints store it, so
/// a resumed scan writes exactly what the original would have.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanSettings {
    /// Roots, as exact bytes in base64.
    roots: Vec<String>,
    format: Format,
    compression: String,
    /// `--columns` as given.
    columns: Vec<String>,
    part_max_bytes: u64,
    part_max_rows: Option<u64>,
    row_group_rows: u64,
    cross_mounts: bool,
    max_depth: Option<u32>,
    /// Exclude globs, including those read from `--exclude-from` files.
    excludes: Vec<String>,
    include_snapshots: bool,
    detect_cycles: bool,
    /// Optional metadata fields read.
    fields: Vec<String>,
    statx_dont_sync: bool,
    raw_paths: RawPaths,
    csv_raw_controls: bool,
    time_unit: TimeUnit,
    time_format: TimeFormat,
    /// `--synthetic`: the generated tree scanned instead of the file system.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    synthetic: Option<String>,
}

impl ScanSettings {
    fn from_args(args: &ScanArgs) -> Result<Self, SetupError> {
        let roots = args
            .roots
            .iter()
            .map(|r| normalize_root(r))
            .collect::<Result<Vec<_>, _>>()?;
        let excludes = read_excludes(&args.traversal.exclude, &args.traversal.exclude_from)?;
        let compression = args.output.compression.unwrap_or(match args.output.format {
            Format::Parquet => Compression::Zstd(3),
            Format::Csv | Format::Jsonl => Compression::None,
        });
        let settings = Self {
            roots: roots.iter().map(|r| BASE64.encode(r)).collect(),
            format: args.output.format,
            compression: describe_compression(compression),
            columns: args.output.columns.clone(),
            part_max_bytes: args.output.part_max_bytes,
            part_max_rows: args.output.part_max_rows,
            row_group_rows: args.output.row_group_rows,
            cross_mounts: args.traversal.cross_mounts,
            max_depth: args.traversal.max_depth,
            excludes,
            include_snapshots: args.traversal.include_snapshots,
            detect_cycles: args.traversal.detect_cycles,
            fields: args
                .metadata
                .fields
                .unwrap_or_default()
                .names()
                .into_iter()
                .map(String::from)
                .collect(),
            statx_dont_sync: args.metadata.statx_dont_sync,
            raw_paths: args.encoding.raw_paths,
            csv_raw_controls: args.encoding.csv_raw_controls,
            time_unit: args.encoding.time_unit,
            time_format: args.encoding.time_format,
            synthetic: args.synthetic.map(|tree| tree.to_string()),
        };
        // Check everything now, before creating any output.
        settings.selection()?;
        build_excludes(&settings.excludes)?;
        Ok(settings)
    }

    fn roots(&self) -> Result<Vec<Vec<u8>>, SetupError> {
        self.roots
            .iter()
            .map(|r| BASE64.decode(r))
            .collect::<Result<_, _>>()
            .map_err(|e| SetupError::fatal(format!("invalid root in checkpoint: {e}")))
    }

    fn compression(&self) -> Result<Compression, SetupError> {
        parse_compression(&self.compression).map_err(SetupError::usage)
    }

    fn selection(&self) -> Result<Selection, SetupError> {
        Selection::parse(&self.columns).map_err(SetupError::usage)
    }

    fn fields(&self) -> Result<Fields, SetupError> {
        Fields::parse(&self.fields.join(",")).map_err(SetupError::usage)
    }

    fn synthetic(&self) -> Result<Option<SyntheticOptions>, SetupError> {
        self.synthetic
            .as_deref()
            .map(SyntheticOptions::parse)
            .transpose()
            .map_err(SetupError::usage)
    }
}

/// Settings that only affect how a scan runs. `resume` starts from the
/// original scan's and can change them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSettings {
    pub threads: u16,
    pub writers: usize,
    pub max_memory: Option<u64>,
    pub stall_timeout: Option<Duration>,
    /// `None` takes no checkpoints.
    pub checkpoint_interval: Option<Duration>,
}

impl RunSettings {
    fn from_args(args: &ScanArgs) -> Self {
        Self {
            threads: args.traversal.threads,
            writers: args.traversal.writers.map_or_else(
                || {
                    let writers = default_writers();
                    match args.robustness.max_memory {
                        Some(m) if args.output.format == Format::Parquet => {
                            writers.min(MemoryBudgets::writers_within(m))
                        }
                        _ => writers,
                    }
                },
                usize::from,
            ),
            max_memory: args.robustness.max_memory,
            stall_timeout: args.robustness.stall_timeout,
            checkpoint_interval: match args.robustness.checkpoint_interval {
                None => Some(CHECKPOINT_INTERVAL),
                Some(interval) => (!interval.is_zero()).then_some(interval),
            },
        }
    }
}

/// Where a run starts.
pub enum Origin {
    /// A new scan, recording these options in its manifest.
    New(BTreeMap<String, serde_json::Value>),
    /// Where a checkpoint left off, holding the scan's lock.
    Resume {
        header: Box<checkpoint::Header>,
        state: WalkState,
        lock: File,
    },
}

pub fn run(args: &ScanArgs) -> Exit {
    report(run_scan(args))
}

fn run_scan(args: &ScanArgs) -> Result<Exit, SetupError> {
    reject_unimplemented(args)?;
    let settings = ScanSettings::from_args(args)?;
    let runtime = RunSettings::from_args(args);
    let destination = if args.output.output == Path::new("-") {
        Destination::Stdout
    } else {
        Destination::Dir(prepare_output_dir(&args.output.output)?)
    };
    let options = manifest_options(args, runtime.writers);
    execute(
        &settings,
        &runtime,
        &args.progress,
        destination,
        Origin::New(options),
    )
}

/// Runs a scan, or the rest of one from a checkpoint.
#[allow(clippy::too_many_lines, reason = "one scan, start to finish")]
pub fn execute(
    settings: &ScanSettings,
    runtime: &RunSettings,
    progress_options: &ProgressOptions,
    destination: Destination,
    origin: Origin,
) -> Result<Exit, SetupError> {
    let selection = settings.selection()?;
    let fields = settings.fields()?;
    let compression = settings.compression()?;
    let synthetic = settings.synthetic()?;
    let roots = settings.roots()?;
    let excludes = build_excludes(&settings.excludes)?;
    let writers = runtime.writers.max(1);
    let scan_dir = match &destination {
        Destination::Dir(dir) => Some(dir.clone()),
        Destination::Stdout => None,
    };
    // Parquet writers hold encoders; text writers hold next to nothing.
    let parquet_writers = match (&scan_dir, settings.format) {
        (Some(_), Format::Parquet) => writers,
        (None, Format::Parquet) => 1,
        _ => 0,
    };
    let budgets = MemoryBudgets::new(
        runtime.max_memory,
        usize::from(runtime.threads),
        parquet_writers,
    );
    if let (Some(max), Some(least)) = (runtime.max_memory, budgets.shortfall) {
        let (who, fewer) = match parquet_writers {
            0 => (String::new(), "--threads"),
            1 => (" and 1 writer".into(), "--threads"),
            n => (format!(" and {n} writers"), "--threads or --writers"),
        };
        eprintln!(
            "fs-inventory: {} crawl threads{who} need about {}, more than --max-memory {}; fewer {fewer} need less",
            runtime.threads,
            progress::format_bytes(least as u64),
            progress::format_bytes(max),
        );
    }
    let checkpointing = runtime.checkpoint_interval.is_some() && scan_dir.is_some();

    let mut skip_dirs = Vec::new();
    if let Some(dir) = &scan_dir
        && let Ok(meta) = fs::metadata(dir)
    {
        // Never inventory our own output.
        skip_dirs.push((meta.dev(), meta.ino()));
    }

    // A new scan with checkpoints takes the scan's lock; a resume has it.
    let (_lock, resume, options) = match origin {
        Origin::Resume {
            header,
            state,
            lock,
        } => {
            let options = header.options.clone();
            (Some(lock), Some((header, state)), options)
        }
        Origin::New(options) => {
            let lock = match &scan_dir {
                Some(dir) if checkpointing => Some(
                    checkpoint::lock(dir)
                        .map_err(|e| SetupError::fatal(format!("cannot create checkpoints: {e}")))?
                        .ok_or_else(|| {
                            SetupError::usage(format!(
                                "another fs-inventory process is writing to {}",
                                dir.display()
                            ))
                        })?,
                ),
                _ => None,
            };
            (lock, None, options)
        }
    };

    if runtime.max_memory.is_some() {
        // A memory budget trades some speed for memory.
        crate::alloc::release_memory_promptly();
    }
    raise_fd_limit();
    let signaled = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    install_signal_handlers(&signaled, &stop);

    let started = Instant::now();
    let (scan_id, host, started_at, run, previous, previous_elapsed, mut resumes) = match &resume {
        Some((header, _)) => (
            header.scan_id.clone(),
            header.host.clone(),
            header.started_at.clone(),
            header.run + 1,
            OutputState {
                entry_parts: header.entry_parts.clone(),
                error_parts: header.error_parts.clone(),
                kinds: std::array::from_fn(|i| header.kinds.get(i).copied().unwrap_or(0)),
                error_rows: header.error_rows,
            },
            header.elapsed_seconds,
            header.resumes.clone(),
        ),
        None => (
            new_scan_id(),
            hostname(),
            iso(now()),
            0,
            OutputState::default(),
            0.0,
            Vec::new(),
        ),
    };
    if let Some((header, _)) = &resume {
        resumes.push(ResumeRecord {
            resumed_at: iso(now()),
            checkpoint_at: header.created_at.clone(),
            tool_version: env!("CARGO_PKG_VERSION").into(),
        });
    }
    let root_records: Vec<Root> = roots.iter().map(|r| root_record(r)).collect();
    let raw_paths_always = settings.raw_paths == RawPaths::Always;
    let format = match settings.format {
        Format::Csv | Format::Jsonl => OutputFormat::Text(Encoder::new(EncodeOptions {
            format: if settings.format == Format::Csv {
                TextFormat::Csv
            } else {
                TextFormat::Jsonl
            },
            raw_paths_always,
            csv_raw_controls: settings.csv_raw_controls,
            precision: match settings.time_unit {
                TimeUnit::Us => Precision::Micros,
                TimeUnit::Ns => Precision::Nanos,
            },
            time_style: match settings.time_format {
                TimeFormat::Iso => TimeStyle::Iso,
                TimeFormat::Epoch => TimeStyle::Epoch,
            },
            selection,
            fields,
        })),
        Format::Parquet => OutputFormat::Parquet {
            arrow: ArrowOptions {
                unit: match settings.time_unit {
                    TimeUnit::Us => arrow_schema::TimeUnit::Microsecond,
                    TimeUnit::Ns => arrow_schema::TimeUnit::Nanosecond,
                },
                raw_paths_always,
                fields,
            },
            settings: ParquetSettings {
                compression,
                row_group_rows: usize::try_from(settings.row_group_rows).unwrap_or(usize::MAX),
                max_row_group_bytes: budgets.row_group(writers),
                metadata: vec![
                    (
                        "fs_inventory.schema_version".into(),
                        fs_inventory_core::SCHEMA_VERSION.to_string(),
                    ),
                    (
                        "fs_inventory.tool_version".into(),
                        env!("CARGO_PKG_VERSION").into(),
                    ),
                    ("fs_inventory.scan_id".into(), scan_id.clone()),
                    ("fs_inventory.host".into(), host.clone()),
                    (
                        "fs_inventory.roots".into(),
                        serde_json::to_string(&root_records).unwrap_or_default(),
                    ),
                ],
            },
        },
    };
    let (format_name, columns, error_columns) = match &format {
        OutputFormat::Text(encoder) => (
            encoder.format().extension(),
            encoder.entry_columns(),
            encoder.error_columns(),
        ),
        OutputFormat::Parquet { .. } => (
            "parquet",
            selection
                .indices()
                .into_iter()
                .map(|i| ENTRY_COLUMNS[i].name)
                .collect(),
            ERROR_COLUMNS.iter().map(|c| c.name).collect(),
        ),
    };
    let mut manifest = Manifest {
        tool: "fs-inventory",
        tool_version: env!("CARGO_PKG_VERSION"),
        schema_version: fs_inventory_core::SCHEMA_VERSION,
        scan_id: scan_id.clone(),
        status: Status::Running,
        host: host.clone(),
        roots: root_records,
        format: format_name,
        compression: describe_compression(compression),
        columns,
        error_columns,
        options,
        started_at: started_at.clone(),
        finished_at: None,
        duration_seconds: None,
        totals: Totals::default(),
        errors_by_kind: Vec::new(),
        stalled: Vec::new(),
        resumes,
        parts: Parts {
            entries: previous.entry_parts.clone(),
            errors: previous.error_parts.clone(),
        },
    };
    if let Some(dir) = &scan_dir {
        manifest
            .write(dir)
            .map_err(|e| SetupError::fatal(format!("cannot write the manifest: {e}")))?;
    }

    let output = Output::start(
        OutputConfig {
            destination: destination.clone(),
            format,
            selection,
            compression,
            writers,
            crawl_threads: usize::from(runtime.threads),
            sink_budget: budgets.sinks,
            limits: RollLimits {
                max_bytes: settings.part_max_bytes,
                max_rows: settings.part_max_rows,
            },
            run,
            previous,
        },
        Arc::clone(&stop),
    )
    .map_err(|e| SetupError::fatal(format!("cannot start output writers: {e}")))?;

    let counters = Arc::new(Counters::default());
    let base_entries = resume.as_ref().map_or(0, |(_, state)| state.entries());
    let style = progress_style(progress_options.progress);
    let summary_style = match style {
        Style::None if progress_options.progress == Progress::Auto => Style::Plain,
        other => other,
    };
    if let Some(tree) = synthetic
        && summary_style != Style::None
    {
        eprintln!("fs-inventory: scanning a generated tree ({tree}), not the file system");
    }
    if let Some((header, state)) = &resume
        && summary_style != Style::None
    {
        eprintln!(
            "fs-inventory: resuming scan {scan_id} from its checkpoint of {}: {} entries written, {} directories left",
            header.created_at,
            state.entries(),
            state.pending()
        );
    }
    let reporter = Reporter::start(
        style,
        progress_options.progress_interval,
        Arc::clone(&counters),
        started,
        base_entries,
    );

    let backend_options = BackendOptions {
        statx_dont_sync: settings.statx_dont_sync,
        fields,
        ..BackendOptions::default()
    };
    let walk_options = WalkOptions {
        threads: usize::from(runtime.threads),
        cross_mounts: settings.cross_mounts,
        max_depth: settings.max_depth,
        excludes,
        include_snapshots: settings.include_snapshots,
        skip_dirs,
        detect_cycles: settings.detect_cycles,
        stall_report: STALL_REPORT,
        stall_timeout: runtime.stall_timeout,
        queue_budget: budgets.queue,
        // Next to the output, on storage the user chose; else the temp dir.
        spill_dir: scan_dir.clone(),
        checkpoint_interval: runtime.checkpoint_interval.filter(|_| checkpointing),
        state_dir: scan_dir
            .as_deref()
            .filter(|_| checkpointing)
            .map(checkpoint::dir),
        ..WalkOptions::default()
    };
    let header = checkpoint::Header {
        scan_id: scan_id.clone(),
        host,
        started_at,
        created_at: String::new(),
        elapsed_seconds: 0.0,
        run,
        settings: settings.clone(),
        runtime: runtime.clone(),
        options: manifest.options.clone(),
        resumes: manifest.resumes.clone(),
        entry_parts: Vec::new(),
        error_parts: Vec::new(),
        kinds: Vec::new(),
        error_rows: 0,
    };
    let start = match resume {
        Some((_, state)) => Start::Resume(state),
        None => Start::Roots(&roots),
    };
    let mut checkpointer = ScanCheckpointer {
        output: &output,
        scan_dir: scan_dir.as_deref(),
        header,
        started,
        previous_elapsed,
        manifest: &mut manifest,
        warned: false,
    };
    let walk = Walk {
        start,
        options: walk_options,
        output: &output,
        counters: &counters,
        stop: &stop,
        checkpointer: checkpointing.then_some(&mut checkpointer as &mut dyn Checkpointer),
    };
    let result = match synthetic {
        Some(tree) => walk.run(SyntheticBackend::new(tree, &roots)),
        None => walk.run(NativeBackend::new(backend_options)),
    }
    .map_err(|e| SetupError::fatal(format!("cannot open the checkpoint files: {e}")))?;
    let header = checkpointer.header;
    let report = output.finish();
    reporter.stop();
    if report.salvaged > 0 {
        eprintln!(
            "fs-inventory: kept the finished directories of {} stalled crawl threads",
            report.salvaged
        );
    }
    let mut snapshot = Snapshot::take(&counters, started);
    snapshot.base_entries = base_entries;

    let stdout_closed = matches!(destination, Destination::Stdout)
        && report
            .failure
            .as_ref()
            .is_some_and(|e| e.kind() == io::ErrorKind::BrokenPipe);
    let (status, exit) = match (&report.failure, signaled.load(Ordering::Relaxed)) {
        _ if stdout_closed => (Status::Interrupted, Exit::Incomplete),
        (Some(e), _) => {
            eprintln!("fs-inventory: writing output failed: {e}");
            (Status::Failed, Exit::Fatal)
        }
        (None, true) => (Status::Interrupted, Exit::Incomplete),
        (None, false) if !result.abandoned.is_empty() => (Status::Incomplete, Exit::Incomplete),
        (None, false) if !result.complete => (Status::Interrupted, Exit::Incomplete),
        (None, false) if snapshot.errors > 0 => (Status::Complete, Exit::CompletedWithErrors),
        (None, false) => (Status::Complete, Exit::Success),
    };

    let elapsed = previous_elapsed + started.elapsed().as_secs_f64();
    let output_label = match &scan_dir {
        Some(dir) => {
            if checkpointing {
                match &result.state {
                    // Stopped early: a final checkpoint, exact because every
                    // worker finished its directory and every part is
                    // committed. After an output failure, the last periodic
                    // checkpoint stays.
                    Some(state) if report.failure.is_none() => {
                        let header = checkpoint::Header {
                            created_at: iso(now()),
                            elapsed_seconds: elapsed,
                            entry_parts: report.entry_parts.clone(),
                            error_parts: report.error_parts.clone(),
                            kinds: report.kinds.to_vec(),
                            error_rows: report.error_rows,
                            ..header
                        };
                        if let Err(e) = checkpoint::write(dir, &header, state) {
                            eprintln!("fs-inventory: cannot write the final checkpoint: {e}");
                        }
                    }
                    Some(_) => {}
                    // Finished: nothing is left to resume.
                    None => {
                        if let Err(e) = checkpoint::remove(dir) {
                            eprintln!("fs-inventory: cannot remove the checkpoint: {e}");
                        }
                    }
                }
            }
            finalize_manifest(&mut manifest, status, &snapshot, &result, report, elapsed);
            if let Err(e) = manifest.write(dir).and_then(|()| sync_dir(dir)) {
                eprintln!("fs-inventory: cannot write the manifest: {e}");
                return Ok(Exit::Fatal);
            }
            dir.display().to_string()
        }
        None => "stdout".to_string(),
    };
    let status_text = match status {
        Status::Complete => "complete",
        Status::Interrupted => "interrupted",
        Status::Failed => "failed",
        Status::Incomplete => "incomplete",
        Status::Running => "running",
    };
    for stalled in &result.abandoned {
        eprintln!(
            "fs-inventory: gave up on {} after {} seconds without progress",
            names::to_text(&stalled.path, Controls::Escape),
            stalled.seconds
        );
    }
    progress::summary(summary_style, status_text, &snapshot, &output_label);
    Ok(exit)
}

/// Writes checkpoints for a scan into its output directory.
struct ScanCheckpointer<'a> {
    output: &'a Output,
    scan_dir: Option<&'a Path>,
    /// Everything but the parts and times, which each checkpoint fills in.
    header: checkpoint::Header,
    started: Instant,
    previous_elapsed: f64,
    /// Kept up to date with the committed parts, for anyone watching.
    manifest: &'a mut Manifest,
    warned: bool,
}

impl Checkpointer for ScanCheckpointer<'_> {
    fn checkpoint(&mut self, state: &WalkState) -> io::Result<()> {
        let Some(dir) = self.scan_dir else {
            return Ok(());
        };
        let result = (|| {
            let committed = self.output.checkpoint(state.abandoned_workers())?;
            let header = checkpoint::Header {
                created_at: iso(now()),
                elapsed_seconds: self.previous_elapsed + self.started.elapsed().as_secs_f64(),
                entry_parts: committed.entry_parts.clone(),
                error_parts: committed.error_parts.clone(),
                kinds: committed.kinds.to_vec(),
                error_rows: committed.error_rows,
                ..self.header.clone()
            };
            checkpoint::write(dir, &header, state)?;
            self.manifest.parts = Parts {
                entries: committed.entry_parts,
                errors: committed.error_parts,
            };
            self.manifest.write(dir)
        })();
        if let Err(e) = &result
            && !std::mem::replace(&mut self.warned, true)
        {
            eprintln!("fs-inventory: cannot write a checkpoint (will keep trying): {e}");
        }
        result
    }

    fn skipped(&mut self) {
        eprintln!(
            "fs-inventory: skipped a checkpoint: a crawl thread has made no progress for 30 seconds"
        );
    }
}

/// Everything a walk needs but the backend, which is chosen at run time.
struct Walk<'a> {
    start: Start<'a>,
    options: WalkOptions,
    output: &'a Output,
    counters: &'a Arc<Counters>,
    stop: &'a Arc<AtomicBool>,
    checkpointer: Option<&'a mut dyn Checkpointer>,
}

impl Walk<'_> {
    /// Walks with `backend`. Debug builds can slow it down for tests.
    fn run<B: Backend + 'static>(self, backend: B) -> io::Result<WalkResult> {
        #[cfg(debug_assertions)]
        let backend = crate::debug::Delayed::from_env(backend);
        walker::walk_from(
            self.start,
            self.options,
            Arc::new(backend),
            |_| self.output.sink(),
            Arc::clone(self.counters),
            Arc::clone(self.stop),
            self.checkpointer,
        )
    }
}

/// Options that parse but do nothing yet. Accepting them silently would be
/// worse than refusing them.
fn reject_unimplemented(args: &ScanArgs) -> Result<(), SetupError> {
    let pending: [(bool, &str, &str); 3] = [
        (args.metadata.symlink_targets, "--symlink-targets", "M5"),
        (args.metadata.resolve_ids, "--resolve-ids", "M5"),
        (args.encoding.guess_encoding, "--guess-encoding", "M5"),
    ];
    match pending.iter().find(|(set, _, _)| *set) {
        Some((_, what, milestone)) => Err(SetupError::usage(format!(
            "{what} is not implemented yet (planned for milestone {milestone})"
        ))),
        None => Ok(()),
    }
}

/// Makes a root absolute and canonical in form (no `.`, `//`, or trailing
/// `/`) without resolving symlinks, and returns its bytes.
fn normalize_root(root: &Path) -> Result<Vec<u8>, SetupError> {
    let absolute = std::path::absolute(root)
        .map_err(|e| SetupError::usage(format!("invalid root {}: {e}", root.display())))?;
    let normalized: PathBuf = absolute.components().collect();
    Ok(normalized.into_os_string().into_vec())
}

/// Exclude globs from `--exclude` and the files named by `--exclude-from`.
fn read_excludes(globs: &[String], files: &[PathBuf]) -> Result<Vec<String>, SetupError> {
    let mut patterns: Vec<String> = globs.to_vec();
    for file in files {
        let text = fs::read_to_string(file).map_err(|e| {
            SetupError::usage(format!("cannot read exclude file {}: {e}", file.display()))
        })?;
        patterns.extend(
            text.lines()
                .map(str::trim)
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .map(String::from),
        );
    }
    Ok(patterns)
}

fn build_excludes(patterns: &[String]) -> Result<Option<GlobSet>, SetupError> {
    if patterns.is_empty() {
        return Ok(None);
    }
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let full = if pattern.contains('/') {
            pattern.clone()
        } else {
            format!("**/{pattern}")
        };
        let glob = GlobBuilder::new(&full)
            .literal_separator(true)
            .backslash_escape(true)
            .build()
            .map_err(|e| SetupError::usage(format!("invalid exclude glob `{pattern}`: {e}")))?;
        builder.add(glob);
    }
    builder
        .build()
        .map(Some)
        .map_err(|e| SetupError::usage(format!("invalid exclude globs: {e}")))
}

/// Creates the scan directory, which must not already contain files.
fn prepare_output_dir(dir: &Path) -> Result<PathBuf, SetupError> {
    match fs::read_dir(dir) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(SetupError::usage(format!(
                    "output directory {} is not empty; choose a new directory for each scan",
                    dir.display()
                )));
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(dir).map_err(|e| {
                SetupError::fatal(format!(
                    "cannot create output directory {}: {e}",
                    dir.display()
                ))
            })?;
        }
        Err(e) => {
            return Err(SetupError::fatal(format!(
                "cannot use output directory {}: {e}",
                dir.display()
            )));
        }
    }
    std::path::absolute(dir)
        .map_err(|e| SetupError::fatal(format!("invalid output directory {}: {e}", dir.display())))
}

/// Raises the open-file limit, since each crawl thread holds a directory open.
fn raise_fd_limit() {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
    let limit = getrlimit(Resource::Nofile);
    let hard = limit.maximum.unwrap_or(1 << 20);
    for want in [hard, 1 << 16, 10_240] {
        let want = want.min(hard);
        if limit.current.is_none_or(|current| current >= want) {
            return;
        }
        let raised = Rlimit {
            current: Some(want),
            maximum: limit.maximum,
        };
        if setrlimit(Resource::Nofile, raised).is_ok() {
            return;
        }
    }
}

/// The first SIGINT or SIGTERM stops the crawl gracefully; a second one
/// exits immediately.
fn install_signal_handlers(signaled: &Arc<AtomicBool>, stop: &Arc<AtomicBool>) {
    use signal_hook::consts::{SIGINT, SIGTERM};
    use signal_hook::flag;
    for signal in [SIGINT, SIGTERM] {
        // Order matters: the shutdown check must see the flag from a
        // previous signal, before this signal sets it.
        let _ = flag::register_conditional_shutdown(signal, 130, Arc::clone(signaled));
        let _ = flag::register(signal, Arc::clone(signaled));
        let _ = flag::register(signal, Arc::clone(stop));
    }
}

fn default_writers() -> usize {
    std::thread::available_parallelism()
        .map_or(4, usize::from)
        .min(8)
}

fn progress_style(progress: Progress) -> Style {
    match progress {
        Progress::Plain => Style::Plain,
        Progress::Json => Style::Json,
        Progress::None => Style::None,
        Progress::Auto if io::stderr().is_terminal() => Style::Plain,
        Progress::Auto => Style::None,
    }
}

fn root_record(root: &[u8]) -> Root {
    let text = names::to_text(root, Controls::Keep);
    let differs = matches!(text, std::borrow::Cow::Owned(_));
    Root {
        path: text.into_owned(),
        path_b64: differs.then(|| BASE64.encode(root)),
    }
}

fn describe_compression(compression: Compression) -> String {
    match compression {
        Compression::None => "none".into(),
        Compression::Zstd(level) => format!("zstd:{level}"),
        Compression::Gzip(level) => format!("gzip:{level}"),
        Compression::Snappy => "snappy".into(),
        Compression::Lz4 => "lz4".into(),
    }
}

fn manifest_options(args: &ScanArgs, writers: usize) -> BTreeMap<String, serde_json::Value> {
    fn enum_name(v: &impl clap::ValueEnum) -> String {
        v.to_possible_value()
            .map_or_else(String::new, |p| p.get_name().to_string())
    }
    BTreeMap::from([
        ("threads", json!(args.traversal.threads)),
        ("writers", json!(writers)),
        ("cross_mounts", json!(args.traversal.cross_mounts)),
        ("max_depth", json!(args.traversal.max_depth)),
        ("exclude", json!(args.traversal.exclude)),
        (
            "exclude_from",
            json!(
                args.traversal
                    .exclude_from
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
            ),
        ),
        ("include_snapshots", json!(args.traversal.include_snapshots)),
        ("detect_cycles", json!(args.traversal.detect_cycles)),
        ("max_memory", json!(args.robustness.max_memory)),
        ("statx_dont_sync", json!(args.metadata.statx_dont_sync)),
        (
            "fields",
            json!(args.metadata.fields.unwrap_or_default().names()),
        ),
        ("raw_paths", json!(enum_name(&args.encoding.raw_paths))),
        ("csv_raw_controls", json!(args.encoding.csv_raw_controls)),
        ("time_unit", json!(enum_name(&args.encoding.time_unit))),
        ("time_format", json!(enum_name(&args.encoding.time_format))),
        ("part_max_bytes", json!(args.output.part_max_bytes)),
        ("part_max_rows", json!(args.output.part_max_rows)),
        ("row_group_rows", json!(args.output.row_group_rows)),
    ])
    .into_iter()
    .chain(
        args.synthetic
            .map(|tree| ("synthetic", json!(tree.to_string()))),
    )
    .map(|(key, value)| (key.to_string(), value))
    .collect()
}

fn finalize_manifest(
    manifest: &mut Manifest,
    status: Status,
    snapshot: &Snapshot,
    result: &WalkResult,
    report: OutputReport,
    elapsed: f64,
) {
    manifest.status = status;
    manifest.finished_at = Some(iso(now()));
    manifest.duration_seconds = Some(elapsed);
    manifest.totals = Totals {
        entries: report.kinds.iter().sum(),
        by_kind: EntryKind::ALL
            .iter()
            .zip(report.kinds)
            .filter(|(_, count)| *count > 0)
            .map(|(kind, count)| (kind.as_str(), count))
            .collect(),
        dirs_read: snapshot.dirs,
        bytes: snapshot.bytes,
        errors: snapshot.errors,
        vanished: snapshot.vanished,
        revisits: snapshot.revisits,
        spilled: snapshot.spilled,
    };
    manifest.errors_by_kind = result
        .errors
        .iter()
        .map(|e| ErrorSummary {
            op: e.op.as_str(),
            errno: e.errno,
            errno_name: e.errno.and_then(errno_name),
            count: e.count,
        })
        .collect();
    manifest.stalled = result
        .abandoned
        .iter()
        .map(|s| {
            let root = root_record(&s.path);
            StalledDir {
                path: root.path,
                path_b64: root.path_b64,
                seconds: s.seconds,
            }
        })
        .collect();
    manifest.parts = Parts {
        entries: report.entry_parts,
        errors: report.error_parts,
    };
}

fn iso(t: fs_inventory_core::model::Timestamp) -> String {
    let mut out = Vec::new();
    timefmt::write_iso(&mut out, t, Precision::Micros);
    String::from_utf8_lossy(&out).into_owned()
}

fn hostname() -> String {
    rustix::system::uname()
        .nodename()
        .to_string_lossy()
        .into_owned()
}

/// A time-ordered UUID (version 7) identifying this scan.
fn new_scan_id() -> String {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis()) as u64
        & 0xffff_ffff_ffff;
    let random = |salt: u64| {
        // RandomState is seeded from the OS's random source.
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(salt);
        hasher.write_u32(std::process::id());
        hasher.finish()
    };
    let high = (millis << 16) | 0x7000 | (random(1) & 0x0fff);
    let low = 0x8000_0000_0000_0000 | (random(2) & 0x3fff_ffff_ffff_ffff);
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        high >> 32,
        (high >> 16) & 0xffff,
        high & 0xffff,
        low >> 48,
        low & 0xffff_ffff_ffff
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_budgets_count_threads_and_writers() {
        let unlimited = MemoryBudgets::new(None, 64, 8);
        assert_eq!(unlimited.sinks, crate::output::SINK_BUDGET_BYTES);
        assert!(unlimited.shortfall.is_none());

        // Everything planned fits in the budget.
        let budget = 2 << 30;
        let roomy = MemoryBudgets::new(Some(budget as u64), 64, 4);
        assert!(roomy.shortfall.is_none());
        let planned = BASE_MEMORY
            + 4 * WRITER_MEMORY
            + thread_memory(64, roomy.sinks / 64)
            + roomy.queue
            + roomy.row_groups;
        assert!(planned <= budget, "{planned}");

        // More threads mean smaller row buffers, then a warning.
        let more = MemoryBudgets::new(Some(budget as u64), 256, 4);
        assert!(more.sinks / 256 < roomy.sinks / 64);
        let tight = MemoryBudgets::new(Some(512 << 20), 256, 2);
        assert!(tight.shortfall.is_some_and(|need| need > 512 << 20));

        assert_eq!(MemoryBudgets::writers_within(512 << 20), 3);
        assert_eq!(MemoryBudgets::writers_within(64 << 20), 1);
    }

    #[test]
    fn roots_are_normalized() {
        let norm = |p: &str| String::from_utf8(normalize_root(Path::new(p)).ok().unwrap()).unwrap();
        assert_eq!(norm("/"), "/");
        assert_eq!(norm("/data/"), "/data");
        assert_eq!(norm("/data//x/./y/"), "/data/x/y");
        assert_eq!(norm("/data/../x"), "/data/../x");
        assert!(norm("relative").starts_with('/'));
    }

    #[test]
    fn excludes_match_names_and_paths() {
        let set = build_excludes(&["*.tmp".into(), "/data/*/scratch".into()])
            .ok()
            .flatten()
            .unwrap();
        assert!(set.is_match("/data/a/file.tmp"));
        assert!(set.is_match("/x.tmp"));
        assert!(set.is_match("/data/proj/scratch"));
        assert!(!set.is_match("/data/proj/sub/scratch"));
        assert!(!set.is_match("/data/file.txt"));
        assert!(build_excludes(&["[".into()]).is_err());
    }

    #[test]
    fn scan_ids_are_uuid_v7() {
        let id = new_scan_id();
        assert_eq!(id.len(), 36);
        assert_eq!(&id[14..15], "7");
        assert!(matches!(&id[19..20], "8" | "9" | "a" | "b"));
        assert_ne!(new_scan_id(), id);
    }

    #[test]
    fn output_dir_must_be_new_or_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let fresh = tmp.path().join("scan");
        assert!(prepare_output_dir(&fresh).is_ok());
        assert!(prepare_output_dir(&fresh).is_ok());
        fs::write(fresh.join("x"), "x").unwrap();
        assert!(prepare_output_dir(&fresh).is_err());
    }
}
