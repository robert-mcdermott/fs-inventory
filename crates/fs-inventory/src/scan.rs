// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! The `scan` command: set up output, crawl, and report.

use std::collections::BTreeMap;
use std::collections::hash_map::RandomState;
use std::fs;
use std::hash::{BuildHasher as _, Hasher as _};
use std::io::{self, IsTerminal as _};
use std::os::unix::ffi::OsStringExt as _;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant, SystemTime};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use fs_inventory_core::backend::{BackendOptions, NativeBackend};
use fs_inventory_core::model::{EntryKind, errno_name};
use fs_inventory_core::names::{self, Controls};
use fs_inventory_core::walker::{self, Counters, WalkOptions, WalkResult};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde_json::json;

use crate::cli::{Format, Progress, RawPaths, ScanArgs, TimeFormat, TimeUnit};
use crate::exit::Exit;
use crate::output::arrow::ArrowOptions;
use crate::output::encode::{EncodeOptions, Encoder, TextFormat, TimeStyle};
use crate::output::manifest::{ErrorSummary, Manifest, Parts, Root, Status, Totals};
use crate::output::parquet::ParquetSettings;
use crate::output::parts::{RollLimits, sync_dir};
use crate::output::schema::{ENTRY_COLUMNS, ERROR_COLUMNS, Selection};
use crate::output::timefmt::{self, Precision};
use crate::output::{Destination, Format as OutputFormat, Output, OutputConfig, OutputReport, now};
use crate::parse::Compression;
use crate::progress::{self, Reporter, Snapshot, Style};

/// A problem found before the crawl starts.
struct SetupError {
    exit: Exit,
    message: String,
}

impl SetupError {
    fn usage(message: impl Into<String>) -> Self {
        Self {
            exit: Exit::Usage,
            message: message.into(),
        }
    }

    fn fatal(message: impl Into<String>) -> Self {
        Self {
            exit: Exit::Fatal,
            message: message.into(),
        }
    }
}

pub fn run(args: &ScanArgs) -> Exit {
    match run_scan(args) {
        Ok(exit) => exit,
        Err(e) => {
            eprintln!("fs-inventory: {}", e.message);
            e.exit
        }
    }
}

fn run_scan(args: &ScanArgs) -> Result<Exit, SetupError> {
    reject_unimplemented(args)?;
    let selection = Selection::parse(&args.output.columns).map_err(SetupError::usage)?;
    let fields = args.metadata.fields.unwrap_or_default();
    let compression = args.output.compression.unwrap_or(match args.output.format {
        Format::Parquet => Compression::Zstd(3),
        Format::Csv | Format::Jsonl => Compression::None,
    });
    let roots = args
        .roots
        .iter()
        .map(|r| normalize_root(r))
        .collect::<Result<Vec<_>, _>>()?;
    let excludes = build_excludes(&args.traversal.exclude, &args.traversal.exclude_from)?;

    let destination = if args.output.output == Path::new("-") {
        Destination::Stdout
    } else {
        Destination::Dir(prepare_output_dir(&args.output.output)?)
    };
    let mut skip_dirs = Vec::new();
    if let Destination::Dir(dir) = &destination
        && let Ok(meta) = fs::metadata(dir)
    {
        // Never inventory our own output.
        skip_dirs.push((meta.dev(), meta.ino()));
    }

    raise_fd_limit();
    let signaled = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    install_signal_handlers(&signaled, &stop);

    let writers = args
        .traversal
        .writers
        .map_or_else(default_writers, usize::from);
    let scan_id = new_scan_id();
    let host = hostname();
    let root_records: Vec<Root> = roots.iter().map(|r| root_record(r)).collect();
    let raw_paths_always = args.encoding.raw_paths == RawPaths::Always;
    let format = match args.output.format {
        Format::Csv | Format::Jsonl => OutputFormat::Text(Encoder::new(EncodeOptions {
            format: if args.output.format == Format::Csv {
                TextFormat::Csv
            } else {
                TextFormat::Jsonl
            },
            raw_paths_always,
            csv_raw_controls: args.encoding.csv_raw_controls,
            precision: match args.encoding.time_unit {
                TimeUnit::Us => Precision::Micros,
                TimeUnit::Ns => Precision::Nanos,
            },
            time_style: match args.encoding.time_format {
                TimeFormat::Iso => TimeStyle::Iso,
                TimeFormat::Epoch => TimeStyle::Epoch,
            },
            selection,
            fields,
        })),
        Format::Parquet => OutputFormat::Parquet {
            arrow: ArrowOptions {
                unit: match args.encoding.time_unit {
                    TimeUnit::Us => arrow_schema::TimeUnit::Microsecond,
                    TimeUnit::Ns => arrow_schema::TimeUnit::Nanosecond,
                },
                raw_paths_always,
                fields,
            },
            settings: ParquetSettings {
                compression,
                row_group_rows: usize::try_from(args.output.row_group_rows).unwrap_or(usize::MAX),
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
    let started_at = now();
    let started = Instant::now();
    let mut manifest = Manifest {
        tool: "fs-inventory",
        tool_version: env!("CARGO_PKG_VERSION"),
        schema_version: fs_inventory_core::SCHEMA_VERSION,
        scan_id,
        status: Status::Running,
        host,
        roots: root_records,
        format: format_name,
        compression: describe_compression(compression),
        columns,
        error_columns,
        options: manifest_options(args, writers),
        started_at: iso(started_at),
        finished_at: None,
        duration_seconds: None,
        totals: Totals::default(),
        errors_by_kind: Vec::new(),
        parts: Parts::default(),
    };
    if let Destination::Dir(dir) = &destination {
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
            crawl_threads: usize::from(args.traversal.threads),
            limits: RollLimits {
                max_bytes: args.output.part_max_bytes,
                max_rows: args.output.part_max_rows,
            },
        },
        Arc::clone(&stop),
    )
    .map_err(|e| SetupError::fatal(format!("cannot start output writers: {e}")))?;

    let counters = Arc::new(Counters::default());
    let style = progress_style(args.progress.progress);
    let reporter = Reporter::start(
        style,
        args.progress.progress_interval,
        Arc::clone(&counters),
        started,
    );

    let backend = NativeBackend::new(BackendOptions {
        statx_dont_sync: args.metadata.statx_dont_sync,
        fields,
        ..BackendOptions::default()
    });
    let walk_options = WalkOptions {
        threads: usize::from(args.traversal.threads),
        cross_mounts: args.traversal.cross_mounts,
        max_depth: args.traversal.max_depth,
        excludes,
        include_snapshots: args.traversal.include_snapshots,
        skip_dirs,
        detect_cycles: args.traversal.detect_cycles,
    };
    let result = walker::walk(
        &roots,
        &walk_options,
        &backend,
        |_| output.sink(),
        &counters,
        &stop,
    );
    let report = output.finish();
    reporter.stop();
    let snapshot = Snapshot::take(&counters, started);

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
        (None, false) if !result.complete => (Status::Interrupted, Exit::Incomplete),
        (None, false) if snapshot.errors > 0 => (Status::Complete, Exit::CompletedWithErrors),
        (None, false) => (Status::Complete, Exit::Success),
    };

    let output_label = match &destination {
        Destination::Dir(dir) => {
            finalize_manifest(&mut manifest, status, &snapshot, &result, report, started);
            if let Err(e) = manifest.write(dir).and_then(|()| sync_dir(dir)) {
                eprintln!("fs-inventory: cannot write the manifest: {e}");
                return Ok(Exit::Fatal);
            }
            dir.display().to_string()
        }
        Destination::Stdout => "stdout".to_string(),
    };
    let status_text = match status {
        Status::Complete => "complete",
        Status::Interrupted => "interrupted",
        Status::Failed => "failed",
        Status::Running => "running",
    };
    progress::summary(
        match style {
            Style::None if args.progress.progress == Progress::Auto => Style::Plain,
            other => other,
        },
        status_text,
        &snapshot,
        &output_label,
    );
    Ok(exit)
}

/// Options that parse but do nothing yet. Accepting them silently would be
/// worse than refusing them.
fn reject_unimplemented(args: &ScanArgs) -> Result<(), SetupError> {
    let pending: [(bool, &str, &str); 6] = [
        (
            args.robustness.stall_timeout.is_some(),
            "--stall-timeout",
            "M3",
        ),
        (args.robustness.max_memory.is_some(), "--max-memory", "M3"),
        (
            args.robustness.checkpoint_interval.is_some(),
            "--checkpoint-interval",
            "M4",
        ),
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

fn build_excludes(globs: &[String], files: &[PathBuf]) -> Result<Option<GlobSet>, SetupError> {
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
    if patterns.is_empty() {
        return Ok(None);
    }
    let mut builder = GlobSetBuilder::new();
    for pattern in &patterns {
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

fn manifest_options(args: &ScanArgs, writers: usize) -> BTreeMap<&'static str, serde_json::Value> {
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
}

fn finalize_manifest(
    manifest: &mut Manifest,
    status: Status,
    snapshot: &Snapshot,
    result: &WalkResult,
    report: OutputReport,
    started: Instant,
) {
    manifest.status = status;
    manifest.finished_at = Some(iso(now()));
    manifest.duration_seconds = Some(started.elapsed().as_secs_f64());
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
        let set = build_excludes(&["*.tmp".into(), "/data/*/scratch".into()], &[])
            .ok()
            .flatten()
            .unwrap();
        assert!(set.is_match("/data/a/file.tmp"));
        assert!(set.is_match("/x.tmp"));
        assert!(set.is_match("/data/proj/scratch"));
        assert!(!set.is_match("/data/proj/sub/scratch"));
        assert!(!set.is_match("/data/file.txt"));
        assert!(build_excludes(&["[".into()], &[]).is_err());
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
