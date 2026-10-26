// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! The `convert` command: re-exports a Parquet scan as CSV or JSON Lines.
//!
//! Each Parquet row is turned back into the walker's [`Entry`] and written by
//! the encoder a CSV or JSON Lines scan uses, so the result is what that scan
//! would have written: the same escaping, `path_b64` rules, and time
//! formatting. Exact name bytes come from `path_raw`.

use std::fs::{self, File};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::cast::AsArray as _;
use arrow_array::types::{
    Int32Type, Int64Type, TimestampMicrosecondType, TimestampNanosecondType, UInt64Type,
};
use arrow_array::{Array, BinaryArray, BooleanArray, PrimitiveArray, RecordBatch, StringArray};
use arrow_schema::{DataType, TimeUnit};
use fs_inventory_core::model::{
    DirTotals, Entry, EntryKind, ErrorInfo, ErrorRecord, Fields, Op, Stat, Timestamp,
};
use fs_inventory_core::names::{self, Controls};
use fs_inventory_core::walker::{name_start, parent_of};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};

use crate::cli::{ConvertArgs, TextFormat as Target};
use crate::exit::Exit;
use crate::output::encode::{EncodeOptions, Encoder, TextFormat, TimeStyle};
use crate::output::parts::{PartInfo, PartWriter, RollLimits, compression_suffix, sync_dir};
use crate::output::schema::Selection;
use crate::output::timefmt::Precision;
use crate::output::{StdoutStream, manifest, now};
use crate::parse::Compression;
use crate::scan::{SetupError, describe_compression, iso, prepare_output_dir, report};

/// Encoded rows are written in chunks of about this size.
const CHUNK_BYTES: usize = 1 << 20;

/// Columns added after schema version 2, which a version 2 scan of every
/// column gets in the converted output, empty.
const ADDED_IN_3: [&str; 2] = ["symlink_target", "symlink_target_raw"];

pub fn run(args: &ConvertArgs) -> Exit {
    report(convert(args))
}

fn convert(args: &ConvertArgs) -> Result<Exit, SetupError> {
    let source = std::path::absolute(&args.scan_dir).map_err(|e| {
        SetupError::usage(format!(
            "invalid scan directory {}: {e}",
            args.scan_dir.display()
        ))
    })?;
    let shown = source.display();
    let manifest: Value = fs::read_to_string(source.join("manifest.json"))
        .map_err(|e| e.to_string())
        .and_then(|text| serde_json::from_str(&text).map_err(|e| e.to_string()))
        .map_err(|e| SetupError::usage(format!("cannot read the manifest in {shown}: {e}")))?;
    let format = manifest["format"].as_str().unwrap_or_default();
    if format != "parquet" {
        return Err(SetupError::usage(format!(
            "{shown} is a {format} scan; convert reads Parquet scans"
        )));
    }
    if !fs_inventory_core::readable_schema(manifest["schema_version"].as_u64()) {
        return Err(SetupError::usage(format!(
            "{shown} has schema version {}, but this fs-inventory reads versions {} to {}",
            manifest["schema_version"],
            fs_inventory_core::OLDEST_READABLE_SCHEMA,
            fs_inventory_core::SCHEMA_VERSION
        )));
    }
    if manifest["status"] == "running" {
        return Err(SetupError::usage(format!(
            "{shown} is still running, or was killed; let it finish, or resume it, first"
        )));
    }
    let compression = args.compression.unwrap_or(Compression::None);
    if matches!(compression, Compression::Snappy | Compression::Lz4) {
        return Err(SetupError::usage(
            "snappy and lz4 are Parquet codecs; use zstd, gzip, or none",
        ));
    }

    let options = &manifest["options"];
    let fields = match options["fields"].as_array() {
        Some(names) => Fields::parse(
            &names
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(","),
        )
        .map_err(SetupError::usage)?,
        None => Fields::ALL,
    };
    let text_format = match args.to {
        Target::Csv => TextFormat::Csv,
        Target::Jsonl => TextFormat::Jsonl,
    };
    let encoder = Encoder::new(EncodeOptions {
        format: text_format,
        raw_paths_always: options["raw_paths"] == "always",
        csv_raw_controls: options["csv_raw_controls"].as_bool().unwrap_or(false),
        precision: if options["time_unit"] == "ns" {
            Precision::Nanos
        } else {
            Precision::Micros
        },
        time_style: if options["time_format"] == "epoch" {
            TimeStyle::Epoch
        } else {
            TimeStyle::Iso
        },
        selection: selection(&manifest)?,
        fields,
    });
    let parts = |table: &str| -> Vec<PathBuf> {
        manifest["parts"][table]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| p["file"].as_str())
            .map(|file| source.join(file))
            .collect()
    };
    let (entry_parts, error_parts) = (parts("entries"), parts("errors"));

    if args.output == Path::new("-") {
        return to_stdout(&encoder, compression, &entry_parts, &error_parts);
    }
    let dir = prepare_output_dir(&args.output)?;
    let limits = RollLimits {
        max_bytes: options["part_max_bytes"].as_u64().unwrap_or(1 << 30),
        max_rows: options["part_max_rows"].as_u64(),
    };
    let extension = format!(
        "{}{}",
        text_format.extension(),
        compression_suffix(compression)
    );
    let writer = |table: &'static str, worker: usize, columns: Vec<&str>| {
        PartWriter::new(
            &dir,
            table,
            format!("part-w{worker:02}"),
            extension.clone(),
            encoder.header(&columns),
            compression,
            limits,
        )
    };

    // Parts convert in parallel, each worker writing its own output parts.
    let workers = std::thread::available_parallelism()
        .map_or(4, usize::from)
        .min(8)
        .min(entry_parts.len())
        .max(1);
    let next = AtomicUsize::new(0);
    let results: Vec<io::Result<(Vec<PartInfo>, u64)>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|worker| {
                let (encoder, next, entry_parts) = (&encoder, &next, &entry_parts);
                let mut out = writer("entries", worker, encoder.entry_columns());
                scope.spawn(move || {
                    let mut rows = 0;
                    while let Some(part) = entry_parts.get(next.fetch_add(1, Ordering::Relaxed)) {
                        rows += entries(part, encoder, &mut |bytes, n| out.write(bytes, n))?;
                    }
                    Ok((out.finish()?, rows))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or_else(|_| Err(io::Error::other("a convert thread panicked")))
            })
            .collect()
    });
    let mut entry_out = Vec::new();
    let mut entry_rows = 0;
    for result in results {
        let (parts, rows) = result.map_err(|e| fail(&dir, e))?;
        entry_out.extend(parts);
        entry_rows += rows;
    }
    let mut out = writer("errors", 0, encoder.error_columns());
    let mut error_rows = 0;
    for part in &error_parts {
        error_rows += errors(part, &encoder, &mut |bytes, n| out.write(bytes, n))
            .map_err(|e| fail(&dir, e))?;
    }
    let error_out = out.finish().map_err(|e| fail(&dir, e))?;

    let mut converted = manifest.clone();
    converted["format"] = json!(text_format.extension());
    converted["compression"] = json!(describe_compression(compression));
    converted["schema_version"] = json!(fs_inventory_core::SCHEMA_VERSION);
    converted["columns"] = json!(encoder.entry_columns());
    converted["error_columns"] = json!(encoder.error_columns());
    converted["parts"] = json!({ "entries": entry_out, "errors": error_out });
    converted["converted_from"] = json!({
        "scan_dir": source.display().to_string(),
        "format": "parquet",
        "schema_version": manifest["schema_version"],
        "converted_at": iso(now()),
        "tool_version": env!("CARGO_PKG_VERSION"),
    });
    // Rollup tables aren't converted.
    if let Some(object) = converted.as_object_mut() {
        object.remove("rollup");
    }
    manifest::write_value(&dir, &converted)
        .and_then(|()| sync_dir(&dir))
        .map_err(|e| fail(&dir, e))?;
    eprintln!(
        "fs-inventory: converted {entry_rows} entries and {error_rows} errors to {} in {}",
        text_format.extension(),
        dir.display()
    );
    Ok(Exit::Success)
}

fn fail(dir: &Path, e: io::Error) -> SetupError {
    SetupError::fatal(format!("converting into {} failed: {e}", dir.display()))
}

/// The columns to write: those the scan wrote. A version 2 scan of every
/// column also gets the columns added since, empty, like a scan today.
fn selection(manifest: &Value) -> Result<Selection, SetupError> {
    let mut names: Vec<String> = manifest["columns"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(String::from)
        .collect();
    if !names.iter().any(|n| n == "path") {
        return Err(SetupError::usage(
            "convert needs the scan's `path` column, which this scan left out with --columns",
        ));
    }
    let selection = Selection::parse(&names).map_err(SetupError::usage)?;
    if manifest["schema_version"] == 2 {
        names.extend(ADDED_IN_3.map(String::from));
        if Selection::parse(&names)
            .map_err(SetupError::usage)?
            .is_all()
        {
            return Ok(Selection::ALL);
        }
    }
    Ok(selection)
}

/// Writes every table to stdout: entries as CSV or JSON Lines, and errors as
/// lines on stderr, as a scan streamed to stdout does.
fn to_stdout(
    encoder: &Encoder,
    compression: Compression,
    entry_parts: &[PathBuf],
    error_parts: &[PathBuf],
) -> Result<Exit, SetupError> {
    let result = (|| -> io::Result<()> {
        let mut stream = StdoutStream::new(compression)?;
        stream
            .writer()
            .write_all(&encoder.header(&encoder.entry_columns()))?;
        for part in entry_parts {
            entries(part, encoder, &mut |bytes, _| {
                stream.writer().write_all(bytes)
            })?;
        }
        stream.finish()?;
        let stderr = io::stderr();
        for part in error_parts {
            each_error(part, &mut |record, _| {
                let line = format!(
                    "fs-inventory: {} {}: {}\n",
                    record.info.op.as_str(),
                    names::to_text(record.path, Controls::Escape),
                    record.info.message
                );
                stderr.lock().write_all(line.as_bytes())
            })?;
        }
        Ok(())
    })();
    match result {
        Ok(()) => Ok(Exit::Success),
        // The reader stopped early, as `head` does.
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => Ok(Exit::Incomplete),
        Err(e) => Err(SetupError::fatal(format!("converting failed: {e}"))),
    }
}

fn batches(part: &Path) -> io::Result<impl Iterator<Item = io::Result<RecordBatch>>> {
    let file = File::open(part).map_err(|e| annotate(part, &e))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .and_then(|b| b.with_batch_size(8192).build())
        .map_err(|e| annotate(part, &io::Error::other(e)))?;
    let part = part.to_path_buf();
    Ok(reader.map(move |batch| batch.map_err(|e| annotate(&part, &io::Error::other(e)))))
}

fn annotate(part: &Path, e: &io::Error) -> io::Error {
    io::Error::new(e.kind(), format!("{}: {e}", part.display()))
}

/// Encodes every entry row of one Parquet part, handing the text to `emit`
/// in chunks of whole rows. Returns the number of rows.
fn entries(
    part: &Path,
    encoder: &Encoder,
    emit: &mut dyn FnMut(&[u8], u64) -> io::Result<()>,
) -> io::Result<u64> {
    let mut out = Vec::with_capacity(CHUNK_BYTES + (CHUNK_BYTES >> 2));
    let (mut rows, mut pending) = (0, 0);
    for batch in batches(part)? {
        let batch = batch?;
        let columns = EntryColumns::of(&batch).map_err(|e| annotate(part, &e))?;
        for row in 0..batch.num_rows() {
            columns.encode(row, encoder, &mut out);
            pending += 1;
            if out.len() >= CHUNK_BYTES {
                emit(&out, pending)?;
                rows += pending;
                (pending, out) = (0, Vec::with_capacity(out.capacity()));
            }
        }
    }
    if pending > 0 {
        emit(&out, pending)?;
        rows += pending;
    }
    Ok(rows)
}

/// Encodes every row of one errors part.
fn errors(
    part: &Path,
    encoder: &Encoder,
    emit: &mut dyn FnMut(&[u8], u64) -> io::Result<()>,
) -> io::Result<u64> {
    let mut rows = 0;
    each_error(part, &mut |record, time| {
        let mut out = Vec::new();
        encoder.error(record, time, &mut out);
        rows += 1;
        emit(&out, 1)
    })?;
    Ok(rows)
}

/// Calls `f` with each row of an errors part and the time it happened.
fn each_error(
    part: &Path,
    f: &mut dyn FnMut(&ErrorRecord<'_>, Timestamp) -> io::Result<()>,
) -> io::Result<()> {
    for batch in batches(part)? {
        let batch = batch?;
        let missing = |name: &str| annotate(part, &io::Error::other(format!("no `{name}` column")));
        let text = |name: &str| batch.column_by_name(name).map(|c| c.as_string::<i32>());
        let (path, op, message) = (
            text("path").ok_or_else(|| missing("path"))?,
            text("op").ok_or_else(|| missing("op"))?,
            text("message").ok_or_else(|| missing("message"))?,
        );
        let raw = batch
            .column_by_name("path_raw")
            .map(|c| c.as_binary::<i32>());
        let errno = batch
            .column_by_name("errno")
            .map(|c| c.as_primitive::<Int32Type>());
        let time = batch
            .column_by_name("time")
            .ok_or_else(|| missing("time"))?;
        for row in 0..batch.num_rows() {
            let info = ErrorInfo {
                op: Op::from_name(op.value(row)).unwrap_or(Op::Internal),
                errno: errno.filter(|e| !e.is_null(row)).map(|e| e.value(row)),
                message: message.value(row).to_string(),
            };
            let record = ErrorRecord {
                path: exact(path, raw, row),
                info: &info,
            };
            f(&record, timestamp(time, row).unwrap_or_default())?;
        }
    }
    Ok(())
}

/// The exact bytes of a text column, from its `_raw` column when set.
fn exact<'a>(text: &'a StringArray, raw: Option<&'a BinaryArray>, row: usize) -> &'a [u8] {
    match raw {
        Some(raw) if !raw.is_null(row) => raw.value(row),
        _ => text.value(row).as_bytes(),
    }
}

fn timestamp(column: &dyn Array, row: usize) -> Option<Timestamp> {
    if column.is_null(row) {
        return None;
    }
    let (value, per_second) = match column.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => (
            column.as_primitive::<TimestampNanosecondType>().value(row),
            1_000_000_000,
        ),
        _ => (
            column.as_primitive::<TimestampMicrosecondType>().value(row),
            1_000_000,
        ),
    };
    let nsec = value.rem_euclid(per_second) * (1_000_000_000 / per_second);
    Some(Timestamp::new(value.div_euclid(per_second), nsec))
}

/// The `st_mode` file type bits of a kind.
const fn type_bits(kind: EntryKind) -> u32 {
    match kind {
        EntryKind::File => 0o100_000,
        EntryKind::Dir => 0o040_000,
        EntryKind::Symlink => 0o120_000,
        EntryKind::Fifo => 0o010_000,
        EntryKind::Socket => 0o140_000,
        EntryKind::CharDevice => 0o020_000,
        EntryKind::BlockDevice => 0o060_000,
        EntryKind::Unknown => 0,
    }
}

/// One batch's entry columns, looked up once. Columns the scan left out
/// with `--columns` are `None`.
struct EntryColumns<'a> {
    path: &'a StringArray,
    path_raw: Option<&'a BinaryArray>,
    depth: Option<&'a PrimitiveArray<Int32Type>>,
    dir_id: Option<&'a PrimitiveArray<Int64Type>>,
    parent_id: Option<&'a PrimitiveArray<Int64Type>>,
    dev: Option<&'a PrimitiveArray<UInt64Type>>,
    ino: Option<&'a PrimitiveArray<UInt64Type>>,
    nlink: Option<&'a PrimitiveArray<Int64Type>>,
    uid: Option<&'a PrimitiveArray<Int64Type>>,
    gid: Option<&'a PrimitiveArray<Int64Type>>,
    mode: Option<&'a PrimitiveArray<Int32Type>>,
    kind: Option<&'a StringArray>,
    size: Option<&'a PrimitiveArray<Int64Type>>,
    alloc_bytes: Option<&'a PrimitiveArray<Int64Type>>,
    times: [Option<&'a dyn Array>; 4],
    rdev: Option<&'a PrimitiveArray<UInt64Type>>,
    totals: [Option<&'a PrimitiveArray<Int64Type>>; 5],
    is_mount_point: Option<&'a BooleanArray>,
    time_flags: Option<&'a PrimitiveArray<Int32Type>>,
    error: Option<&'a StringArray>,
    target: Option<&'a StringArray>,
    target_raw: Option<&'a BinaryArray>,
}

impl<'a> EntryColumns<'a> {
    fn of(batch: &'a RecordBatch) -> io::Result<Self> {
        let column = |name: &str| batch.column_by_name(name).map(|c| &**c);
        let i32s = |name| column(name).map(|c| c.as_primitive::<Int32Type>());
        let i64s = |name| column(name).map(|c| c.as_primitive::<Int64Type>());
        let u64s = |name| column(name).map(|c| c.as_primitive::<UInt64Type>());
        let text = |name| column(name).map(|c| c.as_string::<i32>());
        let binary = |name| column(name).map(|c| c.as_binary::<i32>());
        Ok(Self {
            path: text("path").ok_or_else(|| io::Error::other("no `path` column"))?,
            path_raw: binary("path_raw"),
            depth: i32s("depth"),
            dir_id: i64s("dir_id"),
            parent_id: i64s("parent_id"),
            dev: u64s("dev"),
            ino: u64s("ino"),
            nlink: i64s("nlink"),
            uid: i64s("uid"),
            gid: i64s("gid"),
            mode: i32s("mode"),
            kind: text("kind"),
            size: i64s("size"),
            alloc_bytes: i64s("alloc_bytes"),
            times: ["atime", "mtime", "ctime", "btime"].map(column),
            rdev: u64s("rdev"),
            totals: [
                "dir_entries",
                "dir_files",
                "dir_subdirs",
                "dir_bytes",
                "dir_alloc_bytes",
            ]
            .map(i64s),
            is_mount_point: column("is_mount_point").map(|c| c.as_boolean()),
            time_flags: i32s("time_flags"),
            error: text("error"),
            target: text("symlink_target"),
            target_raw: binary("symlink_target_raw"),
        })
    }

    fn encode(&self, row: usize, encoder: &Encoder, out: &mut Vec<u8>) {
        fn get<T: arrow_array::ArrowPrimitiveType>(
            column: Option<&PrimitiveArray<T>>,
            row: usize,
        ) -> Option<T::Native> {
            column.filter(|c| !c.is_null(row)).map(|c| c.value(row))
        }
        let unsigned = |v: Option<i64>| v.map_or(0, |v| u64::try_from(v).unwrap_or(0));
        let [atime, mtime, ctime, btime] = self.times.map(|c| c.and_then(|c| timestamp(c, row)));
        // Without `mode` (left out with --columns), the type comes from `kind`.
        let mode = get(self.mode, row).map_or_else(
            || {
                self.kind
                    .filter(|k| !k.is_null(row))
                    .and_then(|k| {
                        EntryKind::ALL
                            .into_iter()
                            .find(|kind| kind.as_str() == k.value(row))
                    })
                    .map_or(0, type_bits)
            },
            |m| m as u32,
        );
        let stat = Stat {
            dev: get(self.dev, row).unwrap_or(0),
            ino: get(self.ino, row).unwrap_or(0),
            mode,
            nlink: unsigned(get(self.nlink, row)),
            uid: get(self.uid, row).map_or(0, |v| u32::try_from(v).unwrap_or(u32::MAX)),
            gid: get(self.gid, row).map_or(0, |v| u32::try_from(v).unwrap_or(u32::MAX)),
            rdev: get(self.rdev, row).unwrap_or(0),
            size: get(self.size, row).unwrap_or(0),
            blocks: unsigned(get(self.alloc_bytes, row)) / 512,
            atime: atime.unwrap_or_default(),
            mtime: mtime.unwrap_or_default(),
            ctime: ctime.unwrap_or_default(),
            btime,
        };
        let [entries, files, subdirs, bytes, alloc_bytes] = self.totals.map(|c| get(c, row));
        let totals = entries.map(|entries| DirTotals {
            entries: unsigned(Some(entries)),
            files: unsigned(files),
            subdirs: unsigned(subdirs),
            bytes: unsigned(bytes),
            alloc_bytes: unsigned(alloc_bytes),
        });
        // `error` holds `op: detail`, which an `ErrorInfo` without an errno
        // renders back exactly.
        let error =
            self.error
                .filter(|e| !e.is_null(row))
                .map(|e| match e.value(row).split_once(": ") {
                    Some((op, detail)) if Op::from_name(op).is_some() => ErrorInfo {
                        op: Op::from_name(op).unwrap_or(Op::Internal),
                        errno: None,
                        message: detail.to_string(),
                    },
                    _ => ErrorInfo {
                        op: Op::Internal,
                        errno: None,
                        message: e.value(row).to_string(),
                    },
                });
        let path = exact(self.path, self.path_raw, row);
        let start = name_start(path);
        let target = self
            .target
            .filter(|t| !t.is_null(row))
            .map(|t| exact(t, self.target_raw, row));
        let entry = Entry {
            path,
            parent_path: parent_of(path, start),
            name: &path[start..],
            depth: get(self.depth, row).map_or(0, |d| d as u32),
            dir_id: get(self.dir_id, row).map(|id| unsigned(Some(id))),
            parent_id: unsigned(get(self.parent_id, row)),
            stat: &stat,
            is_mount_point: self
                .is_mount_point
                .is_some_and(|c| !c.is_null(row) && c.value(row)),
            totals,
            error: error.as_ref(),
            symlink_target: target,
        };
        let clamped = get(self.time_flags, row).map_or(0, |f| f as u8);
        encoder.entry_clamped(&entry, clamped, out);
    }
}
