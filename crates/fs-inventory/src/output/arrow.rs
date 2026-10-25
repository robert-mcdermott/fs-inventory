// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Builds Arrow record batches of entry and error rows for Parquet output.

use std::borrow::Cow;
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Int32Builder, Int64Builder, StringBuilder, UInt8Builder,
    UInt32Builder, UInt64Builder,
};
use arrow_array::types::{TimestampMicrosecondType, TimestampNanosecondType};
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{Schema, TimeUnit};
use fs_inventory_core::model::{Entry, EntryKind, ErrorRecord, Timestamp};
use fs_inventory_core::names::{self, Controls};

use super::encode::error_summary;
use super::schema::{ENTRY_COLUMNS, ERROR_COLUMNS, Selection, arrow_schema};

/// How times and paths are stored.
#[derive(Debug, Clone, Copy)]
pub struct ArrowOptions {
    pub unit: TimeUnit,
    pub raw_paths_always: bool,
}

/// Converts a timestamp to the unit, clamping values the unit can't hold.
/// Returns the value and whether it was clamped.
fn to_unit(t: Timestamp, unit: TimeUnit) -> (i64, bool) {
    let (per_second, divisor) = match unit {
        TimeUnit::Nanosecond => (1_000_000_000, 1),
        TimeUnit::Microsecond => (1_000_000, 1_000),
        TimeUnit::Millisecond => (1_000, 1_000_000),
        TimeUnit::Second => (1, 1_000_000_000),
    };
    let fraction = i64::from(t.nsec) / divisor;
    match t
        .sec
        .checked_mul(per_second)
        .and_then(|v| v.checked_add(fraction))
    {
        Some(v) => (v, false),
        None if t.sec < 0 => (i64::MIN, true),
        None => (i64::MAX, true),
    }
}

fn timestamps(values: &mut Int64Builder, unit: TimeUnit) -> ArrayRef {
    let values: Int64Array = values.finish();
    match unit {
        TimeUnit::Nanosecond => Arc::new(
            values
                .reinterpret_cast::<TimestampNanosecondType>()
                .with_timezone("UTC"),
        ),
        _ => Arc::new(
            values
                .reinterpret_cast::<TimestampMicrosecondType>()
                .with_timezone("UTC"),
        ),
    }
}

#[allow(clippy::ptr_arg, reason = "whether the Cow is borrowed is the point")]
/// Exact bytes for the raw-path column, when needed: `text` is the rendered
/// path, which is borrowed only when identical to the bytes.
fn raw_path<'a>(path: &'a [u8], text: &Cow<'_, str>, always: bool) -> Option<&'a [u8]> {
    let differs = matches!(text, Cow::Owned(_));
    (always || differs).then_some(path)
}

/// Accumulates entry rows column by column.
pub struct EntryBatchBuilder {
    options: ArrowOptions,
    schema: Arc<Schema>,
    projection: Option<Vec<usize>>,
    bytes: usize,
    path: StringBuilder,
    parent_path: StringBuilder,
    name: StringBuilder,
    extension: StringBuilder,
    path_raw: BinaryBuilder,
    name_flags: UInt32Builder,
    kind: StringBuilder,
    depth: UInt32Builder,
    dir_id: UInt64Builder,
    parent_id: UInt64Builder,
    dev: UInt64Builder,
    ino: UInt64Builder,
    nlink: UInt64Builder,
    uid: UInt32Builder,
    gid: UInt32Builder,
    mode: UInt32Builder,
    size: Int64Builder,
    alloc_bytes: UInt64Builder,
    atime: Int64Builder,
    mtime: Int64Builder,
    ctime: Int64Builder,
    btime: Int64Builder,
    rdev: UInt64Builder,
    dir_entries: UInt64Builder,
    dir_files: UInt64Builder,
    dir_subdirs: UInt64Builder,
    dir_bytes: UInt64Builder,
    dir_alloc_bytes: UInt64Builder,
    is_mount_point: BooleanBuilder,
    time_flags: UInt8Builder,
    error: StringBuilder,
}

impl EntryBatchBuilder {
    pub fn new(options: ArrowOptions, selection: Selection) -> Self {
        Self {
            options,
            schema: arrow_schema(&ENTRY_COLUMNS, options.unit),
            projection: (!selection.is_all()).then(|| selection.indices()),
            bytes: 0,
            path: StringBuilder::new(),
            parent_path: StringBuilder::new(),
            name: StringBuilder::new(),
            extension: StringBuilder::new(),
            path_raw: BinaryBuilder::new(),
            name_flags: UInt32Builder::new(),
            kind: StringBuilder::new(),
            depth: UInt32Builder::new(),
            dir_id: UInt64Builder::new(),
            parent_id: UInt64Builder::new(),
            dev: UInt64Builder::new(),
            ino: UInt64Builder::new(),
            nlink: UInt64Builder::new(),
            uid: UInt32Builder::new(),
            gid: UInt32Builder::new(),
            mode: UInt32Builder::new(),
            size: Int64Builder::new(),
            alloc_bytes: UInt64Builder::new(),
            atime: Int64Builder::new(),
            mtime: Int64Builder::new(),
            ctime: Int64Builder::new(),
            btime: Int64Builder::new(),
            rdev: UInt64Builder::new(),
            dir_entries: UInt64Builder::new(),
            dir_files: UInt64Builder::new(),
            dir_subdirs: UInt64Builder::new(),
            dir_bytes: UInt64Builder::new(),
            dir_alloc_bytes: UInt64Builder::new(),
            is_mount_point: BooleanBuilder::new(),
            time_flags: UInt8Builder::new(),
            error: StringBuilder::new(),
        }
    }

    /// Approximate memory held by the buffered rows.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Appends one row. Every value is computed before any column is
    /// touched, so a panic can't leave the columns with different lengths.
    pub fn append(&mut self, e: &Entry<'_>) {
        let path = names::to_text(e.path, Controls::Keep);
        let parent_path = names::to_text(e.parent_path, Controls::Keep);
        let name = names::to_text(e.name, Controls::Keep);
        let extension = match e.kind() {
            EntryKind::Dir => None,
            _ => names::extension(e.name),
        };
        let extension = extension
            .as_deref()
            .map(|ext| names::to_text(ext, Controls::Keep).into_owned());
        let raw = raw_path(e.path, &path, self.options.raw_paths_always);
        let flags = names::name_flags(e.name, e.path.len()).bits();
        let stat = e.stat;
        let unit = self.options.unit;
        let atime = to_unit(stat.atime, unit);
        let mtime = to_unit(stat.mtime, unit);
        let ctime = to_unit(stat.ctime, unit);
        let btime = stat.btime.map(|t| to_unit(t, unit));
        let time_flags = u8::from(atime.1)
            | u8::from(mtime.1) << 1
            | u8::from(ctime.1) << 2
            | u8::from(btime.is_some_and(|b| b.1)) << 3;
        let is_device = matches!(e.kind(), EntryKind::CharDevice | EntryKind::BlockDevice);
        let totals = e.totals;
        let error = e.error.map(error_summary);
        let bytes = path.len() + parent_path.len() + name.len() + raw.map_or(0, <[u8]>::len);

        self.path.append_value(&path);
        self.parent_path.append_value(&parent_path);
        self.name.append_value(&name);
        self.extension.append_option(extension.as_deref());
        self.path_raw.append_option(raw);
        self.name_flags.append_value(flags);
        self.kind.append_value(e.kind().as_str());
        self.depth.append_value(e.depth);
        self.dir_id.append_option(e.dir_id);
        self.parent_id.append_value(e.parent_id);
        self.dev.append_value(stat.dev);
        self.ino.append_value(stat.ino);
        self.nlink.append_value(stat.nlink);
        self.uid.append_value(stat.uid);
        self.gid.append_value(stat.gid);
        self.mode.append_value(stat.mode);
        self.size.append_value(stat.size);
        self.alloc_bytes.append_value(stat.alloc_bytes());
        self.atime.append_value(atime.0);
        self.mtime.append_value(mtime.0);
        self.ctime.append_value(ctime.0);
        self.btime.append_option(btime.map(|b| b.0));
        self.rdev.append_option(is_device.then_some(stat.rdev));
        self.dir_entries.append_option(totals.map(|t| t.entries));
        self.dir_files.append_option(totals.map(|t| t.files));
        self.dir_subdirs.append_option(totals.map(|t| t.subdirs));
        self.dir_bytes.append_option(totals.map(|t| t.bytes));
        self.dir_alloc_bytes
            .append_option(totals.map(|t| t.alloc_bytes));
        self.is_mount_point.append_value(e.is_mount_point);
        self.time_flags.append_value(time_flags);
        self.error.append_option(error.as_deref());

        // Strings plus about 200 bytes of fixed-width columns.
        self.bytes += bytes + 200;
    }

    /// Takes the buffered rows as a batch with the selected columns.
    pub fn finish(&mut self) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let unit = self.options.unit;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.path.finish()),
            Arc::new(self.parent_path.finish()),
            Arc::new(self.name.finish()),
            Arc::new(self.extension.finish()),
            Arc::new(self.path_raw.finish()),
            Arc::new(self.name_flags.finish()),
            Arc::new(self.kind.finish()),
            Arc::new(self.depth.finish()),
            Arc::new(self.dir_id.finish()),
            Arc::new(self.parent_id.finish()),
            Arc::new(self.dev.finish()),
            Arc::new(self.ino.finish()),
            Arc::new(self.nlink.finish()),
            Arc::new(self.uid.finish()),
            Arc::new(self.gid.finish()),
            Arc::new(self.mode.finish()),
            Arc::new(self.size.finish()),
            Arc::new(self.alloc_bytes.finish()),
            timestamps(&mut self.atime, unit),
            timestamps(&mut self.mtime, unit),
            timestamps(&mut self.ctime, unit),
            timestamps(&mut self.btime, unit),
            Arc::new(self.rdev.finish()),
            Arc::new(self.dir_entries.finish()),
            Arc::new(self.dir_files.finish()),
            Arc::new(self.dir_subdirs.finish()),
            Arc::new(self.dir_bytes.finish()),
            Arc::new(self.dir_alloc_bytes.finish()),
            Arc::new(self.is_mount_point.finish()),
            Arc::new(self.time_flags.finish()),
            Arc::new(self.error.finish()),
        ];
        self.bytes = 0;
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        match &self.projection {
            Some(indices) => batch.project(indices),
            None => Ok(batch),
        }
    }
}

/// The Arrow schema of finished entry batches.
pub fn entry_schema(options: ArrowOptions, selection: Selection) -> Arc<Schema> {
    let full = arrow_schema(&ENTRY_COLUMNS, options.unit);
    if selection.is_all() {
        return full;
    }
    let fields: Vec<_> = selection
        .indices()
        .into_iter()
        .map(|i| full.field(i).clone())
        .collect();
    Arc::new(Schema::new(fields))
}

/// Accumulates error rows.
pub struct ErrorBatchBuilder {
    options: ArrowOptions,
    schema: Arc<Schema>,
    path: StringBuilder,
    path_raw: BinaryBuilder,
    op: StringBuilder,
    errno: Int32Builder,
    errno_name: StringBuilder,
    message: StringBuilder,
    time: Int64Builder,
}

impl ErrorBatchBuilder {
    pub fn new(options: ArrowOptions) -> Self {
        Self {
            options,
            schema: arrow_schema(&ERROR_COLUMNS, options.unit),
            path: StringBuilder::new(),
            path_raw: BinaryBuilder::new(),
            op: StringBuilder::new(),
            errno: Int32Builder::new(),
            errno_name: StringBuilder::new(),
            message: StringBuilder::new(),
            time: Int64Builder::new(),
        }
    }

    pub fn append(&mut self, r: &ErrorRecord<'_>, now: Timestamp) {
        let path = names::to_text(r.path, Controls::Keep);
        let raw = raw_path(r.path, &path, self.options.raw_paths_always);
        self.path_raw.append_option(raw);
        self.path.append_value(&path);
        self.op.append_value(r.info.op.as_str());
        self.errno.append_option(r.info.errno);
        self.errno_name.append_option(r.info.errno_name());
        self.message.append_value(&r.info.message);
        self.time.append_value(to_unit(now, self.options.unit).0);
    }

    pub fn finish(&mut self) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.path.finish()),
            Arc::new(self.path_raw.finish()),
            Arc::new(self.op.finish()),
            Arc::new(self.errno.finish()),
            Arc::new(self.errno_name.finish()),
            Arc::new(self.message.finish()),
            timestamps(&mut self.time, self.options.unit),
        ];
        RecordBatch::try_new(Arc::clone(&self.schema), columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array as _;
    use arrow_array::cast::AsArray as _;
    use fs_inventory_core::model::{ErrorInfo, Op, Stat};

    fn options() -> ArrowOptions {
        ArrowOptions {
            unit: TimeUnit::Microsecond,
            raw_paths_always: false,
        }
    }

    #[test]
    fn time_conversion_clamps() {
        let t = |sec, nsec| Timestamp { sec, nsec };
        assert_eq!(
            to_unit(t(1, 500_000_000), TimeUnit::Microsecond),
            (1_500_000, false)
        );
        assert_eq!(
            to_unit(t(-1, 500_000_000), TimeUnit::Microsecond),
            (-500_000, false)
        );
        assert_eq!(
            to_unit(t(1, 123_456_789), TimeUnit::Nanosecond),
            (1_123_456_789, false)
        );
        assert_eq!(
            to_unit(t(i64::MAX, 0), TimeUnit::Microsecond),
            (i64::MAX, true)
        );
        assert_eq!(
            to_unit(t(i64::MIN, 0), TimeUnit::Nanosecond),
            (i64::MIN, true)
        );
        // 2262-04-12 is past the nanosecond range.
        assert!(to_unit(t(9_224_000_000, 0), TimeUnit::Nanosecond).1);
        assert!(!to_unit(t(9_224_000_000, 0), TimeUnit::Microsecond).1);
    }

    #[test]
    fn batches_match_the_schema() {
        let stat = Stat {
            mode: 0o100644,
            size: 42,
            ..Stat::default()
        };
        let mut builder = EntryBatchBuilder::new(options(), Selection::ALL);
        for path in [&b"/d/caf\xe9.TXT"[..], b"/d/plain"] {
            builder.append(&Entry {
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
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(builder.finish().unwrap().num_rows(), 0);
        let names: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let expected: Vec<_> = ENTRY_COLUMNS.iter().map(|c| c.name.to_string()).collect();
        assert_eq!(names, expected);
        let path = batch.column(0).as_string::<i32>();
        assert_eq!(path.value(0), "/d/caf\\xE9.TXT");
        let raw = batch.column(4).as_binary::<i32>();
        assert_eq!(raw.value(0), b"/d/caf\xe9.TXT");
        assert!(raw.is_null(1));
        assert_eq!(batch.column(3).as_string::<i32>().value(0), "txt");
    }

    #[test]
    fn selection_projects_columns() {
        let selection = Selection::parse(&["path".into(), "size".into()]).unwrap();
        let mut builder = EntryBatchBuilder::new(options(), selection);
        let stat = Stat::default();
        builder.append(&Entry {
            path: b"/x",
            parent_path: b"/",
            name: b"x",
            depth: 1,
            dir_id: None,
            parent_id: 1,
            stat: &stat,
            is_mount_point: false,
            totals: None,
            error: None,
        });
        let batch = builder.finish().unwrap();
        assert_eq!(batch.schema(), entry_schema(options(), selection));
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn error_batches() {
        let info = ErrorInfo {
            op: Op::Open,
            errno: Some(13),
            message: "Permission denied".into(),
        };
        let mut builder = ErrorBatchBuilder::new(options());
        builder.append(
            &ErrorRecord {
                path: b"/x",
                info: &info,
            },
            Timestamp::default(),
        );
        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), ERROR_COLUMNS.len());
    }
}
