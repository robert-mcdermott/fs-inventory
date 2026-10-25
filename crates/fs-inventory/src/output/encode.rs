// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! CSV and JSON Lines encoding of entry and error rows.

use std::borrow::Cow;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use fs_inventory_core::model::{Entry, EntryKind, ErrorInfo, ErrorRecord, Timestamp};
use fs_inventory_core::names::{self, Controls};

use super::timefmt::{self, Precision};

/// Columns of the entries table, in output order.
pub const ENTRY_COLUMNS: [&str; 31] = [
    "path",
    "parent_path",
    "name",
    "extension",
    "path_b64",
    "name_flags",
    "kind",
    "depth",
    "dir_id",
    "parent_id",
    "dev",
    "ino",
    "nlink",
    "uid",
    "gid",
    "mode",
    "size",
    "alloc_bytes",
    "atime",
    "mtime",
    "ctime",
    "btime",
    "rdev",
    "dir_entries",
    "dir_files",
    "dir_subdirs",
    "dir_bytes",
    "dir_alloc_bytes",
    "is_mount_point",
    "time_flags",
    "error",
];

/// Columns of the errors table, in output order.
pub const ERROR_COLUMNS: [&str; 7] = [
    "path",
    "path_b64",
    "op",
    "errno",
    "errno_name",
    "message",
    "time",
];

/// Bits of the `time_flags` column: the timestamp could not be represented
/// and was clamped.
const ATIME_CLAMPED: u8 = 1 << 0;
const MTIME_CLAMPED: u8 = 1 << 1;
const CTIME_CLAMPED: u8 = 1 << 2;
const BTIME_CLAMPED: u8 = 1 << 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextFormat {
    Csv,
    Jsonl,
}

impl TextFormat {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Jsonl => "jsonl",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeStyle {
    Iso,
    Epoch,
}

/// How rows are rendered.
#[derive(Debug, Clone, Copy)]
pub struct EncodeOptions {
    pub format: TextFormat,
    /// Fill `path_b64` for every row, not only when `path` differs from the
    /// original bytes.
    pub raw_paths_always: bool,
    /// Keep control characters as literals in CSV text fields.
    pub csv_raw_controls: bool,
    pub precision: Precision,
    pub time_style: TimeStyle,
}

/// Encodes rows into a byte buffer.
#[derive(Debug, Clone, Copy)]
pub struct Encoder {
    options: EncodeOptions,
}

impl Encoder {
    pub fn new(options: EncodeOptions) -> Self {
        Self { options }
    }

    pub fn format(&self) -> TextFormat {
        self.options.format
    }

    fn controls(&self) -> Controls {
        match self.options.format {
            TextFormat::Csv if !self.options.csv_raw_controls => Controls::Escape,
            // JSON escapes control characters itself.
            _ => Controls::Keep,
        }
    }

    /// The header line for a table, or nothing for JSON Lines.
    pub fn header(&self, columns: &[&str]) -> Vec<u8> {
        match self.options.format {
            TextFormat::Csv => {
                let mut out = columns.join(",").into_bytes();
                out.push(b'\n');
                out
            }
            TextFormat::Jsonl => Vec::new(),
        }
    }

    pub fn entry(&self, e: &Entry<'_>, out: &mut Vec<u8>) {
        let controls = self.controls();
        let path = names::to_text(e.path, controls);
        let path_b64 = (self.options.raw_paths_always || matches!(path, Cow::Owned(_)))
            .then(|| BASE64.encode(e.path));
        let extension = match e.kind() {
            EntryKind::Dir => None,
            _ => names::extension(e.name),
        };
        let extension = extension
            .as_deref()
            .map(|ext| names::to_text(ext, controls));
        let flags = names::name_flags(e.name, e.path.len());
        let stat = e.stat;
        let is_device = matches!(e.kind(), EntryKind::CharDevice | EntryKind::BlockDevice);

        let mut row = Row::new(self.options.format, out);
        row.text("path", &path);
        row.text("parent_path", &names::to_text(e.parent_path, controls));
        row.text("name", &names::to_text(e.name, controls));
        row.opt_text("extension", extension.as_deref());
        row.opt_text("path_b64", path_b64.as_deref());
        row.uint("name_flags", u64::from(flags.bits()));
        row.text("kind", e.kind().as_str());
        row.uint("depth", u64::from(e.depth));
        row.opt_uint("dir_id", e.dir_id);
        row.uint("parent_id", e.parent_id);
        row.uint("dev", stat.dev);
        row.uint("ino", stat.ino);
        row.uint("nlink", stat.nlink);
        row.uint("uid", u64::from(stat.uid));
        row.uint("gid", u64::from(stat.gid));
        row.uint("mode", u64::from(stat.mode));
        row.int("size", stat.size);
        row.uint("alloc_bytes", stat.alloc_bytes());
        let mut time_flags = 0;
        for (name, time, clamped) in [
            ("atime", Some(stat.atime), ATIME_CLAMPED),
            ("mtime", Some(stat.mtime), MTIME_CLAMPED),
            ("ctime", Some(stat.ctime), CTIME_CLAMPED),
            ("btime", stat.btime, BTIME_CLAMPED),
        ] {
            if !row.time(name, time, self.options) {
                time_flags |= clamped;
            }
        }
        row.opt_uint("rdev", is_device.then_some(stat.rdev));
        let totals = e.totals;
        row.opt_uint("dir_entries", totals.map(|t| t.entries));
        row.opt_uint("dir_files", totals.map(|t| t.files));
        row.opt_uint("dir_subdirs", totals.map(|t| t.subdirs));
        row.opt_uint("dir_bytes", totals.map(|t| t.bytes));
        row.opt_uint("dir_alloc_bytes", totals.map(|t| t.alloc_bytes));
        row.bool("is_mount_point", e.is_mount_point);
        row.uint("time_flags", u64::from(time_flags));
        let error = e.error.map(error_summary);
        row.opt_text("error", error.as_deref());
        row.end();
    }

    pub fn error(&self, r: &ErrorRecord<'_>, now: Timestamp, out: &mut Vec<u8>) {
        let controls = self.controls();
        let path = names::to_text(r.path, controls);
        let path_b64 = (self.options.raw_paths_always || matches!(path, Cow::Owned(_)))
            .then(|| BASE64.encode(r.path));
        let mut row = Row::new(self.options.format, out);
        row.text("path", &path);
        row.opt_text("path_b64", path_b64.as_deref());
        row.text("op", r.info.op.as_str());
        row.opt_int("errno", r.info.errno.map(i64::from));
        row.opt_text("errno_name", r.info.errno_name());
        row.text("message", &r.info.message);
        row.time("time", Some(now), self.options);
        row.end();
    }
}

/// A short description for the `error` column, such as `open: EACCES`.
fn error_summary(info: &ErrorInfo) -> String {
    match info.errno_name() {
        Some(name) => format!("{}: {name}", info.op.as_str()),
        None => format!("{}: {}", info.op.as_str(), info.message),
    }
}

/// Writes one row field by field.
struct Row<'a> {
    format: TextFormat,
    out: &'a mut Vec<u8>,
    first: bool,
}

impl<'a> Row<'a> {
    fn new(format: TextFormat, out: &'a mut Vec<u8>) -> Self {
        if format == TextFormat::Jsonl {
            out.push(b'{');
        }
        Self {
            format,
            out,
            first: true,
        }
    }

    fn key(&mut self, name: &str) {
        if !self.first {
            self.out.push(b',');
        }
        self.first = false;
        if self.format == TextFormat::Jsonl {
            self.out.push(b'"');
            self.out.extend_from_slice(name.as_bytes());
            self.out.extend_from_slice(b"\":");
        }
    }

    fn null(&mut self) {
        if self.format == TextFormat::Jsonl {
            self.out.extend_from_slice(b"null");
        }
    }

    fn text(&mut self, name: &str, value: &str) {
        self.key(name);
        match self.format {
            TextFormat::Csv => csv_field(self.out, value),
            TextFormat::Jsonl => json_string(self.out, value),
        }
    }

    fn opt_text(&mut self, name: &str, value: Option<&str>) {
        match value {
            Some(v) => self.text(name, v),
            None => {
                self.key(name);
                self.null();
            }
        }
    }

    fn uint(&mut self, name: &str, value: u64) {
        self.key(name);
        self.out
            .extend_from_slice(itoa::Buffer::new().format(value).as_bytes());
    }

    fn int(&mut self, name: &str, value: i64) {
        self.key(name);
        self.out
            .extend_from_slice(itoa::Buffer::new().format(value).as_bytes());
    }

    fn opt_uint(&mut self, name: &str, value: Option<u64>) {
        match value {
            Some(v) => self.uint(name, v),
            None => {
                self.key(name);
                self.null();
            }
        }
    }

    fn opt_int(&mut self, name: &str, value: Option<i64>) {
        match value {
            Some(v) => self.int(name, v),
            None => {
                self.key(name);
                self.null();
            }
        }
    }

    fn bool(&mut self, name: &str, value: bool) {
        self.key(name);
        self.out
            .extend_from_slice(if value { b"true" } else { b"false" });
    }

    /// Writes a timestamp. Returns `false` if it was clamped.
    fn time(&mut self, name: &str, value: Option<Timestamp>, options: EncodeOptions) -> bool {
        self.key(name);
        let Some(t) = value else {
            self.null();
            return true;
        };
        match options.time_style {
            TimeStyle::Iso => {
                let quote = self.format == TextFormat::Jsonl;
                if quote {
                    self.out.push(b'"');
                }
                let in_range = timefmt::write_iso(self.out, t, options.precision);
                if quote {
                    self.out.push(b'"');
                }
                in_range
            }
            TimeStyle::Epoch => {
                timefmt::write_epoch(self.out, t, options.precision);
                true
            }
        }
    }

    fn end(self) {
        if self.format == TextFormat::Jsonl {
            self.out.push(b'}');
        }
        self.out.push(b'\n');
    }
}

/// Writes an RFC 4180 field, quoting it only when needed.
fn csv_field(out: &mut Vec<u8>, value: &str) {
    let needs_quotes = value
        .bytes()
        .any(|b| matches!(b, b',' | b'"' | b'\n' | b'\r'));
    if !needs_quotes {
        out.extend_from_slice(value.as_bytes());
        return;
    }
    out.push(b'"');
    for b in value.bytes() {
        if b == b'"' {
            out.push(b'"');
        }
        out.push(b);
    }
    out.push(b'"');
}

/// Writes a JSON string literal.
fn json_string(out: &mut Vec<u8>, value: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push(b'"');
    let bytes = value.as_bytes();
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let escape: &[u8] = match b {
            b'"' => b"\\\"",
            b'\\' => b"\\\\",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            b'\t' => b"\\t",
            0x00..=0x1f => {
                out.extend_from_slice(&bytes[start..i]);
                out.extend_from_slice(b"\\u00");
                out.push(HEX[usize::from(b >> 4)]);
                out.push(HEX[usize::from(b & 0xf)]);
                start = i + 1;
                continue;
            }
            _ => continue,
        };
        out.extend_from_slice(&bytes[start..i]);
        out.extend_from_slice(escape);
        start = i + 1;
    }
    out.extend_from_slice(&bytes[start..]);
    out.push(b'"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs_inventory_core::model::{DirTotals, Op, Stat};

    fn options(format: TextFormat) -> EncodeOptions {
        EncodeOptions {
            format,
            raw_paths_always: false,
            csv_raw_controls: false,
            precision: Precision::Micros,
            time_style: TimeStyle::Iso,
        }
    }

    fn file_stat() -> Stat {
        Stat {
            dev: 66,
            ino: 1234,
            mode: 0o100644,
            nlink: 1,
            uid: 1000,
            gid: 100,
            size: 42,
            blocks: 8,
            mtime: Timestamp {
                sec: 0,
                nsec: 1_500,
            },
            ..Stat::default()
        }
    }

    fn encode(format: TextFormat, path: &[u8], name_start: usize, stat: &Stat) -> String {
        let entry = Entry {
            path,
            parent_path: &path[..name_start - 1],
            name: &path[name_start..],
            depth: 1,
            dir_id: None,
            parent_id: 7,
            stat,
            is_mount_point: false,
            totals: None,
            error: None,
        };
        let mut out = Vec::new();
        Encoder::new(options(format)).entry(&entry, &mut out);
        String::from_utf8(out).expect("output must be UTF-8")
    }

    #[test]
    fn csv_row() {
        let row = encode(TextFormat::Csv, b"/data/Report.PDF", 6, &file_stat());
        assert_eq!(
            row,
            "/data/Report.PDF,/data,Report.PDF,pdf,,0,file,1,,7,66,1234,1,1000,100,33188,42,4096,\
             1970-01-01T00:00:00.000000Z,1970-01-01T00:00:00.000001Z,1970-01-01T00:00:00.000000Z,\
             ,,,,,,,false,0,\n"
        );
        assert_eq!(row.split(',').count(), ENTRY_COLUMNS.len());
    }

    #[test]
    fn csv_quotes_and_escapes() {
        let row = encode(TextFormat::Csv, b"/d/a,\"b\"\nc", 3, &file_stat());
        assert!(
            row.starts_with("\"/d/a,\"\"b\"\"\\nc\",/d,\"a,\"\"b\"\"\\nc\","),
            "{row}"
        );
        // The escaped name differs from the bytes, so the exact bytes are kept.
        assert!(row.contains(&BASE64.encode(b"/d/a,\"b\"\nc")));
        assert_eq!(row.matches('\n').count(), 1);
    }

    #[test]
    fn invalid_utf8_keeps_exact_bytes() {
        let path = b"/d/caf\xe9.TXT";
        for format in [TextFormat::Csv, TextFormat::Jsonl] {
            let row = encode(format, path, 3, &file_stat());
            assert!(
                row.contains("caf\\\\xE9") || row.contains("caf\\xE9"),
                "{row}"
            );
            assert!(row.contains(&BASE64.encode(path)), "{row}");
        }
    }

    #[test]
    fn jsonl_row_is_valid_json() {
        let row = encode(TextFormat::Jsonl, b"/d/tab\there\x01", 3, &file_stat());
        let value: serde_json::Value = serde_json::from_str(&row).unwrap();
        assert_eq!(value["name"], "tab\there\u{1}");
        assert_eq!(value["path_b64"], serde_json::Value::Null);
        assert_eq!(value["kind"], "file");
        assert_eq!(value["size"], 42);
        assert_eq!(value["mtime"], "1970-01-01T00:00:00.000001Z");
        assert_eq!(value["btime"], serde_json::Value::Null);
        assert_eq!(value.as_object().unwrap().len(), ENTRY_COLUMNS.len());
        let keys: Vec<&String> = value.as_object().unwrap().keys().collect();
        assert_eq!(keys.len(), ENTRY_COLUMNS.len());
    }

    #[test]
    fn directory_rows_carry_totals_and_errors() {
        let stat = Stat {
            mode: 0o040755,
            ..Stat::default()
        };
        let info = ErrorInfo {
            op: Op::ReadDir,
            errno: Some(5),
            message: "Input/output error".into(),
        };
        let entry = Entry {
            path: b"/d/sub.dir",
            parent_path: b"/d",
            name: b"sub.dir",
            depth: 1,
            dir_id: Some(9),
            parent_id: 7,
            stat: &stat,
            is_mount_point: true,
            totals: Some(DirTotals {
                entries: 3,
                files: 2,
                subdirs: 1,
                bytes: 10,
                alloc_bytes: 4096,
            }),
            error: Some(&info),
        };
        let mut out = Vec::new();
        Encoder::new(options(TextFormat::Jsonl)).entry(&entry, &mut out);
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["extension"], serde_json::Value::Null);
        assert_eq!(value["dir_id"], 9);
        assert_eq!(value["dir_entries"], 3);
        assert_eq!(value["dir_alloc_bytes"], 4096);
        assert_eq!(value["is_mount_point"], true);
        assert_eq!(value["error"], "readdir: EIO");
    }

    #[test]
    fn clamped_times_are_flagged() {
        let stat = Stat {
            mtime: Timestamp {
                sec: i64::MAX,
                nsec: 0,
            },
            btime: Some(Timestamp {
                sec: i64::MIN,
                nsec: 0,
            }),
            ..file_stat()
        };
        let row = encode(TextFormat::Jsonl, b"/d/x", 3, &stat);
        let value: serde_json::Value = serde_json::from_str(&row).unwrap();
        assert_eq!(value["mtime"], "9999-12-31T23:59:59.999999Z");
        assert_eq!(value["btime"], "0001-01-01T00:00:00.000000Z");
        assert_eq!(
            value["time_flags"],
            u64::from(MTIME_CLAMPED | BTIME_CLAMPED)
        );
    }

    #[test]
    fn error_rows() {
        let info = ErrorInfo {
            op: Op::Open,
            errno: Some(13),
            message: "Permission denied (os error 13)".into(),
        };
        let record = ErrorRecord {
            path: b"/d/secret",
            info: &info,
        };
        let mut out = Vec::new();
        let encoder = Encoder::new(options(TextFormat::Jsonl));
        encoder.error(&record, Timestamp::default(), &mut out);
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["op"], "open");
        assert_eq!(value["errno"], 13);
        assert_eq!(value["errno_name"], "EACCES");
        assert_eq!(value.as_object().unwrap().len(), ERROR_COLUMNS.len());
    }

    #[test]
    fn json_strings_escape_everything_required() {
        let mut out = Vec::new();
        json_string(&mut out, "q\"b\\n\nt\tc\u{1}\u{1f}é");
        assert_eq!(
            String::from_utf8(out.clone()).unwrap(),
            r#""q\"b\\n\nt\tc\u0001\u001fé""#
        );
        let parsed: String = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed, "q\"b\\n\nt\tc\u{1}\u{1f}é");
    }
}
