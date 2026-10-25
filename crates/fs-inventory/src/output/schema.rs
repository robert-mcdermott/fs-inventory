// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! The inventory schema. This one definition drives the Parquet schema, the
//! CSV and JSON Lines columns, `fs-inventory schema`, and `docs/schema.md`.

use std::fmt::Write as _;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use fs_inventory_core::model::EntryKind;
use fs_inventory_core::names::NameFlags;

/// A column's logical type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Text,
    /// Raw bytes: binary in Parquet, base64 text in CSV and JSON Lines.
    Bytes,
    Bool,
    Int32,
    Int64,
    /// Only for opaque 64-bit identifiers whose top bit can be set.
    UInt64,
    /// UTC time, in microseconds (or nanoseconds with `--time-unit ns`).
    Timestamp,
}

impl ColumnType {
    /// The DuckDB type for Parquet output.
    pub fn duckdb(self) -> &'static str {
        match self {
            Self::Text => "VARCHAR",
            Self::Bytes => "BLOB",
            Self::Bool => "BOOLEAN",
            Self::Int32 => "INTEGER",
            Self::Int64 => "BIGINT",
            Self::UInt64 => "UBIGINT",
            Self::Timestamp => "TIMESTAMPTZ",
        }
    }

    /// The Arrow type written to Parquet.
    pub fn arrow(self, unit: TimeUnit) -> DataType {
        match self {
            Self::Text => DataType::Utf8,
            Self::Bytes => DataType::Binary,
            Self::Bool => DataType::Boolean,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::UInt64 => DataType::UInt64,
            Self::Timestamp => DataType::Timestamp(unit, Some("UTC".into())),
        }
    }
}

/// One column.
#[derive(Debug, Clone, Copy)]
pub struct Column {
    /// Name in Parquet and DuckDB.
    pub name: &'static str,
    /// Name in CSV and JSON Lines, which differs only for byte columns.
    pub text_name: &'static str,
    pub kind: ColumnType,
    pub nullable: bool,
    pub description: &'static str,
}

const fn col(
    name: &'static str,
    kind: ColumnType,
    nullable: bool,
    description: &'static str,
) -> Column {
    Column {
        name,
        text_name: name,
        kind,
        nullable,
        description,
    }
}

const fn bytes(name: &'static str, text_name: &'static str, description: &'static str) -> Column {
    Column {
        name,
        text_name,
        kind: ColumnType::Bytes,
        nullable: true,
        description,
    }
}

use ColumnType::*;

/// The entries table: one row per file, directory, or other entry.
pub const ENTRY_COLUMNS: [Column; 31] = [
    col(
        "path",
        Text,
        false,
        "Full path. Always valid UTF-8: bytes that aren't valid UTF-8 appear as `\\xNN`, and in CSV, control characters are escaped (`\\n`, `\\t`, `\\xNN`).",
    ),
    col(
        "parent_path",
        Text,
        false,
        "Path of the containing directory; empty for `/`.",
    ),
    col(
        "name",
        Text,
        false,
        "Final path component. For a scan root, the root's own name.",
    ),
    col(
        "extension",
        Text,
        true,
        "Lowercased text after the last `.` of a name that isn't a directory. None for names starting or ending with `.` or extensions longer than 32 bytes.",
    ),
    bytes(
        "path_raw",
        "path_b64",
        "Exact path bytes, when `path` differs from them (every row with `--raw-paths always`). Base64 in CSV and JSON Lines, as `path_b64`. Exact bytes for any row: `coalesce(path_raw, encode(path))`.",
    ),
    col(
        "name_flags",
        Int32,
        false,
        "Bitmask of name properties that cause trouble; see [Name flags](#name-flags).",
    ),
    col("kind", Text, false, "Entry type; see [Kinds](#kinds)."),
    col(
        "depth",
        Int32,
        false,
        "Levels below the scan root; 0 for the root.",
    ),
    col(
        "dir_id",
        Int64,
        true,
        "For directories, an ID assigned by the scan, unique within it.",
    ),
    col(
        "parent_id",
        Int64,
        false,
        "`dir_id` of the containing directory; 0 for a root.",
    ),
    col(
        "dev",
        UInt64,
        false,
        "Device of the file system holding the entry (`st_dev`). Not stable across hosts or remounts.",
    ),
    col(
        "ino",
        UInt64,
        false,
        "Inode number. Entries with the same `(dev, ino)` are hard links to one file.",
    ),
    col("nlink", Int64, false, "Number of hard links."),
    col("uid", Int64, false, "Owner user ID."),
    col("gid", Int64, false, "Owner group ID."),
    col(
        "mode",
        Int32,
        false,
        "Full `st_mode`: file type and permission bits.",
    ),
    col(
        "size",
        Int64,
        true,
        "Logical size in bytes. Null when skipped with `--fields`.",
    ),
    col(
        "alloc_bytes",
        Int64,
        true,
        "Space allocated on disk (`st_blocks` × 512). Smaller than `size` for sparse, compressed, or tiered files. Null when skipped with `--fields`.",
    ),
    col(
        "atime",
        Timestamp,
        true,
        "Last access. Null when skipped with `--fields`.",
    ),
    col(
        "mtime",
        Timestamp,
        true,
        "Last change to the contents. Copies and restores often preserve it. Null when skipped with `--fields`.",
    ),
    col(
        "ctime",
        Timestamp,
        true,
        "Last change to the contents or metadata. Not creation time. Users can't set it, so it's the reliable signal of recent change. Null when skipped with `--fields`.",
    ),
    col(
        "btime",
        Timestamp,
        true,
        "Creation (birth) time, when the platform and file system provide it and it isn't skipped with `--fields`.",
    ),
    col(
        "rdev",
        UInt64,
        true,
        "Device number, for character and block devices.",
    ),
    col(
        "dir_entries",
        Int64,
        true,
        "For directories that were read: number of immediate children.",
    ),
    col(
        "dir_files",
        Int64,
        true,
        "For directories that were read: immediate children that aren't directories.",
    ),
    col(
        "dir_subdirs",
        Int64,
        true,
        "For directories that were read: immediate subdirectories.",
    ),
    col(
        "dir_bytes",
        Int64,
        true,
        "For directories that were read: sum of `size` over immediate children that aren't directories.",
    ),
    col(
        "dir_alloc_bytes",
        Int64,
        true,
        "For directories that were read: sum of `alloc_bytes` over immediate children that aren't directories.",
    ),
    col(
        "is_mount_point",
        Bool,
        false,
        "True when this directory is on a different device from its parent.",
    ),
    col(
        "time_flags",
        Int32,
        false,
        "Bitmask of timestamps that couldn't be represented and were clamped: 1 `atime`, 2 `mtime`, 4 `ctime`, 8 `btime`.",
    ),
    col(
        "error",
        Text,
        true,
        "For directories that couldn't be fully read: the failed operation and error, such as `open: EACCES`. With `--detect-cycles`, `revisit: already visited through another path` marks a directory that was reached again and not re-read. With `--stall-timeout`, `stall: ...` marks a directory the scan gave up on.",
    ),
];

/// The errors table: one row per failed operation.
pub const ERROR_COLUMNS: [Column; 7] = [
    col(
        "path",
        Text,
        false,
        "Path of the entry the operation was on, rendered like `entries.path`.",
    ),
    bytes(
        "path_raw",
        "path_b64",
        "Exact path bytes, when `path` differs from them. Base64 in CSV and JSON Lines, as `path_b64`.",
    ),
    col(
        "op",
        Text,
        false,
        "The failed operation: `open`, `readdir`, `stat`, `verify` (the directory was replaced during the scan), `stall` (no progress for `--stall-timeout`, so the scan gave up on the directory), or `internal`.",
    ),
    col(
        "errno",
        Int32,
        true,
        "The OS error number, when there is one.",
    ),
    col(
        "errno_name",
        Text,
        true,
        "Symbolic name of `errno`, such as `EACCES`.",
    ),
    col("message", Text, false, "Human-readable description."),
    col("time", Timestamp, false, "When the error happened."),
];

/// The rollup table written by `fs-inventory rollup`: recursive totals for
/// every directory.
pub const ROLLUP_COLUMNS: [Column; 15] = [
    col(
        "dir_id",
        Int64,
        false,
        "The directory's `dir_id` in `entries`.",
    ),
    col(
        "parent_id",
        Int64,
        false,
        "`dir_id` of the containing directory; 0 for a root.",
    ),
    col(
        "depth",
        Int32,
        false,
        "Levels below the scan root; 0 for the root.",
    ),
    col(
        "path",
        Text,
        false,
        "Full path, rendered like `entries.path`.",
    ),
    bytes(
        "path_raw",
        "path_b64",
        "Exact path bytes, when `path` differs from them.",
    ),
    col(
        "entries",
        Int64,
        false,
        "Entries anywhere below this directory, not counting the directory itself.",
    ),
    col(
        "files",
        Int64,
        false,
        "Entries below this directory that aren't directories.",
    ),
    col("dirs", Int64, false, "Directories below this directory."),
    col(
        "bytes",
        Int64,
        false,
        "Sum of `size` over entries below this directory that aren't directories.",
    ),
    col(
        "alloc_bytes",
        Int64,
        false,
        "Sum of `alloc_bytes` over entries below this directory that aren't directories.",
    ),
    col(
        "oldest_mtime",
        Timestamp,
        true,
        "Oldest `mtime` below this directory, over entries that aren't directories. Null when there are none.",
    ),
    col(
        "newest_mtime",
        Timestamp,
        true,
        "Newest `mtime` below this directory, over entries that aren't directories.",
    ),
    col(
        "newest_ctime",
        Timestamp,
        true,
        "Newest `ctime` below this directory, over entries that aren't directories.",
    ),
    col(
        "newest_atime",
        Timestamp,
        true,
        "Newest `atime` below this directory, over entries that aren't directories.",
    ),
    col(
        "incomplete_dirs",
        Int64,
        false,
        "Directories in this subtree, including this one, that weren't read completely: errors, mount points not entered, or `--max-depth`. When it isn't 0, the totals are lower bounds.",
    ),
];

/// The age histogram table written by `fs-inventory rollup`: for directories
/// down to `--age-depth`, files and bytes below them by age.
pub const ROLLUP_AGE_COLUMNS: [Column; 6] = [
    col(
        "dir_id",
        Int64,
        false,
        "The directory's `dir_id`; join with `rollup` or `entries` for its path.",
    ),
    col(
        "time",
        Text,
        false,
        "Which timestamp the age is measured from: `mtime`, `ctime`, or `atime`.",
    ),
    col(
        "bucket",
        Int32,
        false,
        "Age bucket, in order: 0 is `future` (newer than the scan), then increasing age.",
    ),
    col(
        "label",
        Text,
        false,
        "The bucket's label, such as `<30d`, `1y-2y`, or `>=10y`, from `--age-buckets`.",
    ),
    col(
        "files",
        Int64,
        false,
        "Entries below the directory in this bucket that aren't directories. Only buckets with files have rows.",
    ),
    col("bytes", Int64, false, "Sum of `size` over those entries."),
];

/// Every table, with an introduction for the docs.
pub const TABLES: [(&str, &[Column], &str); 4] = [
    (
        "entries",
        &ENTRY_COLUMNS,
        "One row per file, directory, or other entry.",
    ),
    (
        "errors",
        &ERROR_COLUMNS,
        "One row per failed operation. Present only if there were any.",
    ),
    (
        "rollup",
        &ROLLUP_COLUMNS,
        "Written by `fs-inventory rollup`: one row per directory with totals over its whole subtree. Parquet only.",
    ),
    (
        "rollup_ages",
        &ROLLUP_AGE_COLUMNS,
        "Written by `fs-inventory rollup`: file counts and bytes by age for each directory down to `--age-depth`, measured from the scan's start time. Parquet only.",
    ),
];

/// Converts a count or byte total to the signed 64-bit integers the schema
/// uses, capping (never wrapping) values no real file system reaches.
pub fn signed(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// A subset of entry columns, as chosen with `--columns`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Selection(u64);

impl Selection {
    pub const ALL: Self = Self((1 << ENTRY_COLUMNS.len()) - 1);

    /// Parses column names (Parquet or text names), keeping schema order.
    pub fn parse(names: &[String]) -> Result<Self, String> {
        if names.is_empty() {
            return Ok(Self::ALL);
        }
        let mut bits = 0;
        for name in names {
            let index = ENTRY_COLUMNS
                .iter()
                .position(|c| c.name == name || c.text_name == name)
                .ok_or_else(|| {
                    format!("unknown column `{name}`; run `fs-inventory schema` for the list")
                })?;
            bits |= 1 << index;
        }
        Ok(Self(bits))
    }

    pub fn contains(self, index: usize) -> bool {
        self.0 & (1 << index) != 0
    }

    pub fn is_all(self) -> bool {
        self == Self::ALL
    }

    /// Indices of the selected columns, in schema order.
    pub fn indices(self) -> Vec<usize> {
        (0..ENTRY_COLUMNS.len())
            .filter(|&i| self.contains(i))
            .collect()
    }
}

/// The Arrow schema for a table.
pub fn arrow_schema(columns: &[Column], unit: TimeUnit) -> Arc<Schema> {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(c.name, c.kind.arrow(unit), c.nullable))
            .collect::<Vec<_>>(),
    ))
}

/// Ways to describe the schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Description {
    /// DuckDB `CREATE TABLE` statements.
    Sql,
    /// A JSON document.
    Json,
    /// The Markdown reference in `docs/schema.md`.
    Markdown,
}

pub fn describe(how: Description) -> String {
    match how {
        Description::Sql => describe_sql(),
        Description::Json => describe_json(),
        Description::Markdown => describe_markdown(),
    }
}

fn describe_sql() -> String {
    let mut out = String::from("-- fs-inventory schema version ");
    let _ = writeln!(
        out,
        "{}, as read from Parquet.",
        fs_inventory_core::SCHEMA_VERSION
    );
    for (table, columns, _) in TABLES {
        let _ = writeln!(out, "CREATE TABLE {table} (");
        for (i, c) in columns.iter().enumerate() {
            let null = if c.nullable { "" } else { " NOT NULL" };
            let comma = if i + 1 < columns.len() { "," } else { "" };
            let _ = writeln!(out, "    {} {}{null}{comma}", c.name, c.kind.duckdb());
        }
        out.push_str(");\n");
    }
    out
}

fn describe_json() -> String {
    let table = |columns: &[Column]| {
        columns
            .iter()
            .map(|c| {
                serde_json::json!({
                    "name": c.name,
                    "text_name": c.text_name,
                    "type": format!("{:?}", c.kind).to_lowercase(),
                    "duckdb_type": c.kind.duckdb(),
                    "nullable": c.nullable,
                    "description": c.description,
                })
            })
            .collect::<Vec<_>>()
    };
    let doc = serde_json::json!({
        "schema_version": fs_inventory_core::SCHEMA_VERSION,
        "entries": table(&ENTRY_COLUMNS),
        "errors": table(&ERROR_COLUMNS),
        "rollup": table(&ROLLUP_COLUMNS),
        "rollup_ages": table(&ROLLUP_AGE_COLUMNS),
        "name_flags": NameFlags::ALL.iter().map(|(f, n)| serde_json::json!({"bit": f.bits(), "name": n})).collect::<Vec<_>>(),
        "kinds": EntryKind::ALL.iter().map(|k| k.as_str()).collect::<Vec<_>>(),
    });
    let mut out = serde_json::to_string_pretty(&doc).unwrap_or_default();
    out.push('\n');
    out
}

fn describe_markdown() -> String {
    let mut out = String::new();
    let _ = write!(
        out,
        "# Inventory schema\n\n\
         <!-- Generated by `fs-inventory schema --format markdown`; do not edit by hand. -->\n\n\
         Schema version {}. A scan directory holds `manifest.json` and a directory of part files per table; DuckDB reads each table's parts with a glob such as `entries/*.parquet`.\n\n\
         Types are as DuckDB reads them from Parquet. In CSV and JSON Lines, byte columns are base64 text under their text name, and timestamps are ISO-8601 UTC text (or epoch seconds with `--time-format epoch`).\n\n\
         Integer columns are signed and wide enough for every value, so they work in tools and databases without unsigned types (PostgreSQL, Spark). The exceptions are `dev`, `ino`, and `rdev`: opaque 64-bit identifiers whose top bit can be set, so they're unsigned (`UBIGINT`). Byte totals and counts above the signed 64-bit maximum (8 EiB) would be capped, not wrapped.\n",
        fs_inventory_core::SCHEMA_VERSION
    );
    for (title, columns, intro) in TABLES {
        let _ = write!(
            out,
            "\n## `{title}`\n\n{intro}\n\n| Column | Type | Null | Description |\n|---|---|---|---|\n"
        );
        for c in columns {
            let name = if c.text_name == c.name {
                format!("`{}`", c.name)
            } else {
                format!("`{}` (text: `{}`)", c.name, c.text_name)
            };
            let null = if c.nullable { "yes" } else { "" };
            let _ = writeln!(
                out,
                "| {name} | {} | {null} | {} |",
                c.kind.duckdb(),
                c.description
            );
        }
    }
    out.push_str("\n## Name flags\n\nBits of `name_flags`. Decode them in DuckDB with `fs_name_flags(name_flags)` from `sql/fs_inventory.sql`.\n\n| Bit | Name | Meaning |\n|---|---|---|\n");
    for (flag, name) in NameFlags::ALL {
        let _ = writeln!(
            out,
            "| {} | `{name}` | {} |",
            flag.bits(),
            name_flag_meaning(flag)
        );
    }
    out.push_str("\n## Kinds\n\nValues of `kind`.\n\n| Kind | Meaning |\n|---|---|\n");
    for kind in EntryKind::ALL {
        let _ = writeln!(out, "| `{}` | {} |", kind.as_str(), kind_meaning(kind));
    }
    out
}

fn name_flag_meaning(flag: NameFlags) -> &'static str {
    match flag {
        f if f == NameFlags::INVALID_UTF8 => {
            "Not valid UTF-8: a legacy encoding (Latin-1, Shift-JIS, ...) or corruption"
        }
        f if f == NameFlags::C0_CONTROL => {
            "Contains an ASCII control character, such as a newline or tab"
        }
        f if f == NameFlags::C1_CONTROL => "Contains a C1 control character (U+0080 to U+009F)",
        f if f == NameFlags::DEL => "Contains DEL (U+007F)",
        f if f == NameFlags::BIDI_CONTROL => {
            "Contains a bidirectional formatting character that can disguise how a name displays"
        }
        f if f == NameFlags::NOT_NFC => {
            "Valid UTF-8 but not in Unicode Normalization Form C (common from macOS)"
        }
        f if f == NameFlags::LEADING_TRAILING_SPACE => "Starts or ends with a space",
        f if f == NameFlags::LEADING_DASH => {
            "Starts with `-`, which tools can mistake for an option"
        }
        f if f == NameFlags::TRAILING_DOT => "Ends with `.`, which Windows strips",
        f if f == NameFlags::WINDOWS_RESERVED => {
            "Contains a character Windows forbids (`<>:\"\\|?*\\`) or is a device name such as `CON`"
        }
        f if f == NameFlags::LONG_NAME => "Longer than 255 bytes",
        f if f == NameFlags::LONG_PATH => "The full path is at least 4,096 bytes",
        _ => "",
    }
}

fn kind_meaning(kind: EntryKind) -> &'static str {
    match kind {
        EntryKind::File => "Regular file",
        EntryKind::Dir => "Directory",
        EntryKind::Symlink => "Symbolic link (never followed)",
        EntryKind::Fifo => "Named pipe",
        EntryKind::Socket => "Unix domain socket",
        EntryKind::CharDevice => "Character device",
        EntryKind::BlockDevice => "Block device",
        EntryKind::Unknown => "Any other type",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_unique() {
        for (_, columns, _) in TABLES {
            let mut names: Vec<_> = columns.iter().map(|c| c.name).collect();
            names.sort_unstable();
            names.dedup();
            assert_eq!(names.len(), columns.len());
        }
    }

    #[test]
    fn only_opaque_identifiers_are_unsigned() {
        // Everything else must load into databases without unsigned types.
        let unsigned: Vec<_> = ENTRY_COLUMNS
            .iter()
            .chain(&ERROR_COLUMNS)
            .chain(&ROLLUP_COLUMNS)
            .chain(&ROLLUP_AGE_COLUMNS)
            .filter(|c| c.kind == ColumnType::UInt64)
            .map(|c| c.name)
            .collect();
        assert_eq!(unsigned, ["dev", "ino", "rdev"]);
    }

    #[test]
    fn signed_caps_instead_of_wrapping() {
        assert_eq!(signed(0), 0);
        assert_eq!(signed(1 << 40), 1 << 40);
        assert_eq!(signed(i64::MAX as u64), i64::MAX);
        assert_eq!(signed(u64::MAX), i64::MAX);
    }

    #[test]
    fn selection() {
        assert!(Selection::parse(&[]).unwrap().is_all());
        let s = Selection::parse(&["size".into(), "path".into(), "path_b64".into()]).unwrap();
        assert_eq!(s.indices(), [0, 4, 16]);
        assert!(Selection::parse(&["nope".into()]).is_err());
    }

    #[test]
    fn sql_macros_match_the_schema() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../sql/fs_inventory.sql");
        let sql = std::fs::read_to_string(path).unwrap();
        for (name, columns) in [
            ("fs_entries_csv", &ENTRY_COLUMNS[..]),
            ("fs_entries_jsonl", &ENTRY_COLUMNS[..]),
            ("fs_errors_csv", &ERROR_COLUMNS[..]),
            ("fs_errors_jsonl", &ERROR_COLUMNS[..]),
        ] {
            let body = &sql[sql.find(&format!("MACRO {name}(")).unwrap()..];
            let body = &body[body.find("columns = {").unwrap()..];
            let body = &body[..body.find('}').unwrap()];
            let found: Vec<(String, String)> = body
                .lines()
                .filter_map(|line| {
                    let (k, v) = line.trim().trim_end_matches(',').split_once(": ")?;
                    Some((k.trim_matches('\'').into(), v.trim_matches('\'').into()))
                })
                .collect();
            let expected: Vec<(String, String)> = columns
                .iter()
                .map(|c| {
                    let ty = match c.kind {
                        ColumnType::Bytes => "VARCHAR",
                        kind => kind.duckdb(),
                    };
                    (c.text_name.into(), ty.into())
                })
                .collect();
            assert_eq!(found, expected, "columns of {name} in sql/fs_inventory.sql");
        }
    }

    #[test]
    fn docs_are_up_to_date() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/schema.md");
        let current = std::fs::read_to_string(path).unwrap_or_default();
        assert!(
            current == describe(Description::Markdown),
            "docs/schema.md is out of date; run `cargo run -- schema --format markdown > docs/schema.md`"
        );
    }
}
