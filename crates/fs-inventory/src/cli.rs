// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Command-line interface definition.

use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use fs_inventory_core::backend::synthetic::SyntheticOptions;
use serde::{Deserialize, Serialize};

use crate::parse::{Compression, parse_compression, parse_duration, parse_size};

/// High-performance, crash-resilient file system inventory.
///
/// Crawls file systems in parallel and records the metadata of every file and
/// directory as Parquet, CSV, or JSON Lines for analysis in tools like DuckDB.
#[derive(Debug, Parser)]
#[command(name = "fs-inventory", version, about, propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant, reason = "parsed once at startup")]
pub enum Command {
    /// Crawl one or more directory trees and write an inventory.
    Scan(ScanArgs),
    /// Continue an interrupted scan from its last checkpoint.
    Resume(ResumeArgs),
    /// Compute totals over every directory's subtree, and age histograms.
    Rollup(RollupArgs),
    /// Re-export a Parquet scan as CSV or JSON Lines.
    Convert(ConvertArgs),
    /// Check that a scan is complete and its manifest matches its part files.
    Verify(ScanDirArgs),
    /// Print the inventory schema.
    Schema(SchemaArgs),
    /// Time crawls at several thread counts and suggest `--threads`.
    Probe(ProbeArgs),
}

#[derive(Debug, Args)]
pub struct ScanArgs {
    /// Directories to inventory.
    #[arg(required = true, value_name = "ROOT")]
    pub roots: Vec<PathBuf>,

    #[command(flatten)]
    pub output: OutputOptions,

    #[command(flatten)]
    pub traversal: TraversalOptions,

    #[command(flatten)]
    pub metadata: MetadataOptions,

    #[command(flatten)]
    pub encoding: EncodingOptions,

    #[command(flatten)]
    pub robustness: RobustnessOptions,

    #[command(flatten)]
    pub progress: ProgressOptions,

    /// Scan a generated tree at each ROOT instead of the file system, for
    /// benchmarks and scale tests: `100M`, or
    /// `entries=100M,files=32,fanout=8,huge=1M,seed=0`. The roots needn't
    /// exist.
    #[arg(long, hide = true, value_name = "SPEC", value_parser = SyntheticOptions::parse)]
    pub synthetic: Option<SyntheticOptions>,
}

#[derive(Debug, Args)]
#[command(next_help_heading = "Output")]
pub struct OutputOptions {
    /// Output directory, or `-` to stream CSV or JSON Lines to stdout.
    #[arg(short, long, value_name = "DIR|-")]
    pub output: PathBuf,

    /// Output format.
    #[arg(short, long, value_enum, default_value_t = Format::Parquet)]
    pub format: Format,

    /// Compression: zstd, zstd:LEVEL (1-22), gzip, gzip:LEVEL (1-9), snappy,
    /// lz4, or none [default: zstd:3 for Parquet, none for CSV and JSON Lines]
    #[arg(long, value_parser = parse_compression, value_name = "CODEC")]
    pub compression: Option<Compression>,

    /// Output column subset (comma-separated) [default: all columns].
    #[arg(long, value_delimiter = ',', value_name = "LIST")]
    pub columns: Vec<String>,

    /// Start a new part file after this many bytes.
    #[arg(long, value_parser = parse_size, default_value = "1GiB", value_name = "SIZE")]
    pub part_max_bytes: u64,

    /// Start a new part file after this many rows.
    #[arg(long, value_name = "N")]
    pub part_max_rows: Option<u64>,

    /// Target rows per Parquet row group.
    #[arg(long, default_value_t = 1_048_576, value_name = "N")]
    pub row_group_rows: u64,
}

#[derive(Debug, Args)]
#[command(next_help_heading = "Traversal")]
pub struct TraversalOptions {
    /// Crawl threads. Network file systems often benefit from 64-256.
    #[arg(short, long, default_value_t = 32, value_name = "N", value_parser = clap::value_parser!(u16).range(1..=1024))]
    pub threads: u16,

    /// Most encoder and writer threads. One starts, and more join as output
    /// backs up, so small scans write one file per table [default: CPU
    /// cores, at most 8].
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u16).range(1..=256))]
    pub writers: Option<u16>,

    /// Descend into other mounted file systems. Mount points are always recorded.
    #[arg(long)]
    pub cross_mounts: bool,

    /// Do not descend more than N levels below each root.
    #[arg(long, value_name = "N")]
    pub max_depth: Option<u32>,

    /// Skip paths matching this glob (repeatable). Globs without a `/`
    /// match names at any depth, such as `*.tmp` or `node_modules`; others
    /// match whole paths, such as `/data/*/scratch`.
    #[arg(long, value_name = "GLOB")]
    pub exclude: Vec<String>,

    /// Read exclude globs from a file, one per line (repeatable).
    #[arg(long, value_name = "FILE")]
    pub exclude_from: Vec<PathBuf>,

    /// Descend into snapshot directories (.snapshot, ~snapshot, .snapshots, .zfs).
    #[arg(long)]
    pub include_snapshots: bool,

    /// Never enter a directory twice (by device and inode), guarding against
    /// bind-mount loops, hard-linked directories, and overlapping roots.
    /// A directory reached again gets a row with the error `revisit`. Costs
    /// about 50 bytes of memory per directory.
    #[arg(long)]
    pub detect_cycles: bool,
}

#[derive(Debug, Args)]
#[command(next_help_heading = "Metadata")]
pub struct MetadataOptions {
    /// Optional metadata to read: size, blocks, atime, mtime, ctime, btime,
    /// or times (all four). `-size,-blocks` reads everything but those;
    /// `mtime,ctime` reads only those. Skipped fields are written as null.
    /// On Linux, skipping size and blocks avoids a round trip per file to
    /// Lustre's storage servers [default: all].
    #[arg(
        long,
        value_name = "LIST",
        allow_hyphen_values = true,
        value_parser = fs_inventory_core::model::Fields::parse
    )]
    pub fields: Option<fs_inventory_core::model::Fields>,

    /// Accept cached attributes on network file systems (faster, possibly stale).
    #[arg(long)]
    pub statx_dont_sync: bool,

    /// Record symlink targets.
    #[arg(long)]
    pub symlink_targets: bool,

    /// Add user and group name columns.
    #[arg(long)]
    pub resolve_ids: bool,
}

#[derive(Debug, Args)]
#[command(next_help_heading = "Encoding")]
pub struct EncodingOptions {
    /// When to fill the exact-bytes path column.
    #[arg(long, value_enum, default_value_t = RawPaths::Auto)]
    pub raw_paths: RawPaths,

    /// Guess the legacy character set of names that are not valid UTF-8.
    #[arg(long)]
    pub guess_encoding: bool,

    /// Write control characters in CSV names as quoted literals instead of
    /// C-style escapes.
    #[arg(long)]
    pub csv_raw_controls: bool,

    /// Timestamp precision.
    #[arg(long, value_enum, default_value_t = TimeUnit::Us)]
    pub time_unit: TimeUnit,

    /// Timestamp format for CSV and JSON Lines.
    #[arg(long, value_enum, default_value_t = TimeFormat::Iso)]
    pub time_format: TimeFormat,
}

#[derive(Debug, Args)]
#[command(next_help_heading = "Robustness")]
pub struct RobustnessOptions {
    /// Interval between resume checkpoints (`0` disables checkpoints)
    /// [default: 10m]
    #[arg(long, value_parser = parse_duration, value_name = "DURATION")]
    pub checkpoint_interval: Option<Duration>,

    /// Abandon subtrees whose file system calls hang longer than this.
    #[arg(long, value_parser = parse_duration, value_name = "DURATION")]
    pub stall_timeout: Option<Duration>,

    /// Memory budget for the scan. What the crawl threads and Parquet writers
    /// need comes first (with only as many writers as fit, unless --writers
    /// is given); directories waiting to be read (more wait in a spill file
    /// next to the output), row buffers, and row groups share the rest.
    /// Freed memory goes back to the system at once, for a little more CPU
    /// time [default: no limit; queue up to 1 GiB in memory].
    #[arg(long, value_parser = parse_size, value_name = "SIZE")]
    pub max_memory: Option<u64>,
}

#[derive(Debug, Args)]
#[command(next_help_heading = "Progress")]
pub struct ProgressOptions {
    /// Progress reporting style on stderr.
    #[arg(long, value_enum, default_value_t = Progress::Auto)]
    pub progress: Progress,

    /// Interval between progress reports.
    #[arg(long, value_parser = parse_duration, default_value = "10s", value_name = "DURATION")]
    pub progress_interval: Duration,
}

#[derive(Debug, Args)]
pub struct RollupArgs {
    /// Scan output directory (Parquet).
    #[arg(value_name = "SCAN_DIR")]
    pub scan_dir: PathBuf,

    /// Age bucket edges, ascending, measured back from the scan's start time.
    #[arg(
        long,
        value_name = "LIST",
        default_value = crate::rollup::DEFAULT_AGE_BUCKETS,
        value_parser = crate::rollup::AgeBuckets::parse
    )]
    pub age_buckets: crate::rollup::AgeBuckets,

    /// Compute age histograms for directories down to this depth below each
    /// root (0 is the root). Each one costs about 0.5 KB of memory.
    #[arg(long, value_name = "N", default_value_t = 4)]
    pub age_depth: u32,
}

#[derive(Debug, Args)]
pub struct ResumeArgs {
    /// Scan output directory.
    #[arg(value_name = "SCAN_DIR")]
    pub scan_dir: PathBuf,

    /// Crawl threads [default: as the scan ran].
    #[arg(short, long, value_name = "N", value_parser = clap::value_parser!(u16).range(1..=1024))]
    pub threads: Option<u16>,

    /// Most encoder and writer threads [default: as the scan ran].
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u16).range(1..=256))]
    pub writers: Option<u16>,

    /// Interval between checkpoints (`0` disables them) [default: as the
    /// scan ran].
    #[arg(long, value_parser = parse_duration, value_name = "DURATION")]
    pub checkpoint_interval: Option<Duration>,

    /// Abandon subtrees whose file system calls hang longer than this (`0`
    /// waits forever) [default: as the scan ran].
    #[arg(long, value_parser = parse_duration, value_name = "DURATION")]
    pub stall_timeout: Option<Duration>,

    /// Memory budget for the scan [default: as the scan ran].
    #[arg(long, value_parser = parse_size, value_name = "SIZE")]
    pub max_memory: Option<u64>,

    #[command(flatten)]
    pub progress: ProgressOptions,
}

#[derive(Debug, Args)]
pub struct ScanDirArgs {
    /// Scan output directory.
    #[arg(value_name = "SCAN_DIR")]
    pub scan_dir: PathBuf,
}

#[derive(Debug, Args)]
#[allow(dead_code, reason = "consumed when convert is implemented")]
pub struct ConvertArgs {
    /// Scan output directory (Parquet).
    #[arg(value_name = "SCAN_DIR")]
    pub scan_dir: PathBuf,

    /// Target format.
    #[arg(long, value_enum)]
    pub to: TextFormat,

    /// Output directory, or `-` for stdout.
    #[arg(short, long, value_name = "DIR|-")]
    pub output: PathBuf,

    /// Compression: zstd, zstd:LEVEL (1-22), gzip, gzip:LEVEL (1-9), or none
    /// [default: none]
    #[arg(long, value_parser = parse_compression, value_name = "CODEC")]
    pub compression: Option<Compression>,
}

#[derive(Debug, Args)]
pub struct SchemaArgs {
    /// Schema description format.
    #[arg(long, value_enum, default_value_t = SchemaFormat::Sql)]
    pub format: SchemaFormat,
}

#[derive(Debug, Args)]
pub struct ProbeArgs {
    /// Directory to sample.
    #[arg(value_name = "PATH")]
    pub path: PathBuf,

    /// Thread counts to try (comma-separated).
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "8,16,32,64,128,256",
        value_name = "LIST"
    )]
    pub threads: Vec<u16>,

    /// How long to sample each thread count.
    #[arg(long, value_parser = parse_duration, default_value = "30s", value_name = "DURATION")]
    pub duration: Duration,

    /// Drop the page, dentry, and inode caches before each run, so every
    /// run reads from the storage (Linux, root).
    #[arg(long)]
    pub drop_caches: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Parquet,
    Csv,
    Jsonl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TextFormat {
    Csv,
    Jsonl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RawPaths {
    /// Only for paths whose text form differs from the original bytes.
    Auto,
    /// For every row.
    Always,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeUnit {
    /// Microseconds (any date in +/-290,000 years).
    Us,
    /// Nanoseconds (dates outside 1677-2262 are clamped and flagged).
    Ns,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeFormat {
    /// ISO-8601 UTC with fractional seconds.
    Iso,
    /// Seconds since 1970-01-01 UTC, with fractional seconds.
    Epoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Progress {
    /// Plain lines when stderr is a terminal, otherwise none.
    Auto,
    /// Human-readable lines.
    Plain,
    /// One JSON object per line.
    Json,
    /// No progress output.
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SchemaFormat {
    /// DuckDB `CREATE TABLE` statements.
    Sql,
    /// JSON description of each column.
    Json,
    /// Markdown reference (docs/schema.md).
    Markdown,
}

impl ScanArgs {
    /// Checks constraints that span several options.
    pub fn validate(&self) -> Result<(), String> {
        let out = &self.output;
        let to_stdout = out.output == Path::new("-");
        if to_stdout && out.format == Format::Parquet {
            return Err("`--output -` streams text; use `--format csv` or `--format jsonl`".into());
        }
        if let Some(codec) = out.compression
            && out.format != Format::Parquet
            && !codec.supports_text_stream()
        {
            return Err("CSV and JSON Lines support only zstd, gzip, or none compression".into());
        }
        if out.part_max_bytes == 0 {
            return Err("`--part-max-bytes` must be greater than zero".into());
        }
        if out.part_max_rows == Some(0) || out.row_group_rows == 0 {
            return Err("row counts must be greater than zero".into());
        }
        if self.progress.progress_interval.is_zero() {
            return Err("`--progress-interval` must be greater than zero".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn scan(args: &[&str]) -> Result<ScanArgs, clap::Error> {
        let argv = ["fs-inventory", "scan"].iter().chain(args);
        match Cli::try_parse_from(argv)?.command {
            Command::Scan(args) => Ok(args),
            other => panic!("expected scan, got {other:?}"),
        }
    }

    #[test]
    fn cli_definition_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn scan_defaults() {
        let args = scan(&["/data", "-o", "out"]).unwrap();
        assert_eq!(args.roots, [PathBuf::from("/data")]);
        assert_eq!(args.output.format, Format::Parquet);
        assert_eq!(args.output.compression, None);
        assert_eq!(args.output.part_max_bytes, 1 << 30);
        assert_eq!(args.traversal.threads, 32);
        assert!(!args.traversal.cross_mounts);
        assert_eq!(args.encoding.raw_paths, RawPaths::Auto);
        assert_eq!(args.encoding.time_unit, TimeUnit::Us);
        assert_eq!(args.robustness.checkpoint_interval, None);
        assert_eq!(args.progress.progress_interval, Duration::from_secs(10));
        assert_eq!(args.validate(), Ok(()));
    }

    #[test]
    fn scan_requires_root_and_output() {
        assert!(scan(&[]).is_err());
        assert!(scan(&["/data"]).is_err());
        assert!(scan(&["-o", "out"]).is_err());
    }

    #[test]
    fn thread_count_is_bounded() {
        assert!(scan(&["/data", "-o", "out", "-t", "0"]).is_err());
        assert!(scan(&["/data", "-o", "out", "-t", "1025"]).is_err());
        assert_eq!(
            scan(&["/data", "-o", "out", "-t", "256"])
                .unwrap()
                .traversal
                .threads,
            256
        );
    }

    #[test]
    fn stdout_requires_text_format() {
        let parquet = scan(&["/data", "-o", "-"]).unwrap();
        assert!(parquet.validate().is_err());
        let csv = scan(&["/data", "-o", "-", "-f", "csv"]).unwrap();
        assert_eq!(csv.validate(), Ok(()));
    }

    #[test]
    fn text_formats_reject_columnar_codecs() {
        let snappy = scan(&[
            "/data",
            "-o",
            "out",
            "-f",
            "jsonl",
            "--compression",
            "snappy",
        ])
        .unwrap();
        assert!(snappy.validate().is_err());
        let zstd = scan(&["/data", "-o", "out", "-f", "csv", "--compression", "zstd:9"]).unwrap();
        assert_eq!(zstd.validate(), Ok(()));
    }

    #[test]
    fn zero_sizes_are_rejected() {
        let bytes = scan(&["/data", "-o", "out", "--part-max-bytes", "0"]).unwrap();
        assert!(bytes.validate().is_err());
        let rows = scan(&["/data", "-o", "out", "--row-group-rows", "0"]).unwrap();
        assert!(rows.validate().is_err());
        let interval = scan(&["/data", "-o", "out", "--progress-interval", "0"]).unwrap();
        assert!(interval.validate().is_err());
    }

    #[cfg(unix)]
    #[test]
    fn roots_keep_non_utf8_bytes() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        let root = OsStr::from_bytes(b"/data/caf\xe9");
        let argv = [
            OsStr::new("fs-inventory"),
            OsStr::new("scan"),
            root,
            OsStr::new("-o"),
            OsStr::new("out"),
        ];
        let Command::Scan(args) = Cli::try_parse_from(argv).unwrap().command else {
            panic!("expected scan");
        };
        assert_eq!(args.roots[0].as_os_str().as_bytes(), b"/data/caf\xe9");
    }
}
