// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! The `rollup` command: totals over every directory's whole subtree, and age
//! histograms for directories near the top of the tree.
//!
//! It reads the scan's Parquet entries in three passes, each loading only the
//! columns it needs: directories and their parents, then every row's sizes
//! and times, then directory paths for the output. Memory grows with the
//! number of directories (about 100 bytes each), not files.

use std::fs::{self, File};
use std::io;
use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, Int32Builder, Int64Builder, StringBuilder};
use arrow_array::cast::AsArray as _;
use arrow_array::types::{Int32Type, Int64Type, TimestampMicrosecondType, TimestampNanosecondType};
use arrow_array::{Array, ArrayRef, PrimitiveArray, RecordBatch, StringArray};
use arrow_schema::{DataType, TimeUnit};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};

use crate::cli::RollupArgs;
use crate::exit::Exit;
use crate::output::manifest::write_value;
use crate::output::parquet::{ParquetPartWriter, ParquetSettings};
use crate::output::parts::RollLimits;
use crate::output::schema::{ROLLUP_AGE_COLUMNS, ROLLUP_COLUMNS, arrow_schema};
use crate::output::{now, timefmt};
use crate::parse::{Compression, parse_duration};

/// Rows per output batch.
const BATCH_ROWS: usize = 64 * 1024;
/// The timestamps an age can be measured from, in output order.
const TIME_KINDS: [&str; 3] = ["mtime", "ctime", "atime"];

/// Age buckets: "future", then one bucket per gap between edges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgeBuckets {
    /// Ascending bucket edges, in seconds.
    edges: Vec<i64>,
    labels: Vec<String>,
}

impl AgeBuckets {
    /// Parses ascending durations such as `30d,90d,1y,2y,3y,5y,10y`.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let tokens: Vec<&str> = spec.split(',').map(str::trim).collect();
        let mut edges = Vec::with_capacity(tokens.len());
        for token in &tokens {
            let seconds = parse_duration(token)?.as_secs();
            let seconds =
                i64::try_from(seconds).map_err(|_| format!("age `{token}` is too large"))?;
            if seconds == 0 || edges.last().is_some_and(|&last| last >= seconds) {
                return Err(format!(
                    "age buckets must be positive and increasing: `{spec}`"
                ));
            }
            edges.push(seconds);
        }
        let mut labels = vec!["future".to_string(), format!("<{}", tokens[0])];
        labels.extend(tokens.windows(2).map(|w| format!("{}-{}", w[0], w[1])));
        labels.push(format!(">={}", tokens[tokens.len() - 1]));
        Ok(Self { edges, labels })
    }

    fn count(&self) -> usize {
        self.labels.len()
    }

    /// The bucket for an age in seconds; negative ages are in the future.
    fn bucket(&self, age: i64) -> usize {
        if age < 0 {
            0
        } else {
            1 + self.edges.partition_point(|&edge| edge <= age)
        }
    }
}

/// Totals over a directory's subtree. Times are microseconds since the epoch.
#[derive(Debug, Clone, Copy)]
struct Totals {
    entries: u64,
    files: u64,
    dirs: u64,
    bytes: i64,
    alloc_bytes: i64,
    incomplete: u64,
    oldest_mtime: i64,
    newest_mtime: i64,
    newest_ctime: i64,
    newest_atime: i64,
}

impl Default for Totals {
    fn default() -> Self {
        Self {
            entries: 0,
            files: 0,
            dirs: 0,
            bytes: 0,
            alloc_bytes: 0,
            incomplete: 0,
            oldest_mtime: i64::MAX,
            newest_mtime: i64::MIN,
            newest_ctime: i64::MIN,
            newest_atime: i64::MIN,
        }
    }
}

impl Totals {
    fn merge(&mut self, other: &Self) {
        self.entries = self.entries.saturating_add(other.entries);
        self.files = self.files.saturating_add(other.files);
        self.dirs = self.dirs.saturating_add(other.dirs);
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.alloc_bytes = self.alloc_bytes.saturating_add(other.alloc_bytes);
        self.incomplete = self.incomplete.saturating_add(other.incomplete);
        self.oldest_mtime = self.oldest_mtime.min(other.oldest_mtime);
        self.newest_mtime = self.newest_mtime.max(other.newest_mtime);
        self.newest_ctime = self.newest_ctime.max(other.newest_ctime);
        self.newest_atime = self.newest_atime.max(other.newest_atime);
    }
}

/// A directory seen in the scan, indexed by `dir_id`.
#[derive(Debug, Clone, Copy, Default)]
struct Node {
    parent: u64,
    depth: u32,
    seen: bool,
}

const NO_SLOT: u32 = u32::MAX;

/// The whole computation, separate from file handling so tests can drive it.
struct Rollup {
    buckets: AgeBuckets,
    age_depth: u32,
    /// Age reference time, in microseconds.
    reference: i64,
    nodes: Vec<Node>,
    totals: Vec<Totals>,
    /// Histogram slot of each directory: its own for directories down to
    /// `age_depth`, else its ancestor's at that depth.
    slots: Vec<u32>,
    /// Owner `dir_id` of each slot.
    slot_dirs: Vec<u64>,
    /// `[slot][time kind][bucket] -> (files, bytes)`, flattened.
    histograms: Vec<[i64; 2]>,
    /// Directories ordered by depth, ascending.
    by_depth: Vec<u64>,
}

impl Rollup {
    fn new(buckets: AgeBuckets, age_depth: u32, reference: i64) -> Self {
        Self {
            buckets,
            age_depth,
            reference,
            nodes: Vec::new(),
            totals: Vec::new(),
            slots: Vec::new(),
            slot_dirs: Vec::new(),
            histograms: Vec::new(),
            by_depth: Vec::new(),
        }
    }

    fn node_mut(&mut self, id: u64) -> Option<(&mut Node, &mut Totals)> {
        let index = usize::try_from(id).ok()?;
        if index >= self.nodes.len() {
            // dir_ids are dense, so this grows to about the directory count.
            self.nodes.resize(index + 1, Node::default());
            self.totals.resize(index + 1, Totals::default());
        }
        Some((&mut self.nodes[index], &mut self.totals[index]))
    }

    fn seen(&self, id: u64) -> Option<usize> {
        let index = usize::try_from(id).ok()?;
        self.nodes.get(index).filter(|n| n.seen).map(|_| index)
    }

    /// Pass 1: one directory row.
    fn add_dir(&mut self, id: u64, parent: u64, depth: u32, incomplete: bool) {
        if let Some((node, totals)) = self.node_mut(id) {
            *node = Node {
                parent,
                depth,
                seen: true,
            };
            totals.incomplete = u64::from(incomplete);
        }
    }

    /// After pass 1: orders directories by depth and assigns histogram slots.
    fn plan(&mut self) {
        let max_depth = self
            .nodes
            .iter()
            .filter(|n| n.seen)
            .map(|n| n.depth)
            .max()
            .unwrap_or(0) as usize;
        let mut counts = vec![0usize; max_depth + 2];
        for node in self.nodes.iter().filter(|n| n.seen) {
            counts[node.depth as usize + 1] += 1;
        }
        for i in 1..counts.len() {
            counts[i] += counts[i - 1];
        }
        let total = counts[max_depth + 1];
        self.by_depth = vec![0; total];
        for (id, node) in self.nodes.iter().enumerate().filter(|(_, n)| n.seen) {
            let at = &mut counts[node.depth as usize];
            self.by_depth[*at] = id as u64;
            *at += 1;
        }

        self.slots = vec![NO_SLOT; self.nodes.len()];
        for &id in &self.by_depth {
            let node = self.nodes[id as usize];
            let slot = if node.depth <= self.age_depth {
                let slot = self.slot_dirs.len() as u32;
                self.slot_dirs.push(id);
                slot
            } else {
                self.seen(node.parent).map_or(NO_SLOT, |p| self.slots[p])
            };
            self.slots[id as usize] = slot;
        }
        let width = TIME_KINDS.len() * self.buckets.count();
        self.histograms = vec![[0, 0]; self.slot_dirs.len() * width];
    }

    /// Pass 2: one row of any kind. Returns false if its parent is unknown.
    fn add_row(
        &mut self,
        parent: u64,
        is_dir: bool,
        size: i64,
        alloc: i64,
        times: [Option<i64>; 3],
    ) -> bool {
        let Some(p) = self.seen(parent) else {
            return parent == 0;
        };
        let totals = &mut self.totals[p];
        totals.entries += 1;
        if is_dir {
            totals.dirs += 1;
            return true;
        }
        totals.files += 1;
        totals.bytes = totals.bytes.saturating_add(size);
        totals.alloc_bytes = totals.alloc_bytes.saturating_add(alloc);
        let [mtime, ctime, atime] = times;
        if let Some(t) = mtime {
            totals.oldest_mtime = totals.oldest_mtime.min(t);
            totals.newest_mtime = totals.newest_mtime.max(t);
        }
        if let Some(t) = ctime {
            totals.newest_ctime = totals.newest_ctime.max(t);
        }
        if let Some(t) = atime {
            totals.newest_atime = totals.newest_atime.max(t);
        }
        let slot = self.slots[p];
        if slot != NO_SLOT {
            let width = self.buckets.count();
            for (kind, time) in times.into_iter().enumerate() {
                let Some(time) = time else { continue };
                let age = self.reference.saturating_sub(time) / 1_000_000;
                let cell = &mut self.histograms
                    [(slot as usize * TIME_KINDS.len() + kind) * width + self.buckets.bucket(age)];
                cell[0] += 1;
                cell[1] = cell[1].saturating_add(size);
            }
        }
        true
    }

    /// After pass 2: adds every directory's totals into its ancestors.
    fn propagate(&mut self) {
        let block = TIME_KINDS.len() * self.buckets.count();
        for i in (0..self.by_depth.len()).rev() {
            let id = self.by_depth[i] as usize;
            let node = self.nodes[id];
            let Some(parent) = self.seen(node.parent) else {
                continue;
            };
            let child = self.totals[id];
            self.totals[parent].merge(&child);
            if node.depth >= 1 && node.depth <= self.age_depth {
                let (from, to) = (
                    self.slots[id] as usize * block,
                    self.slots[parent] as usize * block,
                );
                for k in 0..block {
                    let [files, bytes] = self.histograms[from + k];
                    let cell = &mut self.histograms[to + k];
                    cell[0] += files;
                    cell[1] = cell[1].saturating_add(bytes);
                }
            }
        }
    }
}

/// A timestamp column in either unit, read as microseconds.
enum Times<'a> {
    Micros(&'a PrimitiveArray<TimestampMicrosecondType>),
    Nanos(&'a PrimitiveArray<TimestampNanosecondType>),
}

impl Times<'_> {
    fn new(array: &ArrayRef) -> io::Result<Times<'_>> {
        match array.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Ok(Times::Micros(array.as_primitive()))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(Times::Nanos(array.as_primitive())),
            other => Err(io::Error::other(format!(
                "unexpected timestamp type {other}"
            ))),
        }
    }

    fn get(&self, i: usize) -> Option<i64> {
        match self {
            Times::Micros(a) => (!a.is_null(i)).then(|| a.value(i)),
            Times::Nanos(a) => (!a.is_null(i)).then(|| a.value(i) / 1000),
        }
    }
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> io::Result<&'a ArrayRef> {
    batch.column_by_name(name).ok_or_else(|| {
        io::Error::other(format!(
            "the scan has no `{name}` column; rollup needs a scan made without --columns"
        ))
    })
}

fn kinds(batch: &RecordBatch) -> io::Result<&StringArray> {
    Ok(column(batch, "kind")?.as_string::<i32>())
}

fn int64<'a>(batch: &'a RecordBatch, name: &str) -> io::Result<&'a PrimitiveArray<Int64Type>> {
    Ok(column(batch, name)?.as_primitive::<Int64Type>())
}

/// Reads the given columns of every entry part, calling `each` per batch.
fn read_entries(
    scan_dir: &Path,
    parts: &[String],
    columns: &[&str],
    mut each: impl FnMut(&RecordBatch) -> io::Result<()>,
) -> io::Result<()> {
    for part in parts {
        let file = File::open(scan_dir.join(part))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(io::Error::other)?;
        let available: Vec<&str> = builder
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let wanted: Vec<&str> = columns
            .iter()
            .copied()
            .filter(|c| available.contains(c))
            .collect();
        let mask = ProjectionMask::columns(builder.parquet_schema(), wanted);
        let reader = builder
            .with_projection(mask)
            .with_batch_size(BATCH_ROWS)
            .build()
            .map_err(io::Error::other)?;
        for batch in reader {
            each(&batch.map_err(io::Error::other)?)?;
        }
    }
    Ok(())
}

/// A problem that stops the rollup.
fn fail(message: impl std::fmt::Display) -> io::Error {
    io::Error::other(message.to_string())
}

pub fn run(args: &RollupArgs) -> Exit {
    match rollup(args) {
        Ok(summary) => {
            println!("{summary}");
            Exit::Success
        }
        Err(e) => {
            eprintln!("fs-inventory: rollup failed: {e}");
            Exit::Fatal
        }
    }
}

fn rollup(args: &RollupArgs) -> io::Result<String> {
    let scan_dir = &args.scan_dir;
    let manifest_path = scan_dir.join("manifest.json");
    let mut manifest: Value = serde_json::from_str(&fs::read_to_string(&manifest_path)?)
        .map_err(|e| fail(format!("{}: {e}", manifest_path.display())))?;
    if manifest["format"] != "parquet" {
        return Err(fail(format!(
            "rollup reads Parquet scans, and this scan is {}",
            manifest["format"]
        )));
    }
    if manifest["schema_version"].as_u64() != Some(u64::from(fs_inventory_core::SCHEMA_VERSION)) {
        return Err(fail(format!(
            "the scan has schema version {}, but this fs-inventory reads version {}; scan again",
            manifest["schema_version"],
            fs_inventory_core::SCHEMA_VERSION
        )));
    }
    let started_at = manifest["started_at"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let reference = timefmt::parse_iso(&started_at)
        .ok_or_else(|| fail("the manifest has no valid started_at time"))?;
    let reference = reference.sec.saturating_mul(1_000_000) + i64::from(reference.nsec / 1000);
    let parts: Vec<String> = manifest["parts"]["entries"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|p| p["file"].as_str().map(String::from))
        .collect();

    let mut state = Rollup::new(args.age_buckets.clone(), args.age_depth, reference);

    // Pass 1: directories.
    read_entries(
        scan_dir,
        &parts,
        &[
            "kind",
            "dir_id",
            "parent_id",
            "depth",
            "dir_entries",
            "error",
        ],
        |batch| {
            let kind = kinds(batch)?;
            let dir_id = int64(batch, "dir_id")?;
            let parent = int64(batch, "parent_id")?;
            let depth = column(batch, "depth")?.as_primitive::<Int32Type>();
            let dir_entries = int64(batch, "dir_entries")?;
            let error = column(batch, "error")?.as_string::<i32>();
            for i in 0..batch.num_rows() {
                if kind.value(i) != "dir" || dir_id.is_null(i) {
                    continue;
                }
                // A revisited directory's contents were counted where it was
                // first found, so it isn't missing anything.
                let revisit = !error.is_null(i) && error.value(i).starts_with("revisit");
                let incomplete = !revisit && (dir_entries.is_null(i) || !error.is_null(i));
                state.add_dir(
                    dir_id.value(i).max(0) as u64,
                    parent.value(i).max(0) as u64,
                    depth.value(i).max(0) as u32,
                    incomplete,
                );
            }
            Ok(())
        },
    )?;
    state.plan();

    // Pass 2: sizes and times of every row.
    let mut orphans = 0u64;
    read_entries(
        scan_dir,
        &parts,
        &[
            "kind",
            "parent_id",
            "size",
            "alloc_bytes",
            "mtime",
            "ctime",
            "atime",
        ],
        |batch| {
            let kind = kinds(batch)?;
            let parent = int64(batch, "parent_id")?;
            let size = int64(batch, "size")?;
            let alloc = int64(batch, "alloc_bytes")?;
            let mtime = Times::new(column(batch, "mtime")?)?;
            let ctime = Times::new(column(batch, "ctime")?)?;
            let atime = Times::new(column(batch, "atime")?)?;
            for i in 0..batch.num_rows() {
                let value =
                    |a: &PrimitiveArray<Int64Type>| if a.is_null(i) { 0 } else { a.value(i) };
                let known = state.add_row(
                    parent.value(i).max(0) as u64,
                    kind.value(i) == "dir",
                    value(size),
                    value(alloc),
                    [mtime.get(i), ctime.get(i), atime.get(i)],
                );
                if !known {
                    orphans += 1;
                }
            }
            Ok(())
        },
    )?;
    state.propagate();

    // Pass 3: write one row per directory, then the age rows.
    let staging = scan_dir.join(".rollup-staging");
    if staging.exists() {
        fs::remove_dir_all(&staging)?;
    }
    let settings = ParquetSettings {
        compression: Compression::Zstd(3),
        row_group_rows: 1 << 20,
        metadata: vec![
            (
                "fs_inventory.schema_version".into(),
                fs_inventory_core::SCHEMA_VERSION.to_string(),
            ),
            (
                "fs_inventory.tool_version".into(),
                env!("CARGO_PKG_VERSION").into(),
            ),
            (
                "fs_inventory.scan_id".into(),
                manifest["scan_id"].as_str().unwrap_or_default().into(),
            ),
            ("fs_inventory.age_reference".into(), started_at.clone()),
        ],
    };
    let limits = RollLimits {
        max_bytes: 1 << 30,
        max_rows: None,
    };
    let rollup_schema = arrow_schema(&ROLLUP_COLUMNS, TimeUnit::Microsecond);
    let mut writer = ParquetPartWriter::new(
        &staging,
        "rollup",
        "part".into(),
        Arc::clone(&rollup_schema),
        &settings,
        limits,
    )?;
    let mut rows = RollupRows::default();
    let mut directories = 0u64;
    read_entries(
        scan_dir,
        &parts,
        &["kind", "dir_id", "parent_id", "depth", "path", "path_raw"],
        |batch| {
            let kind = kinds(batch)?;
            let dir_id = int64(batch, "dir_id")?;
            let parent = int64(batch, "parent_id")?;
            let depth = column(batch, "depth")?.as_primitive::<Int32Type>();
            let path = column(batch, "path")?.as_string::<i32>();
            let raw = batch
                .column_by_name("path_raw")
                .map(|a| a.as_binary::<i32>());
            for i in 0..batch.num_rows() {
                if kind.value(i) != "dir" || dir_id.is_null(i) {
                    continue;
                }
                let Some(index) = state.seen(dir_id.value(i).max(0) as u64) else {
                    continue;
                };
                let raw = raw.filter(|r| !r.is_null(i)).map(|r| r.value(i));
                rows.push(
                    dir_id.value(i),
                    parent.value(i),
                    depth.value(i),
                    path.value(i),
                    raw,
                    &state.totals[index],
                );
                directories += 1;
                if rows.len >= BATCH_ROWS {
                    writer.write(&rows.finish(&rollup_schema)?)?;
                }
            }
            Ok(())
        },
    )?;
    if rows.len > 0 {
        writer.write(&rows.finish(&rollup_schema)?)?;
    }
    let rollup_parts = writer.finish()?;

    let ages_schema = arrow_schema(&ROLLUP_AGE_COLUMNS, TimeUnit::Microsecond);
    let mut writer = ParquetPartWriter::new(
        &staging,
        "rollup_ages",
        "part".into(),
        Arc::clone(&ages_schema),
        &settings,
        limits,
    )?;
    let age_rows = write_ages(&state, &mut writer, &ages_schema)?;
    let age_parts = writer.finish()?;

    // Replace any previous rollup only once the new one is complete.
    for table in ["rollup", "rollup_ages"] {
        let target = scan_dir.join(table);
        if target.exists() {
            fs::remove_dir_all(&target)?;
        }
        let staged = staging.join(table);
        if staged.exists() {
            fs::rename(staged, target)?;
        }
    }
    fs::remove_dir_all(&staging)?;

    let incomplete_roots: u64 = state
        .by_depth
        .iter()
        .take_while(|&&id| state.nodes[id as usize].depth == 0)
        .map(|&id| state.totals[id as usize].incomplete)
        .sum();
    let section = json!({
        "created_at": iso_now(),
        "tool_version": env!("CARGO_PKG_VERSION"),
        "age_reference": started_at,
        "age_buckets": state.buckets.labels,
        "age_depth": args.age_depth,
        "directories": directories,
        "incomplete_dirs": incomplete_roots,
        "unattached_rows": orphans,
        "parts": {
            "rollup": rollup_parts,
            "rollup_ages": age_parts,
        },
    });
    manifest["rollup"] = section;
    write_value(scan_dir, &manifest)?;

    let mut summary = format!(
        "rollup: {directories} directories, {age_rows} age rows (depth <= {}); wrote rollup/ and rollup_ages/ in {}",
        args.age_depth,
        scan_dir.display()
    );
    if incomplete_roots > 0 {
        summary.push_str(&format!(
            "\nnote: {incomplete_roots} directories weren't read completely, so some totals are lower bounds (see incomplete_dirs)"
        ));
    }
    if manifest["status"] != "complete" {
        summary.push_str(&format!(
            "\nnote: the scan's status is {}, so totals are partial",
            manifest["status"]
        ));
    }
    Ok(summary)
}

fn iso_now() -> String {
    let mut out = Vec::new();
    timefmt::write_iso(&mut out, now(), timefmt::Precision::Micros);
    String::from_utf8_lossy(&out).into_owned()
}

/// Buffered rollup rows.
#[derive(Default)]
struct RollupRows {
    len: usize,
    dir_id: Int64Builder,
    parent_id: Int64Builder,
    depth: Int32Builder,
    path: StringBuilder,
    path_raw: BinaryBuilder,
    entries: Int64Builder,
    files: Int64Builder,
    dirs: Int64Builder,
    bytes: Int64Builder,
    alloc_bytes: Int64Builder,
    oldest_mtime: Int64Builder,
    newest_mtime: Int64Builder,
    newest_ctime: Int64Builder,
    newest_atime: Int64Builder,
    incomplete: Int64Builder,
}

impl RollupRows {
    fn push(
        &mut self,
        dir_id: i64,
        parent: i64,
        depth: i32,
        path: &str,
        raw: Option<&[u8]>,
        t: &Totals,
    ) {
        let signed = crate::output::schema::signed;
        let time = |v: i64, none: i64| (v != none).then_some(v);
        self.dir_id.append_value(dir_id);
        self.parent_id.append_value(parent);
        self.depth.append_value(depth);
        self.path.append_value(path);
        self.path_raw.append_option(raw);
        self.entries.append_value(signed(t.entries));
        self.files.append_value(signed(t.files));
        self.dirs.append_value(signed(t.dirs));
        self.bytes.append_value(t.bytes);
        self.alloc_bytes.append_value(t.alloc_bytes);
        self.oldest_mtime
            .append_option(time(t.oldest_mtime, i64::MAX));
        self.newest_mtime
            .append_option(time(t.newest_mtime, i64::MIN));
        self.newest_ctime
            .append_option(time(t.newest_ctime, i64::MIN));
        self.newest_atime
            .append_option(time(t.newest_atime, i64::MIN));
        self.incomplete.append_value(signed(t.incomplete));
        self.len += 1;
    }

    fn finish(&mut self, schema: &Arc<arrow_schema::Schema>) -> io::Result<RecordBatch> {
        let micros = |b: &mut Int64Builder| -> ArrayRef {
            Arc::new(
                b.finish()
                    .reinterpret_cast::<TimestampMicrosecondType>()
                    .with_timezone("UTC"),
            )
        };
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.dir_id.finish()),
            Arc::new(self.parent_id.finish()),
            Arc::new(self.depth.finish()),
            Arc::new(self.path.finish()),
            Arc::new(self.path_raw.finish()),
            Arc::new(self.entries.finish()),
            Arc::new(self.files.finish()),
            Arc::new(self.dirs.finish()),
            Arc::new(self.bytes.finish()),
            Arc::new(self.alloc_bytes.finish()),
            micros(&mut self.oldest_mtime),
            micros(&mut self.newest_mtime),
            micros(&mut self.newest_ctime),
            micros(&mut self.newest_atime),
            Arc::new(self.incomplete.finish()),
        ];
        self.len = 0;
        RecordBatch::try_new(Arc::clone(schema), columns).map_err(io::Error::other)
    }
}

/// Writes the non-empty age buckets of every directory down to the age depth.
fn write_ages(
    state: &Rollup,
    writer: &mut ParquetPartWriter,
    schema: &Arc<arrow_schema::Schema>,
) -> io::Result<u64> {
    let width = state.buckets.count();
    let mut dir_id = Int64Builder::new();
    let mut time = StringBuilder::new();
    let mut bucket = Int32Builder::new();
    let mut label = StringBuilder::new();
    let mut files = Int64Builder::new();
    let mut bytes = Int64Builder::new();
    let mut pending = 0usize;
    let mut total = 0u64;
    let flush = |dir_id: &mut Int64Builder,
                 time: &mut StringBuilder,
                 bucket: &mut Int32Builder,
                 label: &mut StringBuilder,
                 files: &mut Int64Builder,
                 bytes: &mut Int64Builder,
                 writer: &mut ParquetPartWriter|
     -> io::Result<()> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(dir_id.finish()),
            Arc::new(time.finish()),
            Arc::new(bucket.finish()),
            Arc::new(label.finish()),
            Arc::new(files.finish()),
            Arc::new(bytes.finish()),
        ];
        writer.write(&RecordBatch::try_new(Arc::clone(schema), columns).map_err(io::Error::other)?)
    };
    for (slot, &owner) in state.slot_dirs.iter().enumerate() {
        for (kind, name) in TIME_KINDS.iter().enumerate() {
            for b in 0..width {
                let [count, sum] = state.histograms[(slot * TIME_KINDS.len() + kind) * width + b];
                if count == 0 {
                    continue;
                }
                dir_id.append_value(owner as i64);
                time.append_value(name);
                bucket.append_value(b as i32);
                label.append_value(&state.buckets.labels[b]);
                files.append_value(count);
                bytes.append_value(sum);
                pending += 1;
                total += 1;
            }
        }
        if pending >= BATCH_ROWS {
            flush(
                &mut dir_id,
                &mut time,
                &mut bucket,
                &mut label,
                &mut files,
                &mut bytes,
                writer,
            )?;
            pending = 0;
        }
    }
    if pending > 0 {
        flush(
            &mut dir_id,
            &mut time,
            &mut bucket,
            &mut label,
            &mut files,
            &mut bytes,
            writer,
        )?;
    }
    Ok(total)
}

/// The rollup's default buckets, for `--age-buckets`.
pub const DEFAULT_AGE_BUCKETS: &str = "30d,90d,1y,2y,3y,5y,10y";

/// Paths of the rollup tables, for `verify`.
pub fn tables(manifest: &Value) -> Vec<(&'static str, Vec<Value>)> {
    ["rollup", "rollup_ages"]
        .into_iter()
        .filter_map(|t| {
            manifest["rollup"]["parts"][t]
                .as_array()
                .map(|parts| (t, parts.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400;

    #[test]
    fn age_buckets() {
        let b = AgeBuckets::parse(DEFAULT_AGE_BUCKETS).unwrap();
        assert_eq!(
            b.labels,
            [
                "future", "<30d", "30d-90d", "90d-1y", "1y-2y", "2y-3y", "3y-5y", "5y-10y", ">=10y"
            ]
        );
        assert_eq!(b.bucket(-5), 0);
        assert_eq!(b.bucket(0), 1);
        assert_eq!(b.bucket(29 * DAY), 1);
        assert_eq!(b.bucket(30 * DAY), 2);
        assert_eq!(b.bucket(400 * DAY), 4);
        assert_eq!(b.bucket(50 * 365 * DAY), 8);
        assert!(AgeBuckets::parse("90d,30d").is_err());
        assert!(AgeBuckets::parse("30d,30d").is_err());
        assert!(AgeBuckets::parse("soon").is_err());
        assert_eq!(
            AgeBuckets::parse("7d").unwrap().labels,
            ["future", "<7d", ">=7d"]
        );
    }

    /// root(1) -> a(2) -> b(3), root -> c(4); files at several depths.
    #[test]
    fn totals_and_histograms_roll_up() {
        let now = 1_000 * DAY * 1_000_000;
        let ago = |days: i64| Some(now - days * DAY * 1_000_000);
        let mut r = Rollup::new(AgeBuckets::parse("30d,1y").unwrap(), 1, now);
        r.add_dir(1, 0, 0, false);
        r.add_dir(2, 1, 1, false);
        r.add_dir(3, 2, 2, true);
        r.add_dir(4, 1, 1, false);
        r.plan();
        // Directory rows themselves (each counted in its parent).
        assert!(r.add_row(0, true, 0, 0, [None; 3]));
        assert!(r.add_row(1, true, 0, 0, [None; 3]));
        assert!(r.add_row(2, true, 0, 0, [None; 3]));
        assert!(r.add_row(1, true, 0, 0, [None; 3]));
        // Files: one in root, two in b (deep), one in c.
        assert!(r.add_row(1, false, 10, 8, [ago(1), ago(1), ago(1)]));
        assert!(r.add_row(3, false, 100, 64, [ago(400), ago(400), ago(2)]));
        assert!(r.add_row(3, false, 1000, 512, [ago(40), ago(40), ago(40)]));
        assert!(r.add_row(4, false, 5, 0, [ago(-3), ago(1), ago(1)]));
        assert!(!r.add_row(99, false, 1, 1, [None; 3]));
        r.propagate();

        let root = r.totals[1];
        assert_eq!((root.entries, root.files, root.dirs), (7, 4, 3));
        assert_eq!((root.bytes, root.alloc_bytes), (1115, 584));
        assert_eq!(root.incomplete, 1);
        assert_eq!(root.oldest_mtime, ago(400).unwrap());
        assert_eq!(root.newest_mtime, ago(-3).unwrap());
        let a = r.totals[2];
        assert_eq!((a.entries, a.files, a.dirs, a.bytes), (3, 2, 1, 1100));
        let c = r.totals[4];
        assert_eq!((c.entries, c.files, c.bytes, c.incomplete), (1, 1, 5, 0));

        // Slots exist for depth <= 1 only; b's files count toward a.
        assert_eq!(r.slot_dirs, [1, 2, 4]);
        let width = r.buckets.count();
        let mtime = |slot: usize, bucket: usize| r.histograms[(slot * 3) * width + bucket];
        assert_eq!(mtime(1, 2), [1, 1000]); // a: 40 days old
        assert_eq!(mtime(1, 3), [1, 100]); // a: 400 days old
        assert_eq!(mtime(2, 0), [1, 5]); // c: in the future
        assert_eq!(mtime(0, 1), [1, 10]); // root's own file
        assert_eq!(mtime(0, 2), [1, 1000]); // rolled up from a
        assert_eq!(mtime(0, 0), [1, 5]); // rolled up from c
    }
}
