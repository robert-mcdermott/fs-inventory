// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! The parallel, work-stealing directory walker.
//!
//! Each worker owns a LIFO deque of directories, so it walks depth-first and
//! keeps its queue small. Idle workers steal from the other end of a peer's
//! deque, which holds directories nearest the root: the biggest pieces of
//! remaining work. A directory is read by one worker, and its rows (ending
//! with its own row) reach that worker's [`Sink`] before [`Sink::end_dir`]
//! is called, with one exception: past [`SHARE_AFTER`] entries, the reading
//! worker publishes each further batch of names for any idle worker to stat,
//! so huge directories don't serialize the scan on one thread's metadata
//! calls. Those rows reach the helping workers' sinks.

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt as _;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_utils::Backoff;
use globset::GlobSet;

use crate::backend::{Backend, NameBatch};
use crate::model::{DirTotals, Entry, EntryKind, ErrorInfo, ErrorRecord, Op, Stat};

/// Entries a worker reads from one directory before sharing the rest.
pub const SHARE_AFTER: usize = 10_000;

/// Names of snapshot directories, skipped unless
/// [`WalkOptions::include_snapshots`] is set.
pub const SNAPSHOT_DIRS: [&[u8]; 4] = [b".snapshot", b"~snapshot", b".snapshots", b".zfs"];

/// What to traverse.
#[derive(Debug, Clone)]
pub struct WalkOptions {
    /// Number of worker threads.
    pub threads: usize,
    /// Descend into directories on other devices (mount points).
    pub cross_mounts: bool,
    /// List entries at most this many levels below a root; directories at
    /// that depth are listed but not read.
    pub max_depth: Option<u32>,
    /// Paths matching these globs are skipped entirely, and excluded
    /// directories are not entered.
    pub excludes: Option<GlobSet>,
    /// Descend into snapshot directories ([`SNAPSHOT_DIRS`]).
    pub include_snapshots: bool,
    /// Directories to skip entirely, by `(dev, ino)`, such as the scan's own
    /// output directory.
    pub skip_dirs: Vec<(u64, u64)>,
    /// Remember every directory entered, by `(dev, ino)`, and never enter
    /// one twice. Guards against bind-mount loops, hard-linked directories,
    /// and overlapping roots, at about 50 bytes per directory.
    pub detect_cycles: bool,
}

impl Default for WalkOptions {
    fn default() -> Self {
        Self {
            threads: 32,
            cross_mounts: false,
            max_depth: None,
            excludes: None,
            include_snapshots: false,
            skip_dirs: Vec::new(),
            detect_cycles: false,
        }
    }
}

/// Receives the rows produced by one worker. Each worker has its own sink, so
/// sinks need no locking.
pub trait Sink: Send {
    /// One entry row.
    fn entry(&mut self, entry: &Entry<'_>);
    /// One errors-table row.
    fn error(&mut self, error: &ErrorRecord<'_>);
    /// Every row for one directory has been delivered. Directory rows are
    /// delivered after the rows of their children.
    fn end_dir(&mut self);
    /// The worker is stopping; no more rows follow.
    fn finish(&mut self);
}

/// Live crawl totals, readable from other threads while a walk runs. Workers
/// update them once per directory.
#[derive(Debug, Default)]
pub struct Counters {
    /// Rows emitted.
    pub entries: AtomicU64,
    /// Directories read.
    pub dirs: AtomicU64,
    /// Sum of `size` over entries that are not directories.
    pub bytes: AtomicU64,
    /// Errors recorded.
    pub errors: AtomicU64,
    /// Entries deleted between being listed and being read.
    pub vanished: AtomicU64,
    /// Directories not entered because they were already visited.
    pub revisits: AtomicU64,
    /// Directories queued or being read.
    pub pending: AtomicU64,
}

/// Count of errors for one kind of failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorCount {
    /// The failed operation.
    pub op: Op,
    /// The OS error number, if any.
    pub errno: Option<i32>,
    /// How many times it happened.
    pub count: u64,
}

/// The outcome of a walk.
#[derive(Debug, Clone)]
pub struct WalkResult {
    /// False if the walk was stopped before every directory was read.
    pub complete: bool,
    /// Errors grouped by operation and errno, most frequent first.
    pub errors: Vec<ErrorCount>,
}

struct DirTask {
    path: Box<[u8]>,
    /// Index in `path` where the final component starts.
    name_start: usize,
    dir_id: u64,
    parent_id: u64,
    depth: u32,
    stat: Stat,
    is_mount_point: bool,
}

impl DirTask {
    fn parent_path(&self) -> &[u8] {
        parent_of(&self.path, self.name_start)
    }

    fn name(&self) -> &[u8] {
        &self.path[self.name_start..]
    }
}

/// What processing a directory's children needs to know about it.
struct DirCtx {
    path: Box<[u8]>,
    dir_id: u64,
    depth: u32,
    dev: u64,
}

impl DirCtx {
    fn of(task: &DirTask) -> Self {
        Self {
            path: task.path.clone(),
            dir_id: task.dir_id,
            depth: task.depth,
            dev: task.stat.dev,
        }
    }
}

/// An open directory that several workers may stat entries of at once.
struct SharedDir<D> {
    ctx: DirCtx,
    dir: D,
    /// Totals from shared batches; the reading worker adds its own at the end.
    totals: Mutex<DirTotals>,
    /// Batches published and not yet finished.
    outstanding: AtomicUsize,
}

/// A batch of a huge directory's names, for any worker to stat.
struct StatChunk<D> {
    dir: Arc<SharedDir<D>>,
    names: NameBatch,
}

/// Marks a batch finished when dropped, even if processing it panicked, so
/// the reading worker never waits forever.
struct Finished<'a>(&'a AtomicUsize);

impl Drop for Finished<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Splits a path into the index where its final component starts.
fn name_start(path: &[u8]) -> usize {
    if path == b"/" {
        return 0;
    }
    path.iter().rposition(|&b| b == b'/').map_or(0, |i| i + 1)
}

fn parent_of(path: &[u8], name_start: usize) -> &[u8] {
    match name_start {
        0 => b"",
        1 => b"/",
        n => &path[..n - 1],
    }
}

/// Walks `roots` with `options`, sending rows to sinks created by
/// `make_sink(worker_index)`.
///
/// Roots must be absolute paths without trailing slashes. The walk stops
/// early, leaving queued directories unread, once `stop` becomes true; each
/// worker first finishes the directory it is reading.
pub fn walk<B, S>(
    roots: &[Vec<u8>],
    options: &WalkOptions,
    backend: &B,
    mut make_sink: impl FnMut(usize) -> S,
    counters: &Counters,
    stop: &AtomicBool,
) -> WalkResult
where
    B: Backend,
    S: Sink,
{
    let threads = options.threads.max(1);
    let mut sinks: Vec<S> = (0..threads).map(&mut make_sink).collect();
    let injector = Injector::new();
    let next_id = AtomicU64::new(1);
    let visited = options.detect_cycles.then(Visited::new);

    // Roots are read on this thread; errors and non-directory roots go to
    // the first worker's sink.
    let mut root_tally = Tally::default();
    for root in roots {
        let start = name_start(root);
        match backend.stat_root(root) {
            Ok(stat)
                if stat.kind() == EntryKind::Dir
                    && visited
                        .as_ref()
                        .is_some_and(|v| !v.insert((stat.dev, stat.ino))) =>
            {
                let info = revisit_info();
                sinks[0].entry(&Entry {
                    path: root,
                    parent_path: parent_of(root, start),
                    name: &root[start..],
                    depth: 0,
                    dir_id: Some(next_id.fetch_add(1, Ordering::Relaxed)),
                    parent_id: 0,
                    stat: &stat,
                    is_mount_point: false,
                    totals: None,
                    error: Some(&info),
                });
                root_tally.entries += 1;
                root_tally.revisits += 1;
            }
            Ok(stat) if stat.kind() == EntryKind::Dir => {
                counters.pending.fetch_add(1, Ordering::Relaxed);
                injector.push(DirTask {
                    path: root.clone().into_boxed_slice(),
                    name_start: start,
                    dir_id: next_id.fetch_add(1, Ordering::Relaxed),
                    parent_id: 0,
                    depth: 0,
                    stat,
                    is_mount_point: false,
                });
            }
            Ok(stat) => {
                sinks[0].entry(&Entry {
                    path: root,
                    parent_path: parent_of(root, start),
                    name: &root[start..],
                    depth: 0,
                    dir_id: None,
                    parent_id: 0,
                    stat: &stat,
                    is_mount_point: false,
                    totals: None,
                    error: None,
                });
                root_tally.entries += 1;
                root_tally.bytes += stat.size.max(0) as u64;
            }
            Err(e) => {
                let info = ErrorInfo::from_io(Op::Stat, &e);
                sinks[0].error(&ErrorRecord {
                    path: root,
                    info: &info,
                });
                root_tally.record_error(&info);
            }
        }
    }
    root_tally.flush(counters);
    if !sinks.is_empty() {
        sinks[0].end_dir();
    }

    let chunks: Injector<StatChunk<B::Dir>> = Injector::new();
    let deques: Vec<Worker<DirTask>> = (0..threads).map(|_| Worker::new_lifo()).collect();
    let stealers: Vec<Stealer<DirTask>> = deques.iter().map(Worker::stealer).collect();

    let mut errors: HashMap<(Op, Option<i32>), u64> = root_tally.errors;
    std::thread::scope(|scope| {
        let handles: Vec<_> = deques
            .into_iter()
            .zip(sinks)
            .enumerate()
            .map(|(index, (deque, sink))| {
                let worker = WorkerState {
                    index,
                    backend,
                    options,
                    counters,
                    next_id: &next_id,
                    visited: visited.as_ref(),
                    deque,
                    injector: &injector,
                    chunks: &chunks,
                    stealers: &stealers,
                    sink,
                    scratch: backend.new_scratch(),
                    names: NameBatch::default(),
                    child_path: Vec::with_capacity(4096),
                    tally: Tally::default(),
                };
                std::thread::Builder::new()
                    .name(format!("fs-inventory-walk-{index}"))
                    .spawn_scoped(scope, move || worker.run(stop))
            })
            .collect();
        for handle in handles {
            // A worker that cannot be spawned or that panics outside the
            // per-directory guard loses only its own tallies.
            if let Ok(Ok(worker_errors)) = handle.map(|h| h.join()) {
                for (key, count) in worker_errors {
                    *errors.entry(key).or_default() += count;
                }
            }
        }
    });

    let mut errors: Vec<ErrorCount> = errors
        .into_iter()
        .map(|((op, errno), count)| ErrorCount { op, errno, count })
        .collect();
    errors.sort_by(|a, b| b.count.cmp(&a.count).then(a.errno.cmp(&b.errno)));
    WalkResult {
        complete: counters.pending.load(Ordering::Acquire) == 0,
        errors,
    }
}

/// Per-worker counts, flushed to the shared [`Counters`] once per directory.
#[derive(Default)]
struct Tally {
    entries: u64,
    dirs: u64,
    bytes: u64,
    errors_total: u64,
    vanished: u64,
    revisits: u64,
    errors: HashMap<(Op, Option<i32>), u64>,
}

impl Tally {
    fn record_error(&mut self, info: &ErrorInfo) {
        self.errors_total += 1;
        *self.errors.entry((info.op, info.errno)).or_default() += 1;
    }

    fn flush(&mut self, counters: &Counters) {
        let add = |counter: &AtomicU64, value: &mut u64| {
            if *value > 0 {
                counter.fetch_add(*value, Ordering::Relaxed);
                *value = 0;
            }
        };
        add(&counters.entries, &mut self.entries);
        add(&counters.dirs, &mut self.dirs);
        add(&counters.bytes, &mut self.bytes);
        add(&counters.errors, &mut self.errors_total);
        add(&counters.vanished, &mut self.vanished);
        add(&counters.revisits, &mut self.revisits);
    }
}

/// Directories already entered, by `(dev, ino)`, sharded to keep lock
/// contention low.
struct Visited {
    shards: Vec<Mutex<HashSet<(u64, u64)>>>,
}

impl Visited {
    const SHARDS: usize = 64;

    fn new() -> Self {
        Self {
            shards: (0..Self::SHARDS).map(|_| Mutex::default()).collect(),
        }
    }

    /// Records a directory. Returns `false` if it was already recorded.
    fn insert(&self, key: (u64, u64)) -> bool {
        let shard = (key.1 ^ key.0.rotate_left(17)) as usize % Self::SHARDS;
        match self.shards[shard].lock() {
            Ok(mut set) => set.insert(key),
            // A poisoned shard can only come from a panic mid-insert; treat
            // the directory as new rather than skip it.
            Err(_) => true,
        }
    }
}

fn revisit_info() -> ErrorInfo {
    ErrorInfo {
        op: Op::Revisit,
        errno: None,
        message: "already visited through another path".into(),
    }
}

struct WorkerState<'a, B: Backend, S: Sink> {
    index: usize,
    backend: &'a B,
    options: &'a WalkOptions,
    counters: &'a Counters,
    next_id: &'a AtomicU64,
    visited: Option<&'a Visited>,
    deque: Worker<DirTask>,
    injector: &'a Injector<DirTask>,
    chunks: &'a Injector<StatChunk<B::Dir>>,
    stealers: &'a [Stealer<DirTask>],
    sink: S,
    scratch: B::Scratch,
    names: NameBatch,
    child_path: Vec<u8>,
    tally: Tally,
}

impl<B: Backend, S: Sink> WorkerState<'_, B, S> {
    fn run(mut self, stop: &AtomicBool) -> HashMap<(Op, Option<i32>), u64> {
        let backoff = Backoff::new();
        while !stop.load(Ordering::Relaxed) {
            // Own subtree first, then help with huge directories, then steal.
            if let Some(task) = self.deque.pop() {
                backoff.reset();
                self.run_task(task);
                continue;
            }
            if let Some(chunk) = self.steal_chunk() {
                backoff.reset();
                self.run_chunk(chunk);
                continue;
            }
            if let Some(task) = self.steal_task() {
                backoff.reset();
                self.run_task(task);
                continue;
            }
            if self.counters.pending.load(Ordering::Acquire) == 0 {
                break;
            }
            if backoff.is_completed() {
                std::thread::sleep(Duration::from_micros(200));
            } else {
                backoff.snooze();
            }
        }
        self.sink.finish();
        self.tally.flush(self.counters);
        self.tally.errors
    }

    fn record_internal_error(&mut self, path: &[u8]) {
        let info = ErrorInfo {
            op: Op::Internal,
            errno: None,
            message: "internal error; this entry was skipped".into(),
        };
        self.sink.error(&ErrorRecord { path, info: &info });
        self.tally.record_error(&info);
    }

    fn run_task(&mut self, task: DirTask) {
        let path = task.path.clone();
        let outcome = catch_unwind(AssertUnwindSafe(|| self.process(task)));
        if outcome.is_err() {
            self.record_internal_error(&path);
        }
        self.sink.end_dir();
        self.tally.flush(self.counters);
        self.counters.pending.fetch_sub(1, Ordering::AcqRel);
    }

    /// Stats one batch of a huge directory's names.
    fn run_chunk(&mut self, chunk: StatChunk<B::Dir>) {
        let shared = chunk.dir;
        let _finished = Finished(&shared.outstanding);
        let mut totals = DirTotals::default();
        self.stat_names(&shared.ctx, &shared.dir, &chunk.names, &mut totals);
        if let Ok(mut all) = shared.totals.lock() {
            all.merge(&totals);
        }
        self.sink.end_dir();
        self.tally.flush(self.counters);
    }

    fn steal_chunk(&self) -> Option<StatChunk<B::Dir>> {
        loop {
            match self.chunks.steal() {
                Steal::Success(chunk) => return Some(chunk),
                Steal::Empty => return None,
                Steal::Retry => {}
            }
        }
    }

    fn steal_task(&self) -> Option<DirTask> {
        let n = self.stealers.len();
        loop {
            let mut steal = self.injector.steal_batch_and_pop(&self.deque);
            // Try peers one at a time, starting with the next worker so
            // stealing spreads evenly. Stop at the first success: a stolen
            // task must never be dropped.
            for offset in 1..n {
                if steal.is_success() {
                    break;
                }
                let peer = &self.stealers[(self.index + offset) % n];
                steal = steal.or_else(|| peer.steal());
            }
            match steal {
                Steal::Success(task) => return Some(task),
                Steal::Empty => return None,
                Steal::Retry => {}
            }
        }
    }

    fn process(&mut self, task: DirTask) {
        let mut totals = DirTotals::default();
        let error = match self.backend.open_dir(&task.path, task.depth == 0) {
            Err(e) if e.raw_os_error() == Some(rustix::io::Errno::NOENT.raw_os_error()) => {
                // Deleted after its parent listed it.
                self.tally.vanished += 1;
                return;
            }
            Err(e) => Some(ErrorInfo::from_io(Op::Open, &e)),
            Ok(dir) => match self.backend.dir_stat(&dir) {
                Ok(s) if (s.dev, s.ino) == (task.stat.dev, task.stat.ino) => {
                    self.tally.dirs += 1;
                    self.read_entries(&task, dir, &mut totals)
                }
                Ok(_) => Some(ErrorInfo {
                    op: Op::Verify,
                    errno: None,
                    message: "directory was replaced during the scan".into(),
                }),
                Err(e) => Some(ErrorInfo::from_io(Op::Stat, &e)),
            },
        };
        let read = !matches!(
            error,
            Some(ErrorInfo {
                op: Op::Open | Op::Verify | Op::Stat,
                ..
            })
        );
        if let Some(info) = &error {
            self.sink.error(&ErrorRecord {
                path: &task.path,
                info,
            });
            self.tally.record_error(info);
        }
        self.sink.entry(&Entry {
            path: &task.path,
            parent_path: task.parent_path(),
            name: task.name(),
            depth: task.depth,
            dir_id: Some(task.dir_id),
            parent_id: task.parent_id,
            stat: &task.stat,
            is_mount_point: task.is_mount_point,
            totals: read.then_some(totals),
            error: error.as_ref(),
        });
        self.tally.entries += 1;
    }

    /// Reads every entry of an open directory. Returns the error that ended
    /// reading early, if any.
    fn read_entries(
        &mut self,
        task: &DirTask,
        dir: B::Dir,
        totals: &mut DirTotals,
    ) -> Option<ErrorInfo> {
        let shared = Arc::new(SharedDir {
            ctx: DirCtx::of(task),
            dir,
            totals: Mutex::default(),
            outstanding: AtomicUsize::new(0),
        });
        let mut names = std::mem::take(&mut self.names);
        let mut read = 0;
        let mut error = None;
        loop {
            names.clear();
            let more = match self
                .backend
                .read_names(&shared.dir, &mut self.scratch, &mut names)
            {
                Ok(more) => more,
                Err(e) => {
                    error = Some(ErrorInfo::from_io(Op::ReadDir, &e));
                    false
                }
            };
            read += names.len();
            if read > SHARE_AFTER && !names.is_empty() {
                // A huge directory: let idle workers stat this batch while
                // this worker keeps reading names.
                shared.outstanding.fetch_add(1, Ordering::AcqRel);
                self.chunks.push(StatChunk {
                    dir: Arc::clone(&shared),
                    names: std::mem::take(&mut names),
                });
            } else {
                self.stat_names(&shared.ctx, &shared.dir, &names, totals);
            }
            if !more {
                break;
            }
        }
        self.names = names;

        // Help with the shared batches (this directory's or any other) until
        // this directory's are all done.
        let backoff = Backoff::new();
        while shared.outstanding.load(Ordering::Acquire) > 0 {
            if let Some(chunk) = self.steal_chunk() {
                self.run_chunk(chunk);
                backoff.reset();
            } else if backoff.is_completed() {
                std::thread::sleep(Duration::from_micros(50));
            } else {
                backoff.snooze();
            }
        }
        if let Ok(shared_totals) = shared.totals.lock() {
            totals.merge(&shared_totals);
        }
        error
    }

    /// Stats a batch of names in one directory and emits their rows.
    fn stat_names(
        &mut self,
        ctx: &DirCtx,
        dir: &B::Dir,
        names: &NameBatch,
        totals: &mut DirTotals,
    ) {
        let mut child_path = std::mem::take(&mut self.child_path);
        child_path.clear();
        child_path.extend_from_slice(&ctx.path);
        if &*ctx.path != b"/" {
            child_path.push(b'/');
        }
        let base_len = child_path.len();
        for name in names.iter() {
            child_path.truncate(base_len);
            child_path.extend_from_slice(name);
            // Contain a bug triggered by one entry to that entry, so the
            // rest of the directory is still read.
            let outcome =
                catch_unwind(AssertUnwindSafe(|| match self.backend.stat_at(dir, name) {
                    Ok(stat) => self.child(ctx, &child_path, base_len, stat, totals),
                    Err(e) if e.raw_os_error() == Some(rustix::io::Errno::NOENT.raw_os_error()) => {
                        self.tally.vanished += 1;
                    }
                    Err(e) => {
                        let info = ErrorInfo::from_io(Op::Stat, &e);
                        self.sink.error(&ErrorRecord {
                            path: &child_path,
                            info: &info,
                        });
                        self.tally.record_error(&info);
                    }
                }));
            if outcome.is_err() {
                self.record_internal_error(&child_path);
            }
        }
        self.child_path = child_path;
    }

    fn child(
        &mut self,
        dir: &DirCtx,
        path: &[u8],
        name_start: usize,
        stat: Stat,
        totals: &mut DirTotals,
    ) {
        let name = &path[name_start..];
        let is_dir = stat.kind() == EntryKind::Dir;
        if is_dir
            && ((!self.options.include_snapshots && SNAPSHOT_DIRS.contains(&name))
                || self.options.skip_dirs.contains(&(stat.dev, stat.ino)))
        {
            return;
        }
        if let Some(excludes) = &self.options.excludes
            && excludes.is_match(Path::new(OsStr::from_bytes(path)))
        {
            return;
        }
        totals.add(&stat);
        let depth = dir.depth + 1;

        if is_dir {
            let is_mount_point = stat.dev != dir.dev;
            let within_depth = self.options.max_depth.is_none_or(|max| depth < max);
            let descend = within_depth && (self.options.cross_mounts || !is_mount_point);
            let dir_id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let revisit = descend
                && self
                    .visited
                    .is_some_and(|v| !v.insert((stat.dev, stat.ino)));
            if descend && !revisit {
                self.counters.pending.fetch_add(1, Ordering::AcqRel);
                self.deque.push(DirTask {
                    path: path.into(),
                    name_start,
                    dir_id,
                    parent_id: dir.dir_id,
                    depth,
                    stat,
                    is_mount_point,
                });
                return;
            }
            let error = revisit.then(revisit_info);
            self.tally.revisits += u64::from(revisit);
            self.emit(
                path,
                name_start,
                depth,
                Some(dir_id),
                dir.dir_id,
                &stat,
                is_mount_point,
                error.as_ref(),
            );
        } else {
            self.tally.bytes += stat.size.max(0) as u64;
            self.emit(
                path, name_start, depth, None, dir.dir_id, &stat, false, None,
            );
        }
    }

    #[allow(clippy::too_many_arguments, reason = "one row's fields")]
    fn emit(
        &mut self,
        path: &[u8],
        name_start: usize,
        depth: u32,
        dir_id: Option<u64>,
        parent_id: u64,
        stat: &Stat,
        is_mount_point: bool,
        error: Option<&ErrorInfo>,
    ) {
        self.sink.entry(&Entry {
            path,
            parent_path: parent_of(path, name_start),
            name: &path[name_start..],
            depth,
            dir_id,
            parent_id,
            stat,
            is_mount_point,
            totals: None,
            error,
        });
        self.tally.entries += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_splitting() {
        let split = |p: &[u8]| {
            let start = name_start(p);
            (parent_of(p, start).to_vec(), p[start..].to_vec())
        };
        assert_eq!(split(b"/"), (b"".to_vec(), b"/".to_vec()));
        assert_eq!(split(b"/a"), (b"/".to_vec(), b"a".to_vec()));
        assert_eq!(split(b"/a/b"), (b"/a".to_vec(), b"b".to_vec()));
        assert_eq!(split(b"/a/caf\xe9"), (b"/a".to_vec(), b"caf\xe9".to_vec()));
        assert_eq!(split(b"rel"), (b"".to_vec(), b"rel".to_vec()));
    }
}
