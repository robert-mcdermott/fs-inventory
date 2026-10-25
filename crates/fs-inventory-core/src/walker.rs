// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! The parallel, work-stealing directory walker.
//!
//! Each worker owns a LIFO deque of directories, so it walks depth-first and
//! keeps its queue small. Idle workers steal from the other end of a peer's
//! deque, which holds directories nearest the root: the biggest pieces of
//! remaining work. A directory is always processed by one worker, and all of
//! its rows (ending with its own row) reach that worker's [`Sink`] before
//! [`Sink::end_dir`] is called.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt as _;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_utils::Backoff;
use globset::GlobSet;

use crate::backend::{Backend, NameBatch};
use crate::model::{DirTotals, Entry, EntryKind, ErrorInfo, ErrorRecord, Op, Stat};

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

    // Roots are read on this thread; errors and non-directory roots go to
    // the first worker's sink.
    let mut root_tally = Tally::default();
    for root in roots {
        let start = name_start(root);
        match backend.stat_root(root) {
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
                    deque,
                    injector: &injector,
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
    }
}

struct WorkerState<'a, B: Backend, S: Sink> {
    index: usize,
    backend: &'a B,
    options: &'a WalkOptions,
    counters: &'a Counters,
    next_id: &'a AtomicU64,
    deque: Worker<DirTask>,
    injector: &'a Injector<DirTask>,
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
            if let Some(task) = self.find_task() {
                backoff.reset();
                let path = task.path.clone();
                let outcome = catch_unwind(AssertUnwindSafe(|| self.process(task)));
                if outcome.is_err() {
                    self.record_internal_error(&path);
                }
                self.sink.end_dir();
                self.tally.flush(self.counters);
                self.counters.pending.fetch_sub(1, Ordering::AcqRel);
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

    fn find_task(&self) -> Option<DirTask> {
        if let Some(task) = self.deque.pop() {
            return Some(task);
        }
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
            Ok(mut dir) => match self.backend.dir_stat(&dir) {
                Ok(s) if (s.dev, s.ino) == (task.stat.dev, task.stat.ino) => {
                    self.tally.dirs += 1;
                    self.read_entries(&task, &mut dir, &mut totals)
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
        dir: &mut B::Dir,
        totals: &mut DirTotals,
    ) -> Option<ErrorInfo> {
        let mut names = std::mem::take(&mut self.names);
        let mut child_path = std::mem::take(&mut self.child_path);
        child_path.clear();
        child_path.extend_from_slice(&task.path);
        if &*task.path != b"/" {
            child_path.push(b'/');
        }
        let base_len = child_path.len();

        let mut error = None;
        loop {
            names.clear();
            let more = match self.backend.read_names(dir, &mut self.scratch, &mut names) {
                Ok(more) => more,
                Err(e) => {
                    error = Some(ErrorInfo::from_io(Op::ReadDir, &e));
                    false
                }
            };
            for name in names.iter() {
                child_path.truncate(base_len);
                child_path.extend_from_slice(name);
                // Contain a bug triggered by one entry to that entry, so the
                // rest of the directory is still read.
                let outcome =
                    catch_unwind(AssertUnwindSafe(|| match self.backend.stat_at(dir, name) {
                        Ok(stat) => self.child(task, &child_path, base_len, stat, totals),
                        Err(e)
                            if e.raw_os_error()
                                == Some(rustix::io::Errno::NOENT.raw_os_error()) =>
                        {
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
            if !more {
                break;
            }
        }
        self.names = names;
        self.child_path = child_path;
        error
    }

    fn child(
        &mut self,
        task: &DirTask,
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
        let depth = task.depth + 1;

        if is_dir {
            let is_mount_point = stat.dev != task.stat.dev;
            let within_depth = self.options.max_depth.is_none_or(|max| depth < max);
            let descend = within_depth && (self.options.cross_mounts || !is_mount_point);
            let dir_id = self.next_id.fetch_add(1, Ordering::Relaxed);
            if descend {
                self.counters.pending.fetch_add(1, Ordering::AcqRel);
                self.deque.push(DirTask {
                    path: path.into(),
                    name_start,
                    dir_id,
                    parent_id: task.dir_id,
                    depth,
                    stat,
                    is_mount_point,
                });
                return;
            }
            self.emit(
                path,
                name_start,
                depth,
                Some(dir_id),
                task.dir_id,
                &stat,
                is_mount_point,
            );
        } else {
            self.tally.bytes += stat.size.max(0) as u64;
            self.emit(path, name_start, depth, None, task.dir_id, &stat, false);
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
            error: None,
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
