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
//!
//! The calling thread is a watchdog. It reports workers that stop making
//! progress, such as one stuck in a call to a hung NFS server, and with
//! [`WalkOptions::stall_timeout`] it abandons them: their directory gets a
//! `stall` row and error, the other workers finish everything else, and the
//! walk returns without waiting for the stuck thread.

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt as _;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_utils::{Backoff, CachePadded};
use globset::GlobSet;

use crate::backend::{Backend, NameBatch};
use crate::model::{DirTotals, Entry, EntryKind, ErrorInfo, ErrorRecord, Op, Stat, Timestamp};
use crate::spill::{Decode, Encode, Spill};

/// Entries a worker reads from one directory before sharing the rest.
pub const SHARE_AFTER: usize = 10_000;

/// Names of snapshot directories, skipped unless
/// [`WalkOptions::include_snapshots`] is set.
pub const SNAPSHOT_DIRS: [&[u8]; 4] = [b".snapshot", b"~snapshot", b".snapshots", b".zfs"];

/// How often the watchdog checks the workers and advances the walk's clock.
const WATCHDOG_TICK: Duration = Duration::from_millis(250);

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
    /// Report a worker as stalled after this long without progress.
    pub stall_report: Duration,
    /// Abandon a worker after this long without progress. Its directory is
    /// recorded as stalled and the walk finishes without it.
    pub stall_timeout: Option<Duration>,
    /// Memory for directories waiting to be read. Beyond it, they wait in a
    /// spill file instead, read back once memory drains.
    pub queue_budget: usize,
    /// Where the spill file goes [default: the system temporary directory].
    pub spill_dir: Option<PathBuf>,
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
            stall_report: Duration::from_secs(60),
            stall_timeout: None,
            queue_budget: 1 << 30,
            spill_dir: None,
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
    /// The worker is stopping; no more rows follow. Not called for a worker
    /// the watchdog abandoned.
    fn finish(&mut self);
}

/// A worker that stopped making progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stalled {
    /// The directory it was reading.
    pub path: Vec<u8>,
    /// Seconds since its last progress.
    pub seconds: u64,
    /// Whether the walk gave up on it.
    pub abandoned: bool,
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
    /// Directories that waited in the spill file.
    pub spilled: AtomicU64,
    /// Directories queued or being read.
    pub pending: AtomicU64,
    /// Workers currently without progress for at least
    /// [`WalkOptions::stall_report`].
    pub stalled: Mutex<Vec<Stalled>>,
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
    /// False if the walk was stopped, or gave up on stalled directories,
    /// before every directory was read.
    pub complete: bool,
    /// Errors grouped by operation and errno, most frequent first.
    pub errors: Vec<ErrorCount>,
    /// Directories whose workers were abandoned, with how long they had
    /// been stuck.
    pub abandoned: Vec<Stalled>,
}

#[derive(Debug, Clone)]
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
    /// Approximate memory held while queued.
    fn memory(&self) -> i64 {
        (std::mem::size_of::<Self>() + self.path.len()) as i64
    }

    fn encode(&self, out: &mut Vec<u8>) {
        out.clear();
        let s = &self.stat;
        let mut e = Encode(out);
        e.bytes(&self.path)
            .u64(self.name_start as u64)
            .u64(self.dir_id)
            .u64(self.parent_id)
            .u32(self.depth)
            .u32(u32::from(self.is_mount_point))
            .u64(s.dev)
            .u64(s.ino)
            .u32(s.mode)
            .u64(s.nlink)
            .u32(s.uid)
            .u32(s.gid)
            .u64(s.rdev)
            .i64(s.size)
            .u64(s.blocks);
        for t in [s.atime, s.mtime, s.ctime, s.btime.unwrap_or_default()] {
            e.i64(t.sec).u32(t.nsec);
        }
        e.u32(u32::from(s.btime.is_some()));
    }

    fn decode(record: &[u8]) -> std::io::Result<Self> {
        let mut d = Decode(record);
        let path = d.bytes()?.into_boxed_slice();
        let name_start = usize::try_from(d.u64()?).map_err(std::io::Error::other)?;
        let (dir_id, parent_id, depth) = (d.u64()?, d.u64()?, d.u32()?);
        let is_mount_point = d.u32()? != 0;
        let mut stat = Stat {
            dev: d.u64()?,
            ino: d.u64()?,
            mode: d.u32()?,
            nlink: d.u64()?,
            uid: d.u32()?,
            gid: d.u32()?,
            rdev: d.u64()?,
            size: d.i64()?,
            blocks: d.u64()?,
            ..Stat::default()
        };
        let mut times = [Timestamp::default(); 4];
        for t in &mut times {
            *t = Timestamp {
                sec: d.i64()?,
                nsec: d.u32()?,
            };
        }
        [stat.atime, stat.mtime, stat.ctime] = [times[0], times[1], times[2]];
        stat.btime = (d.u32()? != 0).then_some(times[3]);
        Ok(Self {
            path,
            name_start,
            dir_id,
            parent_id,
            depth,
            stat,
            is_mount_point,
        })
    }

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

/// What a worker is doing, for the watchdog.
enum Work {
    /// Reading a whole directory.
    Task(DirTask),
    /// Reading metadata for a batch of a huge directory's names.
    Chunk(Box<[u8]>),
}

/// The value of [`WorkerStatus::progress`] once the watchdog abandons a worker.
const ABANDONED: u64 = u64::MAX;

/// One worker's state, as seen by the watchdog.
#[derive(Default)]
struct WorkerStatus {
    /// [`WalkShared::now`] at the worker's last progress, 0 while idle, or
    /// [`ABANDONED`].
    progress: AtomicU64,
    work: Mutex<Option<Work>>,
}

impl WorkerStatus {
    /// Records progress. Returns `false` if the worker was abandoned, in
    /// which case it must stop and write nothing more.
    fn touch(&self, now: u64) -> bool {
        self.set_unless_abandoned(now.max(1))
    }

    /// Stores `value` unless the worker was abandoned.
    fn set_unless_abandoned(&self, value: u64) -> bool {
        let mut current = self.progress.load(Ordering::Acquire);
        loop {
            if current == ABANDONED {
                return false;
            }
            match self.progress.compare_exchange_weak(
                current,
                value,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn abandoned(&self) -> bool {
        self.progress.load(Ordering::Acquire) == ABANDONED
    }

    fn begin(&self, work: Work, now: u64) -> bool {
        if let Ok(mut slot) = self.work.lock() {
            *slot = Some(work);
        }
        self.touch(now)
    }

    /// Back to idle. Returns `false` if the worker was abandoned.
    fn end(&self) -> bool {
        if let Ok(mut slot) = self.work.lock() {
            *slot = None;
        }
        self.set_unless_abandoned(0)
    }
}

/// State every worker and the watchdog share.
struct WalkShared<B: Backend> {
    backend: Arc<B>,
    options: WalkOptions,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
    next_id: AtomicU64,
    visited: Option<Visited>,
    injector: Injector<DirTask>,
    chunks: Injector<StatChunk<B::Dir>>,
    stealers: Vec<Stealer<DirTask>>,
    statuses: Vec<WorkerStatus>,
    started: Instant,
    /// Milliseconds since `started`, as of the watchdog's last tick. Workers
    /// read this instead of the system clock, which they would otherwise
    /// read for every entry: a system call with static musl builds.
    clock: CachePadded<AtomicU64>,
    /// Directories whose worker was abandoned. They stay pending forever, so
    /// workers finish once only these are left.
    abandoned_tasks: AtomicU64,
    /// Approximate memory held by queued directories.
    queued_bytes: AtomicI64,
    spill: Option<Spill>,
}

impl<B: Backend> WalkShared<B> {
    /// The time in milliseconds since the walk started, at most one
    /// watchdog tick old.
    fn now(&self) -> u64 {
        self.clock.load(Ordering::Relaxed)
    }

    /// Advances [`Self::now`] to the current time and returns it.
    fn tick(&self) -> u64 {
        let now = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(ABANDONED - 1);
        self.clock.store(now, Ordering::Relaxed);
        now
    }

    /// Whether every directory is done, except those given up on.
    fn finished(&self) -> bool {
        self.counters.pending.load(Ordering::Acquire)
            <= self.abandoned_tasks.load(Ordering::Acquire)
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
/// `make_sink(index)`: one per worker, plus one (the last index) for rows the
/// calling thread writes, such as roots that aren't directories and stalled
/// directories.
///
/// Roots must be absolute paths without trailing slashes. The walk stops
/// early, leaving queued directories unread, once `stop` becomes true; each
/// worker first finishes the directory it is reading.
pub fn walk<B, S>(
    roots: &[Vec<u8>],
    options: WalkOptions,
    backend: Arc<B>,
    mut make_sink: impl FnMut(usize) -> S,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
) -> WalkResult
where
    B: Backend + 'static,
    S: Sink + 'static,
{
    let threads = options.threads.max(1);
    let mut sinks: Vec<S> = (0..threads).map(&mut make_sink).collect();
    let mut main_sink = make_sink(threads);
    let deques: Vec<Worker<DirTask>> = (0..threads).map(|_| Worker::new_lifo()).collect();
    // Without a spill file (an unwritable directory), queued directories
    // simply stay in memory.
    let spill_dir = options.spill_dir.clone().unwrap_or_else(std::env::temp_dir);
    let spill = Spill::create(&spill_dir).ok();
    let shared = Arc::new(WalkShared {
        backend,
        visited: options.detect_cycles.then(Visited::new),
        options,
        counters,
        stop,
        next_id: AtomicU64::new(1),
        injector: Injector::new(),
        chunks: Injector::new(),
        stealers: deques.iter().map(Worker::stealer).collect(),
        statuses: (0..threads).map(|_| WorkerStatus::default()).collect(),
        started: Instant::now(),
        clock: CachePadded::new(AtomicU64::new(0)),
        abandoned_tasks: AtomicU64::new(0),
        queued_bytes: AtomicI64::new(0),
        spill,
    });

    let mut main_tally = Tally::default();
    add_roots(&shared, roots, &mut main_sink, &mut main_tally);
    main_sink.end_dir();
    main_tally.flush(&shared.counters);

    // Each worker signals when it exits, so the watchdog notices at once.
    let (exited, exits) = std::sync::mpsc::channel::<()>();
    let mut handles: Vec<Option<JoinHandle<ErrorTally>>> = Vec::new();
    for (index, deque) in deques.into_iter().enumerate() {
        let sink = sinks.remove(0);
        let worker = WorkerState {
            index,
            scratch: shared.backend.new_scratch(),
            shared: Arc::clone(&shared),
            deque,
            sink,
            names: NameBatch::default(),
            child_path: Vec::with_capacity(4096),
            tally: Tally::default(),
        };
        // A worker that can't be spawned leaves its share to the others.
        let exited = exited.clone();
        let handle = std::thread::Builder::new()
            .name(format!("fs-inventory-walk-{index}"))
            .spawn(move || {
                let _exit = NotifyOnExit(exited);
                worker.run()
            })
            .ok();
        handles.push(handle);
    }

    // Watch the workers until each has finished or been abandoned.
    let mut errors = std::mem::take(&mut main_tally.errors);
    let mut abandoned = Vec::new();
    loop {
        for handle in &mut handles {
            if handle.as_ref().is_some_and(JoinHandle::is_finished)
                && let Some(Ok(worker_errors)) = handle.take().map(JoinHandle::join)
            {
                for (key, count) in worker_errors {
                    *errors.entry(key).or_default() += count;
                }
            }
        }
        let running = handles
            .iter()
            .enumerate()
            .any(|(i, h)| h.is_some() && !shared.statuses[i].abandoned());
        if !running {
            break;
        }
        let _ = exits.recv_timeout(WATCHDOG_TICK);
        watchdog(&shared, &mut main_sink, &mut main_tally, &mut abandoned);
        main_tally.flush(&shared.counters);
    }
    // Abandoned workers' threads are left running, detached; they write
    // nothing more.
    drop(handles);
    main_sink.finish();
    main_tally.flush(&shared.counters);
    for (key, count) in std::mem::take(&mut main_tally.errors) {
        *errors.entry(key).or_default() += count;
    }
    if let Ok(mut stalled) = shared.counters.stalled.lock() {
        stalled.clear();
    }

    let mut errors: Vec<ErrorCount> = errors
        .into_iter()
        .map(|((op, errno), count)| ErrorCount { op, errno, count })
        .collect();
    errors.sort_by(|a, b| b.count.cmp(&a.count).then(a.errno.cmp(&b.errno)));
    WalkResult {
        complete: shared.counters.pending.load(Ordering::Acquire) == 0,
        errors,
        abandoned,
    }
}

/// Sends a signal when dropped, as a worker thread exits (even by panic).
struct NotifyOnExit(std::sync::mpsc::Sender<()>);

impl Drop for NotifyOnExit {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

/// Reads the roots on the calling thread, queueing directories and writing
/// rows for the rest.
fn add_roots<B: Backend, S: Sink>(
    shared: &WalkShared<B>,
    roots: &[Vec<u8>],
    sink: &mut S,
    tally: &mut Tally,
) {
    for root in roots {
        let start = name_start(root);
        let row = |stat: &Stat, dir_id: Option<u64>, error: Option<&ErrorInfo>, sink: &mut S| {
            sink.entry(&Entry {
                path: root,
                parent_path: parent_of(root, start),
                name: &root[start..],
                depth: 0,
                dir_id,
                parent_id: 0,
                stat,
                is_mount_point: false,
                totals: None,
                error,
            });
        };
        match shared.backend.stat_root(root) {
            Ok(stat)
                if stat.kind() == EntryKind::Dir
                    && shared
                        .visited
                        .as_ref()
                        .is_some_and(|v| !v.insert((stat.dev, stat.ino))) =>
            {
                let id = shared.next_id.fetch_add(1, Ordering::Relaxed);
                row(&stat, Some(id), Some(&revisit_info()), sink);
                tally.entries += 1;
                tally.revisits += 1;
            }
            Ok(stat) if stat.kind() == EntryKind::Dir => {
                shared.counters.pending.fetch_add(1, Ordering::Relaxed);
                let task = DirTask {
                    path: root.clone().into_boxed_slice(),
                    name_start: start,
                    dir_id: shared.next_id.fetch_add(1, Ordering::Relaxed),
                    parent_id: 0,
                    depth: 0,
                    stat,
                    is_mount_point: false,
                };
                shared
                    .queued_bytes
                    .fetch_add(task.memory(), Ordering::Relaxed);
                shared.injector.push(task);
            }
            Ok(stat) => {
                row(&stat, None, None, sink);
                tally.entries += 1;
                tally.bytes += stat.size.max(0) as u64;
            }
            Err(e) => {
                let info = ErrorInfo::from_io(Op::Stat, &e);
                sink.error(&ErrorRecord {
                    path: root,
                    info: &info,
                });
                tally.record_error(&info);
            }
        }
    }
}

/// One watchdog check: publishes the stalled workers and abandons those past
/// the timeout.
fn watchdog<B: Backend, S: Sink>(
    shared: &WalkShared<B>,
    sink: &mut S,
    tally: &mut Tally,
    abandoned: &mut Vec<Stalled>,
) {
    let now = shared.tick();
    let report = u64::try_from(shared.options.stall_report.as_millis()).unwrap_or(u64::MAX);
    let timeout = shared
        .options
        .stall_timeout
        .map(|t| u64::try_from(t.as_millis()).unwrap_or(u64::MAX));
    let mut stalled = Vec::new();
    for status in &shared.statuses {
        let last = status.progress.load(Ordering::Acquire);
        if last == 0 || last == ABANDONED {
            continue;
        }
        let quiet = now.saturating_sub(last);
        if quiet < report.min(timeout.unwrap_or(u64::MAX)) {
            continue;
        }
        let Ok(work) = status.work.lock() else {
            continue;
        };
        let path = match &*work {
            Some(Work::Task(task)) => task.path.to_vec(),
            Some(Work::Chunk(path)) => path.to_vec(),
            None => continue,
        };
        let give_up = timeout.is_some_and(|t| quiet >= t)
            // Only if it still hasn't moved, so it can't also finish the
            // directory itself.
            && status
                .progress
                .compare_exchange(last, ABANDONED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
        let record = Stalled {
            path,
            seconds: quiet / 1000,
            abandoned: give_up,
        };
        if give_up {
            let info = ErrorInfo {
                op: Op::Stall,
                errno: None,
                message: format!(
                    "no progress for {} seconds; gave up on this directory",
                    record.seconds
                ),
            };
            sink.error(&ErrorRecord {
                path: &record.path,
                info: &info,
            });
            tally.record_error(&info);
            if let Some(Work::Task(task)) = &*work {
                shared.abandoned_tasks.fetch_add(1, Ordering::AcqRel);
                sink.entry(&Entry {
                    path: &task.path,
                    parent_path: task.parent_path(),
                    name: task.name(),
                    depth: task.depth,
                    dir_id: Some(task.dir_id),
                    parent_id: task.parent_id,
                    stat: &task.stat,
                    is_mount_point: task.is_mount_point,
                    totals: None,
                    error: Some(&info),
                });
                tally.entries += 1;
            }
            sink.end_dir();
            abandoned.push(record);
        } else {
            stalled.push(record);
        }
    }
    if let Ok(mut current) = shared.counters.stalled.lock() {
        *current = stalled;
    }
}

/// Errors counted by operation and errno.
type ErrorTally = HashMap<(Op, Option<i32>), u64>;

/// Per-worker counts, flushed to the shared [`Counters`] once per directory.
#[derive(Default)]
struct Tally {
    entries: u64,
    dirs: u64,
    bytes: u64,
    errors_total: u64,
    vanished: u64,
    revisits: u64,
    errors: ErrorTally,
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

struct WorkerState<B: Backend, S: Sink> {
    index: usize,
    shared: Arc<WalkShared<B>>,
    deque: Worker<DirTask>,
    sink: S,
    scratch: B::Scratch,
    names: NameBatch,
    child_path: Vec<u8>,
    tally: Tally,
}

impl<B: Backend, S: Sink> WorkerState<B, S> {
    fn status(&self) -> &WorkerStatus {
        &self.shared.statuses[self.index]
    }

    /// Records progress. Returns `false` if this worker was abandoned.
    fn touch(&self) -> bool {
        self.status().touch(self.shared.now())
    }

    fn run(mut self) -> ErrorTally {
        let backoff = Backoff::new();
        while !self.shared.stop.load(Ordering::Relaxed) {
            if self.status().abandoned() {
                // The watchdog gave up on this thread; its sink is salvaged
                // by the output, so write nothing more.
                return std::mem::take(&mut self.tally.errors);
            }
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
            // Only once memory has drained, read back spilled directories.
            if let Some(task) = self.pop_spilled() {
                backoff.reset();
                self.run_task(task);
                continue;
            }
            if self.shared.finished() {
                break;
            }
            if backoff.is_completed() {
                std::thread::sleep(Duration::from_micros(200));
            } else {
                backoff.snooze();
            }
        }
        if self.status().abandoned() {
            return std::mem::take(&mut self.tally.errors);
        }
        self.sink.finish();
        self.tally.flush(&self.shared.counters);
        std::mem::take(&mut self.tally.errors)
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

    /// The oldest spilled directory, if any.
    fn pop_spilled(&mut self) -> Option<DirTask> {
        let spill = self.shared.spill.as_ref()?;
        let mut record = std::mem::take(&mut self.child_path);
        let popped = spill.pop(&mut record);
        let task = match popped {
            Ok(true) => DirTask::decode(&record).ok(),
            Ok(false) => None,
            Err(_) => None,
        };
        if let (Ok(true), None) = (&popped, &task) {
            // A record we can't decode is a directory that will never be
            // read; count it as done so the walk can finish.
            self.lost_spilled(1);
        } else if popped.is_err() {
            self.lost_spilled(spill.waiting() + 1);
        }
        self.child_path = record;
        let task = task?;
        self.shared
            .queued_bytes
            .fetch_add(task.memory(), Ordering::Relaxed);
        Some(task)
    }

    /// Records directories lost from an unreadable spill file.
    fn lost_spilled(&mut self, count: u64) {
        let info = ErrorInfo {
            op: Op::Internal,
            errno: None,
            message: format!("{count} queued directories were lost from the spill file"),
        };
        let path = self
            .shared
            .options
            .spill_dir
            .as_ref()
            .map_or_else(Vec::new, |p| p.as_os_str().as_bytes().to_vec());
        self.sink.error(&ErrorRecord {
            path: &path,
            info: &info,
        });
        self.tally.record_error(&info);
        self.shared
            .counters
            .pending
            .fetch_sub(count, Ordering::AcqRel);
    }

    fn run_task(&mut self, task: DirTask) {
        let shared = Arc::clone(&self.shared);
        shared
            .queued_bytes
            .fetch_sub(task.memory(), Ordering::Relaxed);
        let status = &shared.statuses[self.index];
        let path = task.path.clone();
        status.begin(Work::Task(task.clone()), shared.now());
        let outcome = catch_unwind(AssertUnwindSafe(|| self.process(task)));
        if !status.end() {
            // Abandoned while reading: the watchdog already wrote this
            // directory's row, and the directory stays pending.
            return;
        }
        if outcome.is_err() {
            self.record_internal_error(&path);
        }
        self.sink.end_dir();
        self.tally.flush(&shared.counters);
        shared.counters.pending.fetch_sub(1, Ordering::AcqRel);
    }

    /// Stats one batch of a huge directory's names.
    fn run_chunk(&mut self, chunk: StatChunk<B::Dir>) {
        let shared = Arc::clone(&self.shared);
        let status = &shared.statuses[self.index];
        let dir = chunk.dir;
        let _finished = Finished(&dir.outstanding);
        status.begin(Work::Chunk(dir.ctx.path.clone()), shared.now());
        let mut totals = DirTotals::default();
        self.stat_names(&dir.ctx, &dir.dir, &chunk.names, &mut totals);
        if !status.end() {
            return;
        }
        if let Ok(mut all) = dir.totals.lock() {
            all.merge(&totals);
        }
        self.sink.end_dir();
        self.tally.flush(&shared.counters);
    }

    fn steal_chunk(&self) -> Option<StatChunk<B::Dir>> {
        loop {
            match self.shared.chunks.steal() {
                Steal::Success(chunk) => return Some(chunk),
                Steal::Empty => return None,
                Steal::Retry => {}
            }
        }
    }

    fn steal_task(&self) -> Option<DirTask> {
        let n = self.shared.stealers.len();
        loop {
            let mut steal = self.shared.injector.steal_batch_and_pop(&self.deque);
            // Try peers one at a time, starting with the next worker so
            // stealing spreads evenly. Stop at the first success: a stolen
            // task must never be dropped.
            for offset in 1..n {
                if steal.is_success() {
                    break;
                }
                let peer = &self.shared.stealers[(self.index + offset) % n];
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
        let shared = Arc::clone(&self.shared);
        let mut totals = DirTotals::default();
        let error = match shared.backend.open_dir(&task.path, task.depth == 0) {
            Err(e) if e.raw_os_error() == Some(rustix::io::Errno::NOENT.raw_os_error()) => {
                // Deleted after its parent listed it.
                self.tally.vanished += 1;
                return;
            }
            Err(e) => Some(ErrorInfo::from_io(Op::Open, &e)),
            Ok(dir) => match shared.backend.dir_stat(&dir) {
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
        // Only the worker that still owns the directory writes its row.
        if !self.touch() {
            return;
        }
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
        let walk = Arc::clone(&self.shared);
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
            let more = match walk
                .backend
                .read_names(&shared.dir, &mut self.scratch, &mut names)
            {
                Ok(more) => more,
                Err(e) => {
                    error = Some(ErrorInfo::from_io(Op::ReadDir, &e));
                    false
                }
            };
            if !self.touch() {
                break;
            }
            read += names.len();
            if read > SHARE_AFTER && !names.is_empty() {
                // A huge directory: let idle workers stat this batch while
                // this worker keeps reading names.
                shared.outstanding.fetch_add(1, Ordering::AcqRel);
                walk.chunks.push(StatChunk {
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
        // this directory's are all done. Each finished batch counts as this
        // worker's progress, so only a batch that truly hangs looks stalled.
        let backoff = Backoff::new();
        let mut last = usize::MAX;
        loop {
            let outstanding = shared.outstanding.load(Ordering::Acquire);
            if outstanding == 0 || self.status().abandoned() {
                break;
            }
            if outstanding != last {
                last = outstanding;
                self.touch();
            }
            if let Some(chunk) = self.steal_chunk() {
                // Running a chunk replaces this worker's status; restore it.
                let work = self.status().work.lock().ok().and_then(|mut w| w.take());
                self.run_chunk(chunk);
                if let (Some(work), Ok(mut slot)) = (work, self.status().work.lock()) {
                    *slot = Some(work);
                }
                self.touch();
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
        let walk = Arc::clone(&self.shared);
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
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                let result = walk.backend.stat_at(dir, name);
                if !self.touch() {
                    return false;
                }
                match result {
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
                }
                true
            }));
            match outcome {
                Ok(true) => {}
                // Abandoned: stop without writing anything more.
                Ok(false) => break,
                Err(_) => self.record_internal_error(&child_path),
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
        let options = &self.shared.options;
        let name = &path[name_start..];
        let is_dir = stat.kind() == EntryKind::Dir;
        if is_dir
            && ((!options.include_snapshots && SNAPSHOT_DIRS.contains(&name))
                || options.skip_dirs.contains(&(stat.dev, stat.ino)))
        {
            return;
        }
        if let Some(excludes) = &options.excludes
            && excludes.is_match(Path::new(OsStr::from_bytes(path)))
        {
            return;
        }
        totals.add(&stat);
        let depth = dir.depth + 1;

        if is_dir {
            let is_mount_point = stat.dev != dir.dev;
            let within_depth = options.max_depth.is_none_or(|max| depth < max);
            let descend = within_depth && (options.cross_mounts || !is_mount_point);
            let dir_id = self.shared.next_id.fetch_add(1, Ordering::Relaxed);
            let revisit = descend
                && self
                    .shared
                    .visited
                    .as_ref()
                    .is_some_and(|v| !v.insert((stat.dev, stat.ino)));
            if descend && !revisit {
                let shared = Arc::clone(&self.shared);
                shared.counters.pending.fetch_add(1, Ordering::AcqRel);
                let task = DirTask {
                    path: path.into(),
                    name_start,
                    dir_id,
                    parent_id: dir.dir_id,
                    depth,
                    stat,
                    is_mount_point,
                };
                let size = task.memory();
                let over_budget = shared.queued_bytes.load(Ordering::Relaxed) + size
                    > i64::try_from(shared.options.queue_budget).unwrap_or(i64::MAX);
                if over_budget && let Some(spill) = &shared.spill {
                    let mut record = Vec::new();
                    task.encode(&mut record);
                    if spill.push(&record).is_ok() {
                        shared.counters.spilled.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                    // A full or failing disk: keep the directory in memory.
                }
                shared.queued_bytes.fetch_add(size, Ordering::Relaxed);
                self.deque.push(task);
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
