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
//!
//! With a [`Checkpointer`], the walk takes checkpoints: every
//! [`WalkOptions::checkpoint_interval`], workers pause as they finish their
//! current directory, so every row delivered to a sink belongs to a finished
//! directory. The checkpointer makes those rows durable and stores a
//! [`WalkState`]: the directories still to read and the totals so far.
//! [`walk_from`] continues a walk from such a state, so a crashed scan loses
//! at most one interval of work and no directory is read twice.

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{self, Read as _, Seek as _, SeekFrom, Write as _};
use std::os::unix::ffi::OsStrExt as _;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_utils::{Backoff, CachePadded};
use globset::GlobSet;

use crate::backend::{Backend, NameBatch};
use crate::model::{DirTotals, Entry, EntryKind, ErrorInfo, ErrorRecord, Op, Stat, Timestamp};
use crate::spill::{Decode, Encode, Spill, SpillMark};

/// Entries a worker reads from one directory before sharing the rest.
pub const SHARE_AFTER: usize = 10_000;

/// Names of snapshot directories, skipped unless
/// [`WalkOptions::include_snapshots`] is set.
pub const SNAPSHOT_DIRS: [&[u8]; 4] = [b".snapshot", b"~snapshot", b".snapshots", b".zfs"];

/// How often the watchdog checks the workers and advances the walk's clock.
const WATCHDOG_TICK: Duration = Duration::from_millis(250);

/// How often the watchdog checks whether every worker has paused for a
/// checkpoint, and paused workers look for huge-directory batches.
const PAUSE_TICK: Duration = Duration::from_millis(2);

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
    /// Ignored with [`Self::state_dir`].
    pub spill_dir: Option<PathBuf>,
    /// Take a checkpoint this often. Needs a [`Checkpointer`] and
    /// [`Self::state_dir`].
    pub checkpoint_interval: Option<Duration>,
    /// How long a checkpoint waits for a worker that has stopped making
    /// progress before it gives up and tries again next interval.
    pub checkpoint_patience: Duration,
    /// Where state that checkpoints refer to lives: the spill file and, with
    /// [`Self::detect_cycles`], the record of directories entered. Unlike the
    /// unlinked spill file used without it, these outlive the process.
    pub state_dir: Option<PathBuf>,
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
            checkpoint_interval: None,
            checkpoint_patience: Duration::from_secs(30),
            state_dir: None,
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

/// Takes checkpoints: called while every worker is paused at a directory
/// boundary.
pub trait Checkpointer {
    /// Make every row delivered to the sinks so far durable, then store
    /// `state` so a walk can later continue from it with [`walk_from`].
    ///
    /// The sinks of [`WalkState::abandoned_workers`] end with rows of the
    /// directory the watchdog gave up on; keep only their rows up to the
    /// last directory boundary.
    fn checkpoint(&mut self, state: &WalkState) -> io::Result<()>;

    /// A checkpoint was skipped because a worker stopped making progress
    /// for [`WalkOptions::checkpoint_patience`].
    fn skipped(&mut self) {}
}

/// Where a walk starts.
pub enum Start<'a> {
    /// At these roots.
    Roots(&'a [Vec<u8>]),
    /// Where a checkpoint left off.
    Resume(WalkState),
}

/// A walk at a checkpoint: the directories still to read, the directory ID
/// counter, and the totals so far.
#[derive(Debug, Clone, Default)]
pub struct WalkState {
    next_id: u64,
    totals: CounterValues,
    spill: Option<SpillMark>,
    /// Directories in the visited log.
    visited: Option<u64>,
    errors: Vec<ErrorCount>,
    abandoned: Vec<Stalled>,
    /// Queued directories, each an encoded [`DirTask`].
    tasks: Vec<Vec<u8>>,
    abandoned_workers: Vec<usize>,
}

impl WalkState {
    const VERSION: u32 = 1;

    /// Directories still to read.
    pub fn pending(&self) -> u64 {
        self.tasks.len() as u64 + self.spill.map_or(0, |s| s.waiting)
    }

    /// Workers whose directory the watchdog gave up on, by sink index.
    pub fn abandoned_workers(&self) -> &[usize] {
        &self.abandoned_workers
    }

    /// The state as bytes, for [`Self::decode`].
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let mut e = Encode(&mut out);
        let t = &self.totals;
        e.u32(Self::VERSION).u64(self.next_id);
        for value in [
            t.entries, t.dirs, t.bytes, t.errors, t.vanished, t.revisits, t.spilled,
        ] {
            e.u64(value);
        }
        let spill = self.spill.unwrap_or_default();
        e.u32(u32::from(self.spill.is_some()))
            .u64(spill.read)
            .u64(spill.written)
            .u64(spill.waiting);
        e.u32(u32::from(self.visited.is_some()))
            .u64(self.visited.unwrap_or_default());
        e.u64(self.errors.len() as u64);
        for error in &self.errors {
            e.bytes(error.op.as_str().as_bytes())
                .u32(u32::from(error.errno.is_some()))
                .i64(i64::from(error.errno.unwrap_or_default()))
                .u64(error.count);
        }
        e.u64(self.abandoned.len() as u64);
        for stalled in &self.abandoned {
            e.bytes(&stalled.path).u64(stalled.seconds);
        }
        e.u64(self.tasks.len() as u64);
        for task in &self.tasks {
            e.bytes(task);
        }
        out
    }

    /// Reads a state written by [`Self::encode`].
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let invalid = |what: &str| io::Error::new(io::ErrorKind::InvalidData, what.to_string());
        let mut d = Decode(bytes);
        if d.u32()? != Self::VERSION {
            return Err(invalid("unknown checkpoint version"));
        }
        let next_id = d.u64()?;
        let totals = CounterValues {
            entries: d.u64()?,
            dirs: d.u64()?,
            bytes: d.u64()?,
            errors: d.u64()?,
            vanished: d.u64()?,
            revisits: d.u64()?,
            spilled: d.u64()?,
        };
        let has_spill = d.u32()? != 0;
        let spill = SpillMark {
            read: d.u64()?,
            written: d.u64()?,
            waiting: d.u64()?,
        };
        let has_visited = d.u32()? != 0;
        let visited = d.u64()?;
        let count = |d: &mut Decode<'_>| usize::try_from(d.u64()?).map_err(io::Error::other);
        let mut errors = Vec::new();
        for _ in 0..count(&mut d)? {
            let name = String::from_utf8(d.bytes()?).map_err(|_| invalid("bad operation"))?;
            let op = Op::from_name(&name).ok_or_else(|| invalid("unknown operation"))?;
            let has_errno = d.u32()? != 0;
            let errno = i32::try_from(d.i64()?).map_err(io::Error::other)?;
            errors.push(ErrorCount {
                op,
                errno: has_errno.then_some(errno),
                count: d.u64()?,
            });
        }
        let mut abandoned = Vec::new();
        for _ in 0..count(&mut d)? {
            abandoned.push(Stalled {
                path: d.bytes()?,
                seconds: d.u64()?,
                abandoned: true,
            });
        }
        let mut tasks = Vec::new();
        for _ in 0..count(&mut d)? {
            let task = d.bytes()?;
            DirTask::decode(&task)?;
            tasks.push(task);
        }
        Ok(Self {
            next_id,
            totals,
            spill: has_spill.then_some(spill),
            visited: has_visited.then_some(visited),
            errors,
            abandoned,
            tasks,
            abandoned_workers: Vec::new(),
        })
    }
}

/// A snapshot of [`Counters`].
#[derive(Debug, Clone, Copy, Default)]
struct CounterValues {
    entries: u64,
    dirs: u64,
    bytes: u64,
    errors: u64,
    vanished: u64,
    revisits: u64,
    spilled: u64,
}

impl CounterValues {
    fn load(c: &Counters) -> Self {
        Self {
            entries: c.entries.load(Ordering::Relaxed),
            dirs: c.dirs.load(Ordering::Relaxed),
            bytes: c.bytes.load(Ordering::Relaxed),
            errors: c.errors.load(Ordering::Relaxed),
            vanished: c.vanished.load(Ordering::Relaxed),
            revisits: c.revisits.load(Ordering::Relaxed),
            spilled: c.spilled.load(Ordering::Relaxed),
        }
    }

    fn add_to(self, c: &Counters) {
        c.entries.fetch_add(self.entries, Ordering::Relaxed);
        c.dirs.fetch_add(self.dirs, Ordering::Relaxed);
        c.bytes.fetch_add(self.bytes, Ordering::Relaxed);
        c.errors.fetch_add(self.errors, Ordering::Relaxed);
        c.vanished.fetch_add(self.vanished, Ordering::Relaxed);
        c.revisits.fetch_add(self.revisits, Ordering::Relaxed);
        c.spilled.fetch_add(self.spilled, Ordering::Relaxed);
    }
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
    /// been stuck, including those from before a resume.
    pub abandoned: Vec<Stalled>,
    /// For a walk with a [`WalkOptions::state_dir`] that stopped early, the
    /// state to continue from, once every row delivered to the sinks is
    /// durable.
    pub state: Option<WalkState>,
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
    pause: Pause,
    /// Errors counted by workers before they paused, merged into the result.
    paused_errors: Mutex<ErrorTally>,
}

/// Pauses workers at directory boundaries for a checkpoint.
#[derive(Default)]
struct Pause {
    requested: AtomicBool,
    /// Workers paused.
    paused: Mutex<usize>,
    resumed: Condvar,
}

impl Pause {
    fn requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    fn request(&self) {
        self.requested.store(true, Ordering::Release);
    }

    fn resume(&self) {
        let _paused = self.paused.lock();
        self.requested.store(false, Ordering::Release);
        self.resumed.notify_all();
    }

    /// Whether at least `live` workers are paused.
    fn all_paused(&self, live: usize) -> bool {
        self.paused.lock().is_ok_and(|paused| *paused >= live)
    }
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
/// worker first finishes the directory it is reading. This walk takes no
/// checkpoints; [`WalkOptions::checkpoint_interval`] and
/// [`WalkOptions::state_dir`] are ignored.
pub fn walk<B, S>(
    roots: &[Vec<u8>],
    mut options: WalkOptions,
    backend: Arc<B>,
    make_sink: impl FnMut(usize) -> S,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
) -> WalkResult
where
    B: Backend + 'static,
    S: Sink + 'static,
{
    options.checkpoint_interval = None;
    options.state_dir = None;
    let spill_dir = options.spill_dir.clone().unwrap_or_else(std::env::temp_dir);
    // Without a spill file (an unwritable directory), queued directories
    // simply stay in memory.
    let state = InitialState {
        spill: Spill::create(&spill_dir).ok(),
        visited: options.detect_cycles.then(Visited::new),
    };
    run(
        Start::Roots(roots),
        state,
        options,
        backend,
        make_sink,
        counters,
        stop,
        None,
    )
}

/// Like [`walk`], but can start where a checkpoint left off and takes
/// checkpoints with `checkpointer` every [`WalkOptions::checkpoint_interval`].
///
/// Checkpoints need [`WalkOptions::state_dir`], which holds the files they
/// refer to. Fails only if those files can't be opened. Resuming needs the
/// same options and roots as the walk that took the checkpoint.
pub fn walk_from<B, S>(
    start: Start<'_>,
    options: WalkOptions,
    backend: Arc<B>,
    make_sink: impl FnMut(usize) -> S,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
    checkpointer: Option<&mut dyn Checkpointer>,
) -> io::Result<WalkResult>
where
    B: Backend + 'static,
    S: Sink + 'static,
{
    let resume = match &start {
        Start::Resume(state) => Some(state),
        Start::Roots(_) => None,
    };
    let state = match &options.state_dir {
        Some(dir) => InitialState {
            spill: Some(Spill::persistent(
                &dir.join("spill"),
                resume.and_then(|r| r.spill),
            )?),
            visited: if options.detect_cycles {
                Some(Visited::open(
                    &dir.join("visited"),
                    resume.and_then(|r| r.visited),
                )?)
            } else {
                None
            },
        },
        None if resume.is_some() => {
            return Err(io::Error::other(
                "resuming a walk needs its state directory",
            ));
        }
        None => InitialState {
            spill: Spill::create(&options.spill_dir.clone().unwrap_or_else(std::env::temp_dir))
                .ok(),
            visited: options.detect_cycles.then(Visited::new),
        },
    };
    Ok(run(
        start,
        state,
        options,
        backend,
        make_sink,
        counters,
        stop,
        checkpointer,
    ))
}

/// The files a walk starts with.
struct InitialState {
    spill: Option<Spill>,
    visited: Option<Visited>,
}

#[allow(clippy::too_many_arguments, reason = "the parts of a walk")]
fn run<B, S>(
    start: Start<'_>,
    initial: InitialState,
    options: WalkOptions,
    backend: Arc<B>,
    mut make_sink: impl FnMut(usize) -> S,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
    mut checkpointer: Option<&mut dyn Checkpointer>,
) -> WalkResult
where
    B: Backend + 'static,
    S: Sink + 'static,
{
    let threads = options.threads.max(1);
    let mut sinks: Vec<S> = (0..threads).map(&mut make_sink).collect();
    let mut main_sink = make_sink(threads);
    let deques: Vec<Worker<DirTask>> = (0..threads).map(|_| Worker::new_lifo()).collect();
    let interval = options
        .checkpoint_interval
        .filter(|_| checkpointer.is_some() && options.state_dir.is_some());
    let shared = Arc::new(WalkShared {
        backend,
        visited: initial.visited,
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
        spill: initial.spill,
        pause: Pause::default(),
        paused_errors: Mutex::default(),
    });

    let mut main_tally = Tally::default();
    let mut errors = ErrorTally::new();
    let mut abandoned = Vec::new();
    match start {
        Start::Roots(roots) => {
            add_roots(&shared, roots, &mut main_sink, &mut main_tally);
            main_sink.end_dir();
            main_tally.flush(&shared.counters);
            // The first checkpoint lets a scan killed before the next one
            // start over from the roots.
            if let Some(checkpointer) = checkpointer.as_deref_mut()
                && interval.is_some()
            {
                take_checkpoint(&shared, checkpointer, &errors, &main_tally, &abandoned);
            }
        }
        Start::Resume(state) => {
            shared.next_id.store(state.next_id, Ordering::Relaxed);
            state.totals.add_to(&shared.counters);
            // Directories given up on before stay pending, as they were, so
            // the walk still reports itself incomplete.
            let given_up = state.abandoned.len() as u64;
            shared
                .counters
                .pending
                .store(state.pending() + given_up, Ordering::Relaxed);
            shared.abandoned_tasks.store(given_up, Ordering::Relaxed);
            for task in &state.tasks {
                // Every task was checked when the state was decoded.
                if let Ok(task) = DirTask::decode(task) {
                    shared
                        .queued_bytes
                        .fetch_add(task.memory(), Ordering::Relaxed);
                    shared.injector.push(task);
                }
            }
            for e in state.errors {
                *errors.entry((e.op, e.errno)).or_default() += e.count;
            }
            abandoned = state.abandoned;
        }
    }

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

    // Watch the workers until each has finished or been abandoned, and take
    // checkpoints along the way.
    let mut next_checkpoint = interval.map(|i| Instant::now() + i);
    let mut pausing = false;
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
        let live = handles
            .iter()
            .enumerate()
            .filter(|(i, h)| h.is_some() && !shared.statuses[*i].abandoned())
            .count();
        if live == 0 {
            break;
        }
        let wait = match next_checkpoint {
            _ if pausing => PAUSE_TICK,
            Some(at) => at
                .saturating_duration_since(Instant::now())
                .clamp(PAUSE_TICK, WATCHDOG_TICK),
            None => WATCHDOG_TICK,
        };
        let _ = exits.recv_timeout(wait);
        watchdog(&shared, &mut main_sink, &mut main_tally, &mut abandoned);
        main_tally.flush(&shared.counters);

        let (Some(checkpointer), Some(interval)) = (checkpointer.as_deref_mut(), interval) else {
            continue;
        };
        if shared.stop.load(Ordering::Relaxed) {
            if pausing {
                shared.pause.resume();
                pausing = false;
            }
            continue;
        }
        if !pausing {
            if next_checkpoint.is_some_and(|at| Instant::now() >= at) {
                shared.pause.request();
                pausing = true;
            }
            continue;
        }
        let live = handles
            .iter()
            .enumerate()
            .filter(|(i, h)| {
                h.as_ref().is_some_and(|h| !h.is_finished()) && !shared.statuses[*i].abandoned()
            })
            .count();
        if shared.pause.all_paused(live) {
            take_checkpoint(&shared, checkpointer, &errors, &main_tally, &abandoned);
        } else if stuck(&shared) {
            checkpointer.skipped();
        } else {
            continue;
        }
        shared.pause.resume();
        pausing = false;
        next_checkpoint = Some(Instant::now() + interval);
    }
    // Abandoned workers' threads are left running, detached; they write
    // nothing more.
    drop(handles);
    main_sink.finish();
    main_tally.flush(&shared.counters);
    merge_errors(&mut errors, std::mem::take(&mut main_tally.errors));
    if let Ok(mut paused) = shared.paused_errors.lock() {
        merge_errors(&mut errors, std::mem::take(&mut *paused));
    }
    if let Ok(mut stalled) = shared.counters.stalled.lock() {
        stalled.clear();
    }

    // What a resumed walk needs, if this one stopped early. Workers left
    // their queued directories in the injector as they exited.
    let state = (shared.options.state_dir.is_some() && !shared.finished())
        .then(|| snapshot(&shared, &errors, &abandoned).ok())
        .flatten();
    let mut errors: Vec<ErrorCount> = errors
        .into_iter()
        .map(|((op, errno), count)| ErrorCount { op, errno, count })
        .collect();
    errors.sort_by(|a, b| b.count.cmp(&a.count).then(a.errno.cmp(&b.errno)));
    WalkResult {
        complete: shared.counters.pending.load(Ordering::Acquire) == 0,
        errors,
        abandoned,
        state,
    }
}

fn merge_errors(into: &mut ErrorTally, from: ErrorTally) {
    for (key, count) in from {
        *into.entry(key).or_default() += count;
    }
}

/// Whether a busy worker has made no progress for the checkpoint patience.
fn stuck<B: Backend>(shared: &WalkShared<B>) -> bool {
    let patience =
        u64::try_from(shared.options.checkpoint_patience.as_millis()).unwrap_or(u64::MAX);
    let now = shared.now();
    shared.statuses.iter().any(|status| {
        let last = status.progress.load(Ordering::Acquire);
        last != 0 && last != ABANDONED && now.saturating_sub(last) >= patience
    })
}

/// Snapshots the walk and hands it to the checkpointer. Every worker must
/// be paused or not started.
fn take_checkpoint<B: Backend>(
    shared: &WalkShared<B>,
    checkpointer: &mut dyn Checkpointer,
    errors: &ErrorTally,
    main_tally: &Tally,
    abandoned: &[Stalled],
) {
    let mut all = errors.clone();
    merge_errors(&mut all, main_tally.errors.clone());
    if let Ok(paused) = shared.paused_errors.lock() {
        merge_errors(&mut all, paused.clone());
    }
    // A checkpoint that fails leaves the previous one in place; the
    // checkpointer reports it.
    if let Ok(state) = snapshot(shared, &all, abandoned) {
        let _ = checkpointer.checkpoint(&state);
    }
}

/// The walk's state, from the shared queue: workers must have moved their
/// own queued directories there.
fn snapshot<B: Backend>(
    shared: &WalkShared<B>,
    errors: &ErrorTally,
    abandoned: &[Stalled],
) -> io::Result<WalkState> {
    let mut queued = Vec::new();
    loop {
        match shared.injector.steal() {
            Steal::Success(task) => queued.push(task),
            Steal::Empty => break,
            Steal::Retry => {}
        }
    }
    let mut tasks = Vec::with_capacity(queued.len());
    for task in queued {
        let mut record = Vec::new();
        task.encode(&mut record);
        tasks.push(record);
        shared.injector.push(task);
    }
    let mut errors: Vec<ErrorCount> = errors
        .iter()
        .map(|(&(op, errno), &count)| ErrorCount { op, errno, count })
        .collect();
    errors.sort_by_key(|e| (e.op, e.errno));
    Ok(WalkState {
        next_id: shared.next_id.load(Ordering::Relaxed),
        totals: CounterValues::load(&shared.counters),
        spill: shared.spill.as_ref().map(Spill::mark).transpose()?,
        visited: match &shared.visited {
            Some(visited) => visited.persist()?,
            None => None,
        },
        errors,
        abandoned: abandoned.to_vec(),
        tasks,
        abandoned_workers: (0..shared.statuses.len())
            .filter(|&i| shared.statuses[i].abandoned())
            .collect(),
    })
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
/// contention low. With a log, checkpoints append the directories entered
/// since the last one, so a resumed walk knows them all.
struct Visited {
    shards: Vec<Mutex<VisitedShard>>,
    log: Option<Mutex<VisitedLog>>,
}

#[derive(Default)]
struct VisitedShard {
    set: HashSet<(u64, u64)>,
    /// Entered since the last checkpoint.
    fresh: Vec<(u64, u64)>,
}

struct VisitedLog {
    file: File,
    /// Directories in the file.
    count: u64,
}

impl Visited {
    const SHARDS: usize = 64;
    const RECORD: u64 = 16;

    fn new() -> Self {
        Self {
            shards: (0..Self::SHARDS).map(|_| Mutex::default()).collect(),
            log: None,
        }
    }

    /// Opens the log at `path`, keeping its first `count` directories and
    /// discarding any written after that checkpoint.
    fn open(path: &Path, count: Option<u64>) -> io::Result<Self> {
        let count = count.unwrap_or(0);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.set_len(count * Self::RECORD)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        file.seek(SeekFrom::End(0))?;
        let mut visited = Self::new();
        let (words, _) = bytes.as_chunks::<8>();
        let (records, _) = words.as_chunks::<2>();
        for [dev, ino] in records {
            let key = (u64::from_le_bytes(*dev), u64::from_le_bytes(*ino));
            if let Ok(mut shard) = visited.shards[Self::shard(key)].lock() {
                shard.set.insert(key);
            }
        }
        visited.log = Some(Mutex::new(VisitedLog { file, count }));
        Ok(visited)
    }

    fn shard(key: (u64, u64)) -> usize {
        (key.1 ^ key.0.rotate_left(17)) as usize % Self::SHARDS
    }

    /// Records a directory. Returns `false` if it was already recorded.
    fn insert(&self, key: (u64, u64)) -> bool {
        match self.shards[Self::shard(key)].lock() {
            Ok(mut shard) => {
                let new = shard.set.insert(key);
                if new && self.log.is_some() {
                    shard.fresh.push(key);
                }
                new
            }
            // A poisoned shard can only come from a panic mid-insert; treat
            // the directory as new rather than skip it.
            Err(_) => true,
        }
    }

    /// Appends the directories entered since the last call to the log, and
    /// returns how many it holds. Call only while workers are paused.
    fn persist(&self) -> io::Result<Option<u64>> {
        let Some(log) = &self.log else {
            return Ok(None);
        };
        let mut log = log
            .lock()
            .map_err(|_| io::Error::other("visited log poisoned"))?;
        let mut bytes = Vec::new();
        for shard in &self.shards {
            if let Ok(mut shard) = shard.lock() {
                for (dev, ino) in shard.fresh.drain(..) {
                    bytes.extend_from_slice(&dev.to_le_bytes());
                    bytes.extend_from_slice(&ino.to_le_bytes());
                }
            }
        }
        log.file.write_all(&bytes)?;
        log.file.sync_data()?;
        log.count += bytes.len() as u64 / Self::RECORD;
        Ok(Some(log.count))
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
            if self.shared.pause.requested() {
                self.pause();
                continue;
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
        // A stopped walk leaves directories queued; keep them where the
        // final state can see them.
        self.surrender();
        self.sink.finish();
        std::mem::take(&mut self.tally.errors)
    }

    /// Waits at a directory boundary until the checkpoint is taken, helping
    /// with huge directories meanwhile so they finish and the pause ends.
    fn pause(&mut self) {
        let shared = Arc::clone(&self.shared);
        let pause = &shared.pause;
        self.surrender();
        let Ok(mut paused) = pause.paused.lock() else {
            return;
        };
        *paused += 1;
        while pause.requested() && !shared.stop.load(Ordering::Relaxed) {
            if shared.chunks.is_empty() {
                paused = match pause.resumed.wait_timeout(paused, PAUSE_TICK) {
                    Ok((paused, _)) => paused,
                    Err(_) => return,
                };
                continue;
            }
            // Busy while helping: the checkpoint waits for the batch.
            *paused -= 1;
            drop(paused);
            if let Some(chunk) = self.steal_chunk() {
                self.run_chunk(chunk);
            }
            self.surrender();
            paused = match pause.paused.lock() {
                Ok(paused) => paused,
                Err(_) => return,
            };
            *paused += 1;
        }
        *paused -= 1;
    }

    /// Moves this worker's queued directories to the shared queue, and its
    /// counts to the shared totals, where a checkpoint can see them.
    fn surrender(&mut self) {
        while let Some(task) = self.deque.pop() {
            self.shared.injector.push(task);
        }
        self.tally.flush(&self.shared.counters);
        if !self.tally.errors.is_empty()
            && let Ok(mut paused) = self.shared.paused_errors.lock()
        {
            merge_errors(&mut paused, std::mem::take(&mut self.tally.errors));
        }
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
