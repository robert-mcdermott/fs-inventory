// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Walker tests against real directory trees: completeness against an
//! independent `std::fs` walk, traversal policies, and fault injection.

#![allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::MetadataExt as _;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fs_inventory_core::backend::{Backend, BackendOptions, NameBatch, NativeBackend};
use fs_inventory_core::model::{DirTotals, Entry, ErrorRecord, Op, Stat};
use fs_inventory_core::walker::{Counters, Sink, WalkOptions, WalkResult, walk};

/// An owned copy of one entry row.
#[derive(Debug, Clone)]
struct Row {
    path: Vec<u8>,
    parent_path: Vec<u8>,
    name: Vec<u8>,
    depth: u32,
    dir_id: Option<u64>,
    parent_id: u64,
    stat: Stat,
    totals: Option<DirTotals>,
    error: Option<Op>,
}

#[derive(Debug, Default)]
struct Collected {
    rows: Vec<Row>,
    errors: Vec<(Vec<u8>, Op, Option<i32>)>,
    /// Rows delivered per directory boundary, per worker, to check that a
    /// directory's own row closes its batch.
    batches: Vec<Vec<Vec<u8>>>,
}

struct CollectingSink {
    shared: Arc<Mutex<Collected>>,
    batch: Vec<Vec<u8>>,
    rows: Vec<Row>,
    errors: Vec<(Vec<u8>, Op, Option<i32>)>,
}

impl Sink for CollectingSink {
    fn entry(&mut self, e: &Entry<'_>) {
        self.batch.push(e.path.to_vec());
        self.rows.push(Row {
            path: e.path.to_vec(),
            parent_path: e.parent_path.to_vec(),
            name: e.name.to_vec(),
            depth: e.depth,
            dir_id: e.dir_id,
            parent_id: e.parent_id,
            stat: *e.stat,
            totals: e.totals,
            error: e.error.map(|i| i.op),
        });
    }

    fn error(&mut self, r: &ErrorRecord<'_>) {
        self.errors.push((r.path.to_vec(), r.info.op, r.info.errno));
    }

    fn end_dir(&mut self) {
        let batch = std::mem::take(&mut self.batch);
        self.shared.lock().unwrap().batches.push(batch);
    }

    fn finish(&mut self) {
        let mut shared = self.shared.lock().unwrap();
        shared.rows.append(&mut self.rows);
        shared.errors.append(&mut self.errors);
    }
}

/// Runs a walk on a separate thread and fails the test if it hangs.
fn run_walk<B: Backend + 'static>(
    roots: Vec<Vec<u8>>,
    options: WalkOptions,
    backend: B,
    stop: bool,
) -> (Collected, WalkResult) {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let shared = Arc::new(Mutex::new(Collected::default()));
        let counters = Counters::default();
        let stop = AtomicBool::new(stop);
        let result = walk(
            &roots,
            &options,
            &backend,
            |_| CollectingSink {
                shared: Arc::clone(&shared),
                batch: Vec::new(),
                rows: Vec::new(),
                errors: Vec::new(),
            },
            &counters,
            &stop,
        );
        let collected = std::mem::take(&mut *shared.lock().unwrap());
        let _ = tx.send((collected, result));
    });
    rx.recv_timeout(Duration::from_secs(60))
        .expect("walk did not finish within 60 seconds")
}

fn bytes(path: &Path) -> Vec<u8> {
    path.as_os_str().as_bytes().to_vec()
}

/// Every path under `root` (inclusive), from `std::fs`, which shares no code
/// with the walker.
fn oracle(root: &Path) -> BTreeMap<Vec<u8>, fs::Metadata> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    out.insert(bytes(root), fs::symlink_metadata(root).unwrap());
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let meta = fs::symlink_metadata(&path).unwrap();
            if meta.is_dir() {
                stack.push(path.clone());
            }
            out.insert(bytes(&path), meta);
        }
    }
    out
}

/// A tree of `width^depth` leaf directories, each with `files` files.
fn make_tree(root: &Path, width: usize, depth: usize, files: usize) {
    fn build(dir: &Path, width: usize, depth: usize, files: usize) {
        for f in 0..files {
            fs::write(dir.join(format!("file-{f}.dat")), vec![b'x'; f * 10]).unwrap();
        }
        if depth == 0 {
            return;
        }
        for w in 0..width {
            let sub = dir.join(format!("dir-{w}"));
            fs::create_dir(&sub).unwrap();
            build(&sub, width, depth - 1, files);
        }
    }
    build(root, width, depth, files);
}

fn options(threads: usize) -> WalkOptions {
    WalkOptions {
        threads,
        ..WalkOptions::default()
    }
}

#[test]
fn walk_matches_std_with_many_threads() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    make_tree(&root, 4, 4, 5); // 341 directories, 1,705 files
    let expected = oracle(&root);

    for threads in [1, 3, 16, 64] {
        let (got, result) = run_walk(
            vec![bytes(&root)],
            options(threads),
            NativeBackend::new(BackendOptions::default()),
            false,
        );
        assert!(result.complete, "threads={threads}");
        assert!(got.errors.is_empty(), "{:?}", got.errors);
        let paths: BTreeSet<&Vec<u8>> = got.rows.iter().map(|r| &r.path).collect();
        assert_eq!(
            paths.len(),
            got.rows.len(),
            "duplicate rows with {threads} threads"
        );
        let expected_paths: BTreeSet<&Vec<u8>> = expected.keys().collect();
        assert_eq!(paths, expected_paths, "threads={threads}");

        for row in &got.rows {
            let meta = &expected[&row.path];
            assert_eq!(row.stat.ino, meta.ino());
            assert_eq!(row.stat.size, meta.size() as i64);
            assert_eq!(row.stat.mode, meta.mode());
            assert_eq!(row.stat.mtime.sec, meta.mtime());
            assert_eq!(i64::from(row.stat.mtime.nsec), meta.mtime_nsec());
        }
    }
}

#[test]
fn ids_link_every_row_to_its_parent() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    make_tree(&root, 3, 3, 2);
    let (got, _) = run_walk(
        vec![bytes(&root)],
        options(8),
        NativeBackend::new(BackendOptions::default()),
        false,
    );
    let dirs: BTreeMap<u64, &Row> = got
        .rows
        .iter()
        .filter_map(|r| r.dir_id.map(|id| (id, r)))
        .collect();
    assert_eq!(
        dirs.len(),
        got.rows
            .iter()
            .filter(|r| r.stat.kind().as_str() == "dir")
            .count()
    );
    for row in &got.rows {
        if row.depth == 0 {
            assert_eq!(row.parent_id, 0);
            continue;
        }
        let parent = dirs[&row.parent_id];
        assert_eq!(parent.path, row.parent_path);
        assert_eq!(parent.depth + 1, row.depth);
        let mut joined = row.parent_path.clone();
        joined.push(b'/');
        joined.extend_from_slice(&row.name);
        assert_eq!(joined, row.path);
    }
}

#[test]
fn directory_rows_close_their_batch_with_correct_totals() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    make_tree(&root, 2, 2, 3);
    let (got, _) = run_walk(
        vec![bytes(&root)],
        options(4),
        NativeBackend::new(BackendOptions::default()),
        false,
    );
    for batch in got.batches.iter().filter(|b| !b.is_empty()) {
        // The last row of each directory batch is the directory itself, and
        // every other row in the batch is its direct child.
        let dir = batch.last().unwrap();
        for child in &batch[..batch.len() - 1] {
            assert!(child.starts_with(dir) && child[dir.len()] == b'/');
        }
    }
    let leaf = got
        .rows
        .iter()
        .find(|r| r.path.ends_with(b"dir-0/dir-1"))
        .unwrap();
    assert_eq!(
        leaf.totals,
        Some(DirTotals {
            entries: 3,
            files: 3,
            subdirs: 0,
            bytes: 30,
            alloc_bytes: leaf.totals.unwrap().alloc_bytes,
        })
    );
    let top = got.rows.iter().find(|r| r.depth == 0).unwrap();
    assert_eq!(top.totals.map(|t| (t.entries, t.subdirs)), Some((5, 2)));
}

#[test]
fn traversal_policies() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    make_tree(&root, 2, 3, 1);
    for name in [".snapshot", ".zfs", "skipme", "keep.tmp"] {
        fs::create_dir(root.join(name)).unwrap();
        fs::write(root.join(name).join("inside"), "x").unwrap();
    }
    std::os::unix::fs::symlink(root.join("dir-0"), root.join("link-to-dir")).unwrap();
    let count =
        |rows: &[Row], suffix: &[u8]| rows.iter().filter(|r| r.path.ends_with(suffix)).count();

    // Snapshot directories are skipped entirely; symlinks are listed, never followed.
    let (got, _) = run_walk(
        vec![bytes(&root)],
        options(4),
        NativeBackend::new(BackendOptions::default()),
        false,
    );
    assert_eq!(count(&got.rows, b"/.snapshot"), 0);
    assert_eq!(count(&got.rows, b"/.zfs"), 0);
    assert_eq!(count(&got.rows, b"/link-to-dir"), 1);
    assert!(
        !got.rows
            .iter()
            .any(|r| r.path.windows(12).any(|w| w == b"link-to-dir/"))
    );

    // --include-snapshots
    let opts = WalkOptions {
        include_snapshots: true,
        ..options(4)
    };
    let (got, _) = run_walk(
        vec![bytes(&root)],
        opts,
        NativeBackend::new(BackendOptions::default()),
        false,
    );
    assert_eq!(count(&got.rows, b"/.snapshot/inside"), 1);

    // --max-depth 1: the root's children are listed, but not read.
    let opts = WalkOptions {
        max_depth: Some(1),
        ..options(4)
    };
    let (got, _) = run_walk(
        vec![bytes(&root)],
        opts,
        NativeBackend::new(BackendOptions::default()),
        false,
    );
    assert_eq!(got.rows.iter().map(|r| r.depth).max(), Some(1));
    let dir0 = got
        .rows
        .iter()
        .find(|r| r.path.ends_with(b"/dir-0"))
        .unwrap();
    assert!(dir0.dir_id.is_some() && dir0.totals.is_none());

    // --exclude and skip_dirs.
    let mut globs = globset::GlobSetBuilder::new();
    globs.add(globset::Glob::new("**/*.tmp").unwrap());
    let skip = fs::metadata(root.join("skipme")).unwrap();
    let opts = WalkOptions {
        excludes: Some(globs.build().unwrap()),
        skip_dirs: vec![(skip.dev(), skip.ino())],
        ..options(4)
    };
    let (got, _) = run_walk(
        vec![bytes(&root)],
        opts,
        NativeBackend::new(BackendOptions::default()),
        false,
    );
    assert_eq!(count(&got.rows, b"/keep.tmp"), 0);
    assert_eq!(count(&got.rows, b"/keep.tmp/inside"), 0);
    assert_eq!(count(&got.rows, b"/skipme"), 0);
    let top = got.rows.iter().find(|r| r.depth == 0).unwrap();
    // dir-0, dir-1, file-0.dat, link-to-dir (the snapshot dirs and exclusions don't count).
    assert_eq!(top.totals.unwrap().entries, 4);
}

#[test]
fn roots_that_are_files_or_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    fs::write(root.join("single"), "abc").unwrap();
    let (got, result) = run_walk(
        vec![bytes(&root.join("single")), bytes(&root.join("missing"))],
        options(2),
        NativeBackend::new(BackendOptions::default()),
        false,
    );
    assert!(result.complete);
    assert_eq!(got.rows.len(), 1);
    assert_eq!(got.rows[0].name, b"single");
    assert_eq!(got.rows[0].stat.size, 3);
    assert_eq!(got.errors.len(), 1);
    assert_eq!(got.errors[0].1, Op::Stat);
}

#[test]
fn stop_leaves_the_walk_incomplete() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    make_tree(&root, 2, 2, 1);
    let (got, result) = run_walk(
        vec![bytes(&root)],
        options(4),
        NativeBackend::new(BackendOptions::default()),
        true,
    );
    assert!(!result.complete);
    assert!(got.rows.is_empty());
}

/// Wraps the native backend and injects failures by name:
/// `fail-open*` directories can't be opened, `fail-stat*` entries can't be
/// read, `vanish*` entries disappear, `fail-list*` directories fail while
/// listing, and `panic*` entries panic.
struct FaultyBackend(NativeBackend);

fn name_of(path: &[u8]) -> &[u8] {
    path.rsplit(|&b| b == b'/').next().unwrap_or(path)
}

impl Backend for FaultyBackend {
    type Dir = (<NativeBackend as Backend>::Dir, bool);
    type Scratch = <NativeBackend as Backend>::Scratch;

    fn new_scratch(&self) -> Self::Scratch {
        self.0.new_scratch()
    }

    fn stat_root(&self, path: &[u8]) -> io::Result<Stat> {
        self.0.stat_root(path)
    }

    fn open_dir(&self, path: &[u8], follow: bool) -> io::Result<Self::Dir> {
        if name_of(path).starts_with(b"fail-open") {
            return Err(io::Error::from_raw_os_error(
                rustix::io::Errno::ACCESS.raw_os_error(),
            ));
        }
        let fail_list = name_of(path).starts_with(b"fail-list");
        Ok((self.0.open_dir(path, follow)?, fail_list))
    }

    fn dir_stat(&self, dir: &Self::Dir) -> io::Result<Stat> {
        self.0.dir_stat(&dir.0)
    }

    fn read_names(
        &self,
        dir: &Self::Dir,
        scratch: &mut Self::Scratch,
        names: &mut NameBatch,
    ) -> io::Result<bool> {
        if dir.1 {
            return Err(io::Error::from_raw_os_error(
                rustix::io::Errno::IO.raw_os_error(),
            ));
        }
        self.0.read_names(&dir.0, scratch, names)
    }

    fn stat_at(&self, dir: &Self::Dir, name: &[u8]) -> io::Result<Stat> {
        if name.starts_with(b"fail-stat") {
            return Err(io::Error::from_raw_os_error(
                rustix::io::Errno::STALE.raw_os_error(),
            ));
        }
        if name.starts_with(b"vanish") {
            return Err(io::Error::from_raw_os_error(
                rustix::io::Errno::NOENT.raw_os_error(),
            ));
        }
        assert!(!name.starts_with(b"panic"), "injected panic");
        self.0.stat_at(&dir.0, name)
    }
}

#[test]
fn failures_are_recorded_and_never_stop_the_walk() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    make_tree(&root, 2, 2, 2);
    for dir in ["fail-open-dir", "fail-list-dir", "boom-dir"] {
        fs::create_dir(root.join(dir)).unwrap();
        fs::write(root.join(dir).join("child"), "x").unwrap();
    }
    fs::write(root.join("boom-dir/panic-file"), "x").unwrap();
    for file in ["fail-stat-file", "vanish-file"] {
        fs::write(root.join(file), "x").unwrap();
    }

    // Silence the expected panic message.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let (got, result) = run_walk(
        vec![bytes(&root)],
        options(4),
        FaultyBackend(NativeBackend::new(BackendOptions::default())),
        false,
    );
    std::panic::set_hook(hook);

    assert!(result.complete);
    let errors: BTreeSet<(Vec<u8>, Op)> = got
        .errors
        .iter()
        .map(|(path, op, _)| (name_of(path).to_vec(), *op))
        .collect();
    assert!(errors.contains(&(b"fail-open-dir".to_vec(), Op::Open)));
    assert!(errors.contains(&(b"fail-list-dir".to_vec(), Op::ReadDir)));
    assert!(errors.contains(&(b"fail-stat-file".to_vec(), Op::Stat)));
    assert!(errors.contains(&(b"panic-file".to_vec(), Op::Internal)));
    assert_eq!(errors.len(), 4, "{errors:?}");

    let row = |name: &[u8]| got.rows.iter().find(|r| name_of(&r.path) == name);
    // Directories that failed still get a row, with the error.
    assert_eq!(row(b"fail-open-dir").unwrap().error, Some(Op::Open));
    assert!(row(b"fail-open-dir").unwrap().totals.is_none());
    assert_eq!(row(b"fail-list-dir").unwrap().error, Some(Op::ReadDir));
    assert!(row(b"fail-stat-file").is_none());
    assert!(row(b"vanish-file").is_none());
    // Everything else was still read, including the panicking entry's siblings.
    assert_eq!(
        got.rows
            .iter()
            .filter(|r| name_of(&r.path) == b"file-1.dat")
            .count(),
        7
    );
    let boom = row(b"boom-dir").unwrap();
    assert_eq!(boom.totals.map(|t| t.entries), Some(1));
    assert!(got.rows.iter().any(|r| r.path.ends_with(b"boom-dir/child")));
}

#[test]
fn detect_cycles_never_reads_a_directory_twice() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    make_tree(&root, 2, 2, 2);
    let sub = root.join("dir-0");
    let opts = || WalkOptions {
        detect_cycles: true,
        ..options(4)
    };

    // Overlapping roots, and the same root twice.
    let roots = vec![bytes(&root), bytes(&sub), bytes(&root)];
    let (got, result) = run_walk(
        roots,
        opts(),
        NativeBackend::new(BackendOptions::default()),
        false,
    );
    assert!(result.complete);
    let revisits: Vec<&Row> = got
        .rows
        .iter()
        .filter(|r| r.error == Some(Op::Revisit))
        .collect();
    // dir-0 is found again inside the first root, and the root is repeated.
    assert_eq!(revisits.len(), 2, "{revisits:?}");
    assert!(
        revisits
            .iter()
            .all(|r| r.totals.is_none() && r.dir_id.is_some())
    );
    // Everything below dir-0 appears once.
    let below: Vec<&Vec<u8>> = got
        .rows
        .iter()
        .map(|r| &r.path)
        .filter(|p| p.starts_with(&bytes(&sub)) && p.len() > bytes(&sub).len())
        .collect();
    let unique: BTreeSet<&Vec<u8>> = below.iter().copied().collect();
    assert_eq!(below.len(), unique.len());
    assert!(!below.is_empty());

    // Without detection, the overlap is read twice.
    let (got, _) = run_walk(
        vec![bytes(&root), bytes(&sub)],
        options(4),
        NativeBackend::new(BackendOptions::default()),
        false,
    );
    let leaf = sub.join("file-0.dat");
    assert_eq!(
        got.rows.iter().filter(|r| r.path == bytes(&leaf)).count(),
        2
    );
}

/// Records which threads stat entries named `h-*`, slowing each slightly so
/// sharing is visible, and panics on `h-panic`.
struct TracingBackend {
    inner: NativeBackend,
    threads: Arc<Mutex<BTreeSet<String>>>,
}

impl Backend for TracingBackend {
    type Dir = <NativeBackend as Backend>::Dir;
    type Scratch = <NativeBackend as Backend>::Scratch;

    fn new_scratch(&self) -> Self::Scratch {
        self.inner.new_scratch()
    }

    fn stat_root(&self, path: &[u8]) -> io::Result<Stat> {
        self.inner.stat_root(path)
    }

    fn open_dir(&self, path: &[u8], follow: bool) -> io::Result<Self::Dir> {
        self.inner.open_dir(path, follow)
    }

    fn dir_stat(&self, dir: &Self::Dir) -> io::Result<Stat> {
        self.inner.dir_stat(dir)
    }

    fn read_names(
        &self,
        dir: &Self::Dir,
        scratch: &mut Self::Scratch,
        names: &mut NameBatch,
    ) -> io::Result<bool> {
        self.inner.read_names(dir, scratch, names)
    }

    fn stat_at(&self, dir: &Self::Dir, name: &[u8]) -> io::Result<Stat> {
        if name.starts_with(b"h-") {
            assert!(name != b"h-panic", "injected panic");
            let thread = format!("{:?}", std::thread::current().id());
            self.threads.lock().unwrap().insert(thread);
            std::thread::sleep(Duration::from_micros(20));
        }
        self.inner.stat_at(dir, name)
    }
}

#[test]
fn huge_directories_are_read_by_several_workers() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let huge = root.join("huge");
    fs::create_dir(&huge).unwrap();
    const FILES: usize = 20_000;
    for i in 0..FILES {
        fs::write(huge.join(format!("h-{i:05}")), "x").unwrap();
    }
    fs::write(huge.join("h-panic"), "x").unwrap();
    fs::create_dir(huge.join("h-subdir")).unwrap();
    fs::write(huge.join("h-subdir").join("inner"), "x").unwrap();

    let threads = Arc::new(Mutex::new(BTreeSet::new()));
    let backend = TracingBackend {
        inner: NativeBackend::new(BackendOptions::default()),
        threads: Arc::clone(&threads),
    };
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let (got, result) = run_walk(vec![bytes(&root)], options(8), backend, false);
    std::panic::set_hook(hook);

    assert!(result.complete);
    assert!(
        threads.lock().unwrap().len() > 1,
        "only one thread read metadata in a {FILES}-entry directory"
    );
    let children: BTreeSet<&Vec<u8>> = got
        .rows
        .iter()
        .filter(|r| r.parent_path == bytes(&huge))
        .map(|r| &r.path)
        .collect();
    // Every entry once, except the one that panicked.
    assert_eq!(children.len(), FILES + 1);
    assert_eq!(
        got.rows
            .iter()
            .filter(|r| r.parent_path == bytes(&huge))
            .count(),
        FILES + 1
    );
    assert!(
        got.rows
            .iter()
            .any(|r| r.path == bytes(&huge.join("h-subdir").join("inner")))
    );
    let internal: Vec<_> = got
        .errors
        .iter()
        .filter(|(_, op, _)| *op == Op::Internal)
        .collect();
    assert_eq!(internal.len(), 1);
    assert!(internal[0].0.ends_with(b"h-panic"));
    // The directory's totals include the batches other workers read.
    let dir = got.rows.iter().find(|r| r.path == bytes(&huge)).unwrap();
    let totals = dir.totals.unwrap();
    assert_eq!(totals.entries as usize, FILES + 1);
    assert_eq!((totals.files as usize, totals.subdirs), (FILES, 1));
    assert_eq!(totals.bytes as usize, FILES);
}
