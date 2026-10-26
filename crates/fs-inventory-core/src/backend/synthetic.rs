// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! A generated file system, for testing and benchmarking at scales real
//! storage can't easily provide. Nothing touches the disk: every listing and
//! every stat is computed from the entry's path, so a tree of a billion
//! entries costs neither storage nor memory, and the same options always
//! give the same tree.
//!
//! Each root's tree has exactly [`SyntheticOptions::entries`] entries,
//! counting the root. Each directory's share of the entries below it is split
//! between its own files and its subdirectories by a hash of its path, so
//! the total is exact while directory sizes and depths vary. The tree also
//! has the variety of a real one: file sizes from bytes to gigabytes, an
//! owner per project directory, ten years of timestamps, symlinks, hard
//! links, an occasional huge directory, and names that aren't valid UTF-8 or
//! need escaping in CSV and JSON.

use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use rustix::io::Errno;

use super::{Backend, NameBatch};
use crate::model::{Stat, Timestamp};

/// Names returned per [`Backend::read_names`] call.
const NAMES_PER_READ: u64 = 4096;

/// The device number every entry reports.
const DEV: u64 = 0x7379_6e74;

/// 2015-01-01T00:00:00Z: about the earliest modification time.
const EPOCH: i64 = 1_420_070_400;
/// Directories' contents were written over ten years from [`EPOCH`].
const SPAN: u64 = 10 * 365 * 86_400;

/// Distinct streams of hash values for each property of an entry.
const SHAPE: u64 = 0x5348_4150;
const WEIGHT: u64 = 0x5745_4947;
const NAME: u64 = 0x4e41_4d45;
const KIND: u64 = 0x4b49_4e44;
const SIZE: u64 = 0x5349_5a45;
const TIME: u64 = 0x5449_4d45;

/// The root's inode number. Every entry's is its position in a depth-first
/// listing after that, so they're unique and, as on most file systems, close
/// together within a directory.
const FIRST_INODE: u64 = 2;

const WORDS: [&[u8]; 16] = [
    b"data",
    b"run",
    b"sample",
    b"results",
    b"raw",
    b"analysis",
    b"scratch",
    b"output",
    b"input",
    b"model",
    b"images",
    b"reads",
    b"logs",
    b"config",
    b"backup",
    b"archive",
];
const SUFFIXES: [&[u8]; 8] = [b"", b"", b"", b"", b"_final", b"_v2", b"_old", b"_copy"];
const EXTENSIONS: [&[u8]; 16] = [
    b".txt",
    b".csv",
    b".dat",
    b".h5",
    b".fastq.gz",
    b".bam",
    b".tif",
    b".py",
    b".log",
    b".gz",
    b".json",
    b".parquet",
    b".nc",
    b".npy",
    b".c",
    b"",
];
/// Added to about one name in a thousand: invalid UTF-8, characters that
/// need quoting or escaping, both Unicode normalization forms, and a
/// control character.
const ODD: [&[u8]; 12] = [
    b"\xff",
    b"_\xe9t\xe9",
    b" copy",
    b",comma",
    b"\"quoted\"",
    b"\nnewline",
    b"\ttab",
    "_caf\u{e9}".as_bytes(),
    "_cafe\u{301}".as_bytes(),
    "_\u{65e5}\u{672c}\u{8a9e}".as_bytes(),
    b"\x7f",
    b"\\back",
];

/// The shape of a generated tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyntheticOptions {
    /// Entries in each root's tree, counting the root.
    pub entries: u64,
    /// Typical files per directory.
    pub files: u64,
    /// Typical subdirectories per directory that has any. At least 2.
    pub fanout: u64,
    /// Files in the occasional huge directory, or 0 for none. Directories
    /// with at least twice this many entries below them become huge one
    /// time in four.
    pub huge: u64,
    /// Varies the tree: the same options give the same tree.
    pub seed: u64,
}

impl Default for SyntheticOptions {
    fn default() -> Self {
        Self {
            entries: 1_000_000,
            files: 32,
            fanout: 8,
            huge: 1_000_000,
            seed: 0,
        }
    }
}

impl SyntheticOptions {
    /// Parses a spec such as `100M` (just the entries) or
    /// `entries=100M,files=32,fanout=8,huge=1M,seed=0`. Numbers take a `K`,
    /// `M`, or `G` suffix (powers of 1,000); options left out keep their
    /// defaults.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let mut options = Self::default();
        for token in spec.split(',').map(str::trim).filter(|t| !t.is_empty()) {
            let (key, value) = token.split_once('=').unwrap_or(("entries", token));
            let number = parse_count(value)
                .ok_or_else(|| format!("invalid number `{value}` in synthetic tree spec"))?;
            match key.trim() {
                "entries" => options.entries = number,
                "files" => options.files = number,
                "fanout" => options.fanout = number,
                "huge" => options.huge = number,
                "seed" => options.seed = number,
                other => {
                    return Err(format!(
                        "unknown synthetic tree option `{other}` (expected entries, files, fanout, huge, or seed)"
                    ));
                }
            }
        }
        if options.entries == 0 {
            return Err("a synthetic tree needs at least 1 entry".into());
        }
        if options.fanout < 2 {
            return Err("a synthetic tree needs a fanout of at least 2".into());
        }
        Ok(options)
    }
}

impl fmt::Display for SyntheticOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "entries={},files={},fanout={},huge={},seed={}",
            self.entries, self.files, self.fanout, self.huge, self.seed
        )
    }
}

fn parse_count(value: &str) -> Option<u64> {
    let value = value.trim();
    let (digits, scale) = match value.as_bytes().last()? {
        b'k' | b'K' => (&value[..value.len() - 1], 1_000),
        b'm' | b'M' => (&value[..value.len() - 1], 1_000_000),
        b'g' | b'G' => (&value[..value.len() - 1], 1_000_000_000),
        _ => (value, 1),
    };
    let digits: String = digits.chars().filter(|&c| c != '_').collect();
    digits.parse::<u64>().ok()?.checked_mul(scale)
}

/// A backend that lists and stats a generated tree instead of the file
/// system. Each root is a different tree; the first root's depends only on
/// the options.
#[derive(Debug)]
pub struct SyntheticBackend {
    options: SyntheticOptions,
    roots: Vec<(Vec<u8>, Node)>,
}

/// An open directory of a generated tree.
#[derive(Debug)]
pub struct SyntheticDir {
    node: Node,
    shape: Shape,
    /// Each subdirectory, with its number and the entries below it.
    subdirs: Vec<(u64, u64)>,
    /// The number of the first file.
    first_file: u64,
    /// The next child to list. Only the reading worker moves it.
    next: AtomicU64,
}

/// A directory in the tree.
#[derive(Debug, Clone, Copy)]
struct Node {
    key: u64,
    /// The directory's position in a depth-first listing of every tree,
    /// which is also its inode number.
    number: u64,
    /// Entries below this directory.
    budget: u64,
    depth: u32,
    uid: u32,
}

/// A directory's children: subdirectories first, then files.
#[derive(Debug, Clone, Copy)]
struct Shape {
    subdirs: u64,
    files: u64,
}

impl Shape {
    const fn children(self) -> u64 {
        self.subdirs + self.files
    }
}

impl SyntheticBackend {
    /// A backend that serves a generated tree at each of `roots`, which
    /// needn't exist.
    pub fn new(options: SyntheticOptions, roots: &[Vec<u8>]) -> Self {
        let roots = roots
            .iter()
            .enumerate()
            .map(|(i, root)| {
                let node = Node {
                    key: mix(options.seed ^ mix(i as u64)),
                    number: FIRST_INODE + i as u64 * options.entries,
                    budget: options.entries - 1,
                    depth: 0,
                    uid: 0,
                };
                (root.clone(), node)
            })
            .collect();
        Self { options, roots }
    }

    /// The directory at `path`, following its components down from a root.
    fn find(&self, path: &[u8]) -> io::Result<Node> {
        let &(ref root, mut node) = self
            .roots
            .iter()
            .filter(|(root, _)| {
                path.strip_prefix(root.as_slice())
                    .is_some_and(|rest| rest.is_empty() || rest[0] == b'/' || root.ends_with(b"/"))
            })
            .max_by_key(|(root, _)| root.len())
            .ok_or_else(|| io::Error::from(Errno::NOENT))?;
        let mut name = Vec::new();
        for component in path[root.len()..].split(|&b| b == b'/') {
            if component.is_empty() {
                continue;
            }
            let shape = self.shape(&node);
            let index = parse_index(component).ok_or_else(|| io::Error::from(Errno::NOENT))?;
            if index >= shape.children() {
                return Err(Errno::NOENT.into());
            }
            if index >= shape.subdirs {
                return Err(Errno::NOTDIR.into());
            }
            let (number, budget) = self.subdirs(&node, shape)[index as usize];
            let child = self.child_dir(&node, index, number, budget);
            entry_name(&mut name, child.key, index, true);
            if name != component {
                return Err(Errno::NOENT.into());
            }
            node = child;
        }
        Ok(node)
    }

    fn shape(&self, node: &Node) -> Shape {
        let o = &self.options;
        if node.budget <= 2 * o.files {
            return Shape {
                subdirs: 0,
                files: node.budget,
            };
        }
        let r = mix(node.key ^ SHAPE);
        let files = if o.huge > 0 && node.budget >= 2 * o.huge && r.is_multiple_of(4) {
            o.huge
        } else if (r >> 2).is_multiple_of(32) {
            // A big directory now and then.
            (r >> 8) % (32 * o.files + 1)
        } else {
            (r >> 8) % (2 * o.files + 1)
        }
        .min(node.budget);
        let rest = node.budget - files;
        // No more subdirectories than leaves them typical sizes.
        let subdirs = (2 + (r >> 40) % (2 * o.fanout - 3))
            .min(rest / o.files.max(1))
            .clamp(rest.min(1), rest);
        Shape { subdirs, files }
    }

    /// Each subdirectory's number and the entries below it, which are
    /// numbered after it, before the next subdirectory.
    fn subdirs(&self, node: &Node, shape: Shape) -> Vec<(u64, u64)> {
        let mut number = node.number + 1;
        self.budgets(node, shape)
            .into_iter()
            .map(|budget| {
                let subdir = (number, budget);
                number += budget + 1;
                subdir
            })
            .collect()
    }

    /// Splits the entries below a directory's subdirectories between them,
    /// unevenly.
    fn budgets(&self, node: &Node, shape: Shape) -> Vec<u64> {
        let spare = node.budget - shape.children();
        let base = mix(node.key ^ WEIGHT);
        let weights: Vec<u64> = (0..shape.subdirs)
            .map(|i| 1 << (mix(base.wrapping_add(i)) % 8))
            .collect();
        let total: u64 = weights.iter().sum();
        let mut budgets: Vec<u64> = weights
            .iter()
            .map(|&w| (u128::from(spare) * u128::from(w) / u128::from(total)) as u64)
            .collect();
        let left = spare - budgets.iter().sum::<u64>();
        for budget in budgets.iter_mut().take(left as usize) {
            *budget += 1;
        }
        budgets
    }

    fn child_dir(&self, parent: &Node, index: u64, number: u64, budget: u64) -> Node {
        let key = child_key(parent.key, index);
        Node {
            key,
            number,
            budget,
            depth: parent.depth + 1,
            // Directories in the top two levels each belong to one user, like
            // home or project directories.
            uid: if parent.depth <= 1 {
                1000 + (mix(key) % 500) as u32
            } else {
                parent.uid
            },
        }
    }

    fn open(&self, node: Node) -> SyntheticDir {
        let shape = self.shape(&node);
        let subdirs = self.subdirs(&node, shape);
        let first_file = node.number + 1 + subdirs.iter().map(|(_, b)| b + 1).sum::<u64>();
        SyntheticDir {
            node,
            shape,
            subdirs,
            first_file,
            next: AtomicU64::new(0),
        }
    }

    fn dir_stat_of(&self, node: &Node, shape: Shape) -> Stat {
        let r = mix(node.key ^ KIND);
        let mode = match r % 20 {
            0..=13 => 0o040_755,
            14..=17 => 0o040_750,
            18 => 0o042_770,
            _ => 0o041_777,
        };
        // Directory sizes grow in blocks with their entries, as on ext4.
        let size = (shape.children() * 32).div_ceil(4096).max(1) * 4096;
        Stat {
            dev: DEV,
            ino: node.number,
            mode,
            nlink: 2 + shape.subdirs,
            uid: node.uid,
            gid: gid(node.uid),
            rdev: 0,
            size: size as i64,
            blocks: size / 512,
            ..times(era(node), node.key)
        }
    }

    fn file_stat(&self, parent: &Node, key: u64, number: u64) -> Stat {
        let r = mix(key ^ KIND);
        let (mode, nlink) = match r % 100_000 {
            0 => (0o010_644, 1),
            1 => (0o140_755, 1),
            2..=500 => (0o120_777, 1),
            n => {
                let perm = match n % 100 {
                    0..=59 => 0o644,
                    60..=74 => 0o664,
                    75..=89 => 0o755,
                    90..=97 => 0o600,
                    _ => 0o444,
                };
                (
                    0o100_000 | perm,
                    if (r >> 20).is_multiple_of(256) { 2 } else { 1 },
                )
            }
        };
        let s = mix(key ^ SIZE);
        let (size, blocks) = match mode & 0o170_000 {
            0o120_000 => (20 + s % 60, 0),
            0o100_000 if s.is_multiple_of(64) => (0, 0),
            0o100_000 => {
                // Log-uniform from 1 byte to 2 GiB.
                let bits = (s >> 6) % 31;
                let size = (1 << bits) + (s >> 12) % (1 << bits);
                let blocks = size.div_ceil(4096) * 8;
                // One file in 256 is sparse.
                (
                    size,
                    if (s >> 50).is_multiple_of(256) {
                        blocks / 4
                    } else {
                        blocks
                    },
                )
            }
            _ => (0, 0),
        };
        let uid = if (r >> 30).is_multiple_of(50) {
            1000 + ((r >> 36) % 500) as u32
        } else {
            parent.uid
        };
        Stat {
            dev: DEV,
            ino: number,
            mode,
            nlink,
            uid,
            gid: gid(uid),
            rdev: 0,
            size: size as i64,
            blocks,
            ..times(era(parent), key)
        }
    }
}

impl Backend for SyntheticBackend {
    type Dir = SyntheticDir;
    type Scratch = Vec<u8>;

    fn new_scratch(&self) -> Vec<u8> {
        Vec::with_capacity(128)
    }

    fn stat_root(&self, path: &[u8]) -> io::Result<Stat> {
        let node = self.find(path)?;
        Ok(self.dir_stat_of(&node, self.shape(&node)))
    }

    fn open_dir(&self, path: &[u8], _follow: bool) -> io::Result<SyntheticDir> {
        Ok(self.open(self.find(path)?))
    }

    fn dir_stat(&self, dir: &SyntheticDir) -> io::Result<Stat> {
        Ok(self.dir_stat_of(&dir.node, dir.shape))
    }

    fn read_names(
        &self,
        dir: &SyntheticDir,
        scratch: &mut Vec<u8>,
        names: &mut NameBatch,
    ) -> io::Result<bool> {
        let total = dir.shape.children();
        let start = dir.next.load(Ordering::Relaxed);
        let end = total.min(start + NAMES_PER_READ);
        for index in start..end {
            let is_dir = index < dir.shape.subdirs;
            entry_name(scratch, child_key(dir.node.key, index), index, is_dir);
            names.push(scratch);
        }
        dir.next.store(end, Ordering::Relaxed);
        Ok(end < total)
    }

    fn stat_at(&self, dir: &SyntheticDir, name: &[u8]) -> io::Result<Stat> {
        let index = parse_index(name).ok_or_else(|| io::Error::from(Errno::NOENT))?;
        if index >= dir.shape.children() {
            return Err(Errno::NOENT.into());
        }
        let is_dir = index < dir.shape.subdirs;
        let key = child_key(dir.node.key, index);
        let mut expected = [0u8; 128];
        let len = entry_name_into(&mut expected, key, index, is_dir);
        if expected[..len] != *name {
            return Err(Errno::NOENT.into());
        }
        if is_dir {
            let (number, budget) = dir.subdirs[index as usize];
            let child = self.child_dir(&dir.node, index, number, budget);
            Ok(self.dir_stat_of(&child, self.shape(&child)))
        } else {
            let number = dir.first_file + index - dir.shape.subdirs;
            Ok(self.file_stat(&dir.node, key, number))
        }
    }
}

/// SplitMix64's finalizer: a fast, well-mixed 64-bit hash.
const fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

const fn child_key(parent: u64, index: u64) -> u64 {
    mix(parent ^ mix(index.wrapping_add(0x9e37_79b9_7f4a_7c15)))
}

fn gid(uid: u32) -> u32 {
    if uid == 0 { 0 } else { 5000 + uid % 64 }
}

/// When a directory's contents were written: some time from 2015 to 2025.
fn era(node: &Node) -> i64 {
    EPOCH + (mix(node.key ^ TIME) % SPAN) as i64
}

/// Times for an entry: modified within 60 days after `era`, accessed and
/// changed after that, born before it.
fn times(era: i64, key: u64) -> Stat {
    let r = mix(key ^ TIME);
    let mtime = era + (r % (60 * 86_400)) as i64;
    let nsec = ((r >> 34) % 1_000_000_000) as i64;
    let after = mix(r);
    Stat {
        mtime: Timestamp::new(mtime, nsec),
        atime: Timestamp::new(mtime + (after % (90 * 86_400)) as i64, nsec),
        ctime: Timestamp::new(mtime + ((after >> 32) % 3_600) as i64, nsec),
        btime: Some(Timestamp::new(
            mtime - ((after >> 16) % (30 * 86_400)) as i64,
            nsec,
        )),
        ..Stat::default()
    }
}

/// An entry's name: its index among its siblings, an underscore, then words
/// chosen by its hash, such as `17_results_v2.csv`.
fn entry_name(out: &mut Vec<u8>, key: u64, index: u64, is_dir: bool) {
    let mut buf = [0u8; 128];
    let len = entry_name_into(&mut buf, key, index, is_dir);
    out.clear();
    out.extend_from_slice(&buf[..len]);
}

fn entry_name_into(buf: &mut [u8; 128], key: u64, index: u64, is_dir: bool) -> usize {
    let r = mix(key ^ NAME);
    let mut len = 0;
    let mut put = |bytes: &[u8]| {
        buf[len..len + bytes.len()].copy_from_slice(bytes);
        len += bytes.len();
    };
    let mut digits = [0u8; 20];
    let mut i = digits.len();
    let mut n = index;
    loop {
        i -= 1;
        digits[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    put(&digits[i..]);
    put(b"_");
    put(WORDS[(r % 16) as usize]);
    put(SUFFIXES[((r >> 4) % 8) as usize]);
    if r >> 54 == 0 {
        put(ODD[((r >> 8) % ODD.len() as u64) as usize]);
    }
    if !is_dir {
        put(EXTENSIONS[((r >> 12) % 16) as usize]);
    }
    len
}

/// The sibling index at the start of a generated name.
fn parse_index(name: &[u8]) -> Option<u64> {
    let digits = name.iter().take_while(|b| b.is_ascii_digit()).count();
    if digits == 0 || name.get(digits) != Some(&b'_') {
        return None;
    }
    name[..digits].iter().try_fold(0u64, |n, &b| {
        n.checked_mul(10)?.checked_add(u64::from(b - b'0'))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::EntryKind;

    /// Counts every entry by listing and statting, like the walker does.
    fn walk(backend: &SyntheticBackend, root: &[u8]) -> (u64, u32, Vec<Stat>) {
        let mut scratch = backend.new_scratch();
        let mut entries = 1;
        let mut max_depth = 0;
        let mut stats = Vec::new();
        let mut pending = vec![(root.to_vec(), 0)];
        while let Some((path, depth)) = pending.pop() {
            max_depth = max_depth.max(depth);
            let dir = backend.open_dir(&path, false).unwrap();
            let mut names = NameBatch::default();
            while backend.read_names(&dir, &mut scratch, &mut names).unwrap() {}
            for name in names.iter() {
                let stat = backend.stat_at(&dir, name).unwrap();
                entries += 1;
                if stat.kind() == EntryKind::Dir {
                    let mut child = path.clone();
                    child.push(b'/');
                    child.extend_from_slice(name);
                    pending.push((child, depth + 1));
                }
                stats.push(stat);
            }
        }
        (entries, max_depth, stats)
    }

    #[test]
    fn parses_specs() {
        assert_eq!(
            SyntheticOptions::parse("100M").unwrap(),
            SyntheticOptions {
                entries: 100_000_000,
                ..SyntheticOptions::default()
            }
        );
        let options =
            SyntheticOptions::parse("entries=2G, files=10,fanout=4,huge=0,seed=7").unwrap();
        assert_eq!(
            options.to_string(),
            "entries=2000000000,files=10,fanout=4,huge=0,seed=7"
        );
        assert_eq!(SyntheticOptions::parse(&options.to_string()), Ok(options));
        assert_eq!(
            SyntheticOptions::parse("1_500k").unwrap().entries,
            1_500_000
        );
        assert!(SyntheticOptions::parse("0").is_err());
        assert!(SyntheticOptions::parse("fanout=1").is_err());
        assert!(SyntheticOptions::parse("depth=3").is_err());
        assert!(SyntheticOptions::parse("10X").is_err());
        assert!(SyntheticOptions::parse("99999999999999G").is_err());
    }

    #[test]
    fn trees_have_exactly_the_requested_entries() {
        for spec in [
            "1",
            "2",
            "65",
            "1000",
            "entries=50000,files=4,fanout=2",
            "123456,huge=10k",
        ] {
            let options = SyntheticOptions::parse(spec).unwrap();
            let backend = SyntheticBackend::new(options, &[b"/synthetic".to_vec()]);
            let (entries, _, _) = walk(&backend, b"/synthetic");
            assert_eq!(entries, options.entries, "{spec}");
        }
    }

    #[test]
    fn trees_are_deterministic_and_seeded() {
        let roots = [b"/a".to_vec(), b"/b".to_vec()];
        let one = SyntheticBackend::new(SyntheticOptions::parse("20000").unwrap(), &roots);
        let two = SyntheticBackend::new(SyntheticOptions::parse("20000").unwrap(), &roots);
        let seeded =
            SyntheticBackend::new(SyntheticOptions::parse("20000,seed=1").unwrap(), &roots);
        assert_eq!(walk(&one, b"/a").2, walk(&two, b"/a").2);
        assert_ne!(walk(&one, b"/a").2, walk(&one, b"/b").2);
        assert_ne!(walk(&one, b"/a").2, walk(&seeded, b"/a").2);
    }

    #[test]
    fn trees_vary_like_real_ones() {
        let options = SyntheticOptions::parse("300000,huge=20k").unwrap();
        let backend = SyntheticBackend::new(options, &[b"/".to_vec()]);
        let (entries, max_depth, stats) = walk(&backend, b"/");
        assert_eq!(entries, 300_000);
        assert!((4..40).contains(&max_depth), "depth {max_depth}");
        let count = |kind| stats.iter().filter(|s| s.kind() == kind).count();
        assert!(count(EntryKind::Dir) > 1000);
        assert!(count(EntryKind::Symlink) > 500);
        assert!(
            stats
                .iter()
                .any(|s| s.nlink == 2 && s.kind() == EntryKind::File)
        );
        assert!(stats.iter().any(|s| s.size > 1 << 30));
        let owners: std::collections::HashSet<u32> = stats.iter().map(|s| s.uid).collect();
        assert!(owners.len() > 50);
        let mut inodes: Vec<u64> = stats.iter().map(|s| s.ino).collect();
        inodes.push(FIRST_INODE);
        inodes.sort_unstable();
        assert_eq!(
            inodes,
            (FIRST_INODE..FIRST_INODE + entries).collect::<Vec<_>>()
        );
        for s in &stats {
            assert!(s.atime >= s.mtime && s.ctime >= s.mtime && s.btime.unwrap() <= s.mtime);
        }
    }

    #[test]
    fn names_include_odd_bytes_and_resolve() {
        let backend = SyntheticBackend::new(
            SyntheticOptions::parse("200000").unwrap(),
            &[b"/s".to_vec()],
        );
        let mut scratch = backend.new_scratch();
        let mut odd = 0;
        let mut pending = vec![b"/s".to_vec()];
        while let Some(path) = pending.pop() {
            let dir = backend.open_dir(&path, false).unwrap();
            let mut names = NameBatch::default();
            while backend.read_names(&dir, &mut scratch, &mut names).unwrap() {}
            for name in names.iter() {
                assert!(!name.contains(&b'/') && !name.contains(&0));
                if std::str::from_utf8(name).is_err() || name.iter().any(|b| b.is_ascii_control()) {
                    odd += 1;
                }
                if backend.stat_at(&dir, name).unwrap().kind() == EntryKind::Dir {
                    pending.push([&path[..], b"/", name].concat());
                }
            }
        }
        assert!(odd > 20, "{odd} odd names");
    }

    #[test]
    fn rejects_paths_outside_the_tree() {
        let backend = SyntheticBackend::new(
            SyntheticOptions::parse("100000").unwrap(),
            &[b"/s".to_vec()],
        );
        let dir = backend.open_dir(b"/s", false).unwrap();
        let mut names = NameBatch::default();
        while backend
            .read_names(&dir, &mut backend.new_scratch(), &mut names)
            .unwrap()
        {}
        let named = |kind| {
            names
                .iter()
                .find(|name| backend.stat_at(&dir, name).unwrap().kind() == kind)
                .unwrap()
                .to_vec()
        };
        let (subdir, file) = (named(EntryKind::Dir), named(EntryKind::File));
        let errno = |path: &[u8]| backend.open_dir(path, false).unwrap_err().raw_os_error();
        assert!(
            backend
                .open_dir(&[b"/s/", &subdir[..]].concat(), false)
                .is_ok()
        );
        assert_eq!(errno(b"/elsewhere"), Some(Errno::NOENT.raw_os_error()));
        assert_eq!(errno(b"/sx"), Some(Errno::NOENT.raw_os_error()));
        assert_eq!(errno(b"/s/0_nope"), Some(Errno::NOENT.raw_os_error()));
        assert_eq!(errno(b"/s/999999_data"), Some(Errno::NOENT.raw_os_error()));
        assert_eq!(
            errno(&[b"/s/", &file[..]].concat()),
            Some(Errno::NOTDIR.raw_os_error())
        );
        assert!(backend.stat_at(&dir, b"00_data").is_err());
        assert!(backend.stat_at(&dir, b"x").is_err());
    }
}
