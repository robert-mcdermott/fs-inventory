// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Platform access to directories and metadata.
//!
//! The walker only talks to the [`Backend`] trait, so tests can wrap the
//! native backend to inject failures and delays, and benchmarks can walk a
//! generated tree ([`synthetic`]) instead of the file system.

use std::io;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicBool, Ordering};

use rustix::fd::OwnedFd;
use rustix::fs::{Mode, OFlags};
use rustix::io::Errno;

use crate::model::{Fields, Stat};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(target_os = "linux"))]
mod posix;
pub mod synthetic;

#[cfg(target_os = "linux")]
pub use linux::NativeBackend;
#[cfg(not(target_os = "linux"))]
pub use posix::NativeBackend;

/// Options that affect how directories and metadata are read.
#[derive(Debug, Clone, Copy)]
pub struct BackendOptions {
    /// Accept cached attributes on network file systems instead of asking the
    /// server (Linux `AT_STATX_DONT_SYNC`). Faster, possibly stale.
    pub statx_dont_sync: bool,
    /// Size in bytes of each worker's directory-read buffer.
    pub readdir_buffer: usize,
    /// Optional fields to request. On Linux, fields left out aren't asked
    /// for at all, which on Lustre avoids a round trip to the storage
    /// servers for sizes. Elsewhere, every field is read regardless.
    pub fields: Fields,
}

impl Default for BackendOptions {
    fn default() -> Self {
        Self {
            statx_dont_sync: false,
            readdir_buffer: 256 * 1024,
            fields: Fields::ALL,
        }
    }
}

/// Access to a file system: opening directories, listing them, and reading
/// metadata. Implementations must never modify the file system.
pub trait Backend: Send + Sync {
    /// An open directory. Several workers may read metadata from one open
    /// directory at once, so it must be shareable.
    type Dir: Send + Sync;
    /// Per-worker scratch space, such as a directory-read buffer.
    type Scratch: Send;

    /// Creates scratch space for one worker.
    fn new_scratch(&self) -> Self::Scratch;

    /// Reads metadata for a scan root, following a symlink at the root itself.
    fn stat_root(&self, path: &[u8]) -> io::Result<Stat>;

    /// Opens a directory by full path. A symlink in the final component is
    /// followed only when `follow` is true (used for scan roots).
    fn open_dir(&self, path: &[u8], follow: bool) -> io::Result<Self::Dir>;

    /// Reads metadata for an open directory.
    fn dir_stat(&self, dir: &Self::Dir) -> io::Result<Stat>;

    /// Appends the directory's next entry names to `names`, never `.` or `..`.
    /// Returns `false` once the directory is exhausted.
    fn read_names(
        &self,
        dir: &Self::Dir,
        scratch: &mut Self::Scratch,
        names: &mut NameBatch,
    ) -> io::Result<bool>;

    /// Reads metadata for the entry `name` in `dir` without following symlinks
    /// or triggering automounts.
    fn stat_at(&self, dir: &Self::Dir, name: &[u8]) -> io::Result<Stat>;

    /// Reads the target of the symlink `name` in `dir`, as exact bytes.
    fn read_link_at(&self, dir: &Self::Dir, name: &[u8]) -> io::Result<Vec<u8>>;
}

/// A batch of directory entry names stored in one buffer.
#[derive(Debug, Default, Clone)]
pub struct NameBatch {
    bytes: Vec<u8>,
    ends: Vec<usize>,
}

impl NameBatch {
    /// Removes every name, keeping the allocations.
    pub fn clear(&mut self) {
        self.bytes.clear();
        self.ends.clear();
    }

    /// Appends one name.
    pub fn push(&mut self, name: &[u8]) {
        self.bytes.extend_from_slice(name);
        self.ends.push(self.bytes.len());
    }

    /// Number of names.
    pub fn len(&self) -> usize {
        self.ends.len()
    }

    /// Whether there are no names.
    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    /// The names, in the order they were added.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        let starts = std::iter::once(0).chain(self.ends.iter().copied());
        starts
            .zip(self.ends.iter().copied())
            .map(|(start, end)| &self.bytes[start..end])
    }
}

/// Opens directories, avoiding atime updates where the OS allows it and
/// handling paths longer than `PATH_MAX`.
#[derive(Debug)]
struct DirOpener {
    /// Cleared after the first `EPERM`: `O_NOATIME` needs ownership or
    /// `CAP_FOWNER`.
    #[cfg(target_os = "linux")]
    noatime: AtomicBool,
}

impl DirOpener {
    fn new() -> Self {
        Self {
            #[cfg(target_os = "linux")]
            noatime: AtomicBool::new(true),
        }
    }

    fn open(&self, path: &[u8], follow: bool) -> io::Result<OwnedFd> {
        let mut flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
        if !follow {
            flags |= OFlags::NOFOLLOW;
        }
        #[cfg(target_os = "linux")]
        if self.noatime.load(Ordering::Relaxed) {
            match open_path(path, flags | OFlags::NOATIME) {
                Err(Errno::PERM) => self.noatime.store(false, Ordering::Relaxed),
                result => return result.map_err(io::Error::from),
            }
        }
        open_path(path, flags).map_err(io::Error::from)
    }
}

fn open_path(path: &[u8], flags: OFlags) -> Result<OwnedFd, Errno> {
    match rustix::fs::open(path, flags, Mode::empty()) {
        Err(Errno::NAMETOOLONG) => open_long_path(path, flags),
        result => result,
    }
}

/// The longest path `open` takes, counting the terminating NUL.
#[cfg(target_os = "linux")]
const PATH_MAX: usize = 4096;
#[cfg(not(target_os = "linux"))]
const PATH_MAX: usize = 1024;

/// Opens a path too long for a single `open` in pieces of under `PATH_MAX`
/// bytes, split at `/`, each opened relative to the one before: a few calls
/// even 2,000 levels down, where opening one component at a time would take
/// 2,000. Like `open`, symlinks in leading components are followed; `flags`
/// applies to the final component.
fn open_long_path(path: &[u8], flags: OFlags) -> Result<OwnedFd, Errno> {
    let dir_flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
    let (start, mut rest): (&[u8], &[u8]) = match path.strip_prefix(b"/") {
        Some(rest) => (b"/", rest),
        None => (b".", path),
    };
    let mut fd = rustix::fs::open(start, dir_flags, Mode::empty())?;
    loop {
        // A leading `/` would make the piece absolute.
        while let Some(r) = rest.strip_prefix(b"/") {
            rest = r;
        }
        if rest.is_empty() {
            return Ok(fd);
        }
        if rest.len() < PATH_MAX {
            return rustix::fs::openat(&fd, rest, flags, Mode::empty());
        }
        let cut = rest[..PATH_MAX - 1]
            .iter()
            .rposition(|&b| b == b'/')
            .ok_or(Errno::NAMETOOLONG)?;
        fd = rustix::fs::openat(&fd, &rest[..cut], dir_flags, Mode::empty())?;
        rest = &rest[cut..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_batch_round_trips() {
        let mut batch = NameBatch::default();
        assert!(batch.is_empty());
        batch.push(b"a");
        batch.push(b"");
        batch.push(b"caf\xe9");
        assert_eq!(batch.len(), 3);
        let names: Vec<&[u8]> = batch.iter().collect();
        assert_eq!(names, [&b"a"[..], b"", b"caf\xe9"]);
        batch.clear();
        assert_eq!(batch.iter().count(), 0);
    }

    #[test]
    fn opens_paths_longer_than_path_max() {
        use std::os::unix::ffi::OsStrExt as _;

        let tmp = tempfile::tempdir().unwrap();
        let component = "d".repeat(200);
        // Build 30 levels of 200-byte names (about 6,000 bytes, past PATH_MAX)
        // relative to open directories, since full paths that long fail.
        let mut fd = rustix::fs::open(
            tmp.path(),
            OFlags::RDONLY | OFlags::DIRECTORY,
            Mode::empty(),
        )
        .unwrap();
        let mut path = tmp.path().as_os_str().as_bytes().to_vec();
        for _ in 0..30 {
            rustix::fs::mkdirat(&fd, component.as_str(), Mode::from_raw_mode(0o755)).unwrap();
            fd = rustix::fs::openat(
                &fd,
                component.as_str(),
                OFlags::RDONLY | OFlags::DIRECTORY,
                Mode::empty(),
            )
            .unwrap();
            path.push(b'/');
            path.extend_from_slice(component.as_bytes());
        }
        assert!(path.len() > 4096);
        assert!(rustix::fs::open(&path[..], OFlags::RDONLY, Mode::empty()).is_err());

        let opened = DirOpener::new().open(&path, false).unwrap();
        let expected = rustix::fs::fstat(&fd).unwrap();
        let actual = rustix::fs::fstat(&opened).unwrap();
        assert_eq!(
            (actual.st_dev, actual.st_ino),
            (expected.st_dev, expected.st_ino)
        );
        // Doubled and trailing slashes change nothing.
        let mut slashes = path.clone();
        let at = path[..2000].iter().rposition(|&b| b == b'/').unwrap();
        slashes.splice(at..at, *b"//");
        slashes.push(b'/');
        let opened = DirOpener::new().open(&slashes, false).unwrap();
        assert_eq!(rustix::fs::fstat(&opened).unwrap().st_ino, expected.st_ino);

        // Symlinks are followed on the way, and at the end only if asked.
        rustix::fs::symlinkat(".", &fd, "link").unwrap();
        let mut link = path.clone();
        link.extend_from_slice(b"/link");
        assert!(DirOpener::new().open(&link, false).is_err());
        let opened = DirOpener::new().open(&link, true).unwrap();
        assert_eq!(rustix::fs::fstat(&opened).unwrap().st_ino, expected.st_ino);
        let mut through = link.clone();
        through.extend_from_slice(b"/link/link/");
        let opened = DirOpener::new().open(&through, true).unwrap();
        assert_eq!(rustix::fs::fstat(&opened).unwrap().st_ino, expected.st_ino);
    }
}
