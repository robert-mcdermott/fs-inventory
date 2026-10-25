// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Platform access to directories and metadata.
//!
//! The walker only talks to the [`Backend`] trait, so tests can wrap the
//! native backend to inject failures and delays.

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

/// Opens a path too long for a single `open` by walking it one component at
/// a time with `openat`. Like `open`, symlinks in leading components are
/// followed; `flags` applies to the final component.
fn open_long_path(path: &[u8], flags: OFlags) -> Result<OwnedFd, Errno> {
    let dir_flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC;
    let (start, rest): (&[u8], &[u8]) = match path.strip_prefix(b"/") {
        Some(rest) => (b"/", rest),
        None => (b".", path),
    };
    let mut fd = rustix::fs::open(start, dir_flags, Mode::empty())?;
    let mut components = rest
        .split(|&b| b == b'/')
        .filter(|c| !c.is_empty())
        .peekable();
    while let Some(component) = components.next() {
        let last = components.peek().is_none();
        let component_flags = if last { flags } else { dir_flags };
        fd = rustix::fs::openat(&fd, component, component_flags, Mode::empty())?;
    }
    Ok(fd)
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
    }
}
