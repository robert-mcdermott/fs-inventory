// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Portable POSIX backend (macOS, BSD): `readdir` and `fstatat` relative to
//! the open directory.

use std::io;

use rustix::fd::OwnedFd;
use rustix::fs::{AtFlags, CWD, Dir};

use super::{Backend, BackendOptions, DirOpener, NameBatch};
use crate::model::{Stat, Timestamp};

/// Names read per [`Backend::read_names`] call.
const NAMES_PER_READ: usize = 4096;

/// The native backend for platforms other than Linux.
#[derive(Debug)]
pub struct NativeBackend {
    opener: DirOpener,
}

impl NativeBackend {
    /// Creates a backend with the given options.
    pub fn new(_options: BackendOptions) -> Self {
        Self {
            opener: DirOpener::new(),
        }
    }
}

/// An open directory.
pub struct PosixDir {
    fd: OwnedFd,
    entries: Dir,
}

impl Backend for NativeBackend {
    type Dir = PosixDir;
    type Scratch = ();

    fn new_scratch(&self) -> Self::Scratch {}

    fn stat_root(&self, path: &[u8]) -> io::Result<Stat> {
        Ok(from_stat(&rustix::fs::statat(CWD, path, AtFlags::empty())?))
    }

    fn open_dir(&self, path: &[u8], follow: bool) -> io::Result<PosixDir> {
        let fd = self.opener.open(path, follow)?;
        let entries = Dir::read_from(&fd)?;
        Ok(PosixDir { fd, entries })
    }

    fn dir_stat(&self, dir: &PosixDir) -> io::Result<Stat> {
        Ok(from_stat(&rustix::fs::fstat(&dir.fd)?))
    }

    fn read_names(
        &self,
        dir: &mut PosixDir,
        _scratch: &mut (),
        names: &mut NameBatch,
    ) -> io::Result<bool> {
        for _ in 0..NAMES_PER_READ {
            match dir.entries.read() {
                None => return Ok(false),
                Some(Err(e)) => return Err(e.into()),
                Some(Ok(entry)) => {
                    let name = entry.file_name().to_bytes();
                    if name != b"." && name != b".." {
                        names.push(name);
                    }
                }
            }
        }
        Ok(true)
    }

    fn stat_at(&self, dir: &PosixDir, name: &[u8]) -> io::Result<Stat> {
        Ok(from_stat(&rustix::fs::statat(
            &dir.fd,
            name,
            AtFlags::SYMLINK_NOFOLLOW,
        )?))
    }
}

/// Converts `st_dev`, which is a signed 32-bit value on Apple platforms.
#[cfg(target_vendor = "apple")]
fn dev(d: rustix::fs::Dev) -> u64 {
    u64::from(d as u32)
}

#[cfg(not(target_vendor = "apple"))]
#[allow(clippy::unnecessary_cast, reason = "`dev_t` differs across platforms")]
fn dev(d: rustix::fs::Dev) -> u64 {
    d as u64
}

#[cfg(any(target_vendor = "apple", target_os = "freebsd", target_os = "netbsd"))]
#[allow(
    clippy::unnecessary_cast,
    reason = "`struct stat` differs across platforms"
)]
fn btime(s: &rustix::fs::Stat) -> Option<Timestamp> {
    // File systems without birth times report zero or -1 seconds.
    (s.st_birthtime > 0).then(|| Timestamp::new(s.st_birthtime as i64, s.st_birthtime_nsec as i64))
}

#[cfg(not(any(target_vendor = "apple", target_os = "freebsd", target_os = "netbsd")))]
fn btime(_s: &rustix::fs::Stat) -> Option<Timestamp> {
    None
}

#[allow(
    clippy::unnecessary_cast,
    clippy::cast_lossless,
    reason = "`struct stat` field types differ across platforms"
)]
fn from_stat(s: &rustix::fs::Stat) -> Stat {
    Stat {
        dev: dev(s.st_dev),
        ino: s.st_ino as u64,
        mode: s.st_mode as u32,
        nlink: s.st_nlink as u64,
        uid: s.st_uid as u32,
        gid: s.st_gid as u32,
        rdev: dev(s.st_rdev),
        size: s.st_size as i64,
        blocks: s.st_blocks as u64,
        atime: Timestamp::new(s.st_atime as i64, s.st_atime_nsec as i64),
        mtime: Timestamp::new(s.st_mtime as i64, s.st_mtime_nsec as i64),
        ctime: Timestamp::new(s.st_ctime as i64, s.st_ctime_nsec as i64),
        btime: btime(s),
    }
}
