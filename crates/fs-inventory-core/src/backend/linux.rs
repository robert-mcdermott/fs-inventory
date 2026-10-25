// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Linux backend: `getdents64` into a large reusable buffer and `statx`
//! relative to the open directory, falling back to `fstatat` on kernels
//! without `statx` (before 4.11).

use std::io;

use rustix::fd::{AsFd as _, OwnedFd};
use rustix::fs::{AtFlags, CWD, RawDir, Statx, StatxFlags, StatxTimestamp};
use rustix::io::Errno;

use super::{Backend, BackendOptions, DirOpener, NameBatch};
use crate::model::{Fields, Stat, Timestamp};

/// The native backend for Linux.
#[derive(Debug)]
pub struct NativeBackend {
    options: BackendOptions,
    opener: DirOpener,
    /// The statx fields requested for every entry.
    mask: StatxFlags,
}

impl NativeBackend {
    /// Creates a backend with the given options.
    pub fn new(options: BackendOptions) -> Self {
        let mut mask = StatxFlags::TYPE
            | StatxFlags::MODE
            | StatxFlags::NLINK
            | StatxFlags::UID
            | StatxFlags::GID
            | StatxFlags::INO;
        for (field, flag) in [
            (Fields::SIZE, StatxFlags::SIZE),
            (Fields::BLOCKS, StatxFlags::BLOCKS),
            (Fields::ATIME, StatxFlags::ATIME),
            (Fields::MTIME, StatxFlags::MTIME),
            (Fields::CTIME, StatxFlags::CTIME),
            (Fields::BTIME, StatxFlags::BTIME),
        ] {
            if options.fields.contains(field) {
                mask |= flag;
            }
        }
        Self {
            options,
            opener: DirOpener::new(),
            mask,
        }
    }

    fn statx(&self, dirfd: impl rustix::fd::AsFd, path: &[u8], flags: AtFlags) -> io::Result<Stat> {
        let mut flags = flags | AtFlags::NO_AUTOMOUNT;
        if self.options.statx_dont_sync {
            flags |= AtFlags::STATX_DONT_SYNC;
        }
        let mask = self.mask;
        match rustix::fs::statx(dirfd.as_fd(), path, flags, mask) {
            Ok(x) => Ok(from_statx(&x)),
            Err(Errno::NOSYS) => {
                let flags = flags & AtFlags::SYMLINK_NOFOLLOW;
                Ok(from_stat(&rustix::fs::statat(dirfd, path, flags)?))
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// An open directory.
#[derive(Debug)]
pub struct LinuxDir {
    fd: OwnedFd,
}

impl Backend for NativeBackend {
    type Dir = LinuxDir;
    /// An empty vector whose spare capacity is the getdents64 buffer.
    type Scratch = Vec<u8>;

    fn new_scratch(&self) -> Self::Scratch {
        Vec::with_capacity(self.options.readdir_buffer.max(64 * 1024))
    }

    fn stat_root(&self, path: &[u8]) -> io::Result<Stat> {
        self.statx(CWD, path, AtFlags::empty())
    }

    fn open_dir(&self, path: &[u8], follow: bool) -> io::Result<LinuxDir> {
        Ok(LinuxDir {
            fd: self.opener.open(path, follow)?,
        })
    }

    fn dir_stat(&self, dir: &LinuxDir) -> io::Result<Stat> {
        Ok(from_stat(&rustix::fs::fstat(&dir.fd)?))
    }

    fn read_names(
        &self,
        dir: &mut LinuxDir,
        scratch: &mut Self::Scratch,
        names: &mut NameBatch,
    ) -> io::Result<bool> {
        // A new RawDir starts with an empty buffer, so it issues exactly one
        // getdents64 call; stop when that buffer is drained.
        let mut raw = RawDir::new(dir.fd.as_fd(), scratch.spare_capacity_mut());
        loop {
            match raw.next() {
                None => return Ok(false),
                Some(Err(e)) => return Err(e.into()),
                Some(Ok(entry)) => {
                    let name = entry.file_name().to_bytes();
                    if name != b"." && name != b".." {
                        names.push(name);
                    }
                }
            }
            if raw.is_buffer_empty() {
                return Ok(true);
            }
        }
    }

    fn stat_at(&self, dir: &LinuxDir, name: &[u8]) -> io::Result<Stat> {
        self.statx(&dir.fd, name, AtFlags::SYMLINK_NOFOLLOW)
    }
}

fn timestamp(t: &StatxTimestamp) -> Timestamp {
    Timestamp::new(t.tv_sec, i64::from(t.tv_nsec))
}

fn from_statx(x: &Statx) -> Stat {
    let has_btime = StatxFlags::from_bits_retain(x.stx_mask).contains(StatxFlags::BTIME);
    Stat {
        dev: rustix::fs::makedev(x.stx_dev_major, x.stx_dev_minor),
        ino: x.stx_ino,
        mode: u32::from(x.stx_mode),
        nlink: u64::from(x.stx_nlink),
        uid: x.stx_uid,
        gid: x.stx_gid,
        rdev: rustix::fs::makedev(x.stx_rdev_major, x.stx_rdev_minor),
        size: i64::try_from(x.stx_size).unwrap_or(i64::MAX),
        blocks: x.stx_blocks,
        atime: timestamp(&x.stx_atime),
        mtime: timestamp(&x.stx_mtime),
        ctime: timestamp(&x.stx_ctime),
        btime: has_btime.then(|| timestamp(&x.stx_btime)),
    }
}

#[allow(
    clippy::unnecessary_cast,
    clippy::cast_lossless,
    reason = "`struct stat` field types differ across architectures"
)]
fn from_stat(s: &rustix::fs::Stat) -> Stat {
    Stat {
        dev: s.st_dev as u64,
        ino: s.st_ino as u64,
        mode: s.st_mode as u32,
        nlink: s.st_nlink as u64,
        uid: s.st_uid as u32,
        gid: s.st_gid as u32,
        rdev: s.st_rdev as u64,
        size: s.st_size as i64,
        blocks: s.st_blocks as u64,
        atime: Timestamp::new(s.st_atime as i64, s.st_atime_nsec as i64),
        mtime: Timestamp::new(s.st_mtime as i64, s.st_mtime_nsec as i64),
        ctime: Timestamp::new(s.st_ctime as i64, s.st_ctime_nsec as i64),
        btime: None,
    }
}
