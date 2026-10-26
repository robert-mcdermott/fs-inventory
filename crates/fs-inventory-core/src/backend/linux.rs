// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Linux backend: `getdents64` into a large reusable buffer and `statx`
//! relative to the open directory, falling back to `fstatat` on kernels
//! without `statx` (before 4.11).

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

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
    /// Set once `statx` fails with `ENOSYS`, so later stats go straight to
    /// `fstatat` instead of failing a system call each time.
    no_statx: AtomicBool,
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
            no_statx: AtomicBool::new(false),
        }
    }

    fn statx(&self, dirfd: impl rustix::fd::AsFd, path: &[u8], flags: AtFlags) -> io::Result<Stat> {
        let flags = flags | AtFlags::NO_AUTOMOUNT;
        if !self.no_statx.load(Ordering::Relaxed) {
            let mut statx_flags = flags;
            if self.options.statx_dont_sync {
                statx_flags |= AtFlags::STATX_DONT_SYNC;
            }
            match rustix::fs::statx(dirfd.as_fd(), path, statx_flags, self.mask) {
                Ok(x) => return Ok(from_statx(&x)),
                Err(Errno::NOSYS) => self.no_statx.store(true, Ordering::Relaxed),
                Err(e) => return Err(e.into()),
            }
        }
        // `fstatat` takes the same flags, except `AT_STATX_DONT_SYNC`.
        Ok(from_stat(&rustix::fs::statat(dirfd, path, flags)?))
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
        dir: &LinuxDir,
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

#[cfg(test)]
mod tests {
    use std::os::unix::ffi::OsStrExt as _;

    use super::*;

    /// The `fstatat` fallback for kernels before 4.11 reads the same
    /// metadata as `statx`, except birth time.
    #[test]
    fn fstatat_fallback_matches_statx() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("file"), b"hello").unwrap();
        std::fs::create_dir(tmp.path().join("dir")).unwrap();
        std::os::unix::fs::symlink("file", tmp.path().join("link")).unwrap();
        let root = tmp.path().as_os_str().as_bytes();

        let statx = NativeBackend::new(BackendOptions::default());
        let fallback = NativeBackend::new(BackendOptions {
            // Valid only for statx; the fallback must leave it out.
            statx_dont_sync: true,
            ..BackendOptions::default()
        });
        fallback.no_statx.store(true, Ordering::Relaxed);

        let without_btime = |stat: Stat| Stat {
            btime: None,
            ..stat
        };
        assert_eq!(
            without_btime(statx.stat_root(root).unwrap()),
            fallback.stat_root(root).unwrap()
        );
        let (a, b) = (
            statx.open_dir(root, false).unwrap(),
            fallback.open_dir(root, false).unwrap(),
        );
        for name in [&b"file"[..], b"dir", b"link"] {
            let stat = fallback.stat_at(&b, name).unwrap();
            assert_eq!(stat.btime, None);
            assert_eq!(
                without_btime(statx.stat_at(&a, name).unwrap()),
                stat,
                "{}",
                String::from_utf8_lossy(name)
            );
        }
        assert_eq!(
            fallback.stat_at(&b, b"missing").unwrap_err().raw_os_error(),
            Some(Errno::NOENT.raw_os_error())
        );
    }
}
