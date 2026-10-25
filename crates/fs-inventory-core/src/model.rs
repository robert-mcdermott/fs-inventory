// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! The inventory record model shared by the walker and the output writers.

/// The kind of a file system entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryKind {
    /// Regular file.
    File,
    /// Directory.
    Dir,
    /// Symbolic link (never followed).
    Symlink,
    /// Named pipe.
    Fifo,
    /// Unix domain socket.
    Socket,
    /// Character device.
    CharDevice,
    /// Block device.
    BlockDevice,
    /// Any other type, such as a Solaris door or BSD whiteout.
    Unknown,
}

impl EntryKind {
    /// Every kind, in a stable order.
    pub const ALL: [Self; 8] = [
        Self::File,
        Self::Dir,
        Self::Symlink,
        Self::Fifo,
        Self::Socket,
        Self::CharDevice,
        Self::BlockDevice,
        Self::Unknown,
    ];

    /// Classifies a `st_mode` value. The file type bits are the same on every
    /// supported platform.
    pub const fn from_mode(mode: u32) -> Self {
        match mode & 0o170000 {
            0o100000 => Self::File,
            0o040000 => Self::Dir,
            0o120000 => Self::Symlink,
            0o010000 => Self::Fifo,
            0o140000 => Self::Socket,
            0o020000 => Self::CharDevice,
            0o060000 => Self::BlockDevice,
            _ => Self::Unknown,
        }
    }

    /// The stable name used in output.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Dir => "dir",
            Self::Symlink => "symlink",
            Self::Fifo => "fifo",
            Self::Socket => "socket",
            Self::CharDevice => "char",
            Self::BlockDevice => "block",
            Self::Unknown => "unknown",
        }
    }
}

/// A point in time: seconds and nanoseconds since 1970-01-01 UTC. Seconds
/// may be negative for dates before 1970.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct Timestamp {
    /// Whole seconds since the Unix epoch.
    pub sec: i64,
    /// Nanoseconds within the second, `0..1_000_000_000`.
    pub nsec: u32,
}

impl Timestamp {
    /// Builds a timestamp, normalizing out-of-range nanoseconds (some file
    /// systems return them) into the seconds field.
    pub fn new(sec: i64, nsec: i64) -> Self {
        let sec = sec.saturating_add(nsec.div_euclid(1_000_000_000));
        let nsec = nsec.rem_euclid(1_000_000_000) as u32;
        Self { sec, nsec }
    }
}

/// Metadata for one entry, normalized across platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Stat {
    /// Device of the file system holding the entry.
    pub dev: u64,
    /// Inode number.
    pub ino: u64,
    /// Full `st_mode`: file type and permission bits.
    pub mode: u32,
    /// Number of hard links.
    pub nlink: u64,
    /// Owner user ID.
    pub uid: u32,
    /// Owner group ID.
    pub gid: u32,
    /// Device number, for character and block devices.
    pub rdev: u64,
    /// Logical size in bytes.
    pub size: i64,
    /// Allocated space in 512-byte blocks.
    pub blocks: u64,
    /// Last access time.
    pub atime: Timestamp,
    /// Last modification time.
    pub mtime: Timestamp,
    /// Last status change time (not creation time).
    pub ctime: Timestamp,
    /// Creation (birth) time, when the platform and file system provide it.
    pub btime: Option<Timestamp>,
}

impl Stat {
    /// The entry's kind.
    pub const fn kind(&self) -> EntryKind {
        EntryKind::from_mode(self.mode)
    }

    /// Allocated bytes on disk, which is smaller than `size` for sparse,
    /// compressed, or tiered files.
    pub const fn alloc_bytes(&self) -> u64 {
        self.blocks.saturating_mul(512)
    }
}

/// Totals over a directory's immediate children.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DirTotals {
    /// Every child entry.
    pub entries: u64,
    /// Children that are not directories.
    pub files: u64,
    /// Children that are directories.
    pub subdirs: u64,
    /// Sum of `size` over children that are not directories.
    pub bytes: u64,
    /// Sum of allocated bytes over children that are not directories.
    pub alloc_bytes: u64,
}

impl DirTotals {
    /// Adds one child.
    pub fn add(&mut self, stat: &Stat) {
        self.entries += 1;
        if stat.kind() == EntryKind::Dir {
            self.subdirs += 1;
        } else {
            self.files += 1;
            self.bytes = self.bytes.saturating_add(stat.size.max(0) as u64);
            self.alloc_bytes = self.alloc_bytes.saturating_add(stat.alloc_bytes());
        }
    }
}

/// One output row: a file, directory, or other entry.
#[derive(Debug, Clone, Copy)]
pub struct Entry<'a> {
    /// Full path bytes.
    pub path: &'a [u8],
    /// Path of the containing directory (empty for `/`).
    pub parent_path: &'a [u8],
    /// Final path component (the whole path for a root).
    pub name: &'a [u8],
    /// Depth below the scan root (0 for the root).
    pub depth: u32,
    /// For directories, the crawl-assigned directory ID.
    pub dir_id: Option<u64>,
    /// `dir_id` of the containing directory, or 0 for a root.
    pub parent_id: u64,
    /// Metadata.
    pub stat: &'a Stat,
    /// Whether this directory is on a different device from its parent.
    pub is_mount_point: bool,
    /// For directories that were read, totals over their immediate children.
    pub totals: Option<DirTotals>,
    /// For directories that could not be read, why.
    pub error: Option<&'a ErrorInfo>,
}

impl Entry<'_> {
    /// The entry's kind.
    pub fn kind(&self) -> EntryKind {
        self.stat.kind()
    }
}

/// The operation that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Op {
    /// Opening a directory.
    Open,
    /// Reading directory entries.
    ReadDir,
    /// Reading an entry's metadata.
    Stat,
    /// A directory changed identity between being listed and being opened.
    Verify,
    /// An internal error, such as a caught panic.
    Internal,
}

impl Op {
    /// The stable name used in output.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::ReadDir => "readdir",
            Self::Stat => "stat",
            Self::Verify => "verify",
            Self::Internal => "internal",
        }
    }
}

/// Why an operation failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorInfo {
    /// The operation that failed.
    pub op: Op,
    /// The OS error number, when there is one.
    pub errno: Option<i32>,
    /// A human-readable description.
    pub message: String,
}

impl ErrorInfo {
    /// Describes a failed system call.
    pub fn from_io(op: Op, err: &std::io::Error) -> Self {
        Self {
            op,
            errno: err.raw_os_error(),
            message: err.to_string(),
        }
    }

    /// The symbolic errno name, such as `EACCES`, for common errors.
    pub fn errno_name(&self) -> Option<&'static str> {
        self.errno.and_then(errno_name)
    }
}

/// One row of the errors table.
#[derive(Debug, Clone, Copy)]
pub struct ErrorRecord<'a> {
    /// Path of the entry the operation was on.
    pub path: &'a [u8],
    /// What failed.
    pub info: &'a ErrorInfo,
}

/// Symbolic names for the errno values a crawl commonly sees.
pub fn errno_name(errno: i32) -> Option<&'static str> {
    use rustix::io::Errno;
    let e = Errno::from_raw_os_error(errno);
    Some(match e {
        Errno::ACCESS => "EACCES",
        Errno::PERM => "EPERM",
        Errno::NOENT => "ENOENT",
        Errno::NOTDIR => "ENOTDIR",
        Errno::IO => "EIO",
        Errno::STALE => "ESTALE",
        Errno::LOOP => "ELOOP",
        Errno::NAMETOOLONG => "ENAMETOOLONG",
        Errno::NOMEM => "ENOMEM",
        Errno::MFILE => "EMFILE",
        Errno::NFILE => "ENFILE",
        Errno::OVERFLOW => "EOVERFLOW",
        Errno::INTR => "EINTR",
        Errno::TIMEDOUT => "ETIMEDOUT",
        Errno::NOTCONN => "ENOTCONN",
        Errno::INVAL => "EINVAL",
        Errno::ILSEQ => "EILSEQ",
        Errno::NOSYS => "ENOSYS",
        Errno::BUSY => "EBUSY",
        Errno::NXIO => "ENXIO",
        Errno::NODEV => "ENODEV",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_from_mode() {
        assert_eq!(EntryKind::from_mode(0o100644), EntryKind::File);
        assert_eq!(EntryKind::from_mode(0o040755), EntryKind::Dir);
        assert_eq!(EntryKind::from_mode(0o120777), EntryKind::Symlink);
        assert_eq!(EntryKind::from_mode(0o010644), EntryKind::Fifo);
        assert_eq!(EntryKind::from_mode(0o140755), EntryKind::Socket);
        assert_eq!(EntryKind::from_mode(0o020666), EntryKind::CharDevice);
        assert_eq!(EntryKind::from_mode(0o060660), EntryKind::BlockDevice);
        assert_eq!(EntryKind::from_mode(0o150000), EntryKind::Unknown);
    }

    #[test]
    fn timestamps_normalize_nanoseconds() {
        assert_eq!(Timestamp::new(10, 5), Timestamp { sec: 10, nsec: 5 });
        assert_eq!(
            Timestamp::new(10, 1_500_000_000),
            Timestamp {
                sec: 11,
                nsec: 500_000_000
            }
        );
        assert_eq!(
            Timestamp::new(0, -1),
            Timestamp {
                sec: -1,
                nsec: 999_999_999
            }
        );
        assert_eq!(Timestamp::new(i64::MAX, 2_000_000_000).sec, i64::MAX);
    }

    #[test]
    fn directory_totals() {
        let file = Stat {
            mode: 0o100644,
            size: 100,
            blocks: 8,
            ..Stat::default()
        };
        let dir = Stat {
            mode: 0o040755,
            size: 4096,
            ..Stat::default()
        };
        let corrupt = Stat {
            mode: 0o100644,
            size: -5,
            blocks: u64::MAX,
            ..Stat::default()
        };
        let mut totals = DirTotals::default();
        totals.add(&file);
        totals.add(&dir);
        totals.add(&corrupt);
        assert_eq!(totals.entries, 3);
        assert_eq!(totals.files, 2);
        assert_eq!(totals.subdirs, 1);
        assert_eq!(totals.bytes, 100);
        assert_eq!(totals.alloc_bytes, u64::MAX);
    }
}
