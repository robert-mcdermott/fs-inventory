// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Rolling part files with an atomic commit: each part is written as
//! `NAME.tmp`, fsynced, then renamed, so a finished part is always complete
//! and a crash leaves only `.tmp` files behind.

use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use flate2::write::GzEncoder;
use serde::Serialize;

use crate::parse::Compression;

/// A committed part file.
#[derive(Debug, Clone, Serialize)]
pub struct PartInfo {
    /// Path relative to the scan directory, such as `entries/part-w00-000000.csv`.
    pub file: String,
    pub rows: u64,
    pub bytes: u64,
}

/// Limits that start a new part.
#[derive(Debug, Clone, Copy)]
pub struct RollLimits {
    /// Bytes written to disk (after compression).
    pub max_bytes: u64,
    pub max_rows: Option<u64>,
}

/// Counts bytes on their way to the file.
struct Counting<W> {
    inner: W,
    count: u64,
}

impl<W: Write> Write for Counting<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.count += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

type Sink = BufWriter<Counting<File>>;

/// A compressed or plain stream over a file.
enum Stream {
    Plain(Sink),
    Zstd(zstd::Encoder<'static, Sink>),
    Gzip(GzEncoder<Sink>),
}

impl Stream {
    fn new(file: File, compression: Compression) -> io::Result<Self> {
        let sink = BufWriter::with_capacity(
            1 << 20,
            Counting {
                inner: file,
                count: 0,
            },
        );
        Ok(match compression {
            Compression::Zstd(level) => {
                let mut encoder = zstd::Encoder::new(sink, level)?;
                encoder.include_checksum(true)?;
                Self::Zstd(encoder)
            }
            Compression::Gzip(level) => {
                Self::Gzip(GzEncoder::new(sink, flate2::Compression::new(level)))
            }
            // Snappy and LZ4 are rejected for text output during validation.
            Compression::None | Compression::Snappy | Compression::Lz4 => Self::Plain(sink),
        })
    }

    fn writer(&mut self) -> &mut dyn Write {
        match self {
            Self::Plain(w) => w,
            Self::Zstd(w) => w,
            Self::Gzip(w) => w,
        }
    }

    fn bytes_on_disk(&self) -> u64 {
        match self {
            Self::Plain(w) => w.get_ref().count,
            Self::Zstd(w) => w.get_ref().get_ref().count,
            Self::Gzip(w) => w.get_ref().get_ref().count,
        }
    }

    /// Finishes compression and flushes everything to the file.
    fn finish(self) -> io::Result<(File, u64)> {
        let sink = match self {
            Self::Plain(w) => w,
            Self::Zstd(w) => w.finish()?,
            Self::Gzip(w) => w.finish()?,
        };
        let counting = sink.into_inner().map_err(io::IntoInnerError::into_error)?;
        Ok((counting.inner, counting.count))
    }
}

/// File-name suffix for a compression codec.
pub fn compression_suffix(compression: Compression) -> &'static str {
    match compression {
        Compression::Zstd(_) => ".zst",
        Compression::Gzip(_) => ".gz",
        _ => "",
    }
}

struct OpenPart {
    tmp: PathBuf,
    file_name: String,
    stream: Stream,
    rows: u64,
}

/// Writes one table's rows into a series of part files.
pub struct PartWriter {
    scan_dir: PathBuf,
    table: &'static str,
    prefix: String,
    extension: String,
    header: Vec<u8>,
    compression: Compression,
    limits: RollLimits,
    next_seq: u32,
    open: Option<OpenPart>,
    committed: Vec<PartInfo>,
}

impl PartWriter {
    /// Parts are named `{scan_dir}/{table}/{prefix}-{seq}.{extension}`; each
    /// starts with `header`.
    pub fn new(
        scan_dir: &Path,
        table: &'static str,
        prefix: String,
        extension: String,
        header: Vec<u8>,
        compression: Compression,
        limits: RollLimits,
    ) -> Self {
        Self {
            scan_dir: scan_dir.to_path_buf(),
            table,
            prefix,
            extension,
            header,
            compression,
            limits,
            next_seq: 0,
            open: None,
            committed: Vec::new(),
        }
    }

    /// Appends rows. The caller passes whole rows, and parts only roll
    /// between calls, so a call's rows always land in one part.
    pub fn write(&mut self, bytes: &[u8], rows: u64) -> io::Result<()> {
        let mut part = match self.open.take() {
            Some(part) => part,
            None => self.start_part()?,
        };
        part.stream.writer().write_all(bytes)?;
        part.rows += rows;
        let full = part.stream.bytes_on_disk() >= self.limits.max_bytes
            || self.limits.max_rows.is_some_and(|max| part.rows >= max);
        self.open = Some(part);
        if full {
            self.commit()?;
        }
        Ok(())
    }

    /// Commits the open part, if any, and returns every committed part.
    pub fn finish(mut self) -> io::Result<Vec<PartInfo>> {
        self.commit()?;
        Ok(self.committed)
    }

    fn start_part(&mut self) -> io::Result<OpenPart> {
        let dir = self.scan_dir.join(self.table);
        fs::create_dir_all(&dir)?;
        let file_name = format!("{}-{:06}.{}", self.prefix, self.next_seq, self.extension);
        self.next_seq += 1;
        let tmp = dir.join(format!("{file_name}.tmp"));
        let file = File::create(&tmp)?;
        let mut stream = Stream::new(file, self.compression)?;
        stream.writer().write_all(&self.header)?;
        Ok(OpenPart {
            tmp,
            file_name,
            stream,
            rows: 0,
        })
    }

    fn commit(&mut self) -> io::Result<()> {
        let Some(part) = self.open.take() else {
            return Ok(());
        };
        let (file, bytes) = part.stream.finish()?;
        file.sync_all()?;
        drop(file);
        let dir = self.scan_dir.join(self.table);
        fs::rename(&part.tmp, dir.join(&part.file_name))?;
        self.committed.push(PartInfo {
            file: format!("{}/{}", self.table, part.file_name),
            rows: part.rows,
            bytes,
        });
        Ok(())
    }
}

/// Fsyncs a directory so renames inside it are durable.
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read as _;

    fn writer(dir: &Path, compression: Compression, limits: RollLimits) -> PartWriter {
        PartWriter::new(
            dir,
            "entries",
            "part-w00".into(),
            format!("csv{}", compression_suffix(compression)),
            b"a,b\n".to_vec(),
            compression,
            limits,
        )
    }

    #[test]
    fn rolls_by_rows_and_commits_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        let limits = RollLimits {
            max_bytes: u64::MAX,
            max_rows: Some(2),
        };
        let mut w = writer(tmp.path(), Compression::None, limits);
        w.write(b"1,2\n", 1).unwrap();
        // The open part is still a temporary file.
        assert!(tmp.path().join("entries/part-w00-000000.csv.tmp").exists());
        w.write(b"3,4\n", 1).unwrap();
        w.write(b"5,6\n", 1).unwrap();
        let parts = w.finish().unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].file, "entries/part-w00-000000.csv");
        assert_eq!(parts[0].rows, 2);
        assert_eq!(parts[1].rows, 1);
        let first = fs::read_to_string(tmp.path().join(&parts[0].file)).unwrap();
        assert_eq!(first, "a,b\n1,2\n3,4\n");
        assert_eq!(parts[0].bytes, first.len() as u64);
        let leftovers: Vec<_> = fs::read_dir(tmp.path().join("entries"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty());
    }

    #[test]
    fn compressed_parts_decompress() {
        let tmp = tempfile::tempdir().unwrap();
        let limits = RollLimits {
            max_bytes: u64::MAX,
            max_rows: None,
        };
        for compression in [Compression::Zstd(3), Compression::Gzip(6)] {
            let mut w = writer(tmp.path(), compression, limits);
            w.write(b"1,2\n", 1).unwrap();
            let parts = w.finish().unwrap();
            let file = File::open(tmp.path().join(&parts[0].file)).unwrap();
            let mut text = String::new();
            match compression {
                Compression::Zstd(_) => zstd::Decoder::new(file).unwrap().read_to_string(&mut text),
                _ => flate2::read::GzDecoder::new(file).read_to_string(&mut text),
            }
            .unwrap();
            assert_eq!(text, "a,b\n1,2\n");
            fs::remove_dir_all(tmp.path().join("entries")).unwrap();
        }
    }

    #[test]
    fn no_rows_means_no_parts() {
        let tmp = tempfile::tempdir().unwrap();
        let limits = RollLimits {
            max_bytes: 1,
            max_rows: None,
        };
        let parts = writer(tmp.path(), Compression::None, limits)
            .finish()
            .unwrap();
        assert!(parts.is_empty());
        assert!(!tmp.path().join("entries").exists());
    }
}
