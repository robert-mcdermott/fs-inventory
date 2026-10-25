// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! A first-in, first-out queue of records on disk, for directories waiting
//! to be read when the in-memory queue is over its budget.
//!
//! The file is unlinked as soon as it's created, so it never outlives the
//! process, even after a crash.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

struct Files {
    writer: BufWriter<File>,
    reader: BufReader<File>,
}

/// An unlinked spill file of length-prefixed records.
pub struct Spill {
    files: Mutex<Files>,
    /// Records written and not yet read.
    waiting: AtomicU64,
}

impl Spill {
    /// Creates a spill file in `dir` and unlinks it at once.
    pub fn create(dir: &Path) -> io::Result<Self> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let path = dir.join(format!(
            ".fs-inventory-spill-{}-{nanos}",
            std::process::id()
        ));
        let writer = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let reader = File::open(&path);
        // Unlink before anything else can fail, so the file never lingers.
        fs::remove_file(&path)?;
        Ok(Self {
            files: Mutex::new(Files {
                writer: BufWriter::with_capacity(1 << 20, writer),
                reader: BufReader::with_capacity(1 << 20, reader?),
            }),
            waiting: AtomicU64::new(0),
        })
    }

    /// Records written and not yet read.
    pub fn waiting(&self) -> u64 {
        self.waiting.load(Ordering::Acquire)
    }

    /// Appends one record.
    pub fn push(&self, record: &[u8]) -> io::Result<()> {
        let mut files = self
            .files
            .lock()
            .map_err(|_| io::Error::other("spill file poisoned"))?;
        let len = u32::try_from(record.len()).map_err(io::Error::other)?;
        files.writer.write_all(&len.to_le_bytes())?;
        files.writer.write_all(record)?;
        self.waiting.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Reads the oldest record into `out`. Returns `Ok(false)` when empty.
    pub fn pop(&self, out: &mut Vec<u8>) -> io::Result<bool> {
        let mut files = self
            .files
            .lock()
            .map_err(|_| io::Error::other("spill file poisoned"))?;
        if self.waiting.load(Ordering::Acquire) == 0 {
            return Ok(false);
        }
        files.writer.flush()?;
        let mut len = [0; 4];
        files.reader.read_exact(&mut len)?;
        out.clear();
        out.resize(u32::from_le_bytes(len) as usize, 0);
        files.reader.read_exact(out)?;
        self.waiting.fetch_sub(1, Ordering::AcqRel);
        Ok(true)
    }
}

/// Appends little-endian fields to a record.
pub(crate) struct Encode<'a>(pub &'a mut Vec<u8>);

impl Encode<'_> {
    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn i64(&mut self, v: i64) -> &mut Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn bytes(&mut self, v: &[u8]) -> &mut Self {
        self.u64(v.len() as u64);
        self.0.extend_from_slice(v);
        self
    }
}

/// Reads little-endian fields from a record.
pub(crate) struct Decode<'a>(pub &'a [u8]);

impl Decode<'_> {
    fn take<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let (head, rest) = self
            .0
            .split_first_chunk::<N>()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated spill record"))?;
        self.0 = rest;
        Ok(*head)
    }

    pub fn u64(&mut self) -> io::Result<u64> {
        self.take().map(u64::from_le_bytes)
    }

    pub fn i64(&mut self) -> io::Result<i64> {
        self.take().map(i64::from_le_bytes)
    }

    pub fn u32(&mut self) -> io::Result<u32> {
        self.take().map(u32::from_le_bytes)
    }

    pub fn bytes(&mut self) -> io::Result<Vec<u8>> {
        let len = usize::try_from(self.u64()?).map_err(io::Error::other)?;
        if len > self.0.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated spill record",
            ));
        }
        let (head, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(head.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_round_trip_and_no_file_left() {
        let tmp = tempfile::tempdir().unwrap();
        let spill = Spill::create(tmp.path()).unwrap();
        assert_eq!(
            fs::read_dir(tmp.path()).unwrap().count(),
            0,
            "spill file should be unlinked"
        );
        let mut out = Vec::new();
        assert!(!spill.pop(&mut out).unwrap());
        for i in 0..1000u32 {
            spill
                .push(&i.to_le_bytes().repeat(i as usize % 7 + 1))
                .unwrap();
        }
        assert_eq!(spill.waiting(), 1000);
        for i in 0..500u32 {
            assert!(spill.pop(&mut out).unwrap());
            assert_eq!(out, i.to_le_bytes().repeat(i as usize % 7 + 1));
        }
        // Interleaved pushes keep FIFO order.
        spill.push(b"late").unwrap();
        for _ in 500..1000 {
            assert!(spill.pop(&mut out).unwrap());
        }
        assert!(spill.pop(&mut out).unwrap());
        assert_eq!(out, b"late");
        assert!(!spill.pop(&mut out).unwrap());
    }

    #[test]
    fn encode_decode() {
        let mut record = Vec::new();
        Encode(&mut record).u64(7).i64(-3).u32(9).bytes(b"caf\xe9");
        let mut d = Decode(&record);
        assert_eq!(d.u64().unwrap(), 7);
        assert_eq!(d.i64().unwrap(), -3);
        assert_eq!(d.u32().unwrap(), 9);
        assert_eq!(d.bytes().unwrap(), b"caf\xe9");
        assert!(d.u64().is_err());
    }
}
