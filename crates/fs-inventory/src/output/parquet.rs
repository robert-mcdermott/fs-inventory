// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Rolling Parquet part files, committed like text parts: written as
//! `NAME.tmp` (the footer last), fsynced, then renamed.

use std::fs::{self, File};
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression as Codec, Encoding, GzipLevel, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

use super::parts::{PartInfo, RollLimits};
use crate::parse::Compression;

/// Default largest in-progress row group, so memory stays bounded however
/// wide the rows are.
pub const MAX_ROW_GROUP_BYTES: usize = 128 << 20;

/// Columns whose values rarely repeat within a row group. Dictionary encoding
/// made them larger (timestamps by about 70%) as well as slower.
const NO_DICTIONARY: [&str; 9] = [
    "path", "name", "path_raw", "ino", "dir_id", "atime", "mtime", "ctime", "btime",
];

/// Columns that compress better with a non-default encoding. Measured on 1.2
/// million real entries (zstd level 3): DELTA_BYTE_ARRAY stores each path as
/// a suffix of the previous one, cutting `path` from 7.9 to 5.0 bytes per
/// entry, and DELTA_BINARY_PACKED cuts `ino` from 1.6 to 1.3. Delta encoding
/// made `name` and the timestamps larger, so they stay plain.
const ENCODINGS: [(&str, Encoding); 2] = [
    ("path", Encoding::DELTA_BYTE_ARRAY),
    ("ino", Encoding::DELTA_BINARY_PACKED),
];

/// Writer settings shared by every part of one table.
#[derive(Debug, Clone)]
pub struct ParquetSettings {
    pub compression: Compression,
    pub row_group_rows: usize,
    /// Largest in-progress row group, in bytes.
    pub max_row_group_bytes: usize,
    /// Key-value metadata stored in every file's footer.
    pub metadata: Vec<(String, String)>,
}

impl ParquetSettings {
    fn properties(&self, schema: &Schema) -> io::Result<WriterProperties> {
        let codec = match self.compression {
            Compression::None => Codec::UNCOMPRESSED,
            Compression::Snappy => Codec::SNAPPY,
            Compression::Lz4 => Codec::LZ4_RAW,
            Compression::Zstd(level) => {
                Codec::ZSTD(ZstdLevel::try_new(level).map_err(io::Error::other)?)
            }
            Compression::Gzip(level) => {
                Codec::GZIP(GzipLevel::try_new(level).map_err(io::Error::other)?)
            }
        };
        let mut builder = WriterProperties::builder()
            .set_created_by(format!(
                "fs-inventory version {}",
                env!("CARGO_PKG_VERSION")
            ))
            .set_compression(codec)
            .set_max_row_group_row_count(Some(self.row_group_rows.max(1)))
            .set_max_row_group_bytes(Some(self.max_row_group_bytes.max(1)))
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_key_value_metadata(Some(
                self.metadata
                    .iter()
                    .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                    .collect(),
            ));
        let present = |name: &str| schema.fields().iter().any(|f| f.name() == name);
        for name in NO_DICTIONARY.into_iter().filter(|n| present(n)) {
            builder = builder.set_column_dictionary_enabled(ColumnPath::from(name), false);
        }
        for (name, encoding) in ENCODINGS.into_iter().filter(|(n, _)| present(n)) {
            builder = builder.set_column_encoding(ColumnPath::from(name), encoding);
        }
        Ok(builder.build())
    }
}

struct OpenPart {
    tmp: PathBuf,
    file_name: String,
    writer: ArrowWriter<BufWriter<File>>,
    rows: u64,
}

/// Writes one table's batches into a series of Parquet part files.
pub struct ParquetPartWriter {
    scan_dir: PathBuf,
    table: &'static str,
    prefix: String,
    schema: Arc<Schema>,
    properties: WriterProperties,
    limits: RollLimits,
    next_seq: u32,
    open: Option<OpenPart>,
    committed: Vec<PartInfo>,
}

impl ParquetPartWriter {
    /// Parts are named `{scan_dir}/{table}/{prefix}-{seq}.parquet`.
    pub fn new(
        scan_dir: &Path,
        table: &'static str,
        prefix: String,
        schema: Arc<Schema>,
        settings: &ParquetSettings,
        limits: RollLimits,
    ) -> io::Result<Self> {
        Ok(Self {
            scan_dir: scan_dir.to_path_buf(),
            table,
            prefix,
            properties: settings.properties(&schema)?,
            schema,
            limits,
            next_seq: 0,
            open: None,
            committed: Vec::new(),
        })
    }

    /// Appends a batch. Parts roll only between batches, so a batch always
    /// lands in one part.
    pub fn write(&mut self, batch: &RecordBatch) -> io::Result<()> {
        let mut part = match self.open.take() {
            Some(part) => part,
            None => self.start_part()?,
        };
        part.writer.write(batch).map_err(io::Error::other)?;
        part.rows += batch.num_rows() as u64;
        let size = (part.writer.bytes_written() + part.writer.in_progress_size()) as u64;
        let full = size >= self.limits.max_bytes
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

    /// Commits the open part, if any, and returns every part committed so
    /// far. The next batch starts a new part.
    pub fn checkpoint(&mut self) -> io::Result<Vec<PartInfo>> {
        self.commit()?;
        Ok(self.committed.clone())
    }

    fn start_part(&mut self) -> io::Result<OpenPart> {
        let dir = self.scan_dir.join(self.table);
        fs::create_dir_all(&dir)?;
        let file_name = format!("{}-{:06}.parquet", self.prefix, self.next_seq);
        self.next_seq += 1;
        let tmp = dir.join(format!("{file_name}.tmp"));
        let file = BufWriter::with_capacity(1 << 20, File::create(&tmp)?);
        let writer = ArrowWriter::try_new(
            file,
            Arc::clone(&self.schema),
            Some(self.properties.clone()),
        )
        .map_err(io::Error::other)?;
        Ok(OpenPart {
            tmp,
            file_name,
            writer,
            rows: 0,
        })
    }

    fn commit(&mut self) -> io::Result<()> {
        let Some(part) = self.open.take() else {
            return Ok(());
        };
        // Writes the remaining row group and the footer.
        let buffered = part.writer.into_inner().map_err(io::Error::other)?;
        let file = buffered
            .into_inner()
            .map_err(io::IntoInnerError::into_error)?;
        file.sync_all()?;
        let bytes = file.metadata()?.len();
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

/// Reads the row count from a Parquet file's footer.
pub fn row_count(path: &Path) -> io::Result<u64> {
    use parquet::file::reader::{FileReader as _, SerializedFileReader};
    let reader = SerializedFileReader::new(File::open(path)?).map_err(io::Error::other)?;
    Ok(u64::try_from(reader.metadata().file_metadata().num_rows()).unwrap_or(0))
}

/// Reads one key from a Parquet file's footer metadata.
pub fn footer_value(path: &Path, key: &str) -> io::Result<Option<String>> {
    use parquet::file::reader::{FileReader as _, SerializedFileReader};
    let reader = SerializedFileReader::new(File::open(path)?).map_err(io::Error::other)?;
    Ok(reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .and_then(|kv| kv.iter().find(|e| e.key == key))
        .and_then(|e| e.value.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};

    fn batch(rows: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("size", DataType::Int64, false),
        ]));
        let paths: Vec<String> = (0..rows).map(|i| format!("/data/file-{i}")).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(paths)),
                Arc::new(Int64Array::from_iter_values(0..rows)),
            ],
        )
        .unwrap()
    }

    fn settings() -> ParquetSettings {
        ParquetSettings {
            compression: Compression::Zstd(3),
            row_group_rows: 1000,
            max_row_group_bytes: MAX_ROW_GROUP_BYTES,
            metadata: vec![("fs_inventory.schema_version".into(), "1".into())],
        }
    }

    #[test]
    fn rolls_commits_and_records_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let limits = RollLimits {
            max_bytes: u64::MAX,
            max_rows: Some(2500),
        };
        let mut w = ParquetPartWriter::new(
            tmp.path(),
            "entries",
            "part-w00".into(),
            batch(1).schema(),
            &settings(),
            limits,
        )
        .unwrap();
        w.write(&batch(1000)).unwrap();
        // The open part is still a temporary file.
        assert!(
            tmp.path()
                .join("entries/part-w00-000000.parquet.tmp")
                .exists()
        );
        w.write(&batch(1000)).unwrap();
        w.write(&batch(1000)).unwrap();
        w.write(&batch(10)).unwrap();
        let parts = w.finish().unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!((parts[0].rows, parts[1].rows), (3000, 10));
        let first = tmp.path().join(&parts[0].file);
        assert_eq!(row_count(&first).unwrap(), 3000);
        assert_eq!(fs::metadata(&first).unwrap().len(), parts[0].bytes);
        assert_eq!(
            footer_value(&first, "fs_inventory.schema_version")
                .unwrap()
                .as_deref(),
            Some("1")
        );
        let leftovers = fs::read_dir(tmp.path().join("entries"))
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".tmp")
            })
            .count();
        assert_eq!(leftovers, 0);
    }

    #[test]
    fn every_codec_writes() {
        for compression in [
            Compression::None,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd(9),
            Compression::Gzip(6),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let settings = ParquetSettings {
                compression,
                ..settings()
            };
            let limits = RollLimits {
                max_bytes: u64::MAX,
                max_rows: None,
            };
            let mut w = ParquetPartWriter::new(
                tmp.path(),
                "entries",
                "p".into(),
                batch(1).schema(),
                &settings,
                limits,
            )
            .unwrap();
            w.write(&batch(100)).unwrap();
            let parts = w.finish().unwrap();
            assert_eq!(
                row_count(&tmp.path().join(&parts[0].file)).unwrap(),
                100,
                "{compression:?}"
            );
        }
    }
}
