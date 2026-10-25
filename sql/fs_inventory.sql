-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 Robert McDermott
--
-- DuckDB macros for reading fs-inventory scans.
--
-- Usage, in the DuckDB shell:
--   .read sql/fs_inventory.sql
--   SELECT kind, count(*) FROM fs_entries('/scratch/inventory/scan-2026-10-25') GROUP BY kind;
--
-- fs_entries and fs_errors read Parquet scans (the default format), whose
-- files carry their own column types. fs_errors needs at least one error
-- part, which exists only if the scan recorded errors.
--
-- The _csv and _jsonl variants read the text formats. They give column types
-- explicitly, so DuckDB never guesses them from a sample (a column of
-- numeric-looking file names would otherwise be read as numbers), read
-- compressed parts (.zst, .gz) transparently, and add `path_raw` decoded from
-- `path_b64`. The CSV macros need every column; for scans made with
-- --columns, use read_csv with your own column list, or Parquet. Timestamps
-- assume the default --time-format iso.
--
-- In every format, `coalesce(path_raw, encode(path))` is the exact path bytes.

CREATE OR REPLACE MACRO fs_entries(scan_dir) AS TABLE
SELECT * FROM read_parquet(scan_dir || '/entries/*.parquet');

CREATE OR REPLACE MACRO fs_errors(scan_dir) AS TABLE
SELECT * FROM read_parquet(scan_dir || '/errors/*.parquet');

-- Written by `fs-inventory rollup SCAN_DIR`: totals over each directory's
-- subtree, and file ages for directories near the top of the tree.
CREATE OR REPLACE MACRO fs_rollup(scan_dir) AS TABLE
SELECT * FROM read_parquet(scan_dir || '/rollup/*.parquet');

CREATE OR REPLACE MACRO fs_rollup_ages(scan_dir) AS TABLE
SELECT * FROM read_parquet(scan_dir || '/rollup_ages/*.parquet');

CREATE OR REPLACE MACRO fs_entries_csv(scan_dir) AS TABLE
SELECT *, from_base64(path_b64) AS path_raw
FROM read_csv(
    scan_dir || '/entries/*.csv*',
    header = true,
    auto_detect = false,
    delim = ',',
    quote = '"',
    escape = '"',
    columns = {
        'path': 'VARCHAR',
        'parent_path': 'VARCHAR',
        'name': 'VARCHAR',
        'extension': 'VARCHAR',
        'path_b64': 'VARCHAR',
        'name_flags': 'INTEGER',
        'kind': 'VARCHAR',
        'depth': 'INTEGER',
        'dir_id': 'BIGINT',
        'parent_id': 'BIGINT',
        'dev': 'UBIGINT',
        'ino': 'UBIGINT',
        'nlink': 'BIGINT',
        'uid': 'BIGINT',
        'gid': 'BIGINT',
        'mode': 'INTEGER',
        'size': 'BIGINT',
        'alloc_bytes': 'BIGINT',
        'atime': 'TIMESTAMPTZ',
        'mtime': 'TIMESTAMPTZ',
        'ctime': 'TIMESTAMPTZ',
        'btime': 'TIMESTAMPTZ',
        'rdev': 'UBIGINT',
        'dir_entries': 'BIGINT',
        'dir_files': 'BIGINT',
        'dir_subdirs': 'BIGINT',
        'dir_bytes': 'BIGINT',
        'dir_alloc_bytes': 'BIGINT',
        'is_mount_point': 'BOOLEAN',
        'time_flags': 'INTEGER',
        'error': 'VARCHAR'
    }
);

CREATE OR REPLACE MACRO fs_entries_jsonl(scan_dir) AS TABLE
SELECT *, from_base64(path_b64) AS path_raw
FROM read_json(
    scan_dir || '/entries/*.jsonl*',
    format = 'newline_delimited',
    columns = {
        'path': 'VARCHAR',
        'parent_path': 'VARCHAR',
        'name': 'VARCHAR',
        'extension': 'VARCHAR',
        'path_b64': 'VARCHAR',
        'name_flags': 'INTEGER',
        'kind': 'VARCHAR',
        'depth': 'INTEGER',
        'dir_id': 'BIGINT',
        'parent_id': 'BIGINT',
        'dev': 'UBIGINT',
        'ino': 'UBIGINT',
        'nlink': 'BIGINT',
        'uid': 'BIGINT',
        'gid': 'BIGINT',
        'mode': 'INTEGER',
        'size': 'BIGINT',
        'alloc_bytes': 'BIGINT',
        'atime': 'TIMESTAMPTZ',
        'mtime': 'TIMESTAMPTZ',
        'ctime': 'TIMESTAMPTZ',
        'btime': 'TIMESTAMPTZ',
        'rdev': 'UBIGINT',
        'dir_entries': 'BIGINT',
        'dir_files': 'BIGINT',
        'dir_subdirs': 'BIGINT',
        'dir_bytes': 'BIGINT',
        'dir_alloc_bytes': 'BIGINT',
        'is_mount_point': 'BOOLEAN',
        'time_flags': 'INTEGER',
        'error': 'VARCHAR'
    }
);

CREATE OR REPLACE MACRO fs_errors_csv(scan_dir) AS TABLE
SELECT *, from_base64(path_b64) AS path_raw
FROM read_csv(
    scan_dir || '/errors/*.csv*',
    header = true,
    auto_detect = false,
    delim = ',',
    quote = '"',
    escape = '"',
    columns = {
        'path': 'VARCHAR',
        'path_b64': 'VARCHAR',
        'op': 'VARCHAR',
        'errno': 'INTEGER',
        'errno_name': 'VARCHAR',
        'message': 'VARCHAR',
        'time': 'TIMESTAMPTZ'
    }
);

CREATE OR REPLACE MACRO fs_errors_jsonl(scan_dir) AS TABLE
SELECT *, from_base64(path_b64) AS path_raw
FROM read_json(
    scan_dir || '/errors/*.jsonl*',
    format = 'newline_delimited',
    columns = {
        'path': 'VARCHAR',
        'path_b64': 'VARCHAR',
        'op': 'VARCHAR',
        'errno': 'INTEGER',
        'errno_name': 'VARCHAR',
        'message': 'VARCHAR',
        'time': 'TIMESTAMPTZ'
    }
);

-- Decodes `name_flags` into a list of flag names:
--   SELECT path, fs_name_flags(name_flags) FROM fs_entries_csv('...') WHERE name_flags <> 0;
CREATE OR REPLACE MACRO fs_name_flags(flags) AS
list_transform(
    list_filter(
        [
            {'bit': 1, 'name': 'invalid_utf8'},
            {'bit': 2, 'name': 'c0_control'},
            {'bit': 4, 'name': 'c1_control'},
            {'bit': 8, 'name': 'del'},
            {'bit': 16, 'name': 'bidi_control'},
            {'bit': 32, 'name': 'not_nfc'},
            {'bit': 64, 'name': 'leading_trailing_space'},
            {'bit': 128, 'name': 'leading_dash'},
            {'bit': 256, 'name': 'trailing_dot'},
            {'bit': 512, 'name': 'windows_reserved'},
            {'bit': 1024, 'name': 'long_name'},
            {'bit': 2048, 'name': 'long_path'}
        ],
        lambda f: (flags & f.bit) != 0
    ),
    lambda f: f.name
);
