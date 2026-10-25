-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 Robert McDermott
--
-- DuckDB macros for reading fs-inventory scans written as CSV or JSON Lines.
-- Column types are given explicitly so DuckDB never guesses them from a
-- sample (a column of numeric-looking file names would otherwise be read as
-- numbers). Compressed parts (.zst, .gz) are read transparently.
--
-- Usage, in the DuckDB shell:
--   .read sql/fs_inventory.sql
--   SELECT kind, count(*) FROM fs_entries_csv('/scratch/inventory/scan-2026-10-25') GROUP BY kind;
--
-- Each table macro adds `path_raw`, the exact path bytes when `path` differs
-- from them, so `coalesce(path_raw, encode(path))` is exact for every row.
-- Timestamps assume the default --time-format iso.

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
        'name_flags': 'UINTEGER',
        'kind': 'VARCHAR',
        'depth': 'UINTEGER',
        'dir_id': 'UBIGINT',
        'parent_id': 'UBIGINT',
        'dev': 'UBIGINT',
        'ino': 'UBIGINT',
        'nlink': 'UBIGINT',
        'uid': 'UINTEGER',
        'gid': 'UINTEGER',
        'mode': 'UINTEGER',
        'size': 'BIGINT',
        'alloc_bytes': 'UBIGINT',
        'atime': 'TIMESTAMPTZ',
        'mtime': 'TIMESTAMPTZ',
        'ctime': 'TIMESTAMPTZ',
        'btime': 'TIMESTAMPTZ',
        'rdev': 'UBIGINT',
        'dir_entries': 'UBIGINT',
        'dir_files': 'UBIGINT',
        'dir_subdirs': 'UBIGINT',
        'dir_bytes': 'UBIGINT',
        'dir_alloc_bytes': 'UBIGINT',
        'is_mount_point': 'BOOLEAN',
        'time_flags': 'UTINYINT',
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
        'name_flags': 'UINTEGER',
        'kind': 'VARCHAR',
        'depth': 'UINTEGER',
        'dir_id': 'UBIGINT',
        'parent_id': 'UBIGINT',
        'dev': 'UBIGINT',
        'ino': 'UBIGINT',
        'nlink': 'UBIGINT',
        'uid': 'UINTEGER',
        'gid': 'UINTEGER',
        'mode': 'UINTEGER',
        'size': 'BIGINT',
        'alloc_bytes': 'UBIGINT',
        'atime': 'TIMESTAMPTZ',
        'mtime': 'TIMESTAMPTZ',
        'ctime': 'TIMESTAMPTZ',
        'btime': 'TIMESTAMPTZ',
        'rdev': 'UBIGINT',
        'dir_entries': 'UBIGINT',
        'dir_files': 'UBIGINT',
        'dir_subdirs': 'UBIGINT',
        'dir_bytes': 'UBIGINT',
        'dir_alloc_bytes': 'UBIGINT',
        'is_mount_point': 'BOOLEAN',
        'time_flags': 'UTINYINT',
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
