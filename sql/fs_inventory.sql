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
-- compressed parts (.zst, .gz) transparently, and add `path_raw` and
-- `symlink_target_raw` decoded from their base64 columns. The CSV macros need every column; for scans made with
-- --columns, use read_csv with your own column list, or Parquet. Timestamps
-- assume the default --time-format iso.
--
-- In every format, `coalesce(path_raw, encode(path))` is the exact path bytes.

CREATE OR REPLACE MACRO fs_entries(scan_dir) AS TABLE
SELECT * FROM read_parquet(scan_dir || '/entries/*.parquet');

CREATE OR REPLACE MACRO fs_errors(scan_dir) AS TABLE
SELECT * FROM read_parquet(scan_dir || '/errors/*.parquet');

-- Written with --resolve-ids: the name of every user and group ID in the
-- scan (null for IDs with no account), and every entry with its owner's
-- names added as user_name and group_name.
CREATE OR REPLACE MACRO fs_users(scan_dir) AS TABLE
SELECT * FROM read_parquet(scan_dir || '/users/*.parquet');

CREATE OR REPLACE MACRO fs_groups(scan_dir) AS TABLE
SELECT * FROM read_parquet(scan_dir || '/groups/*.parquet');

CREATE OR REPLACE MACRO fs_entries_named(scan_dir) AS TABLE
SELECT e.*, u.name AS user_name, g.name AS group_name
FROM read_parquet(scan_dir || '/entries/*.parquet') AS e
LEFT JOIN read_parquet(scan_dir || '/users/*.parquet') AS u USING (uid)
LEFT JOIN read_parquet(scan_dir || '/groups/*.parquet') AS g USING (gid);

-- Written by `fs-inventory rollup SCAN_DIR`: totals over each directory's
-- subtree, and file ages for directories near the top of the tree.
CREATE OR REPLACE MACRO fs_rollup(scan_dir) AS TABLE
SELECT * FROM read_parquet(scan_dir || '/rollup/*.parquet');

CREATE OR REPLACE MACRO fs_rollup_ages(scan_dir) AS TABLE
SELECT * FROM read_parquet(scan_dir || '/rollup_ages/*.parquet');

CREATE OR REPLACE MACRO fs_entries_csv(scan_dir) AS TABLE
SELECT *, from_base64(path_b64) AS path_raw,
    from_base64(symlink_target_b64) AS symlink_target_raw
FROM read_csv(
    scan_dir || '/entries/*.csv*',
    header = true,
    auto_detect = false,
    -- Scans before schema version 3 lack the last two columns.
    null_padding = true,
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
        'error': 'VARCHAR',
        'symlink_target': 'VARCHAR',
        'symlink_target_b64': 'VARCHAR'
    }
);

CREATE OR REPLACE MACRO fs_entries_jsonl(scan_dir) AS TABLE
SELECT *, from_base64(path_b64) AS path_raw,
    from_base64(symlink_target_b64) AS symlink_target_raw
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
        'error': 'VARCHAR',
        'symlink_target': 'VARCHAR',
        'symlink_target_b64': 'VARCHAR'
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

CREATE OR REPLACE MACRO fs_users_csv(scan_dir) AS TABLE
SELECT * FROM read_csv(
    scan_dir || '/users/*.csv*',
    header = true,
    auto_detect = false,
    delim = ',',
    quote = '"',
    escape = '"',
    columns = {
        'uid': 'BIGINT',
        'name': 'VARCHAR'
    }
);

CREATE OR REPLACE MACRO fs_users_jsonl(scan_dir) AS TABLE
SELECT * FROM read_json(
    scan_dir || '/users/*.jsonl*',
    format = 'newline_delimited',
    columns = {
        'uid': 'BIGINT',
        'name': 'VARCHAR'
    }
);

CREATE OR REPLACE MACRO fs_groups_csv(scan_dir) AS TABLE
SELECT * FROM read_csv(
    scan_dir || '/groups/*.csv*',
    header = true,
    auto_detect = false,
    delim = ',',
    quote = '"',
    escape = '"',
    columns = {
        'gid': 'BIGINT',
        'name': 'VARCHAR'
    }
);

CREATE OR REPLACE MACRO fs_groups_jsonl(scan_dir) AS TABLE
SELECT * FROM read_json(
    scan_dir || '/groups/*.jsonl*',
    format = 'newline_delimited',
    columns = {
        'gid': 'BIGINT',
        'name': 'VARCHAR'
    }
);

-- What changed between two Parquet scans of the same roots, matching entries
-- by their exact path bytes. One row per entry that was added, removed, or
-- changed, with `changes` listing the columns that differ and the whole row
-- before (`old`) and after (`new`) as structs. Columns named in `ignore` don't
-- count as changes: by default `atime`, which reading files updates, and
-- `dev`, which can change when a file system is remounted. Pass
-- `ignore := []` for every difference, as an exact copy of the new scan needs.
-- Directory and parent IDs differ between scans and are never compared.
--   SELECT change, changes, path FROM fs_diff('/scans/monday', '/scans/tuesday');
--   COPY (FROM fs_diff('/scans/monday', '/scans/tuesday')) TO 'changes.parquet';
CREATE OR REPLACE MACRO fs_diff(old_scan, new_scan, ignore := ['atime', 'dev']) AS TABLE
WITH
    -- Schema version 2 scans have no symlink target columns.
    old AS (
        FROM read_parquet(old_scan || '/entries/*.parquet')
        UNION ALL BY NAME
        SELECT NULL::VARCHAR AS symlink_target, NULL::BLOB AS symlink_target_raw WHERE false
    ),
    new AS (
        FROM read_parquet(new_scan || '/entries/*.parquet')
        UNION ALL BY NAME
        SELECT NULL::VARCHAR AS symlink_target, NULL::BLOB AS symlink_target_raw WHERE false
    ),
    pairs AS (
        SELECT o, n,
            list_filter([
                CASE WHEN o.kind IS DISTINCT FROM n.kind THEN 'kind' END,
                CASE WHEN o.size IS DISTINCT FROM n.size THEN 'size' END,
                CASE WHEN o.alloc_bytes IS DISTINCT FROM n.alloc_bytes THEN 'alloc_bytes' END,
                CASE WHEN o.mode IS DISTINCT FROM n.mode THEN 'mode' END,
                CASE WHEN o.uid IS DISTINCT FROM n.uid THEN 'uid' END,
                CASE WHEN o.gid IS DISTINCT FROM n.gid THEN 'gid' END,
                CASE WHEN o.nlink IS DISTINCT FROM n.nlink THEN 'nlink' END,
                CASE WHEN o.dev IS DISTINCT FROM n.dev THEN 'dev' END,
                CASE WHEN o.ino IS DISTINCT FROM n.ino THEN 'ino' END,
                CASE WHEN o.rdev IS DISTINCT FROM n.rdev THEN 'rdev' END,
                CASE WHEN o.atime IS DISTINCT FROM n.atime THEN 'atime' END,
                CASE WHEN o.mtime IS DISTINCT FROM n.mtime THEN 'mtime' END,
                CASE WHEN o.ctime IS DISTINCT FROM n.ctime THEN 'ctime' END,
                CASE WHEN o.btime IS DISTINCT FROM n.btime THEN 'btime' END,
                CASE WHEN o.dir_entries IS DISTINCT FROM n.dir_entries THEN 'dir_entries' END,
                CASE WHEN o.dir_files IS DISTINCT FROM n.dir_files THEN 'dir_files' END,
                CASE WHEN o.dir_subdirs IS DISTINCT FROM n.dir_subdirs THEN 'dir_subdirs' END,
                CASE WHEN o.dir_bytes IS DISTINCT FROM n.dir_bytes THEN 'dir_bytes' END,
                CASE WHEN o.dir_alloc_bytes IS DISTINCT FROM n.dir_alloc_bytes THEN 'dir_alloc_bytes' END,
                CASE WHEN o.is_mount_point IS DISTINCT FROM n.is_mount_point THEN 'is_mount_point' END,
                CASE WHEN coalesce(o.symlink_target_raw, encode(o.symlink_target))
                     IS DISTINCT FROM coalesce(n.symlink_target_raw, encode(n.symlink_target))
                     THEN 'symlink_target' END,
                CASE WHEN o.error IS DISTINCT FROM n.error THEN 'error' END
            ], lambda c: c IS NOT NULL AND NOT list_contains(ignore, c)) AS changes
        FROM old AS o
        FULL OUTER JOIN new AS n
            ON coalesce(o.path_raw, encode(o.path)) = coalesce(n.path_raw, encode(n.path))
    )
SELECT
    CASE WHEN o.path IS NULL THEN 'added' WHEN n.path IS NULL THEN 'removed' ELSE 'changed' END
        AS change,
    CASE WHEN o.path IS NULL OR n.path IS NULL THEN [] ELSE changes END AS changes,
    coalesce(n.path, o.path) AS path,
    CASE WHEN n.path IS NULL THEN o.path_raw ELSE n.path_raw END AS path_raw,
    coalesce(n.kind, o.kind) AS kind,
    CASE WHEN o.path IS NULL THEN NULL ELSE o END AS old,
    CASE WHEN n.path IS NULL THEN NULL ELSE n END AS new
FROM pairs
WHERE o.path IS NULL OR n.path IS NULL OR len(changes) > 0;

-- Counts of fs_diff's rows by change and kind, with the net change in the
-- size of everything but directories.
CREATE OR REPLACE MACRO fs_diff_summary(old_scan, new_scan) AS TABLE
SELECT change, kind, count(*) AS entries,
    (sum(coalesce(new.size, 0) - coalesce(old.size, 0)) FILTER (WHERE kind <> 'dir'))::BIGINT
        AS size_change
FROM fs_diff(old_scan, new_scan)
GROUP BY ALL
ORDER BY change, kind;

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
