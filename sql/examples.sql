-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 Robert McDermott
--
-- Example queries over an fs-inventory scan. Run in the DuckDB shell after
--   .read sql/fs_inventory.sql
-- and point these at your scan:
SET VARIABLE scan = '/scratch/inventory/scan-2026-10-25';
CREATE OR REPLACE VIEW fs AS SELECT * FROM fs_entries(getvariable('scan'));

-- Timestamps are stored in UTC. DuckDB displays and groups them (year(),
-- date_trunc()) in the session time zone, so a file dated 1970-01-01 UTC
-- counts as 1969 west of Greenwich unless the session uses UTC.
SET TimeZone = 'UTC';

-- Totals by kind.
SELECT kind, count(*) AS entries, sum(size) AS bytes, sum(alloc_bytes) AS allocated
FROM fs GROUP BY kind ORDER BY entries DESC;

-- Who wrote the most data in the last 7 days? ctime, because copies and
-- restores preserve mtime but can't set ctime.
SELECT uid, count(*) AS files, sum(alloc_bytes) AS bytes
FROM fs
WHERE kind = 'file' AND ctime > now() - INTERVAL 7 DAY
GROUP BY uid ORDER BY bytes DESC LIMIT 20;

-- Age profile: files and bytes by year of last modification.
SELECT year(mtime) AS year, count(*) AS files, sum(size) AS bytes
FROM fs WHERE kind = 'file' GROUP BY year ORDER BY year;

-- Age buckets by top-level project directory (the shape of a heat map).
SELECT
    split_part(path, '/', 4) AS project,
    CASE
        WHEN mtime > now() - INTERVAL 30 DAY THEN '< 30 days'
        WHEN mtime > now() - INTERVAL 1 YEAR THEN '< 1 year'
        WHEN mtime > now() - INTERVAL 3 YEAR THEN '< 3 years'
        WHEN mtime > now() - INTERVAL 10 YEAR THEN '< 10 years'
        ELSE '10+ years'
    END AS age,
    sum(size) AS bytes
FROM fs WHERE kind = 'file'
GROUP BY ALL ORDER BY project, age;

-- Top extensions by size.
SELECT extension, count(*) AS files, sum(size) AS bytes
FROM fs WHERE kind = 'file' GROUP BY extension ORDER BY bytes DESC LIMIT 50;

-- Physical usage, counting each hard-linked file once.
SELECT sum(alloc_bytes) AS bytes
FROM (SELECT DISTINCT ON (dev, ino) alloc_bytes FROM fs WHERE kind = 'file');

-- Directories with more than a million immediate children.
SELECT path, dir_entries FROM fs
WHERE kind = 'dir' AND dir_entries > 1000000 ORDER BY dir_entries DESC;

-- Sparse, compressed, or tiered files: much less allocated than their size.
SELECT path, size, alloc_bytes FROM fs
WHERE kind = 'file' AND size > 1e9 AND alloc_bytes < size / 10
ORDER BY size DESC LIMIT 100;

-- Names that will break a migration to SMB, Windows, or object storage,
-- with their exact bytes.
SELECT path, fs_name_flags(name_flags) AS problems, coalesce(path_raw, encode(path)) AS exact_bytes
FROM fs WHERE name_flags <> 0;

-- Directories that couldn't be read.
SELECT path, error FROM fs WHERE error IS NOT NULL;

-- Several scans side by side, such as scans/2026-10-01/, scans/2026-11-01/:
-- SELECT parse_path(filename)[2] AS scan, count(*), sum(size)
-- FROM read_parquet('scans/*/entries/*.parquet', filename = true) GROUP BY scan;

-- After `fs-inventory rollup SCAN_DIR`:

-- The largest directories two levels down, counting everything below them.
-- (Sort on the numeric column, not the formatted text.)
SELECT path, files, format_bytes(bytes) AS size, incomplete_dirs
FROM fs_rollup(getvariable('scan'))
WHERE depth = 2 ORDER BY bytes DESC LIMIT 20;

-- Directories whose newest change is more than three years old: archive candidates.
SELECT path, files, format_bytes(bytes) AS size, newest_ctime
FROM fs_rollup(getvariable('scan'))
WHERE newest_ctime < now() - INTERVAL 3 YEAR AND bytes > 1e9
ORDER BY bytes DESC LIMIT 50;

-- Heat map: bytes by age of last modification for each directory one level
-- down. Pivot label into columns for a grid.
SELECT r.path, a.label, a.bytes
FROM fs_rollup_ages(getvariable('scan')) a
JOIN fs_rollup(getvariable('scan')) r USING (dir_id)
WHERE r.depth = 1 AND a.time = 'mtime'
ORDER BY r.path, a.bucket;

PIVOT (
    SELECT r.path, a.label, a.bytes
    FROM fs_rollup_ages(getvariable('scan')) a
    JOIN fs_rollup(getvariable('scan')) r USING (dir_id)
    WHERE r.depth = 1 AND a.time = 'mtime'
) ON label USING sum(bytes) ORDER BY path;
