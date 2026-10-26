// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! `--resolve-ids`: the user and group IDs a scan saw, and their names.
//!
//! Crawl threads collect the unique IDs as they go, checkpoints save them,
//! and when the scan finishes each ID is looked up once. On Linux and the
//! BSDs that's `getent`, which asks every source the host is configured
//! for (files, LDAP, sssd, NIS), even from the static binary, which can't
//! load those sources itself. macOS has no `getent`; there it's
//! `dscacheutil`, which asks Directory Services.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read as _};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use serde::{Deserialize, Serialize};

use crate::output::Format as OutputFormat;
use crate::output::manifest::IdSummary;
use crate::output::parquet::ParquetPartWriter;
use crate::output::parts::{PartInfo, PartWriter, RollLimits, compression_suffix};
use crate::output::schema::{Column, GROUP_COLUMNS, USER_COLUMNS, arrow_schema};
use crate::parse::Compression;

/// IDs per `getent` call, well within any command-line limit.
const BATCH: usize = 1000;
/// Longest wait for one lookup command, such as when a directory server is
/// down.
const TIMEOUT: Duration = Duration::from_secs(60);

/// Unique user and group IDs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ids {
    pub uids: BTreeSet<u32>,
    pub gids: BTreeSet<u32>,
}

impl Ids {
    pub fn merge(&mut self, other: &Self) {
        self.uids.extend(&other.uids);
        self.gids.extend(&other.gids);
    }
}

/// Collects one crawl thread's IDs. Entries in a directory usually share an
/// owner, so a repeat of the last pair costs one comparison.
#[derive(Debug, Default)]
pub struct IdCollector {
    pub ids: Ids,
    last: Option<(u32, u32)>,
}

impl IdCollector {
    pub fn add(&mut self, uid: u32, gid: u32) {
        if self.last != Some((uid, gid)) {
            self.last = Some((uid, gid));
            self.ids.uids.insert(uid);
            self.ids.gids.insert(gid);
        }
    }
}

/// Names for IDs: `None` for an ID with no account (a deleted user, or one
/// from another system).
/// IDs and their names.
type NameMap = BTreeMap<u32, Option<String>>;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Names {
    pub users: NameMap,
    pub groups: NameMap,
    /// The command that looked them up, if any could.
    pub source: Option<&'static str>,
}

/// Looks up the names of `ids`, reporting problems in `warnings`. IDs it
/// couldn't look up get no name.
pub fn resolve(ids: &Ids, warnings: &mut Vec<String>) -> Names {
    let mut names = Names {
        users: ids.uids.iter().map(|&id| (id, None)).collect(),
        groups: ids.gids.iter().map(|&id| (id, None)).collect(),
        source: None,
    };
    let lookups: [(&str, &BTreeSet<u32>, &mut NameMap); 2] = [
        ("passwd", &ids.uids, &mut names.users),
        ("group", &ids.gids, &mut names.groups),
    ];
    for (database, wanted, found) in lookups {
        let keys: Vec<u32> = wanted.iter().copied().collect();
        match lookup(database, &keys) {
            Ok((source, pairs)) => {
                names.source = Some(source);
                for (id, name) in pairs {
                    if let Some(slot) = found.get_mut(&id) {
                        *slot = Some(name);
                    }
                }
            }
            Err(e) => warnings.push(format!("cannot look up {database} names: {e}")),
        }
    }
    names
}

/// `getent` where it exists, else `dscacheutil`.
fn lookup(database: &str, keys: &[u32]) -> io::Result<(&'static str, Vec<(u32, String)>)> {
    match getent(database, keys) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            dscacheutil(database, keys).map(|pairs| ("dscacheutil", pairs))
        }
        result => result.map(|pairs| ("getent", pairs)),
    }
}

fn getent(database: &str, keys: &[u32]) -> io::Result<Vec<(u32, String)>> {
    let mut found = Vec::new();
    for batch in keys.chunks(BATCH) {
        let mut command = Command::new("getent");
        command.arg(database).args(batch.iter().map(u32::to_string));
        // Exit code 2 means some keys weren't found; the rest are printed.
        let (code, out) = run(command)?;
        if !matches!(code, Some(0 | 2)) {
            return Err(io::Error::other(format!("getent exited with {code:?}")));
        }
        found.extend(parse_getent(&out));
    }
    Ok(found)
}

/// `name:password:id:...` lines, as `getent passwd` and `getent group`
/// print them.
fn parse_getent(out: &str) -> Vec<(u32, String)> {
    out.lines()
        .filter_map(|line| {
            let mut fields = line.split(':');
            let name = fields.next()?;
            let id = fields.nth(1)?.parse().ok()?;
            Some((id, name.to_string()))
        })
        .collect()
}

fn dscacheutil(database: &str, keys: &[u32]) -> io::Result<Vec<(u32, String)>> {
    let (kind, attribute) = match database {
        "passwd" => ("user", "uid"),
        _ => ("group", "gid"),
    };
    let mut found = Vec::new();
    for &id in keys {
        let mut command = Command::new("dscacheutil");
        command.args(["-q", kind, "-a", attribute, &id.to_string()]);
        let (_, out) = run(command)?;
        if let Some(name) = parse_dscacheutil(&out) {
            found.push((id, name));
        }
    }
    Ok(found)
}

/// The `name:` of a `dscacheutil -q user` or `-q group` record.
fn parse_dscacheutil(out: &str) -> Option<String> {
    out.lines()
        .find_map(|line| line.strip_prefix("name: "))
        .map(|name| name.trim().to_string())
}

/// Runs a command, killing it after [`TIMEOUT`]. Returns its exit code and
/// standard output.
fn run(mut command: Command) -> io::Result<(Option<i32>, String)> {
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("no stdout"))?;
    // Read while it runs, so a long answer can't fill the pipe and stall it.
    let reader = std::thread::spawn(move || {
        let mut out = Vec::new();
        stdout.read_to_end(&mut out).map(|_| out)
    });
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if started.elapsed() > TIMEOUT {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("no answer in {} seconds", TIMEOUT.as_secs()),
            ));
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let out = reader
        .join()
        .map_err(|_| io::Error::other("reading the output failed"))??;
    Ok((status.code(), String::from_utf8_lossy(&out).into_owned()))
}

/// Writes the `users` and `groups` tables of a finished scan into `dir`, in
/// the scan's format, and summarizes them for the manifest.
pub fn write_tables(
    dir: &Path,
    format: &OutputFormat,
    compression: Compression,
    limits: RollLimits,
    names: &Names,
) -> io::Result<(Vec<PartInfo>, Vec<PartInfo>, IdSummary)> {
    let table = |name, column, columns, ids| {
        write_table(dir, name, column, columns, ids, format, compression, limits)
    };
    let users = table("users", "uid", &USER_COLUMNS[..], &names.users)?;
    let groups = table("groups", "gid", &GROUP_COLUMNS[..], &names.groups)?;
    let unnamed = |ids: &NameMap| ids.values().filter(|n| n.is_none()).count();
    let summary = IdSummary {
        users: names.users.len(),
        groups: names.groups.len(),
        users_unnamed: unnamed(&names.users),
        groups_unnamed: unnamed(&names.groups),
        source: names.source,
    };
    Ok((users, groups, summary))
}

#[allow(clippy::too_many_arguments, reason = "one table's description")]
fn write_table(
    dir: &Path,
    table: &'static str,
    column: &str,
    columns: &[Column],
    ids: &NameMap,
    format: &OutputFormat,
    compression: Compression,
    limits: RollLimits,
) -> io::Result<Vec<PartInfo>> {
    match format {
        OutputFormat::Parquet { arrow, settings } => {
            let schema = arrow_schema(columns, arrow.unit);
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        ids.keys().map(|&id| i64::from(id)),
                    )),
                    Arc::new(StringArray::from_iter(ids.values().map(Option::as_deref))),
                ],
            )
            .map_err(io::Error::other)?;
            let mut writer =
                ParquetPartWriter::new(dir, table, "part-w00".into(), schema, settings, limits)?;
            if batch.num_rows() > 0 {
                writer.write(&batch)?;
            }
            writer.finish()
        }
        OutputFormat::Text(encoder) => {
            let extension = format!(
                "{}{}",
                encoder.format().extension(),
                compression_suffix(compression)
            );
            let names: Vec<&str> = columns.iter().map(|c| c.text_name).collect();
            let mut writer = PartWriter::new(
                dir,
                table,
                "part-w00".into(),
                extension,
                encoder.header(&names),
                compression,
                limits,
            );
            let mut out = Vec::new();
            for (&id, name) in ids {
                encoder.id(column, id, name.as_deref(), &mut out);
            }
            if !ids.is_empty() {
                writer.write(&out, ids.len() as u64)?;
            }
            writer.finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collects_unique_ids() {
        let mut collector = IdCollector::default();
        for (uid, gid) in [(1000, 100), (1000, 100), (0, 0), (1000, 100), (1001, 100)] {
            collector.add(uid, gid);
        }
        assert_eq!(collector.ids.uids, BTreeSet::from([0, 1000, 1001]));
        assert_eq!(collector.ids.gids, BTreeSet::from([0, 100]));
        let mut all = Ids::default();
        all.merge(&collector.ids);
        all.merge(&Ids {
            uids: BTreeSet::from([7]),
            gids: BTreeSet::new(),
        });
        assert_eq!(all.uids, BTreeSet::from([0, 7, 1000, 1001]));
    }

    #[test]
    fn parses_lookup_output() {
        let passwd = "root:x:0:0:root:/root:/bin/bash\ndaemon:x:1:1:daemon:/usr/sbin:/usr/sbin/nologin\nbad line\n";
        assert_eq!(
            parse_getent(passwd),
            [(0, "root".to_string()), (1, "daemon".to_string())]
        );
        assert_eq!(
            parse_getent("staff:x:20:alice,bob\n"),
            [(20, "staff".to_string())]
        );
        let record = "name: staff\npassword: *\ngid: 20\nusers: root\n\n";
        assert_eq!(parse_dscacheutil(record), Some("staff".to_string()));
        assert_eq!(parse_dscacheutil(""), None);
    }

    #[test]
    fn resolves_the_superuser() {
        let ids = Ids {
            uids: BTreeSet::from([0, 3_999_999_999]),
            gids: BTreeSet::from([0]),
        };
        let mut warnings = Vec::new();
        let names = resolve(&ids, &mut warnings);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(names.users[&0].as_deref(), Some("root"));
        assert_eq!(names.users[&3_999_999_999], None);
        assert!(names.groups[&0].is_some());
        assert!(names.source.is_some());
    }
}
