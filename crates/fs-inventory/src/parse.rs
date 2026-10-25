// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Value parsers for command-line options.

use std::time::Duration;

/// Output compression codec and level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Zstd(i32),
    Gzip(u32),
    Snappy,
    Lz4,
}

impl Compression {
    /// Whether this codec can be applied to a CSV or JSON Lines stream.
    pub fn supports_text_stream(self) -> bool {
        matches!(self, Self::None | Self::Zstd(_) | Self::Gzip(_))
    }
}

/// Parses `zstd[:LEVEL]`, `gzip[:LEVEL]`, `snappy`, `lz4`, or `none`.
pub fn parse_compression(s: &str) -> Result<Compression, String> {
    let (codec, level) = match s.split_once(':') {
        Some((codec, level)) => (codec, Some(level)),
        None => (s, None),
    };
    let level_in = |range: std::ops::RangeInclusive<i64>, default: i64| -> Result<i64, String> {
        let Some(level) = level else {
            return Ok(default);
        };
        match level.parse::<i64>() {
            Ok(n) if range.contains(&n) => Ok(n),
            _ => Err(format!(
                "invalid {codec} level `{level}` (expected {}..={})",
                range.start(),
                range.end()
            )),
        }
    };
    let no_level = |c: Compression| match level {
        None => Ok(c),
        Some(_) => Err(format!("`{codec}` does not take a level")),
    };
    match codec.to_ascii_lowercase().as_str() {
        "zstd" => level_in(1..=22, 3).map(|n| Compression::Zstd(n as i32)),
        "gzip" => level_in(1..=9, 6).map(|n| Compression::Gzip(n as u32)),
        "snappy" => no_level(Compression::Snappy),
        "lz4" => no_level(Compression::Lz4),
        "none" => no_level(Compression::None),
        _ => Err(format!(
            "unknown compression `{codec}` (expected zstd, gzip, snappy, lz4, or none)"
        )),
    }
}

/// Parses a byte size such as `1048576`, `512MiB`, `1G`, or `100GB`.
///
/// `K`/`M`/`G`/`T` and `KiB`/`MiB`/`GiB`/`TiB` are binary (powers of 1024);
/// `KB`/`MB`/`GB`/`TB` are decimal (powers of 1000).
pub fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (digits, unit) = s.split_at(split);
    if digits.is_empty() {
        return Err(format!(
            "invalid size `{s}` (expected a number with an optional unit)"
        ));
    }
    let multiplier: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kib" => 1 << 10,
        "m" | "mib" => 1 << 20,
        "g" | "gib" => 1 << 30,
        "t" | "tib" => 1 << 40,
        "kb" => 1_000,
        "mb" => 1_000_000,
        "gb" => 1_000_000_000,
        "tb" => 1_000_000_000_000,
        other => return Err(format!("unknown size unit `{other}` in `{s}`")),
    };
    digits
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
        .ok_or_else(|| format!("size `{s}` is too large"))
}

/// Parses a duration such as `30s`, `10m`, `1h 30m`, or `0`.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s == "0" {
        return Ok(Duration::ZERO);
    }
    humantime::parse_duration(s).map_err(|e| format!("invalid duration `{s}`: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_defaults_and_levels() {
        assert_eq!(parse_compression("zstd"), Ok(Compression::Zstd(3)));
        assert_eq!(parse_compression("ZSTD:19"), Ok(Compression::Zstd(19)));
        assert_eq!(parse_compression("gzip"), Ok(Compression::Gzip(6)));
        assert_eq!(parse_compression("gzip:1"), Ok(Compression::Gzip(1)));
        assert_eq!(parse_compression("snappy"), Ok(Compression::Snappy));
        assert_eq!(parse_compression("lz4"), Ok(Compression::Lz4));
        assert_eq!(parse_compression("none"), Ok(Compression::None));
    }

    #[test]
    fn compression_rejects_bad_input() {
        assert!(parse_compression("zstd:0").is_err());
        assert!(parse_compression("zstd:23").is_err());
        assert!(parse_compression("zstd:fast").is_err());
        assert!(parse_compression("gzip:10").is_err());
        assert!(parse_compression("snappy:3").is_err());
        assert!(parse_compression("brotli").is_err());
        assert!(parse_compression("").is_err());
    }

    #[test]
    fn text_stream_codecs() {
        assert!(Compression::Zstd(3).supports_text_stream());
        assert!(Compression::Gzip(6).supports_text_stream());
        assert!(Compression::None.supports_text_stream());
        assert!(!Compression::Snappy.supports_text_stream());
        assert!(!Compression::Lz4.supports_text_stream());
    }

    #[test]
    fn sizes() {
        assert_eq!(parse_size("0"), Ok(0));
        assert_eq!(parse_size("4096"), Ok(4096));
        assert_eq!(parse_size("4096B"), Ok(4096));
        assert_eq!(parse_size("1K"), Ok(1024));
        assert_eq!(parse_size("512MiB"), Ok(512 << 20));
        assert_eq!(parse_size("1GiB"), Ok(1 << 30));
        assert_eq!(parse_size("1g"), Ok(1 << 30));
        assert_eq!(parse_size("2 TiB"), Ok(2 << 40));
        assert_eq!(parse_size("100GB"), Ok(100_000_000_000));
        assert_eq!(parse_size("5kb"), Ok(5_000));
    }

    #[test]
    fn sizes_reject_bad_input() {
        assert!(parse_size("").is_err());
        assert!(parse_size("GiB").is_err());
        assert!(parse_size("1.5GiB").is_err());
        assert!(parse_size("-1").is_err());
        assert!(parse_size("1PB").is_err());
        assert!(parse_size("99999999999TiB").is_err());
        assert!(parse_size("18446744073709551616").is_err());
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("0"), Ok(Duration::ZERO));
        assert_eq!(parse_duration("30s"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_duration("10m"), Ok(Duration::from_secs(600)));
        assert_eq!(parse_duration("1h 30m"), Ok(Duration::from_secs(5400)));
        assert!(parse_duration("10").is_err());
        assert!(parse_duration("soon").is_err());
    }
}
