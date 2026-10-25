// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Timestamp formatting for text output.

use fs_inventory_core::model::Timestamp;

/// Timestamp precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Precision {
    Micros,
    Nanos,
}

impl Precision {
    fn digits(self) -> usize {
        match self {
            Self::Micros => 6,
            Self::Nanos => 9,
        }
    }

    fn fraction(self, nsec: u32) -> u32 {
        match self {
            Self::Micros => nsec / 1_000,
            Self::Nanos => nsec,
        }
    }
}

/// Seconds from 1970 to 0001-01-01T00:00:00Z and to 10000-01-01T00:00:00Z:
/// the range ISO-8601 readers accept with four-digit years.
const MIN_ISO_SECONDS: i64 = -62_135_596_800;
const MAX_ISO_SECONDS: i64 = 253_402_300_800;

/// Writes `t` as ISO-8601 UTC, such as `2026-10-25T01:30:00.123456Z`.
/// Returns `false` if the time was outside years 1 to 9999 and was clamped.
pub fn write_iso(out: &mut Vec<u8>, t: Timestamp, precision: Precision) -> bool {
    let (t, in_range) = if t.sec < MIN_ISO_SECONDS {
        (
            Timestamp {
                sec: MIN_ISO_SECONDS,
                nsec: 0,
            },
            false,
        )
    } else if t.sec >= MAX_ISO_SECONDS {
        let max = Timestamp {
            sec: MAX_ISO_SECONDS - 1,
            nsec: 999_999_999,
        };
        (max, false)
    } else {
        (t, true)
    };
    let days = t.sec.div_euclid(86_400);
    let secs = t.sec.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let mut buf = itoa::Buffer::new();
    push_padded(out, buf.format(year), 4);
    out.push(b'-');
    push_padded(out, buf.format(month), 2);
    out.push(b'-');
    push_padded(out, buf.format(day), 2);
    out.push(b'T');
    push_padded(out, buf.format(secs / 3600), 2);
    out.push(b':');
    push_padded(out, buf.format(secs / 60 % 60), 2);
    out.push(b':');
    push_padded(out, buf.format(secs % 60), 2);
    out.push(b'.');
    push_padded(
        out,
        buf.format(precision.fraction(t.nsec)),
        precision.digits(),
    );
    out.push(b'Z');
    in_range
}

/// Writes `t` as decimal seconds since the epoch, such as `1761355800.123456`
/// or `-0.500000`.
pub fn write_epoch(out: &mut Vec<u8>, t: Timestamp, precision: Precision) {
    let total = i128::from(t.sec) * 1_000_000_000 + i128::from(t.nsec);
    if total < 0 {
        out.push(b'-');
    }
    let total = total.unsigned_abs();
    let mut buf = itoa::Buffer::new();
    out.extend_from_slice(buf.format(total / 1_000_000_000).as_bytes());
    out.push(b'.');
    let nsec = (total % 1_000_000_000) as u32;
    push_padded(
        out,
        buf.format(precision.fraction(nsec)),
        precision.digits(),
    );
}

fn push_padded(out: &mut Vec<u8>, digits: &str, width: usize) {
    out.extend(std::iter::repeat_n(
        b'0',
        width.saturating_sub(digits.len()),
    ));
    out.extend_from_slice(digits.as_bytes());
}

/// Converts days since 1970-01-01 to a proleptic Gregorian (year, month,
/// day). From Howard Hinnant's `civil_from_days` algorithm.
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iso(sec: i64, nsec: u32, precision: Precision) -> (String, bool) {
        let mut out = Vec::new();
        let ok = write_iso(&mut out, Timestamp { sec, nsec }, precision);
        (String::from_utf8(out).unwrap(), ok)
    }

    fn epoch(sec: i64, nsec: u32, precision: Precision) -> String {
        let mut out = Vec::new();
        write_epoch(&mut out, Timestamp { sec, nsec }, precision);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn iso_formatting() {
        use Precision::*;
        assert_eq!(
            iso(0, 0, Micros),
            ("1970-01-01T00:00:00.000000Z".into(), true)
        );
        assert_eq!(
            iso(1_761_355_800, 123_456_789, Micros),
            ("2025-10-25T01:30:00.123456Z".into(), true)
        );
        assert_eq!(
            iso(1_761_355_800, 123_456_789, Nanos),
            ("2025-10-25T01:30:00.123456789Z".into(), true)
        );
        assert_eq!(
            iso(-1, 0, Micros),
            ("1969-12-31T23:59:59.000000Z".into(), true)
        );
        assert_eq!(
            iso(-2_208_988_800, 0, Micros),
            ("1900-01-01T00:00:00.000000Z".into(), true)
        );
        assert_eq!(
            iso(951_782_400, 0, Micros),
            ("2000-02-29T00:00:00.000000Z".into(), true)
        );
        assert_eq!(
            iso(4_294_967_295, 0, Micros),
            ("2106-02-07T06:28:15.000000Z".into(), true)
        );
    }

    #[test]
    fn iso_clamps_out_of_range_years() {
        use Precision::*;
        assert_eq!(
            iso(MIN_ISO_SECONDS, 0, Micros),
            ("0001-01-01T00:00:00.000000Z".into(), true)
        );
        assert_eq!(
            iso(i64::MIN, 0, Micros),
            ("0001-01-01T00:00:00.000000Z".into(), false)
        );
        assert!(iso(MAX_ISO_SECONDS - 1, 0, Micros).1);
        assert_eq!(
            iso(i64::MAX, 0, Nanos),
            ("9999-12-31T23:59:59.999999999Z".into(), false)
        );
    }

    #[test]
    fn epoch_formatting() {
        use Precision::*;
        assert_eq!(epoch(0, 0, Micros), "0.000000");
        assert_eq!(
            epoch(1_761_355_800, 123_456_789, Micros),
            "1761355800.123456"
        );
        assert_eq!(
            epoch(1_761_355_800, 123_456_789, Nanos),
            "1761355800.123456789"
        );
        assert_eq!(epoch(-1, 500_000_000, Micros), "-0.500000");
        assert_eq!(epoch(-2, 0, Nanos), "-2.000000000");
        assert_eq!(epoch(i64::MIN, 0, Micros), "-9223372036854775808.000000");
    }

    #[test]
    fn civil_dates_round_trip_known_days() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
        assert_eq!(civil_from_days(-719_162), (1, 1, 1));
        assert_eq!(civil_from_days(2_932_896), (9999, 12, 31));
    }
}
