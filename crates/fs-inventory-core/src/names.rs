// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! File-name analysis and the rendering of raw name bytes as text.
//!
//! File names are arbitrary bytes (anything but `/` and NUL). Output formats
//! need valid UTF-8, so [`to_text`] renders bytes as text: valid UTF-8 passes
//! through unchanged, and anything else is escaped. When the text differs from
//! the original bytes, writers also store the exact bytes in a separate column.

use std::borrow::Cow;
use std::fmt::Write as _;
use std::ops::{BitOr, BitOrAssign};

use unicode_normalization::{IsNormalized, UnicodeNormalization as _, is_nfc_quick};

/// Names longer than this many bytes get [`NameFlags::LONG_NAME`].
pub const NAME_MAX: usize = 255;

/// Paths at least this many bytes long get [`NameFlags::LONG_PATH`]; most
/// systems cannot open them by full path.
pub const PATH_MAX: usize = 4096;

/// Extensions longer than this many bytes are treated as no extension.
pub const MAX_EXTENSION_LEN: usize = 32;

/// Properties of a name that matter for analytics and for migrating data to
/// other systems. Stored as a bitmask in the `name_flags` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct NameFlags(u32);

impl NameFlags {
    /// Not valid UTF-8: a legacy encoding or corruption.
    pub const INVALID_UTF8: Self = Self(1 << 0);
    /// Contains an ASCII control character (U+0000 to U+001F), such as a newline.
    pub const C0_CONTROL: Self = Self(1 << 1);
    /// Contains a C1 control character (U+0080 to U+009F).
    pub const C1_CONTROL: Self = Self(1 << 2);
    /// Contains DEL (U+007F).
    pub const DEL: Self = Self(1 << 3);
    /// Contains a bidirectional formatting character that can disguise how a
    /// name displays.
    pub const BIDI_CONTROL: Self = Self(1 << 4);
    /// Valid UTF-8 that is not in Unicode Normalization Form C.
    pub const NOT_NFC: Self = Self(1 << 5);
    /// Starts or ends with a space.
    pub const LEADING_TRAILING_SPACE: Self = Self(1 << 6);
    /// Starts with `-`, which command-line tools can mistake for an option.
    pub const LEADING_DASH: Self = Self(1 << 7);
    /// Ends with `.`, which Windows strips.
    pub const TRAILING_DOT: Self = Self(1 << 8);
    /// Contains a character Windows forbids (`<>:"|?*\`) or is a reserved
    /// device name such as `CON` or `LPT1`.
    pub const WINDOWS_RESERVED: Self = Self(1 << 9);
    /// Longer than [`NAME_MAX`] bytes.
    pub const LONG_NAME: Self = Self(1 << 10);
    /// The full path is at least [`PATH_MAX`] bytes.
    pub const LONG_PATH: Self = Self(1 << 11);

    /// Every flag with its stable name, in bit order.
    pub const ALL: [(Self, &'static str); 12] = [
        (Self::INVALID_UTF8, "invalid_utf8"),
        (Self::C0_CONTROL, "c0_control"),
        (Self::C1_CONTROL, "c1_control"),
        (Self::DEL, "del"),
        (Self::BIDI_CONTROL, "bidi_control"),
        (Self::NOT_NFC, "not_nfc"),
        (Self::LEADING_TRAILING_SPACE, "leading_trailing_space"),
        (Self::LEADING_DASH, "leading_dash"),
        (Self::TRAILING_DOT, "trailing_dot"),
        (Self::WINDOWS_RESERVED, "windows_reserved"),
        (Self::LONG_NAME, "long_name"),
        (Self::LONG_PATH, "long_path"),
    ];

    /// No flags.
    pub const fn empty() -> Self {
        Self(0)
    }

    /// The raw bitmask.
    pub const fn bits(self) -> u32 {
        self.0
    }

    /// Whether no flags are set.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Whether every flag in `other` is set.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl BitOr for NameFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for NameFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

/// Analyzes one name. `path_len` is the length in bytes of the full path.
pub fn name_flags(name: &[u8], path_len: usize) -> NameFlags {
    let mut flags = NameFlags::empty();
    if name.len() > NAME_MAX {
        flags |= NameFlags::LONG_NAME;
    }
    if path_len >= PATH_MAX {
        flags |= NameFlags::LONG_PATH;
    }
    if name.first() == Some(&b' ') || name.last() == Some(&b' ') {
        flags |= NameFlags::LEADING_TRAILING_SPACE;
    }
    if name.first() == Some(&b'-') {
        flags |= NameFlags::LEADING_DASH;
    }
    if name.last() == Some(&b'.') && name != b"." && name != b".." {
        flags |= NameFlags::TRAILING_DOT;
    }
    if is_windows_reserved_name(name) {
        flags |= NameFlags::WINDOWS_RESERVED;
    }

    let mut ascii = true;
    for &b in name {
        match b {
            0x00..=0x1f => flags |= NameFlags::C0_CONTROL,
            0x7f => flags |= NameFlags::DEL,
            b'<' | b'>' | b':' | b'"' | b'|' | b'?' | b'*' | b'\\' => {
                flags |= NameFlags::WINDOWS_RESERVED;
            }
            0x80.. => ascii = false,
            _ => {}
        }
    }
    if ascii {
        return flags;
    }

    let mut valid = true;
    for chunk in name.utf8_chunks() {
        for c in chunk.valid().chars() {
            match c {
                '\u{80}'..='\u{9f}' => flags |= NameFlags::C1_CONTROL,
                '\u{200e}'
                | '\u{200f}'
                | '\u{061c}'
                | '\u{202a}'..='\u{202e}'
                | '\u{2066}'..='\u{2069}' => flags |= NameFlags::BIDI_CONTROL,
                _ => {}
            }
        }
        if !chunk.invalid().is_empty() {
            valid = false;
        }
    }
    if !valid {
        flags |= NameFlags::INVALID_UTF8;
    } else if let Ok(s) = std::str::from_utf8(name) {
        let nfc = match is_nfc_quick(s.chars()) {
            IsNormalized::Yes => true,
            IsNormalized::No => false,
            IsNormalized::Maybe => s.chars().eq(s.nfc()),
        };
        if !nfc {
            flags |= NameFlags::NOT_NFC;
        }
    }
    flags
}

/// Whether the part of the name before the first `.` is a Windows device name.
fn is_windows_reserved_name(name: &[u8]) -> bool {
    let stem = name.split(|&b| b == b'.').next().unwrap_or(name);
    let stem = stem.strip_suffix(b" ").unwrap_or(stem);
    match stem.len() {
        3 => ["CON", "PRN", "AUX", "NUL"]
            .iter()
            .any(|r| stem.eq_ignore_ascii_case(r.as_bytes())),
        4 => {
            let (prefix, digit) = stem.split_at(3);
            (prefix.eq_ignore_ascii_case(b"COM") || prefix.eq_ignore_ascii_case(b"LPT"))
                && matches!(digit[0], b'1'..=b'9')
        }
        _ => false,
    }
}

/// How ASCII control characters are rendered by [`to_text`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Controls {
    /// Keep them as they are (for formats that escape them, such as JSON).
    Keep,
    /// Render them as `\t`, `\n`, `\r`, or `\xNN`, so every record stays on
    /// one line (for CSV).
    Escape,
}

/// Renders name or path bytes as UTF-8 text.
///
/// Valid UTF-8 is returned unchanged and borrowed (unless control characters
/// must be escaped). Each byte of an invalid UTF-8 sequence is rendered as
/// `\xNN`. The result is for display and search, not exact reconstruction: a
/// [`Cow::Owned`] result means the text differs from the original bytes, so
/// writers must also store the bytes themselves.
pub fn to_text(bytes: &[u8], controls: Controls) -> Cow<'_, str> {
    let escape_controls = controls == Controls::Escape;
    if let Ok(s) = std::str::from_utf8(bytes)
        && !(escape_controls && s.bytes().any(is_ascii_control))
    {
        return Cow::Borrowed(s);
    }

    let mut out = String::with_capacity(bytes.len() + 8);
    for chunk in bytes.utf8_chunks() {
        let valid = chunk.valid();
        if escape_controls && valid.bytes().any(is_ascii_control) {
            for c in valid.chars() {
                match c {
                    '\t' => out.push_str("\\t"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    c if c.is_ascii_control() => push_hex_escape(&mut out, c as u8),
                    c => out.push(c),
                }
            }
        } else {
            out.push_str(valid);
        }
        for &b in chunk.invalid() {
            push_hex_escape(&mut out, b);
        }
    }
    Cow::Owned(out)
}

fn is_ascii_control(b: u8) -> bool {
    b < 0x20 || b == 0x7f
}

fn push_hex_escape(out: &mut String, b: u8) {
    // Writing to a String cannot fail.
    let _ = write!(out, "\\x{b:02X}");
}

/// Returns the extension of a name: the bytes after the last `.`, with ASCII
/// letters lowercased.
///
/// Names with no `.`, a leading `.` only (`.bashrc`), or a trailing `.` have
/// no extension. Extensions longer than [`MAX_EXTENSION_LEN`] bytes are
/// treated as no extension.
pub fn extension(name: &[u8]) -> Option<Cow<'_, [u8]>> {
    let dot = name.iter().rposition(|&b| b == b'.')?;
    if dot == 0 {
        return None;
    }
    let ext = &name[dot + 1..];
    if ext.is_empty() || ext.len() > MAX_EXTENSION_LEN {
        return None;
    }
    if ext.iter().any(u8::is_ascii_uppercase) {
        Some(Cow::Owned(ext.to_ascii_lowercase()))
    } else {
        Some(Cow::Borrowed(ext))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn flags(name: &[u8]) -> NameFlags {
        name_flags(name, name.len() + 10)
    }

    #[test]
    fn plain_names_have_no_flags() {
        for name in [
            &b"report.txt"[..],
            b"data_2026",
            b"a.b.c",
            "日本語.txt".as_bytes(),
        ] {
            assert_eq!(flags(name), NameFlags::empty(), "{name:?}");
        }
    }

    #[test]
    fn invalid_utf8_is_flagged() {
        // Latin-1 "café", Shift-JIS "テスト", a lone continuation byte.
        for name in [&b"caf\xe9"[..], b"\x83e\x83X\x83g", b"bad\x80name"] {
            assert!(flags(name).contains(NameFlags::INVALID_UTF8), "{name:?}");
        }
    }

    #[test]
    fn control_characters_are_flagged() {
        assert!(flags(b"line1\nline2").contains(NameFlags::C0_CONTROL));
        assert!(flags(b"tab\there").contains(NameFlags::C0_CONTROL));
        assert!(flags(b"del\x7f").contains(NameFlags::DEL));
        assert!(flags("next\u{85}line".as_bytes()).contains(NameFlags::C1_CONTROL));
        assert!(flags("evil\u{202e}txt.exe".as_bytes()).contains(NameFlags::BIDI_CONTROL));
    }

    #[test]
    fn normalization_is_checked() {
        let nfd = "cafe\u{301}.txt";
        let nfc = "caf\u{e9}.txt";
        assert!(flags(nfd.as_bytes()).contains(NameFlags::NOT_NFC));
        assert!(!flags(nfc.as_bytes()).contains(NameFlags::NOT_NFC));
    }

    #[test]
    fn hazardous_names_are_flagged() {
        assert!(flags(b" lead").contains(NameFlags::LEADING_TRAILING_SPACE));
        assert!(flags(b"trail ").contains(NameFlags::LEADING_TRAILING_SPACE));
        assert!(flags(b"-rf").contains(NameFlags::LEADING_DASH));
        assert!(flags(b"name.").contains(NameFlags::TRAILING_DOT));
        assert!(flags(b"a:b").contains(NameFlags::WINDOWS_RESERVED));
        assert!(flags(b"back\\slash").contains(NameFlags::WINDOWS_RESERVED));
        for reserved in [&b"CON"[..], b"nul.txt", b"Com1", b"lpt9.log", b"aux .c"] {
            assert!(
                flags(reserved).contains(NameFlags::WINDOWS_RESERVED),
                "{reserved:?}"
            );
        }
        for fine in [&b"CONSOLE"[..], b"com0", b"lpt", b"nullable.txt"] {
            assert!(
                !flags(fine).contains(NameFlags::WINDOWS_RESERVED),
                "{fine:?}"
            );
        }
    }

    #[test]
    fn lengths_are_flagged() {
        let long = vec![b'a'; NAME_MAX + 1];
        assert!(name_flags(&long, 300).contains(NameFlags::LONG_NAME));
        assert!(!name_flags(&long[..NAME_MAX], 300).contains(NameFlags::LONG_NAME));
        assert!(name_flags(b"x", PATH_MAX).contains(NameFlags::LONG_PATH));
        assert!(!name_flags(b"x", PATH_MAX - 1).contains(NameFlags::LONG_PATH));
    }

    #[test]
    fn flag_bits_are_distinct() {
        let mut seen = 0u32;
        for (flag, _) in NameFlags::ALL {
            assert_eq!(seen & flag.bits(), 0);
            seen |= flag.bits();
        }
    }

    #[test]
    fn text_rendering() {
        assert!(matches!(
            to_text(b"plain", Controls::Escape),
            Cow::Borrowed("plain")
        ));
        assert_eq!(to_text(b"caf\xe9", Controls::Keep), "caf\\xE9");
        assert_eq!(
            to_text(b"a\nb\tc\rd\x1b", Controls::Escape),
            "a\\nb\\tc\\rd\\x1B"
        );
        assert!(matches!(
            to_text(b"a\nb", Controls::Keep),
            Cow::Borrowed("a\nb")
        ));
        assert_eq!(to_text(b"\xff\nok", Controls::Escape), "\\xFF\\nok");
        assert_eq!(to_text("日本".as_bytes(), Controls::Escape), "日本");
    }

    #[test]
    fn extensions() {
        let ext = |n: &[u8]| extension(n).map(|e| e.into_owned());
        assert_eq!(ext(b"report.PDF"), Some(b"pdf".to_vec()));
        assert_eq!(ext(b"archive.tar.gz"), Some(b"gz".to_vec()));
        assert_eq!(ext(b".bashrc"), None);
        assert_eq!(ext(b"trailing."), None);
        assert_eq!(ext(b"noext"), None);
        assert_eq!(ext(b"..hidden.TXT"), Some(b"txt".to_vec()));
        assert_eq!(ext(b"caf\xe9.d\xe9"), Some(b"d\xe9".to_vec()));
        let long = [b"x.".as_slice(), &[b'a'; MAX_EXTENSION_LEN + 1]].concat();
        assert_eq!(ext(&long), None);
    }

    proptest! {
        #[test]
        fn text_is_borrowed_exactly_when_identical(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            for controls in [Controls::Keep, Controls::Escape] {
                let text = to_text(&bytes, controls);
                match &text {
                    Cow::Borrowed(s) => prop_assert_eq!(s.as_bytes(), &bytes[..]),
                    Cow::Owned(s) => prop_assert_ne!(s.as_bytes(), &bytes[..]),
                }
            }
        }

        #[test]
        fn escaped_text_has_no_control_characters(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            let text = to_text(&bytes, Controls::Escape);
            prop_assert!(!text.bytes().any(is_ascii_control));
        }

        #[test]
        fn valid_utf8_without_controls_is_never_altered(s in "\\PC{0,32}") {
            let text = to_text(s.as_bytes(), Controls::Escape);
            prop_assert!(matches!(text, Cow::Borrowed(_)));
        }

        #[test]
        fn analysis_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..300), path_len in 0usize..10_000) {
            let _ = name_flags(&bytes, path_len);
            let _ = extension(&bytes);
        }
    }
}
