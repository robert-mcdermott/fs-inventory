// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! The memory allocator.

use libmimalloc_sys::{mi_option_set, mi_option_t, mi_version};

/// mimalloc instead of the system allocator: crawl threads allocate row
/// buffers constantly, and musl's allocator (used by the static Linux build)
/// serializes them on a lock, making Parquet scans slower as threads are
/// added.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// mimalloc 3's numbers for the options set here (`mi_option_t` in
/// `mimalloc.h`), which the Rust bindings don't name.
const ARENA_EAGER_COMMIT: mi_option_t = 4;
const PURGE_DELAY: mi_option_t = 15;

/// Whether the linked mimalloc numbers its options as mimalloc 3 does.
fn is_mimalloc_3() -> bool {
    #[allow(unsafe_code, reason = "a C call with no arguments")]
    // SAFETY: `mi_version` only returns a constant.
    let version = unsafe { mi_version() };
    (30_000..40_000).contains(&version)
}

/// Makes mimalloc return freed memory to the system at once.
///
/// By default, mimalloc waits a second before returning freed memory, and
/// crawl threads build row buffers that writer threads free, so a scan
/// holds about 20 MB more per crawl thread than it uses. Returning memory
/// at once cut peak memory by 40 to 50% on Linux, but cost 5 to 8% more CPU
/// time on scans limited by the CPU. Scans of network file systems mostly
/// wait on the server, but a scan without a memory budget keeps the faster
/// default.
///
/// Call it before starting any threads: setting a mimalloc option isn't
/// thread-safe.
pub fn release_memory_promptly() {
    if !is_mimalloc_3() {
        return;
    }
    #[allow(unsafe_code, reason = "C calls with integer arguments only")]
    // SAFETY: the options exist in mimalloc 3, and no other thread is
    // running to read them concurrently.
    unsafe {
        mi_option_set(PURGE_DELAY, 0);
        mi_option_set(ARENA_EAGER_COMMIT, 0);
    }
}

#[cfg(test)]
mod tests {
    use libmimalloc_sys::mi_option_get;

    use super::*;

    /// The option numbers are right if each option has its documented
    /// default (unless a `MIMALLOC_` variable overrides it).
    #[test]
    fn option_numbers_match_mimalloc() {
        assert!(is_mimalloc_3(), "mimalloc 3 is no longer linked");
        for (option, name, default) in [
            (PURGE_DELAY, "MIMALLOC_PURGE_DELAY", 1000),
            (ARENA_EAGER_COMMIT, "MIMALLOC_ARENA_EAGER_COMMIT", 2),
        ] {
            if std::env::var_os(name).is_none() {
                #[allow(unsafe_code, reason = "a C call with an integer argument")]
                // SAFETY: reading an option that exists.
                let value = unsafe { mi_option_get(option) };
                assert_eq!(value, default, "{name}");
            }
        }
    }
}
