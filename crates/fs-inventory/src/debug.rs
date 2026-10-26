// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! Test hooks, in debug builds only: `FS_INVENTORY_DEBUG_STAT_DELAY_US`
//! slows every `stat` by that many microseconds, so tests can interrupt or
//! kill a scan partway through a small tree.

use std::io;
use std::time::Duration;

use fs_inventory_core::backend::{Backend, NameBatch};
use fs_inventory_core::model::Stat;

/// A backend that sleeps before every `stat`.
pub struct Delayed<B> {
    inner: B,
    delay: Duration,
}

impl<B> Delayed<B> {
    pub fn from_env(inner: B) -> Self {
        let micros = std::env::var("FS_INVENTORY_DEBUG_STAT_DELAY_US")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        Self {
            inner,
            delay: Duration::from_micros(micros),
        }
    }
}

impl<B: Backend> Backend for Delayed<B> {
    type Dir = B::Dir;
    type Scratch = B::Scratch;

    fn new_scratch(&self) -> Self::Scratch {
        self.inner.new_scratch()
    }

    fn stat_root(&self, path: &[u8]) -> io::Result<Stat> {
        self.inner.stat_root(path)
    }

    fn open_dir(&self, path: &[u8], follow: bool) -> io::Result<Self::Dir> {
        self.inner.open_dir(path, follow)
    }

    fn dir_stat(&self, dir: &Self::Dir) -> io::Result<Stat> {
        self.inner.dir_stat(dir)
    }

    fn read_names(
        &self,
        dir: &Self::Dir,
        scratch: &mut Self::Scratch,
        names: &mut NameBatch,
    ) -> io::Result<bool> {
        self.inner.read_names(dir, scratch, names)
    }

    fn read_link_at(&self, dir: &Self::Dir, name: &[u8]) -> io::Result<Vec<u8>> {
        self.inner.read_link_at(dir, name)
    }

    fn stat_at(&self, dir: &Self::Dir, name: &[u8]) -> io::Result<Stat> {
        if !self.delay.is_zero() {
            std::thread::sleep(self.delay);
        }
        self.inner.stat_at(dir, name)
    }
}
