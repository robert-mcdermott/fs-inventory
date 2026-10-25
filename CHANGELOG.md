# Changelog

All notable changes to this project are documented in this file. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Cargo workspace with the `fs-inventory-core` library and `fs-inventory` command-line crates.
- Command-line interface for `scan`, `resume`, `rollup`, `convert`, `verify`, `schema`, and `probe`, with argument validation and documented exit codes. The commands are not implemented yet.
- Apache-2.0 license and NOTICE.
- CI on Linux (x86_64 and aarch64) and macOS: formatting, Clippy, tests, the minimum supported Rust version, static musl builds, and cargo-deny license and advisory checks.
