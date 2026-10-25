# Contributing to fs-inventory

## License of contributions

fs-inventory is licensed under the Apache License, Version 2.0. Unless you state otherwise, any contribution you submit is licensed under the same terms, as described in section 5 of the [license](LICENSE).

Only add dependencies whose licenses are on the allowlist in [deny.toml](deny.toml). Copyleft dependencies are not accepted.

## Source file headers

Every source file starts with:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott
```

## Before you open a pull request

Run the same checks as CI:

```bash
cargo fmt --all --check
```

```bash
cargo clippy --workspace --all-targets --all-features --locked
```

```bash
cargo test --workspace --all-features --locked
```

```bash
cargo deny check
```

Install cargo-deny with `cargo install --locked cargo-deny`.

## Robustness rules

The crawler must never stop because of a single bad entry. In library and tool code:

- Don't use `unwrap()` or `expect()`. Clippy warns about them outside tests. Return or record the error instead.
- Treat file names as bytes (`OsStr`, `[u8]`). Convert them to text only in the output layer.
- Never index or slice without a bounds check, and never use unchecked arithmetic on values read from the file system.
