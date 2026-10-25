// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! `fs-inventory` command-line tool.

mod cli;
mod exit;
mod output;
mod parse;
mod progress;
mod scan;
mod verify;

use std::process::ExitCode;

use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};

use crate::cli::{Cli, Command};
use crate::exit::Exit;

/// mimalloc instead of the system allocator: crawl threads allocate row
/// buffers constantly, and musl's allocator (used by the static Linux build)
/// serializes them on a lock, making Parquet scans slower as threads are
/// added.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Command::Scan(args) = &cli.command
        && let Err(message) = args.validate()
    {
        usage_error("scan", message);
    }
    run(&cli.command).into()
}

/// Reports a usage error the way clap does, with the subcommand's usage line,
/// and exits with the usage code (2).
fn usage_error(subcommand: &str, message: String) -> ! {
    let mut cmd = Cli::command();
    cmd.build();
    match cmd.find_subcommand_mut(subcommand) {
        Some(sub) => sub.error(ErrorKind::ArgumentConflict, message).exit(),
        None => cmd.error(ErrorKind::ArgumentConflict, message).exit(),
    }
}

fn run(command: &Command) -> Exit {
    let (name, milestone) = match command {
        Command::Scan(args) => return scan::run(args),
        Command::Verify(args) => return verify::run(&args.scan_dir),
        Command::Schema(args) => return schema(args.format),
        Command::Rollup(_) => ("rollup", "M3"),
        Command::Probe(_) => ("probe", "M3"),
        Command::Resume(_) => ("resume", "M4"),
        Command::Convert(_) => ("convert", "M5"),
    };
    eprintln!("fs-inventory: `{name}` is not implemented yet (planned for milestone {milestone})");
    Exit::Fatal
}

fn schema(format: cli::SchemaFormat) -> Exit {
    use output::schema::{Description, describe};
    let how = match format {
        cli::SchemaFormat::Sql => Description::Sql,
        cli::SchemaFormat::Json => Description::Json,
        cli::SchemaFormat::Markdown => Description::Markdown,
    };
    print!("{}", describe(how));
    Exit::Success
}
