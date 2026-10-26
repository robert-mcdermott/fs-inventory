// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! `fs-inventory` command-line tool.

mod alloc;
mod checkpoint;
mod cli;
mod convert;
#[cfg(debug_assertions)]
mod debug;
mod exit;
mod output;
mod parse;
mod probe;
mod progress;
mod resume;
mod rollup;
mod scan;
mod verify;

use std::process::ExitCode;

use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};

use crate::cli::{Cli, Command};
use crate::exit::Exit;

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
    match command {
        Command::Scan(args) => scan::run(args),
        Command::Verify(args) => verify::run(&args.scan_dir),
        Command::Schema(args) => schema(args.format),
        Command::Rollup(args) => rollup::run(args),
        Command::Probe(args) => probe::run(args),
        Command::Resume(args) => resume::run(args),
        Command::Convert(args) => convert::run(args),
    }
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
