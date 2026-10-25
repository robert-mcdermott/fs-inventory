// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Robert McDermott

//! `fs-inventory` command-line tool.

mod cli;
mod exit;
mod output;
mod parse;
mod progress;
mod scan;

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
    let (name, milestone) = match command {
        Command::Scan(args) => return scan::run(args),
        Command::Verify(_) => ("verify", "M2"),
        Command::Schema(_) => ("schema", "M2"),
        Command::Rollup(_) => ("rollup", "M3"),
        Command::Probe(_) => ("probe", "M3"),
        Command::Resume(_) => ("resume", "M4"),
        Command::Convert(_) => ("convert", "M5"),
    };
    eprintln!("fs-inventory: `{name}` is not implemented yet (planned for milestone {milestone})");
    Exit::Fatal
}
