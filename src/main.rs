#![deny(unsafe_code)]

use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "herdr-top")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Emit {
        #[arg(long)]
        strict: bool,
    },
    Doctor,
}

fn main() -> ExitCode {
    match Cli::parse().command {
        None => {
            println!(
                "herdr-top: TUI arrives with the vertical-slice TUI task; scaffold exits cleanly."
            );
            ExitCode::SUCCESS
        }
        Some(Command::Emit { strict }) => {
            println!("NotImplementedInVerticalSlice: Controller event input");
            if strict {
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            }
        }
        Some(Command::Doctor) => {
            // `resolver_source` and `state_root` become real when session_key (T2) and lockfile
            // (T3) land; `controller_socket` stays NotImplementedInVerticalSlice for this slice.
            println!("resolver_source: unavailable (scaffold; wired in a later slice task)");
            println!("state_root: unavailable (scaffold; wired in a later slice task)");
            println!("controller_socket: NotImplementedInVerticalSlice");
            ExitCode::SUCCESS
        }
    }
}
