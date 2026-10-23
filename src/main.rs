//! litecd: GitOps continuous delivery that reads Argo CD's manifests
//! unchanged. See docs/spec.md for the plan; today only `check` works.

mod check;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(
    name = "litecd",
    version,
    about = "GitOps continuous delivery for Argo CD manifests, in one small binary"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Report which features your Argo CD manifests use and which litecd phase supports them
    Check {
        /// Files or directories with Application, AppProject and ApplicationSet manifests
        #[arg(short = 'f', long = "filename", required = true)]
        paths: Vec<PathBuf>,
        #[arg(short, long, value_enum, default_value_t = Output::Text)]
        output: Output,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum Output {
    Text,
    Json,
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Check { paths, output } => {
            let objects = check::scan(&paths)?;
            match output {
                Output::Text => print!("{}", check::report(&objects)),
                Output::Json => println!("{}", serde_json::to_string_pretty(&objects)?),
            }
        }
    }
    Ok(())
}
