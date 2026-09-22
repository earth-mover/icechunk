#![allow(clippy::expect_used, clippy::unwrap_used)]
//! Benchmark tool for garbage collection and repo stats.
//!
//! `build` writes a synthetic repository on local `RustFS`; `gc` and `stats`
//! run the operations against it and print timings and request metrics.

mod build;
mod cli;
mod list;
mod report;
mod run;
mod storage;
mod toxi;

use clap::Parser as _;

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cli = cli::Cli::parse();
    match cli.command {
        cli::Command::Build(args) => build::build(args).await,
        cli::Command::Gc(args) => run::gc(args).await,
        cli::Command::Stats(args) => run::stats(args).await,
        cli::Command::List(args) => list::list(args).await,
    }
}
