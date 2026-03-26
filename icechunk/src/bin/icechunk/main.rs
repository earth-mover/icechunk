//! Icechunk CLI binary entry point.

use anyhow::Result;
use clap::Parser as _;
use icechunk::cli::interface::{IcechunkCLI, run_cli};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = IcechunkCLI::parse();
    run_cli(cli).await
}
