use anyhow::Result;
use clap::Parser;
use icechunk::cli::interface::{run_cli, IcechunkCLI};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = IcechunkCLI::parse();
    run_cli(cli).await
}
