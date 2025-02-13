use clap::Parser;
use icechunk::cli::interface::{run_cli, IcechunkCLI};

#[tokio::main]
async fn main() {
    let cli = IcechunkCLI::parse();
    let _ = run_cli(cli).await;
}
