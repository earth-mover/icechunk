use crate::repository::VersionInfo;
use clap::{Args, Parser, Subcommand};
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::pin;

use anyhow::{Context, Result};

use crate::storage::new_local_filesystem_storage;
use crate::{Repository, Storage};

#[derive(Debug, Parser)]
#[clap()]
pub struct IcechunkCLI {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(subcommand)]
    Repo(RepoCommand),
}

#[derive(Debug, Subcommand)]
enum RepoCommand {
    #[clap(name = "create")]
    Create(CreateCommand),
    #[clap(name = "list")]
    List(ListCommand),
}

#[derive(Debug, Args)]
struct CreateCommand {
    /// The path to the IceChunk repository.
    path: PathBuf,
}

#[derive(Debug, Args)]
struct ListCommand {
    /// The path to the IceChunk repository.
    path: PathBuf,
}

async fn repo_create(init_cmd: CreateCommand) -> Result<()> {
    let storage: Arc<dyn Storage + Send + Sync> =
        new_local_filesystem_storage(init_cmd.path.as_path())
            .await
            .context(format!("❌ Failed to create storage at {:?}", init_cmd.path))?;
    Repository::create(None, Arc::clone(&storage), HashMap::new())
        .await
        .context(format!("❌ Failed to create repository at {:?}", init_cmd.path))?;

    println!("✅ Created repository at {:?}", init_cmd.path);

    Ok(())
}

async fn repo_list(list_cmd: ListCommand) -> Result<()> {
    let storage: Arc<dyn Storage + Send + Sync> =
        new_local_filesystem_storage(list_cmd.path.as_path())
            .await
            .context(format!("❌ Failed to create storage at {:?}", list_cmd.path))?;
    let repository =
        Repository::open(None, Arc::clone(&storage), HashMap::new())
            .await
            .context(format!("❌ Failed to open repository at {:?}", list_cmd.path))?;

    let snapshot_id =
        repository.lookup_branch("main").await.context("❌ Failed to lookup branch")?;
    let ancestry = repository.ancestry(&VersionInfo::SnapshotId(snapshot_id)).await?;
    pin!(ancestry);
    while let Some(snapshot_info) = ancestry.next().await {
        println!("{:?}", snapshot_info?);
    }
    Ok(())
}

pub async fn run_cli(args: IcechunkCLI) -> Result<()> {
    match args.cmd {
        Command::Repo(RepoCommand::Create(init_cmd)) => repo_create(init_cmd).await,
        Command::Repo(RepoCommand::List(list_cmd)) => repo_list(list_cmd).await,
    }
}
