use crate::repository::VersionInfo;
use clap::{Args, Parser, Subcommand};
use futures::stream::StreamExt;
use serde_yaml_ng;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::pin;

use anyhow::{Context, Result};

use crate::storage::new_local_filesystem_storage;
use crate::{new_s3_storage, Repository, Storage};

use crate::cli::config::{Repositories, RepositoryAlias, RepositoryDefinition};

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
    #[command(subcommand)]
    Snapshot(SnapshotCommand),
}

#[derive(Debug, Subcommand)]
enum RepoCommand {
    #[clap(name = "create")]
    Create(CreateCommand),
}

#[derive(Debug, Subcommand)]
enum SnapshotCommand {
    #[clap(name = "list")]
    List(ListCommand),
}

#[derive(Debug, Args)]
struct CreateCommand {
    repo: RepositoryAlias,
}

#[derive(Debug, Args)]
struct ListCommand {
    repo: RepositoryAlias,
}

fn load_repositories() -> Result<Repositories> {
    let path = PathBuf::from("default.yaml");
    let file = std::fs::File::open(path).context("❌ Failed to open config")?;
    let deserialized: Repositories = serde_yaml_ng::from_reader(file)?;
    Ok(deserialized)
}

async fn get_storage(
    repository_definition: &RepositoryDefinition,
) -> Result<Arc<dyn Storage + Send + Sync>> {
    match repository_definition {
        RepositoryDefinition::LocalFileSystem { path, .. } => {
            let storage = new_local_filesystem_storage(&path)
                .await
                .context(format!("❌ Failed to create storage at {:?}", path))?;
            Ok(storage)
        }
        RepositoryDefinition::S3 {
            location, object_store_config, credentials, ..
        } => {
            let storage = new_s3_storage(
                object_store_config.clone(),
                location.bucket.clone(),
                Some(location.prefix.clone()),
                Some(crate::config::S3Credentials::from(credentials.clone())),
            )?;
            Ok(storage)
        }
    }
}

async fn repo_create(init_cmd: CreateCommand) -> Result<()> {
    let repos = load_repositories()?;
    let repo = repos.repos.get(&init_cmd.repo).context("❌ Repository not found")?;
    let storage = get_storage(repo).await?;

    let config = Some(repo.get_config().clone());

    Repository::create(config, Arc::clone(&storage), HashMap::new())
        .await
        .context(format!("❌ Failed to create repository {:?}", init_cmd.repo))?;

    println!("✅ Created repository {:?}", init_cmd.repo);

    Ok(())
}

async fn snapshot_list(list_cmd: ListCommand) -> Result<()> {
    let repos = load_repositories()?;
    let repo = repos.repos.get(&list_cmd.repo).context("❌ Repository not found")?;
    let storage = get_storage(repo).await?;
    let config = Some(repo.get_config().clone());

    let repository = Repository::open(config, Arc::clone(&storage), HashMap::new())
        .await
        .context(format!("❌ Failed to open repository {:?}", list_cmd.repo))?;

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
        Command::Snapshot(SnapshotCommand::List(list_cmd)) => {
            snapshot_list(list_cmd).await
        }
    }
    .map_err(|e| {
        eprintln!("❌ CLI Error: {:#}", e);

        let mut source = e.source();
        while let Some(cause) = source {
            eprintln!("   ↳ Caused by: {}", cause);
            source = cause.source();
        }

        e
    })
}
