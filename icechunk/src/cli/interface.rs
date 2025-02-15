use crate::repository::VersionInfo;
use clap::{Args, Parser, Subcommand};
use futures::stream::StreamExt;
use serde_yaml_ng as serde_yaml;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::pin;

use anyhow::{Context, Result};

use crate::storage::new_local_filesystem_storage;
use crate::{new_s3_storage, ObjectStoreConfig, Repository, Storage};

use crate::cli::config::{
    Credentials, Repositories, RepositoryAlias, RepositoryDefinition, S3Credentials,
};

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
    repo: RepositoryAlias,
}

#[derive(Debug, Args)]
struct ListCommand {
    repo: RepositoryAlias,
}

fn load_repositories() -> Result<Repositories> {
    let path = PathBuf::from("default.yaml");
    let file = std::fs::File::open(path)?;
    let deserialized: Repositories = serde_yaml::from_reader(file)?;
    Ok(deserialized)
}

impl From<S3Credentials> for crate::config::S3Credentials {
    fn from(s3_credentials: S3Credentials) -> Self {
        match s3_credentials {
            S3Credentials::Anonymous => crate::config::S3Credentials::Anonymous,
            S3Credentials::FromEnv => crate::config::S3Credentials::FromEnv,
            S3Credentials::Static(credentials) => {
                crate::config::S3Credentials::Static(credentials)
            }
        }
    }
}

async fn get_storage(
    repository_definition: &RepositoryDefinition,
) -> Result<Arc<dyn Storage + Send + Sync>> {
    let object_store_config = repository_definition.object_store_config.clone();

    match object_store_config {
        ObjectStoreConfig::LocalFileSystem(path) => {
            let storage = new_local_filesystem_storage(&path)
                .await
                .context(format!("❌ Failed to create storage at {:?}", path))?;
            Ok(storage)
        }
        ObjectStoreConfig::S3(options) => {
            let location =
                repository_definition.location.as_ref().context("❌ Missing location")?;

            if let Credentials::S3(creds) = &repository_definition.credentials {
                let storage = new_s3_storage(
                    options,
                    location.bucket.clone(),
                    Some(location.prefix.clone()),
                    Some(crate::config::S3Credentials::from(creds.clone())),
                )?;
                Ok(storage)
            } else {
                Err(anyhow::anyhow!("❌ Unsupported S3 credentials"))
            }
        }
        _ => Err(anyhow::anyhow!("❌ Unsupported object store config")),
    }
}

async fn repo_create(init_cmd: CreateCommand) -> Result<()> {
    let repos = load_repositories()?;
    let repo = repos.repos.get(&init_cmd.repo).context("❌ Repository not found")?;
    let storage = get_storage(repo).await?;

    let config = Some(repo.config.clone());

    Repository::create(config, Arc::clone(&storage), HashMap::new())
        .await
        .context(format!("❌ Failed to create repository at {:?}", init_cmd.repo))?;

    println!("✅ Created repository at {:?}", init_cmd.repo);

    Ok(())
}

async fn repo_list(list_cmd: ListCommand) -> Result<()> {
    let repos = load_repositories()?;
    let repo = repos.repos.get(&list_cmd.repo).context("❌ Repository not found")?;
    let storage = get_storage(repo).await?;
    let config = Some(repo.config.clone());

    let repository = Repository::open(config, Arc::clone(&storage), HashMap::new())
        .await
        .context(format!("❌ Failed to open repository at {:?}", list_cmd.repo))?;

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
