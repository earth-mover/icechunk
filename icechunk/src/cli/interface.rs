use crate::config::S3Options;
use crate::repository::VersionInfo;
use clap::{Args, Parser, Subcommand};
use dialoguer::{Input, Select};
use futures::stream::StreamExt;
use serde_yaml_ng;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::pin;

use anyhow::{Context, Ok, Result};

use crate::storage::new_local_filesystem_storage;
use crate::{new_s3_storage, Repository, RepositoryConfig, Storage};

use crate::cli::config::{Repositories, RepositoryAlias, RepositoryDefinition};

use super::config::{RepoLocation, S3Credentials};

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
    #[command(subcommand)]
    Config(ConfigCommand),
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

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    #[clap(name = "init")]
    Init,
    #[clap(name = "list")]
    List,
}

#[derive(Debug, Args)]
struct CreateCommand {
    repo: RepositoryAlias,
}

#[derive(Debug, Args)]
struct ListCommand {
    repo: RepositoryAlias,
    #[arg(short = 'n', default_value_t = 10, help = "Number of snapshots to list")]
    n: usize,
    #[arg(
        short = 'b',
        long = "branch",
        default_value = "main",
        help = "Branch to list snapshots from"
    )]
    branch: String,
}

const CONFIG_PATH: &str = "default.yaml";

fn load_repositories() -> Result<Repositories> {
    let path = PathBuf::from(CONFIG_PATH);
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
                .context(format!("❌ Failed to create storage at {}", path))?;
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

    let branch_ref = VersionInfo::BranchTipRef(list_cmd.branch.clone());
    let ancestry = repository.ancestry(&branch_ref).await?;
    pin!(ancestry);
    ancestry
        .take(list_cmd.n)
        .for_each(|snapshot| async move {
            println!("{:?}", snapshot);
        })
        .await;

    Ok(())
}

async fn config_init() -> Result<()> {
    let mut repositories =
        load_repositories().unwrap_or(Repositories { repos: HashMap::new() });

    let alias: String = Input::new()
        .with_prompt("Enter alias")
        .validate_with(|input: &String| {
            if repositories.repos.contains_key(&RepositoryAlias(input.clone())) {
                Err(anyhow::anyhow!("Alias already exists"))
            } else {
                Ok(())
            }
        })
        .interact()
        .context("❌ Failed to get alias")?;

    let repo_types = vec!["Local", "S3"];

    let repo_type = Select::new()
        .with_prompt("Select repository type")
        .items(&repo_types)
        .default(0)
        .interact()
        .context("❌ Failed to select repository type")?;

    let repo = match repo_type {
        0 => {
            let path: String = Input::new()
                .with_prompt("Enter path")
                .interact()
                .context("❌ Failed to get path")?;
            RepositoryDefinition::LocalFileSystem {
                path: std::path::PathBuf::from(path),
                config: RepositoryConfig::default(),
            }
        }
        1 => {
            let bucket: String = Input::new()
                .with_prompt("Enter bucket")
                .interact()
                .context("❌ Failed to get bucket")?;
            let prefix: String = Input::new()
                .with_prompt("Enter prefix")
                .interact()
                .context("❌ Failed to get prefix")?;
            let region: String = Input::new()
                .with_prompt("Enter region")
                .interact()
                .context("❌ Failed to get region")?;

            RepositoryDefinition::S3 {
                location: RepoLocation { bucket, prefix },
                object_store_config: S3Options {
                    region: Some(region),
                    endpoint_url: None,
                    anonymous: false,
                    allow_http: false,
                },
                credentials: S3Credentials::FromEnv,
                config: RepositoryConfig::default(),
            }
        }
        _ => unreachable!(),
    };

    repositories.repos.insert(RepositoryAlias(alias), repo);

    let path = PathBuf::from(CONFIG_PATH);
    let file = std::fs::File::create(path).context("❌ Failed to create config file")?;
    serde_yaml_ng::to_writer(file, &repositories)?;

    Ok(())
}

async fn config_list() -> Result<()> {
    let repositories = load_repositories()?;
    let serialized = serde_yaml_ng::to_string(&repositories)?;
    println!("{}", serialized);

    Ok(())
}

pub async fn run_cli(args: IcechunkCLI) -> Result<()> {
    match args.cmd {
        Command::Repo(RepoCommand::Create(init_cmd)) => repo_create(init_cmd).await,
        Command::Snapshot(SnapshotCommand::List(list_cmd)) => {
            snapshot_list(list_cmd).await
        }
        Command::Config(ConfigCommand::Init) => config_init().await,
        Command::Config(ConfigCommand::List) => config_list().await,
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
