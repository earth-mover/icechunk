use crate::repository::VersionInfo;
use clap::{Args, Parser, Subcommand};
use dialoguer::{Input, Select};
use futures::stream::StreamExt;
use serde_yaml_ng;
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Ok, Result};

use crate::storage::{
    new_azure_blob_storage, new_gcs_storage, new_local_filesystem_storage,
    new_tigris_storage,
};
use crate::{new_s3_storage, Repository, RepositoryConfig, Storage};

use crate::cli::config::{CliConfig, RepositoryAlias, RepositoryDefinition};
use crate::config::{AzureCredentials, GcsCredentials, S3Credentials, S3Options};
use dirs::config_dir;

use super::config::{AzureRepoLocation, RepoLocation};

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
    #[clap(
        name = "add",
        about = concat!(
            "Interactively add a repository to the config. ",
            "Config is created if it doesn't exist."
        )
    )]
    Add,
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

const CONFIG_DIR: &str = "icechunk";
const CONFIG_NAME: &str = "cli-config.yaml";

fn config_path() -> PathBuf {
    let mut path = config_dir().unwrap();
    path.push(CONFIG_DIR);
    path.push(CONFIG_NAME);
    path
}

fn load_config() -> Result<CliConfig> {
    let path = config_path();
    let file = File::open(path).context("Failed to open config")?;
    let deserialized: CliConfig =
        serde_yaml_ng::from_reader(file).context("Failed to parse config")?;
    Ok(deserialized)
}

fn write_config(config: &CliConfig) -> Result<(), anyhow::Error> {
    let path = config_path();
    create_dir_all(path.parent().unwrap())
        .context("Failed to create config directory")?;
    let file = File::create(path).context("Failed to create config file")?;
    serde_yaml_ng::to_writer(file, &config).context("Failed to write config to file")?;
    Ok(())
}

async fn get_storage(
    repository_definition: &RepositoryDefinition,
) -> Result<Arc<dyn Storage + Send + Sync>> {
    match repository_definition {
        RepositoryDefinition::LocalFileSystem { path, .. } => {
            let storage = new_local_filesystem_storage(path)
                .await
                .context(format!("Failed to create storage at {:?}", path))?;
            Ok(storage)
        }
        RepositoryDefinition::S3 {
            location, object_store_config, credentials, ..
        } => {
            let storage = new_s3_storage(
                object_store_config.clone(),
                location.bucket.clone(),
                location.prefix.clone(),
                Some(credentials.clone()),
            )
            .context("Failed to create S3 storage")?;
            Ok(storage)
        }
        RepositoryDefinition::Tigris {
            location,
            object_store_config,
            credentials,
            ..
        } => {
            let storage = new_tigris_storage(
                object_store_config.clone(),
                location.bucket.clone(),
                location.prefix.clone(),
                Some(credentials.clone()),
            )
            .context("Failed to create Tigris storage")?;
            Ok(storage)
        }
        RepositoryDefinition::GCS {
            location, object_store_config, credentials, ..
        } => {
            let storage = new_gcs_storage(
                location.bucket.clone(),
                location.prefix.clone(),
                Some(credentials.clone()),
                Some(object_store_config.clone()),
            )
            .await
            .context("Failed to create GCS storage")?;
            Ok(storage)
        }
        RepositoryDefinition::Azure {
            location,
            object_store_config,
            credentials,
            ..
        } => {
            let storage = new_azure_blob_storage(
                location.account.clone(),
                location.container.clone(),
                location.prefix.clone(),
                Some(credentials.clone()),
                Some(object_store_config.clone()),
            )
            .await
            .context("Failed to create Azure storage")?;
            Ok(storage)
        }
    }
}

async fn repo_create(init_cmd: CreateCommand, config: CliConfig) -> Result<()> {
    let repo =
        config.repos.get(&init_cmd.repo).context("Repository not found in config")?;
    let storage = get_storage(repo).await?;

    let config = Some(repo.get_config().clone());

    Repository::create(config, Arc::clone(&storage), HashMap::new())
        .await
        .context(format!("Failed to create repository {:?}", init_cmd.repo))?;

    println!("✅ Created repository {:?}", init_cmd.repo);

    Ok(())
}

async fn snapshot_list(list_cmd: ListCommand, config: CliConfig) -> Result<()> {
    let repo =
        config.repos.get(&list_cmd.repo).context("Repository not found in config")?;
    let storage = get_storage(repo).await?;
    let config = Some(repo.get_config().clone());

    let repository = Repository::open(config, Arc::clone(&storage), HashMap::new())
        .await
        .context(format!("Failed to open repository {:?}", list_cmd.repo))?;

    let branch_ref = VersionInfo::BranchTipRef(list_cmd.branch.clone());
    let ancestry = repository.ancestry(&branch_ref).await?;
    ancestry
        .take(list_cmd.n)
        .for_each(|snapshot| async move {
            println!("{:?}", snapshot);
        })
        .await;

    Ok(())
}

async fn config_add(config: &CliConfig) -> Result<CliConfig> {
    let alias: String = Input::new()
        .with_prompt("Enter alias")
        .validate_with(|input: &String| {
            if config.repos.contains_key(&RepositoryAlias(input.clone())) {
                Err(anyhow::anyhow!("Alias already exists"))
            } else {
                Ok(())
            }
        })
        .interact()
        .context("Failed to get alias")?;

    let repo_types = vec!["Local", "S3", "Tigris", "GCS", "Azure"];

    let repo_type = Select::new()
        .with_prompt("Select repository type")
        .items(&repo_types)
        .default(0)
        .interact()
        .context("Failed to select repository type")?;

    let repo = match repo_types[repo_type] {
        "Local" => {
            let path: String = Input::new()
                .with_prompt("Enter path")
                .interact()
                .context("Failed to get path")?;
            RepositoryDefinition::LocalFileSystem {
                path: PathBuf::from(path),
                config: RepositoryConfig::default(),
            }
        }
        "S3" => {
            let bucket: String = Input::new()
                .with_prompt("Enter bucket")
                .interact()
                .context("Failed to get bucket")?;
            let prefix: String = Input::new()
                .with_prompt("Enter prefix")
                .interact()
                .context("Failed to get prefix")?;
            let region: String = Input::new()
                .with_prompt("Enter region")
                .interact()
                .context("Failed to get region")?;

            RepositoryDefinition::S3 {
                location: RepoLocation { bucket, prefix: Some(prefix) },
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
        "Tigris" => {
            let bucket: String = Input::new()
                .with_prompt("Enter bucket")
                .interact()
                .context("Failed to get bucket")?;
            let prefix: String = Input::new()
                .with_prompt("Enter prefix")
                .interact()
                .context("Failed to get prefix")?;
            let region: String = Input::new()
                .with_prompt("Enter region")
                .interact()
                .context("Failed to get region")?;
            let endpoint_url: String = Input::new()
                .with_prompt("Enter endpoint URL")
                .interact()
                .context("Failed to get endpoint URL")?;

            RepositoryDefinition::Tigris {
                location: RepoLocation { bucket, prefix: Some(prefix) },
                object_store_config: S3Options {
                    region: Some(region),
                    endpoint_url: Some(endpoint_url),
                    anonymous: false,
                    allow_http: false,
                },
                credentials: S3Credentials::FromEnv,
                config: RepositoryConfig::default(),
            }
        }
        "GCS" => {
            let bucket: String = Input::new()
                .with_prompt("Enter bucket")
                .interact()
                .context("Failed to get bucket")?;
            let prefix: String = Input::new()
                .with_prompt("Enter prefix")
                .interact()
                .context("Failed to get prefix")?;

            RepositoryDefinition::GCS {
                location: RepoLocation { bucket, prefix: Some(prefix) },
                object_store_config: HashMap::new(),
                credentials: GcsCredentials::FromEnv,
                config: RepositoryConfig::default(),
            }
        }
        "Azure" => {
            let account: String = Input::new()
                .with_prompt("Enter account")
                .interact()
                .context("Failed to get account")?;
            let container: String = Input::new()
                .with_prompt("Enter container")
                .interact()
                .context("Failed to get container")?;
            let prefix: String = Input::new()
                .with_prompt("Enter prefix")
                .interact()
                .context("Failed to get prefix")?;

            RepositoryDefinition::Azure {
                location: AzureRepoLocation { account, container, prefix: Some(prefix) },
                object_store_config: HashMap::new(),
                credentials: AzureCredentials::FromEnv,
                config: RepositoryConfig::default(),
            }
        }

        _ => unreachable!(),
    };

    let mut new_config = (*config).clone();
    new_config.repos.insert(RepositoryAlias(alias), repo);

    Ok(new_config)
}

async fn config_list(config: CliConfig) -> Result<()> {
    let serialized = serde_yaml_ng::to_string(&config)?;
    println!("{}", serialized);

    Ok(())
}

pub async fn run_cli(args: IcechunkCLI) -> Result<()> {
    let config = load_config().unwrap_or_default();
    match args.cmd {
        Command::Repo(RepoCommand::Create(init_cmd)) => {
            repo_create(init_cmd, config).await
        }
        Command::Snapshot(SnapshotCommand::List(list_cmd)) => {
            snapshot_list(list_cmd, config).await
        }
        Command::Config(ConfigCommand::Add) => {
            let new_config = config_add(&config).await?;
            write_config(&new_config)?;
            Ok(())
        }
        Command::Config(ConfigCommand::List) => config_list(config).await,
    }
    .map_err(|e| anyhow::anyhow!("❌ {}", e))
}
