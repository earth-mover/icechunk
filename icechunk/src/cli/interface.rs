//! CLI command definitions and handlers.

use crate::format::{SnapshotId, snapshot::SnapshotInfo};
use crate::repository::VersionInfo;
use chrono::Local;
use clap::{Args, Parser, Subcommand};
use dialoguer::{Input, Select};
use futures::stream::StreamExt as _;
use serde_yaml_ng;
use std::collections::HashMap;
use std::fs::{File, create_dir_all};
use std::io::stdout;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, Result};

use crate::storage::{
    new_azure_blob_storage, new_gcs_storage, new_local_filesystem_storage,
    new_tigris_storage,
};
use crate::{Repository, RepositoryConfig, Storage, new_s3_storage};

use crate::cli::config::{CliConfig, RepositoryAlias, RepositoryDefinition};
use crate::config::{AzureCredentials, GcsCredentials, S3Credentials, S3Options};
use dirs::config_dir;

use super::config::{AzureRepoLocation, RepoLocation};

#[derive(Debug, Parser)]
#[command()]
pub struct IcechunkCLI {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(subcommand, about = "Manage configuration")]
    Config(ConfigCommand),
    #[command(subcommand, about = "Manage repositories")]
    Repo(RepoCommand),
    #[command(name = "ancestry", about = "Show ancestry of a branch, tag or snapshot")]
    Ancestry(AncestryArgs),
    #[command(subcommand, about = "Manage branches")]
    Branch(BranchCommand),
    #[command(subcommand, about = "Manage tags")]
    Tag(TagCommand),
}

#[derive(Debug, Subcommand)]
enum RepoCommand {
    #[command(name = "create", about = "Create a repository")]
    Create(CreateCommand),
}

#[derive(Debug, Args)]
struct AncestryArgs {
    #[arg(name = "alias", help = "Alias of the repository in the config")]
    repo: RepositoryAlias,
    #[arg(
        name = "reference",
        default_value = "main",
        help = "Branch, tag, or snapshot id to show ancestry for"
    )]
    reference: String,
    #[arg(short = 'n', default_value_t = 10, help = "Number of snapshots to list")]
    n: usize,
}

#[derive(Debug, Subcommand)]
enum BranchCommand {
    #[command(name = "list", about = "List branches")]
    List(BranchListArgs),
    #[command(name = "create", about = "Create branch")]
    Create(BranchCreateArgs),
    #[command(name = "delete", about = "Delete branch")]
    Delete(BranchDeleteArgs),
}

#[derive(Debug, Args)]
struct BranchListArgs {
    #[arg(name = "alias", help = "Alias of the repository in the config")]
    repo: RepositoryAlias,
}

#[derive(Debug, Args)]
struct BranchCreateArgs {
    #[arg(name = "alias", help = "Alias of the repository in the config")]
    repo: RepositoryAlias,
    #[arg(name = "branch", help = "Name of the branch to create")]
    branch: String,
    #[arg(name = "from", help = "Branch, tag, or snapshot id to create the branch from")]
    from: String,
}

#[derive(Debug, Args)]
struct BranchDeleteArgs {
    #[arg(name = "alias", help = "Alias of the repository in the config")]
    repo: RepositoryAlias,
    #[arg(name = "branch", help = "Name of the branch to delete")]
    branch: String,
}

#[derive(Debug, Subcommand)]
enum TagCommand {
    #[command(name = "list", about = "List tags")]
    List(TagListArgs),
    #[command(name = "create", about = "Create tag")]
    Create(TagCreateArgs),
    #[command(name = "delete", about = "Delete tag")]
    Delete(TagDeleteArgs),
}

#[derive(Debug, Args)]
struct TagListArgs {
    #[arg(name = "alias", help = "Alias of the repository in the config")]
    repo: RepositoryAlias,
}

#[derive(Debug, Args)]
struct TagCreateArgs {
    #[arg(name = "alias", help = "Alias of the repository in the config")]
    repo: RepositoryAlias,
    #[arg(name = "tag", help = "Name of the tag to create")]
    tag: String,
    #[arg(name = "target", help = "Branch, tag, or snapshot id to create the tag for")]
    target: String,
}

#[derive(Debug, Args)]
struct TagDeleteArgs {
    #[arg(name = "alias", help = "Alias of the repository in the config")]
    repo: RepositoryAlias,
    #[arg(name = "tag", help = "Name of the tag to delete")]
    tag: String,
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    #[command(name = "init", about = "Interactively create a new config file.")]
    Init(InitCommand),
    #[command(
        name = "add",
        about = concat!(
            "Interactively add a repository to the config. ",
            "Config is created if it doesn't exist."
        )
    )]
    Add(AddCommand),
    #[command(name = "list", about = "Print the current config.")]
    List,
}

#[derive(Debug, Args)]
struct CreateCommand {
    #[arg(name = "alias", help = "Alias of the repository in the config")]
    repo: RepositoryAlias,
}

#[derive(Debug, Args)]
struct InitCommand {
    #[arg(
        short = 'f',
        long = "force",
        help = "Overwrite existing config",
        default_value = "false"
    )]
    force: bool,
}

#[derive(Debug, Args)]
struct AddCommand {
    #[arg(name = "alias", help = "Alias of the repository in the config")]
    repo: RepositoryAlias,
}

const CONFIG_DIR: &str = "icechunk";
const CONFIG_NAME: &str = "cli-config.yaml";

fn config_path() -> Result<PathBuf, anyhow::Error> {
    let mut path =
        config_dir().ok_or(anyhow::anyhow!("Config file path cannot be found"))?;
    path.push(CONFIG_DIR);
    path.push(CONFIG_NAME);
    Ok(path)
}

fn load_config() -> Result<CliConfig> {
    let path = config_path()?;
    let file = File::open(path).context("Failed to open config")?;
    let deserialized: CliConfig =
        serde_yaml_ng::from_reader(file).context("Failed to parse config")?;
    Ok(deserialized)
}

fn write_config(config: &CliConfig) -> Result<(), anyhow::Error> {
    let path = config_path()?;
    create_dir_all(path.parent().ok_or(anyhow::anyhow!("No parent directory"))?)
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
                .context(format!("Failed to create storage at {path:?}"))?;
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
                Vec::new(),
                Vec::new(),
                None, // auto-detect key layout
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
                false,
                Vec::new(),
                Vec::new(),
                None, // auto-detect key layout
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
                Vec::new(),
                Vec::new(),
            )
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

async fn open_repository(
    repo_alias: &RepositoryAlias,
    config: &CliConfig,
) -> Result<Repository> {
    let repo = config
        .repos
        .get(repo_alias)
        .context(format!("Repository {repo_alias:?} not found in config"))?;
    let storage = get_storage(repo).await?;
    let config = Some(repo.get_config().clone());

    let repository = Repository::open(config, storage, HashMap::new())
        .await
        .context(format!("Failed to open repository {repo_alias:?}"))?;

    Ok(repository)
}

async fn parse_reference(repository: &Repository, reference: &str) -> Result<SnapshotId> {
    if let Ok(snapshot_id) = SnapshotId::try_from(reference) {
        return Ok(snapshot_id);
    }
    if let Ok(snapshot_id) = repository.lookup_branch(reference).await {
        return Ok(snapshot_id);
    }
    if let Ok(snapshot_id) = repository.lookup_tag(reference).await {
        return Ok(snapshot_id);
    }
    Err(anyhow::anyhow!("`{reference}` is not a valid snapshot id, branch, or tag"))
}

async fn repo_create(init_cmd: &CreateCommand, config: &CliConfig) -> Result<()> {
    let repo =
        config.repos.get(&init_cmd.repo).context("Repository not found in config")?;
    let storage = get_storage(repo).await?;

    let config = Some(repo.get_config().clone());

    Repository::create(config, storage, HashMap::new(), None, true)
        .await
        .context(format!("Failed to create repository {:?}", init_cmd.repo))?;

    println!("✅ Created repository {:?}", init_cmd.repo);

    Ok(())
}

async fn list_branches(
    args: &BranchListArgs,
    config: &CliConfig,
    mut writer: impl std::io::Write,
) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;
    let branches = repository.list_branches().await.context("Failed to list branches")?;
    for branch in branches {
        let snapshot = repository.lookup_branch(branch.as_str()).await?;
        writeln!(writer, "{snapshot}  {branch}")?;
    }
    Ok(())
}

async fn create_branch(args: &BranchCreateArgs, config: &CliConfig) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;
    let from_snapshot = parse_reference(&repository, &args.from).await?;
    repository.create_branch(&args.branch, &from_snapshot).await.context(format!(
        "Failed to create branch {:?} from {:?}",
        args.branch, args.from
    ))?;

    println!(
        "✅ Created branch {:?} from {:?} in repository {:?}",
        args.branch, args.from, args.repo
    );

    Ok(())
}

async fn delete_branch(args: &BranchDeleteArgs, config: &CliConfig) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;

    repository
        .delete_branch(&args.branch)
        .await
        .context(format!("Failed to delete branch {:?}", args.branch))?;

    println!("✅ Deleted branch {:?} in repository {:?}", args.branch, args.repo);

    Ok(())
}

async fn list_tags(
    args: &TagListArgs,
    config: &CliConfig,
    mut writer: impl std::io::Write,
) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;
    let tags = repository.list_tags().await.context("Failed to list tags")?;
    for tag in tags {
        let snapshot = repository.lookup_tag(tag.as_str()).await?;
        writeln!(writer, "{snapshot}  {tag}")?;
    }
    Ok(())
}

async fn create_tag(args: &TagCreateArgs, config: &CliConfig) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;
    let target_snapshot = parse_reference(&repository, &args.target).await?;

    repository
        .create_tag(&args.tag, &target_snapshot)
        .await
        .context(format!("Failed to create tag {:?} from {:?}", args.tag, args.target))?;

    println!(
        "✅ Created tag {:?} for {:?} in repository {:?}",
        args.tag, target_snapshot, args.repo
    );

    Ok(())
}

async fn delete_tag(args: &TagDeleteArgs, config: &CliConfig) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;

    repository
        .delete_tag(&args.tag)
        .await
        .context(format!("Failed to delete tag {:?}", args.tag))?;

    println!("✅ Deleted tag {:?} in repository {:?}", args.tag, args.repo);

    Ok(())
}

fn show_snapshot(
    mut writer: impl std::io::Write,
    snapshot: SnapshotInfo,
    with_meta: bool,
) -> Result<()> {
    writeln!(writer, "Snapshot: {}", snapshot.id)?;
    writeln!(
        writer,
        "Date: {}",
        snapshot.flushed_at.with_timezone(&Local).format("%B %d %Y  %H:%M:%S")
    )?;
    if with_meta && !snapshot.metadata.is_empty() {
        writeln!(writer, "Metadata:")?;
        for (key, value) in snapshot.metadata {
            writeln!(writer, "  {key}: {value}")?;
        }
    }
    if !snapshot.message.is_empty() {
        writeln!(writer, "\n  {}", snapshot.message)?;
    }
    Ok(())
}

async fn ancestry(
    args: &AncestryArgs,
    config: &CliConfig,
    mut writer: impl std::io::Write,
) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;
    let snapshot = parse_reference(&repository, &args.reference).await?;
    let mut ancestry = Box::pin(
        repository.ancestry(&VersionInfo::SnapshotId(snapshot)).await?.take(args.n),
    );
    while let Some(snapshot) = ancestry.next().await {
        show_snapshot(&mut writer, snapshot.context("Failed to get snapshot")?, false)?;
        writeln!(writer)?;
    }
    Ok(())
}

async fn config_add(add_cmd: &AddCommand, config: &CliConfig) -> Result<CliConfig> {
    if config.repos.contains_key(&add_cmd.repo) {
        return Err(anyhow::anyhow!("Repository {:?} already exists", add_cmd.repo));
    }

    let alias = add_cmd.repo.clone();
    let new_config = add_repo_to_config(&alias, config)?;

    Ok(new_config)
}

async fn config_init(init_cmd: &InitCommand, config: &CliConfig) -> Result<CliConfig> {
    if !config.repos.is_empty() && !init_cmd.force {
        return Err(anyhow::anyhow!(
            "Config already exists and contains repositories. Use --force to overwrite."
        ));
    }

    let alias: String = Input::new()
        .with_prompt("Enter alias")
        .interact()
        .context("Failed to get alias")?;

    let repo = add_repo_to_config(&RepositoryAlias(alias), &CliConfig::default())?;

    Ok(repo)
}

fn add_repo_to_config(
    repo_alias: &RepositoryAlias,
    config: &CliConfig,
) -> Result<CliConfig> {
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
                object_store_config: S3Options::default().with_region(region),
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
                object_store_config: S3Options::default()
                    .with_region(region)
                    .with_endpoint_url(endpoint_url),
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
    new_config.repos.insert((*repo_alias).clone(), repo);

    Ok(new_config)
}

async fn config_list(config: &CliConfig, mut writer: impl std::io::Write) -> Result<()> {
    let serialized = serde_yaml_ng::to_string(&config)?;
    writeln!(writer, "{serialized}")?;

    Ok(())
}

pub async fn run_cli(args: IcechunkCLI) -> Result<()> {
    let config = load_config().unwrap_or_default();
    match args.cmd {
        Command::Repo(RepoCommand::Create(init_cmd)) => {
            repo_create(&init_cmd, &config).await
        }
        Command::Ancestry(ancestry_args) => {
            ancestry(&ancestry_args, &config, stdout()).await?;
            Ok(())
        }
        Command::Branch(BranchCommand::List(list_args)) => {
            list_branches(&list_args, &config, stdout()).await?;
            Ok(())
        }
        Command::Branch(BranchCommand::Create(create_args)) => {
            create_branch(&create_args, &config).await?;
            Ok(())
        }
        Command::Branch(BranchCommand::Delete(delete_args)) => {
            delete_branch(&delete_args, &config).await?;
            Ok(())
        }
        Command::Tag(TagCommand::List(list_args)) => {
            list_tags(&list_args, &config, stdout()).await?;
            Ok(())
        }
        Command::Tag(TagCommand::Create(create_args)) => {
            create_tag(&create_args, &config).await?;
            Ok(())
        }
        Command::Tag(TagCommand::Delete(delete_args)) => {
            delete_tag(&delete_args, &config).await?;
            Ok(())
        }
        Command::Config(ConfigCommand::Init(init_cmd)) => {
            let new_config = config_init(&init_cmd, &config).await?;
            write_config(&new_config)?;
            Ok(())
        }
        Command::Config(ConfigCommand::Add(add_cmd)) => {
            let new_config = config_add(&add_cmd, &config).await?;
            write_config(&new_config)?;
            Ok(())
        }
        Command::Config(ConfigCommand::List) => config_list(&config, stdout()).await,
    }
    .map_err(|e| anyhow::anyhow!("❌ {e}"))
}

#[cfg(test)]
mod tests {
    use std::fs::read_dir;

    use icechunk_macros::tokio_test;

    use super::*;

    use regex::Regex;

    #[tokio_test]
    async fn test_repo_create() {
        let temp = assert_fs::TempDir::new().unwrap();
        let path = temp.path().to_path_buf();

        let repo_alias = RepositoryAlias("test-repo".to_string());
        let repo = RepositoryDefinition::LocalFileSystem {
            path: path.clone(),
            config: RepositoryConfig::default(),
        };

        let mut repos = HashMap::new();
        repos.insert(repo_alias.clone(), repo);

        let config = CliConfig { repos };

        let init_cmd = CreateCommand { repo: repo_alias.clone() };

        repo_create(&init_cmd, &config).await.unwrap();

        let repo = path.join("repo");
        let snapshots = path.join("snapshots");

        assert!(repo.is_file());
        assert!(snapshots.is_dir());

        let mut snapshots_contents = read_dir(snapshots).unwrap();
        assert!(snapshots_contents.next().is_some());
    }

    #[tokio_test]
    async fn test_config_list() {
        let temp = assert_fs::TempDir::new().unwrap();
        let path = temp.path().to_path_buf();

        let repo_alias = RepositoryAlias("test-repo".to_string());
        let repo = RepositoryDefinition::LocalFileSystem {
            path: path.clone(),
            config: RepositoryConfig::default(),
        };

        let mut repos = HashMap::new();
        repos.insert(repo_alias.clone(), repo);

        let config = CliConfig { repos };

        let mut writer = Vec::new();

        config_list(&config, &mut writer).await.unwrap();

        let output = String::from_utf8(writer).unwrap();

        assert!(output.contains("LocalFileSystem"));
    }

    #[tokio_test]
    async fn test_ancestry() {
        let temp = assert_fs::TempDir::new().unwrap();
        let path = temp.path().to_path_buf();

        let repo_alias = RepositoryAlias("test-repo".to_string());
        let repo = RepositoryDefinition::LocalFileSystem {
            path: path.clone(),
            config: RepositoryConfig::default(),
        };

        let mut repos = HashMap::new();
        repos.insert(repo_alias.clone(), repo);

        let config = CliConfig { repos };

        let init_cmd = CreateCommand { repo: repo_alias.clone() };
        repo_create(&init_cmd, &config).await.unwrap();

        let repository = open_repository(&repo_alias, &config).await.unwrap();
        let main_tip = repository.lookup_branch("main").await.unwrap();

        let args = AncestryArgs {
            repo: repo_alias.clone(),
            reference: main_tip.to_string(),
            n: 10,
        };
        let mut writer = Vec::new();
        ancestry(&args, &config, &mut writer).await.unwrap();
        let output = String::from_utf8(writer).unwrap();
        println!("{}", output);

        let re = Regex::new(
            r"Snapshot: [0-9A-Z]{20}
Date: \w+ \d{2} \d+  \d{2}:\d{2}:\d{2}

  Repository initialized
",
        )
        .unwrap();
        assert!(re.is_match(output.as_str()));
    }

    async fn setup_test_repo(
        temp: &assert_fs::TempDir,
    ) -> (RepositoryAlias, CliConfig, SnapshotId) {
        let path = temp.path().to_path_buf();

        let repo_alias = RepositoryAlias("test-repo".to_string());
        let repo = RepositoryDefinition::LocalFileSystem {
            path,
            config: RepositoryConfig::default(),
        };

        let mut repos = HashMap::new();
        repos.insert(repo_alias.clone(), repo);
        let config = CliConfig { repos };

        let init_cmd = CreateCommand { repo: repo_alias.clone() };
        repo_create(&init_cmd, &config).await.unwrap();

        let repository = open_repository(&repo_alias, &config).await.unwrap();
        let root = repository.lookup_branch("main").await.unwrap();

        (repo_alias, config, root)
    }

    #[tokio_test]
    async fn test_branch_lifecycle() {
        let temp = assert_fs::TempDir::new().unwrap();
        let (repo_alias, config, root) = setup_test_repo(&temp).await;

        create_branch(
            &BranchCreateArgs {
                repo: repo_alias.clone(),
                branch: "feature".to_string(),
                from: root.to_string(),
            },
            &config,
        )
        .await
        .unwrap();

        let mut writer = Vec::new();
        list_branches(&BranchListArgs { repo: repo_alias.clone() }, &config, &mut writer)
            .await
            .unwrap();
        let output = String::from_utf8(writer).unwrap();
        assert!(output.contains("feature"));
        assert!(output.contains("main"));

        delete_branch(
            &BranchDeleteArgs { repo: repo_alias.clone(), branch: "feature".to_string() },
            &config,
        )
        .await
        .unwrap();

        let mut writer = Vec::new();
        list_branches(&BranchListArgs { repo: repo_alias.clone() }, &config, &mut writer)
            .await
            .unwrap();
        let output = String::from_utf8(writer).unwrap();
        assert!(!output.contains("feature"));
        assert!(output.contains("main"));
    }

    #[tokio_test]
    async fn test_tag_lifecycle() {
        let temp = assert_fs::TempDir::new().unwrap();
        let (repo_alias, config, root) = setup_test_repo(&temp).await;

        create_tag(
            &TagCreateArgs {
                repo: repo_alias.clone(),
                tag: "v1".to_string(),
                target: root.to_string(),
            },
            &config,
        )
        .await
        .unwrap();

        let mut writer = Vec::new();
        list_tags(&TagListArgs { repo: repo_alias.clone() }, &config, &mut writer)
            .await
            .unwrap();
        let output = String::from_utf8(writer).unwrap();
        assert!(output.contains("v1"));

        delete_tag(
            &TagDeleteArgs { repo: repo_alias.clone(), tag: "v1".to_string() },
            &config,
        )
        .await
        .unwrap();

        let mut writer = Vec::new();
        list_tags(&TagListArgs { repo: repo_alias.clone() }, &config, &mut writer)
            .await
            .unwrap();
        let output = String::from_utf8(writer).unwrap();
        assert!(!output.contains("v1"));
    }

    #[tokio_test]
    async fn test_parse_reference() {
        let temp = assert_fs::TempDir::new().unwrap();
        let (repo_alias, config, root) = setup_test_repo(&temp).await;
        let repository = open_repository(&repo_alias, &config).await.unwrap();

        create_tag(
            &TagCreateArgs {
                repo: repo_alias.clone(),
                tag: "v1".to_string(),
                target: root.to_string(),
            },
            &config,
        )
        .await
        .unwrap();

        // resolves a raw snapshot id
        assert_eq!(parse_reference(&repository, &root.to_string()).await.unwrap(), root);

        // resolves a branch name
        assert_eq!(parse_reference(&repository, "main").await.unwrap(), root);

        // resolves a tag name
        assert_eq!(parse_reference(&repository, "v1").await.unwrap(), root);

        // errors clearly on something that is none of the above
        let err = parse_reference(&repository, "does-not-exist").await.unwrap_err();
        assert!(err.to_string().contains("does-not-exist"));
    }
}
