use crate::format::ChunkIndices;
use crate::format::snapshot::{NodeData, DimensionName};
use crate::repository::{VersionInfo, RepositoryErrorKind};
use chrono::{Local};
use clap::{Args, Parser, Subcommand};
use dialoguer::{Input, Select};
use futures::stream::{StreamExt};
use itertools::Itertools;
use serde_yaml_ng;
use std::collections::{HashMap, HashSet};
use std::fs::{File, create_dir_all};
use std::io::stdout;
use std::path::PathBuf;
use std::sync::Arc;
use std::iter::zip;

use anyhow::{Context, Ok, Result};

use crate::storage::{
    new_azure_blob_storage, new_gcs_storage, new_local_filesystem_storage,
    new_tigris_storage,
};
use crate::{Repository, RepositoryConfig, Storage, new_s3_storage};
use crate::format::{SnapshotId, snapshot::SnapshotInfo};

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
    #[command(subcommand, about = "Manage configuration")]
    Config(ConfigCommand),
    #[command(subcommand, about = "Manage repositories")]
    Repo(RepoCommand),
    #[clap(name = "history", about = "Show history of a branch, tag or snapshot")]
    History(HistoryArgs),
    #[clap(name = "inspect", about = "Show snapshot details")]
    Inspect(InspectArgs),
    #[command(subcommand, about = "Manage branches")]
    Branch(BranchCommand),
    #[command(subcommand, about = "Manage tags")]
    Tag(TagCommand),
    #[clap(name = "diff", about = "Show diff between two refs.")]
    Diff(DiffArgs)
}

#[derive(Debug, Subcommand)]
enum RepoCommand {
    #[clap(name = "create", about = "Create a repository")]
    Create(CreateCommand),
}

#[derive(Debug, Args)]
struct HistoryArgs {
    #[arg(name = "alias", help = "Alias of the repository in the config")]
    repo: RepositoryAlias,
    #[arg(
        name = "reference",
        help = "ID of snapshot to show history for"
    )]
    reference: String,
    #[arg(short = 'n', default_value_t = 10, help = "Number of snapshots to list")]
    n: usize,
}

#[derive(Debug, Args)]
struct InspectArgs {
    #[arg(name = "alias", help = "Alias of the repository in the config")]
    repo: RepositoryAlias,
    #[arg(
        name = "reference",
        help = "ID of snapshot to inspect"
    )]
    reference: String,
}

#[derive(Debug, Subcommand)]
enum BranchCommand {
    #[clap(name = "list", about = "List branches")]
    List(BranchListArgs),
    #[clap(name = "create", about = "Create branch")]
    Create(BranchCreateArgs),
    #[clap(name = "delete", about = "Delete branch")]
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
    #[arg(
        name = "from",
        help = "ID of snapshot to create the branch from"
    )]
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
    #[clap(name = "list", about = "List tags")]
    List(TagListArgs),
    #[clap(name = "create", about = "Create tag")]
    Create(TagCreateArgs),
    #[clap(name = "delete", about = "Delete tag")]
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
    #[arg(
        name = "target",
        help = "ID of snapshot to create the tag for"
    )]
    target: String,
}

#[derive(Debug, Args)]
struct TagDeleteArgs {
    #[arg(name = "alias", help = "Alias of the repository in the config")]
    repo: RepositoryAlias,
    #[arg(name = "tag", help = "Name of the tag to delete")]
    tag: String,
}

#[derive(Debug, Args)]
struct DiffArgs {
    #[arg(name = "alias", help = "Alias of the repository in the config")]
    repo: RepositoryAlias,
    #[arg(name = "from", help = "Source snapshot ID")]
    from: String,
    #[arg(name = "to", help = "Target snapshot ID")]
    to: String,
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    #[clap(name = "init", about = "Interactively create a new config file.")]
    Init(InitCommand),
    #[clap(
        name = "add",
        about = concat!(
            "Interactively add a repository to the config. ",
            "Config is created if it doesn't exist."
        )
    )]
    Add(AddCommand),
    #[clap(name = "list", about = "Print the current config.")]
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
    let repo =
        config.repos.get(repo_alias).context(format!("Repository {:?} not found in config", repo_alias))?;
    let storage = get_storage(repo).await?;
    let config = Some(repo.get_config().clone());

    let repository = Repository::open(config, Arc::clone(&storage), HashMap::new())
        .await
        .context(format!("Failed to open repository {:?}", repo_alias))?;

    Ok(repository)
}

fn parse_snapshot(reference: &String) -> Result<SnapshotId> {
    let snapshot_id = SnapshotId::try_from(reference.as_str())
        .map_err(|_| RepositoryErrorKind::InvalidSnapshotId(reference.clone()))?;
    Ok(snapshot_id)
}

async fn repo_create(init_cmd: &CreateCommand, config: &CliConfig) -> Result<()> {
    let repo =
        config.repos.get(&init_cmd.repo).context("Repository not found in config")?;
    let storage = get_storage(repo).await?;

    let config = Some(repo.get_config().clone());

    Repository::create(config, Arc::clone(&storage), HashMap::new(), None)
        .await
        .context(format!("Failed to create repository {:?}", init_cmd.repo))?;

    println!("✅ Created repository {:?}", init_cmd.repo);

    Ok(())
}

async fn list_branches(
    args: &BranchListArgs,
    config: &CliConfig,
) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;
    let branches = repository.list_branches().await.context("Failed to list branches")?;
    for branch in branches {
        let snapshot = repository.lookup_branch(branch.as_str()).await?;
        println!("{}  {}", snapshot, branch);
    }
    Ok(())
}

async fn create_branch(
    args: &BranchCreateArgs,
    config: &CliConfig,
) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;
    let from_snapshot = parse_snapshot(&args.from)?;
    repository
        .create_branch(&args.branch, &from_snapshot)
        .await
        .context(format!(
            "Failed to create branch {:?} from {:?}",
            args.branch, args.from
        ))?;

    println!(
        "✅ Created branch {:?} from {:?} in repository {:?}",
        args.branch, args.from, args.repo
    );

    Ok(())
}

async fn delete_branch(
    args: &BranchDeleteArgs,
    config: &CliConfig,
) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;

    repository
        .delete_branch(&args.branch)
        .await
        .context(format!("Failed to delete branch {:?}", args.branch))?;

    println!(
        "✅ Deleted branch {:?} in repository {:?}",
        args.branch, args.repo
    );

    Ok(())
}

async fn list_tags(
    args: &TagListArgs,
    config: &CliConfig,
) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;
    let tags = repository.list_tags().await.context("Failed to list tags")?;
    for tag in tags {
        let snapshot = repository.lookup_tag(tag.as_str()).await?;
        println!("{}  {}", snapshot, tag);
    }
    Ok(())
}

async fn create_tag(
    args: &TagCreateArgs,
    config: &CliConfig,
) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;
    let target_snapshot = parse_snapshot(&args.target)?;
    
    repository
        .create_tag(&args.tag, &target_snapshot)
        .await
        .context(format!(
            "Failed to create tag {:?} from {:?}",
            args.tag, args.target
        ))?;

    println!(
        "✅ Created tag {:?} for {:?} in repository {:?}",
        args.tag, target_snapshot, args.repo
    );

    Ok(())
}

async fn delete_tag(
    args: &TagDeleteArgs,
    config: &CliConfig,
) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;

    repository
        .delete_tag(&args.tag)
        .await
        .context(format!("Failed to delete tag {:?}", args.tag))?;

    println!(
        "✅ Deleted tag {:?} in repository {:?}",
        args.tag, args.repo
    );

    Ok(())
}

fn show_snapshot(mut writer: impl std::io::Write, snapshot: SnapshotInfo, with_meta: bool) -> Result<()> {
    writeln!(writer, "Snapshot: {}", snapshot.id)?;
    writeln!(writer, "Date: {}", snapshot.flushed_at.with_timezone(&Local).format("%B %d %Y  %H:%M:%S"))?;
    if with_meta && !snapshot.metadata.is_empty() {
        writeln!(writer, "Metadata:")?;
        for (key, value) in snapshot.metadata {
            writeln!(writer, "  {}: {}", key, value)?;
        }
    }
    if !snapshot.message.is_empty() {
        writeln!(writer, "\n  {}", snapshot.message)?;
    }
    Ok(())
}

async fn history(
    args: &HistoryArgs,
    config: &CliConfig,
    mut writer: impl std::io::Write,
) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;
    let snapshot = parse_snapshot(&args.reference)?;
    let ancestry = repository.ancestry(&VersionInfo::SnapshotId(snapshot)).await?;
    let snapshots: Vec<_> = ancestry.take(args.n).collect().await;
    for snapshot in snapshots {
        show_snapshot(&mut writer, snapshot.context("Failed to get snapshot")?, false)?;
        writeln!(writer, "")?;
    }
    Ok(())
}


fn array_shape_string(dimension_name: &Option<Vec<DimensionName>>, values: Vec<String>) -> String {
    match dimension_name {
        Some(names) => {
            zip(names.iter(), values.iter())
                .map(|(name, v)| 
                    match name {
                        DimensionName::Name(n) => format!("{}={}", n, v),
                        DimensionName::NotSpecified => format!("{}", v),
                    }
                )
                .join(", ")
        },
        None => {
            values.join(", ")
        }
    }
}

async fn inspect(
    args: &InspectArgs,
    config: &CliConfig,
    mut writer: impl std::io::Write,
) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;
    let snapshot_id = parse_snapshot(&args.reference)?;
    let snapshot = repository.asset_manager().fetch_snapshot(&snapshot_id).await?;

    writeln!(writer, "Snapshot: {}", snapshot_id)?;
    writeln!(writer, "Date: {}", snapshot.flushed_at()?.with_timezone(&Local).format("%B %d %Y  %H:%M:%S"))?;
    let metadata = snapshot.metadata()?;
    if !metadata.is_empty() {
        writeln!(writer, "Metadata:")?;
        for (key, value) in metadata {
            writeln!(writer, "  {}: {}", key, value)?;
        }
    }
    let message = snapshot.message();
    if !message.is_empty() {
        writeln!(writer, "Message:\n  {}", message)?;
    }

    writeln!(writer, "\n-- Nodes --")?;
    for node_info_res in snapshot.iter() {
        let node_info = node_info_res.context("Failed to get snapshot info")?;
        let node_data = node_info.node_data;
        match node_data {
            NodeData::Array {shape, dimension_names, manifests} => {
                let array_size: Vec<_> = shape.iter().map(|s| s.array_length().to_string()).collect();
                let chunk_size: Vec<_> = shape.iter().map(|s| format!(
                    "{}({})",
                    s.chunk_length(),
                    (s.array_length() + s.chunk_length() - 1)/s.chunk_length()
                )).collect();
                writeln!(writer, "{}", node_info.path)?;
                writeln!(writer, "    size: {}", array_shape_string(&dimension_names, array_size))?;
                writeln!(writer, "    chunk: {}", array_shape_string(&dimension_names, chunk_size))?;
                for manifest in manifests {
                    let extents = manifest.extents.iter().map(|e|
                        if e.end - 1 == e.start {
                            e.start.to_string()
                        } else {
                            format!("{}-{}", e.start, e.end - 1)
                        }
                    ).collect();
                    writeln!(writer, "    manifest: {} {}",
                        manifest.object_id,
                        array_shape_string(&dimension_names, extents)
                    )?;
                }
            },
            NodeData::Group => {
                writeln!(writer, "{}", node_info.path)?;
            },
        }
    }

    writeln!(writer, "\n-- Manifests --")?;
    for manifest_info in snapshot.manifest_files() {
        writeln!(writer, "{}", manifest_info.id)?;
        writeln!(writer, "    num chunk refs: {}", manifest_info.num_chunk_refs)?;
        // TODO: Human-friendly size
        writeln!(writer, "    size (bytes): {}", manifest_info.size_bytes)?;
    }


    Ok(())
}

async fn diff(
    args: &DiffArgs,
    config: &CliConfig,
    mut writer: impl std::io::Write,
) -> Result<()> {
    let repository = open_repository(&args.repo, config).await?;

    let from_ref = VersionInfo::SnapshotId(parse_snapshot(&args.from)?);
    let to_ref = VersionInfo::SnapshotId(parse_snapshot(&args.to)?);

    let diff = repository
        .diff(&from_ref, &to_ref)
        .await
        .context(format!(
            "Failed to compute diff between {:?} and {:?}",
            args.from, args.to
        ))?;

    let new_arrays_hash: HashSet<_> = diff.new_arrays.iter().cloned().collect();
    let new_groups_hash: HashSet<_> = diff.new_groups.iter().cloned().collect();
    let deleted_arrays_hash: HashSet<_> = diff.deleted_arrays.iter().cloned().collect();
    let deleted_groups_hash: HashSet<_> = diff.deleted_groups.iter().cloned().collect();
    let updated_arrays_hash: HashSet<_> = diff.updated_arrays.iter().cloned().collect();
    let updated_groups_hash: HashSet<_> = diff.updated_groups.iter().cloned().collect();
    let updated_chunks_hash: HashSet<_> = diff.updated_chunks.keys().cloned().collect();

    let modified_paths = {
        let mut modified_paths: Vec<_> = new_arrays_hash
            .union(&new_groups_hash)
            .cloned()
            .collect::<HashSet<_>>()
            .union(&deleted_arrays_hash)
            .cloned()
            .collect::<HashSet<_>>()
            .union(&deleted_groups_hash)
            .cloned()
            .collect::<HashSet<_>>()
            .union(&updated_arrays_hash)
            .cloned()
            .collect::<HashSet<_>>()
            .union(&updated_groups_hash)
            .cloned()
            .collect::<HashSet<_>>()
            .union(&updated_chunks_hash)
            .cloned()
            .collect();

        modified_paths.sort();
        modified_paths
    };


    for path in modified_paths {
        let is_new = new_arrays_hash.contains(&path) || new_groups_hash.contains(&path);
        let is_deleted = deleted_arrays_hash.contains(&path) || deleted_groups_hash.contains(&path);
        let is_updated = updated_arrays_hash.contains(&path) || updated_groups_hash.contains(&path);
        let has_updated_chunks = updated_chunks_hash.contains(&path);

        // Sometimes a path will be new or deleted and also have updated chunks.
        // In that case we do not print updated chunks as they are all new or all deleted.
        if is_new && is_deleted {
            writeln!(writer, "-+ {}", path.to_string())?;
            continue;
        }
        else if is_new {
            writeln!(writer, "+  {}", path.to_string())?;
            continue;
        } else if is_deleted {
            writeln!(writer, "-  {}", path.to_string())?;
            continue;
        } else if is_updated {
            writeln!(writer, "~  {}", path.to_string())?;
        } else if has_updated_chunks {
            writeln!(writer, "   {}", path.to_string())?;
        }

        fn chunk_to_string(c: &ChunkIndices) -> String {
            let idxs = c.0.iter().map(|i| i.to_string()).join(", ");
            format!("({})", idxs)
        }

        let updated_chunks = diff.updated_chunks.get(&path);
        match updated_chunks {
            Some(chunks) => {
                write!(writer, "  Modified {} chunk(s): ", chunks.len())?;
                let t = chunks.iter().take(10).map(chunk_to_string).join(", ");
                write!(writer, "  {}", t)?;
                if chunks.len() > 10 {
                    writeln!(writer, ", ...")?;
                } else {
                    writeln!(writer, "")?;
                }
            }
            None => {}
        }
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
                object_store_config: S3Options {
                    region: Some(region),
                    endpoint_url: None,
                    anonymous: false,
                    allow_http: false,
                    force_path_style: false,
                    network_stream_timeout_seconds: None,
                    requester_pays: false,
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
                    force_path_style: false,
                    network_stream_timeout_seconds: None,
                    requester_pays: false,
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
        Command::History(history_args) => {
            history(&history_args, &config, stdout()).await?;
            Ok(())
        }
        Command::Inspect(inspect_args) => {
            inspect(&inspect_args, &config, stdout()).await?;
            Ok(())
        }
        Command::Branch(BranchCommand::List(list_args)) => {
            list_branches(&list_args, &config).await?;
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
            list_tags(&list_args, &config).await?;
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
        Command::Diff(diff_args) => {
            diff(&diff_args, &config, stdout()).await?;
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
#[allow(clippy::unwrap_used)]
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
    async fn test_history() {
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

        let args = HistoryArgs {
            repo: repo_alias.clone(),
            reference: "main".to_string(),
            n: 10,
        };
        let mut writer = Vec::new();
        history(&args, &config, &mut writer).await.unwrap();
        let output = String::from_utf8(writer).unwrap();
        println!("{}", output);

        let re = Regex::new(
r"Snapshot: [0-9A-Z]{20}
Date: \w+ \d{2} \d+  \d{2}:\d{2}:\d{2}

  Repository initialized
"
        ).unwrap();
        assert!(re.is_match(output.as_str()));
    }
}
