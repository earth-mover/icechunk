//! Generators of fixtures for reading old repositories that only Rust can
//! write: checked-in ones read by `icechunk-python/tests/test_can_read_old.py`,
//! and ones kept outside the repository.

use std::{error::Error, path::Path};

use chrono::Utc;
use icechunk::format::{
    OVERWRITTEN_FILES_PATH, REPO_INFO_FILE_PATH,
    format_constants::SpecVersionBin,
    repo_info::DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE,
    repo_info_file::{self, new_backup_path},
    writer_features::FeatureName,
};

/// Writes `icechunk-python/tests/data/test-repo-v2-unknown-feature`: a copy of
/// `test-repo-v2` whose repo info lists `test-feature-v1`, a writer feature no
/// Icechunk implements, added as a library implementing it would add it (the
/// replaced repo info backed up to `overwritten/`). Refuses to overwrite.
///
/// Does nothing unless `ICECHUNK_GENERATE_CAN_READ_OLD_FIXTURE` is set, so that
/// `just test-integration` (which runs every ignored test) leaves the fixture
/// alone. To regenerate it, delete the directory, then run
/// `ICECHUNK_GENERATE_CAN_READ_OLD_FIXTURE=1 cargo test -p icechunk --test main generate_unknown_feature_fixture -- --ignored`.
#[icechunk_macros::test]
#[ignore = "generates a checked-in fixture"]
fn generate_unknown_feature_fixture() -> Result<(), Box<dyn Error>> {
    if std::env::var_os("ICECHUNK_GENERATE_CAN_READ_OLD_FIXTURE").is_none() {
        println!(
            "skipped: set ICECHUNK_GENERATE_CAN_READ_OLD_FIXTURE to generate the fixture"
        );
        return Ok(());
    }
    let data =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../icechunk-python/tests/data");
    let dir = data.join("test-repo-v2-unknown-feature");
    if dir.exists() {
        return Err(format!("{} exists, delete it first", dir.display()).into());
    }
    #[expect(clippy::create_dir, reason = "never generates into an existing directory")]
    std::fs::create_dir(&dir)?;
    let options = fs_extra::dir::CopyOptions::new().content_only(true);
    fs_extra::dir::copy(data.join("test-repo-v2"), &dir, &options)?;
    add_test_feature(&dir)
}

/// Adds `test-feature-v1` to the standard-layout repository at `dir` with one
/// status change, backing the replaced repo info up to `overwritten/`.
fn add_test_feature(dir: &Path) -> Result<(), Box<dyn Error>> {
    let repo = dir.join(REPO_INFO_FILE_PATH);
    let bytes = std::fs::read(&repo)?;
    let info = repo_info_file::decode(&bytes)?;
    let backup = new_backup_path();
    std::fs::write(dir.join(OVERWRITTEN_FILES_PATH).join(&backup), &bytes)?;
    let with_feature = info.with_writer_features_for_tests(
        SpecVersionBin::current(),
        &[FeatureName::new("test-feature-v1")],
        &[],
        &backup,
        Utc::now(),
        info.num_updates_per_repo_info_file()?
            .unwrap_or(DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE),
    )?;
    std::fs::write(
        &repo,
        repo_info_file::encode(&with_feature, SpecVersionBin::current(), 3)?,
    )?;
    Ok(())
}

#[cfg(all(feature = "fs", unix))]
const DEEP_OPS_LOG_PAGE_SIZE: u16 = 50;
#[cfg(all(feature = "fs", unix))]
const DEEP_OPS_LOG_CYCLES: i8 = 100;

/// Writes, at the absolute path in `ICECHUNK_GENERATE_DEEP_OPS_LOG_FIXTURE`, a
/// standard-layout repository listing `test-feature-v1` (like
/// `test-repo-v2-unknown-feature`) with a long ops log split into pages of
/// [`DEEP_OPS_LOG_PAGE_SIZE`] entries, for checks against older Icechunk
/// releases (not in the repository).
///
/// The history is written in the filesystem layout, where every status change
/// stores the gate with `underlying`: [`DEEP_OPS_LOG_CYCLES`] cycles of a
/// commit, three `ReadOnly`/`Online` status-change pairs, a branch created and
/// deleted and a metadata change. Then it's converted to the standard layout
/// and `test-feature-v1` is added. `group1/small_chunks` (int8, 5 chunks of 1)
/// holds the cycle number: 0 on `dev` and the tag `v0`, the last cycle on
/// `main` and the tag `last`.
///
/// Does nothing unless the variable is set, so that `just test-integration`
/// (which runs every ignored test) writes nothing. Refuses an existing
/// directory, a relative path and a path inside this repository. Run
/// `ICECHUNK_GENERATE_DEEP_OPS_LOG_FIXTURE=<absolute path> cargo nextest run -p icechunk --test main --run-ignored only generate_deep_ops_log_fixture`.
#[cfg(all(feature = "fs", unix))]
#[icechunk_macros::tokio_test]
#[ignore = "generates a fixture outside the repository"]
async fn generate_deep_ops_log_fixture() -> Result<(), Box<dyn Error>> {
    use std::collections::BTreeMap;

    use icechunk::{
        Repository, RepositoryConfig,
        format::repo_info::{RepoAvailability, RepoStatus},
        storage::{ConversionOutcome, FilesystemStorage, new_filesystem_storage},
    };

    const VAR: &str = "ICECHUNK_GENERATE_DEEP_OPS_LOG_FIXTURE";
    let Some(dir) = std::env::var_os(VAR) else {
        println!("skipped: set {VAR} to the absolute path to generate the fixture at");
        return Ok(());
    };
    let dir = Path::new(&dir);
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").canonicalize()?;
    if !dir.is_absolute() {
        return Err(format!("{VAR} must be an absolute path: {}", dir.display()).into());
    }
    if dir.starts_with(&workspace) {
        return Err(format!("{} is inside {}", dir.display(), workspace.display()).into());
    }
    if dir.exists() {
        return Err(format!("{} exists, delete it first", dir.display()).into());
    }
    if let Some(parent) = dir.parent() {
        std::fs::create_dir_all(parent)?;
    }
    #[expect(clippy::create_dir, reason = "never generates into an existing directory")]
    std::fs::create_dir(dir)?;

    let status = |availability, limited_availability_reason| RepoStatus {
        availability,
        set_at: Utc::now(),
        limited_availability_reason,
    };
    let config = RepositoryConfig {
        num_updates_per_repo_info_file: Some(DEEP_OPS_LOG_PAGE_SIZE),
        ..Default::default()
    };
    let repo = Repository::create(new_filesystem_storage(dir).await?)
        .config(config)
        .execute()
        .await?;
    let mut tip = None;
    for cycle in 0..DEEP_OPS_LOG_CYCLES {
        let snapshot = commit_cycle(&repo, cycle).await?;
        if cycle == 0 {
            repo.create_branch("dev", &snapshot).await?;
            repo.create_tag("v0", &snapshot).await?;
        }
        for step in 0..3 {
            let reason = format!("deep-ops-log cycle {cycle} step {step}");
            repo.set_status(&status(RepoAvailability::ReadOnly, Some(reason))).await?;
            repo.set_status(&status(RepoAvailability::Online, None)).await?;
            match step {
                0 => {
                    repo.create_branch("scratch", &snapshot).await?;
                    repo.delete_branch("scratch").await?;
                }
                1 => {
                    let metadata = BTreeMap::from([("cycle".to_string(), cycle.into())]);
                    repo.set_metadata(&metadata).await?;
                }
                _ => {}
            }
        }
        tip = Some(snapshot);
    }
    repo.create_tag("last", &tip.ok_or("no cycles")?).await?;
    drop(repo);

    let outcome = FilesystemStorage::new(dir)?.convert_to_standard_layout().await?;
    if outcome != ConversionOutcome::Converted {
        return Err(format!("the conversion returned {outcome:?}").into());
    }
    add_test_feature(dir)
}

/// Commits `cycle` into every element of `group1/small_chunks` on `main`,
/// creating the hierarchy in cycle 0.
#[cfg(all(feature = "fs", unix))]
async fn commit_cycle(
    repo: &icechunk::Repository,
    cycle: i8,
) -> Result<icechunk::format::SnapshotId, Box<dyn Error>> {
    use std::sync::Arc;

    use bytes::Bytes;
    use icechunk::Store;
    use tokio::sync::RwLock;

    let session = Arc::new(RwLock::new(repo.writable_session("main").await?));
    let store = Store::from_session(Arc::clone(&session)).await;
    if cycle == 0 {
        let group = Bytes::from_static(br#"{"zarr_format":3,"node_type":"group"}"#);
        store.set("zarr.json", group.clone()).await?;
        store.set("group1/zarr.json", group).await?;
        store
            .set(
                "group1/small_chunks/zarr.json",
                Bytes::from_static(
                    br#"{"zarr_format":3,"node_type":"array","shape":[5],"data_type":"int8","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes"}],"attributes":{}}"#,
                ),
            )
            .await?;
    }
    for index in 0..5 {
        store
            .set(
                &format!("group1/small_chunks/c/{index}"),
                Bytes::from(cycle.to_le_bytes().to_vec()),
            )
            .await?;
    }
    let message = format!("deep-ops-log cycle {cycle}");
    Ok(session.write().await.commit(message).execute().await?)
}
