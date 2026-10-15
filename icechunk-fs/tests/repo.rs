//! `repo` through the public `Storage` API: the `repo.d` protocol of design
//! document 020.
#![cfg(unix)]
#![expect(clippy::unwrap_used, clippy::panic)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io,
    num::{NonZeroU16, NonZeroU64},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use futures::TryStreamExt as _;
use icechunk_fs::{FilesystemStorage, FreezeOutcome, MigrateOutcome, UnfreezeOutcome};
use icechunk_macros::tokio_test;
use icechunk_storage::{
    ConcurrencySettings, DeleteObjectsResult, ETag, Generation, GetModifiedResult,
    PutCondition, Settings, Storage as _, StorageContext, StorageErrorKind,
    StorageResult, VersionInfo, VersionedUpdateResult, WriteHint,
};
use tokio::io::AsyncReadExt as _;
use uuid::Uuid;

const V0: &[u8] = b"v0";
const LEGACY: &[u8] = b"a repo info written by local_filesystem_storage";

fn storage(dir: &Path) -> FilesystemStorage {
    FilesystemStorage::new(dir).unwrap()
}

async fn put_conditional(
    st: &FilesystemStorage,
    key: &str,
    content: &[u8],
    condition: PutCondition<'_>,
) -> StorageResult<VersionedUpdateResult> {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    st.put_object_conditional(
        &ctx,
        key,
        content.to_vec().into(),
        None,
        Vec::new(),
        condition,
    )
    .await
}

async fn create(st: &FilesystemStorage, content: &[u8]) -> VersionInfo {
    assert_updated(put_conditional(st, "repo", content, PutCondition::Create).await)
}

async fn update(
    st: &FilesystemStorage,
    content: &[u8],
    base: &VersionInfo,
) -> StorageResult<VersionedUpdateResult> {
    put_conditional(st, "repo", content, PutCondition::IfVersion(base)).await
}

async fn copy(
    st: &FilesystemStorage,
    version: &VersionInfo,
) -> StorageResult<VersionedUpdateResult> {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    st.copy_object(&ctx, "repo", "overwritten/x", None, version).await
}

async fn try_get_conditional(
    st: &FilesystemStorage,
    previous: Option<&VersionInfo>,
) -> StorageResult<Option<(Vec<u8>, VersionInfo)>> {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    Ok(match st.get_object_conditional(&ctx, "repo", previous).await? {
        GetModifiedResult::Modified { mut data, new_version } => {
            let mut buf = Vec::new();
            data.read_to_end(&mut buf).await.unwrap();
            Some((buf, new_version))
        }
        GetModifiedResult::OnLatestVersion => None,
    })
}

async fn get_conditional(
    st: &FilesystemStorage,
    previous: Option<&VersionInfo>,
) -> Option<(Vec<u8>, VersionInfo)> {
    try_get_conditional(st, previous).await.unwrap()
}

async fn get(st: &FilesystemStorage) -> (Vec<u8>, VersionInfo) {
    get_conditional(st, None).await.unwrap()
}

async fn get_object(st: &FilesystemStorage, key: &str) -> StorageResult<Vec<u8>> {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    let mut buf = Vec::new();
    st.get_object(&ctx, key, None).await?.read_to_end(&mut buf).await.unwrap();
    Ok(buf)
}

fn assert_updated(result: StorageResult<VersionedUpdateResult>) -> VersionInfo {
    match result.unwrap() {
        VersionedUpdateResult::Updated { new_version } => new_version,
        VersionedUpdateResult::NotOnLatestVersion => panic!("expected Updated"),
    }
}

fn assert_not_on_latest(result: &StorageResult<VersionedUpdateResult>) {
    assert!(
        matches!(result, Ok(VersionedUpdateResult::NotOnLatestVersion)),
        "{result:?}"
    );
}

fn assert_other_error<T: std::fmt::Debug>(result: StorageResult<T>) -> String {
    let err = result.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::Other(_)), "{err}");
    err.to_string()
}

fn version(etag: &str, generation: Option<&str>) -> VersionInfo {
    VersionInfo {
        etag: Some(ETag(etag.to_string())),
        generation: generation.map(|generation| Generation(generation.to_string())),
    }
}

fn etag(version: &VersionInfo) -> String {
    version.etag().unwrap().clone()
}

fn generation(version: &VersionInfo) -> u64 {
    version.generation().unwrap().parse().unwrap()
}

fn names(dir: &Path) -> BTreeSet<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect()
}

/// The names `repo.d` holds when `version` is current and nothing is in flight.
fn settled(version: &VersionInfo) -> BTreeSet<String> {
    let n = generation(version);
    let h = etag(version);
    BTreeSet::from([
        format!("{n}.{h}"),
        format!("latest.{:015}.{n}.{h}", 999_999_999_999_999 - n),
    ])
}

/// Every file and directory under `dir`, relative to it.
fn tree(dir: &Path) -> BTreeSet<PathBuf> {
    fn walk(base: &Path, dir: &Path, out: &mut BTreeSet<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            out.insert(path.strip_prefix(base).unwrap().to_path_buf());
            if path.is_dir() {
                walk(base, &path, out);
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(dir, dir, &mut out);
    out
}

#[tokio_test]
async fn create_read_update_read() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());

    let v0 = create(&st, V0).await;
    assert_eq!(v0, version(&etag(&v0), Some("0")));
    assert_eq!(get(&st).await, (V0.to_vec(), v0.clone()));
    assert_eq!(get_object(&st, "repo").await.unwrap(), V0);
    assert_eq!(get_conditional(&st, Some(&v0)).await, None);

    let v1 = assert_updated(update(&st, b"v1", &v0).await);
    assert_eq!(generation(&v1), 1);
    assert_ne!(etag(&v1), etag(&v0));
    assert_eq!(get_conditional(&st, Some(&v0)).await, Some((b"v1".to_vec(), v1.clone())));
    assert_eq!(get_conditional(&st, Some(&v1)).await, None);
    assert_eq!(get_object(&st, "repo").await.unwrap(), b"v1");
    assert_eq!(names(&dir.path().join("repo.d")), settled(&v1));
}

#[tokio_test]
async fn a_second_create_is_not_on_latest() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, V0).await;

    // Creating with the same bytes is the outcome of a retry after a lost reply.
    assert_eq!(create(&st, V0).await, v0);
    assert_not_on_latest(
        &put_conditional(&st, "repo", b"other", PutCondition::Create).await,
    );
    assert_eq!(get(&st).await, (V0.to_vec(), v0));
}

#[tokio_test]
async fn a_stale_version_cannot_update_or_back_up() {
    let dir = tempfile::tempdir().unwrap();
    let a = storage(dir.path());
    let b = storage(dir.path());
    create(&a, V0).await;
    let (_, a_base) = get(&a).await;
    let (_, b_base) = get(&b).await;

    assert_updated(update(&a, b"a", &a_base).await);
    assert_not_on_latest(&update(&b, b"b", &b_base).await);
    assert_not_on_latest(&copy(&b, &b_base).await);

    assert_eq!(get(&a).await.0, b"a");
    assert!(!dir.path().join("overwritten").exists());
}

#[tokio_test]
async fn copy_writes_the_bytes_of_its_version() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, V0).await;
    let v1 = assert_updated(update(&st, b"v1", &v0).await);

    assert_eq!(assert_updated(copy(&st, &v1).await), v1);
    assert_eq!(std::fs::read(dir.path().join("overwritten/x")).unwrap(), b"v1");

    // A newer commit whose tidy-up hasn't deleted its base yet.
    let base = dir.path().join("repo.d").join(format!("0.{}", etag(&v0)));
    std::fs::write(&base, V0).unwrap();
    assert_eq!(assert_updated(copy(&st, &v0).await), v0);
    assert_eq!(std::fs::read(dir.path().join("overwritten/x")).unwrap(), V0);

    // Once it has, the base is gone.
    std::fs::remove_file(&base).unwrap();
    assert_not_on_latest(&copy(&st, &v0).await);
    assert_eq!(std::fs::read(dir.path().join("overwritten/x")).unwrap(), V0);
}

#[tokio_test]
async fn copy_of_a_corrupt_version_file_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, V0).await;
    std::fs::write(dir.path().join("repo.d").join(format!("0.{}", etag(&v0))), b"x")
        .unwrap();

    let message = assert_other_error(copy(&st, &v0).await);
    assert!(message.contains(&format!("0.{}", etag(&v0))), "{message}");
    assert!(!dir.path().join("overwritten").exists());
}

#[tokio_test]
async fn a_version_without_etag_or_generation_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, V0).await;

    for invalid in [
        VersionInfo::for_creation(),
        version(&etag(&v0), None),
        VersionInfo { etag: None, ..v0.clone() },
    ] {
        assert_other_error(update(&st, b"v1", &invalid).await);
        assert_other_error(copy(&st, &invalid).await);
    }
    assert_eq!(get(&st).await, (V0.to_vec(), v0));
    assert!(!dir.path().join("overwritten").exists());
}

#[tokio_test]
async fn an_invalid_version_is_an_error_and_touches_nothing() {
    let outer = tempfile::tempdir().unwrap();
    let root = outer.path().join("a").join("b").join("root");
    let st = storage(&root);
    let v0 = create(&st, V0).await;
    let before = tree(outer.path());

    for invalid in [
        version("../../escape", Some("0")),
        version(&etag(&v0).to_uppercase(), Some("0")),
        version(&etag(&v0), Some("../0")),
        version(&etag(&v0), Some("00")),
        version(&etag(&v0), Some("1000000000000000")),
    ] {
        assert_other_error(update(&st, b"v1", &invalid).await);
        assert_other_error(copy(&st, &invalid).await);
    }

    assert_eq!(tree(outer.path()), before);
    assert_eq!(get(&st).await, (V0.to_vec(), v0));
}

#[tokio_test]
async fn a_previous_version_that_does_not_parse_reads_in_full() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, V0).await;

    for previous in [version(&etag(&v0), None), version("x", Some("0"))] {
        assert_eq!(
            get_conditional(&st, Some(&previous)).await,
            Some((V0.to_vec(), v0.clone()))
        );
    }
}

#[tokio_test]
async fn updating_a_missing_repo_is_not_on_latest() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = version(&"0".repeat(64), Some("0"));

    assert_not_on_latest(&update(&st, b"v1", &v0).await);
    assert_not_on_latest(&copy(&st, &v0).await);
    assert_eq!(names(dir.path()), BTreeSet::new());

    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    let err = st.get_object_conditional(&ctx, "repo", None).await.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::ObjectNotFound), "{err}");
    let err = get_object(&st, "repo").await.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::ObjectNotFound), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_updates_from_one_base_exactly_one_wins() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    create(&st, V0).await;
    let (_, base) = get(&st).await;

    let tasks: Vec<_> = (0..16)
        .map(|task| {
            let st = storage(dir.path());
            let base = base.clone();
            tokio::spawn(async move {
                let content = format!("writer {task}").into_bytes();
                let result = update(&st, &content, &base).await.unwrap();
                (content, result)
            })
        })
        .collect();
    let results = futures::future::try_join_all(tasks).await.unwrap();

    let winners: Vec<_> = results
        .iter()
        .filter_map(|(content, result)| match result {
            VersionedUpdateResult::Updated { new_version } => {
                Some((content, new_version))
            }
            VersionedUpdateResult::NotOnLatestVersion => None,
        })
        .collect();
    assert_eq!(winners.len(), 1, "{results:?}");
    let (content, version) = winners[0];
    assert_eq!(get(&st).await, (content.clone(), version.clone()));
    assert_eq!(names(&dir.path().join("repo.d")), settled(version));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn retrying_writers_lose_no_update() {
    const TASKS: usize = 8;
    const SUCCESSES: usize = 5;
    const MAX_ATTEMPTS: usize = 1000;

    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let initial = create(&st, V0).await;

    let tasks: Vec<_> = (0..TASKS)
        .map(|task| {
            let st = storage(dir.path());
            tokio::spawn(async move {
                let mut updates = Vec::new();
                for i in 0..SUCCESSES {
                    let content = format!("{task}-{i}").into_bytes();
                    let mut attempts = 0;
                    loop {
                        attempts += 1;
                        assert!(
                            attempts <= MAX_ATTEMPTS,
                            "task {task} update {i}: no success in {MAX_ATTEMPTS} attempts"
                        );
                        let (_, base) = get(&st).await;
                        match update(&st, &content, &base).await.unwrap() {
                            VersionedUpdateResult::Updated { new_version } => {
                                updates.push((base, new_version));
                                break;
                            }
                            VersionedUpdateResult::NotOnLatestVersion => {}
                        }
                    }
                }
                updates
            })
        })
        .collect();
    let updates: Vec<_> = futures::future::try_join_all(tasks)
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(updates.len(), TASKS * SUCCESSES);
    for (base, new) in &updates {
        assert_eq!(generation(new), generation(base) + 1, "{base:?} -> {new:?}");
    }

    let next: BTreeMap<_, _> =
        updates.iter().map(|(base, new)| (etag(base), new.clone())).collect();
    assert_eq!(next.len(), updates.len(), "a base was replaced twice");
    let mut visited = BTreeSet::new();
    let mut current = initial;
    while let Some(new) = next.get(&etag(&current)) {
        assert!(visited.insert(etag(&current)), "the chain has a cycle");
        current = new.clone();
    }
    assert_eq!(visited.len(), updates.len(), "updates outside the chain");
    assert_eq!(current, get(&st).await.1);
    assert_eq!(generation(&current), (TASKS * SUCCESSES) as u64);
    assert_eq!(names(&dir.path().join("repo.d")), settled(&current));
}

#[tokio_test]
async fn if_version_on_another_key_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = assert_updated(put_conditional(&st, "a", V0, PutCondition::Create).await);

    let message = assert_other_error(
        put_conditional(&st, "a", b"v1", PutCondition::IfVersion(&v0)).await,
    );
    assert!(message.contains("only supported for `repo`"), "{message}");
    assert_eq!(get_object(&st, "a").await.unwrap(), V0);
}

#[tokio_test]
async fn unconditional_writes_of_repo_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, V0).await;
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);

    for hint in [WriteHint::Atomic, WriteHint::NewObject] {
        let message = assert_other_error(
            st.put_object(&ctx, "repo", b"v1".to_vec().into(), None, Vec::new(), hint)
                .await,
        );
        assert!(message.contains("put_object_conditional"), "{message}");
    }
    put_conditional(&st, "a", V0, PutCondition::Create).await.unwrap();
    let a = version(&etag(&v0), None);
    assert_other_error(st.copy_object(&ctx, "a", "repo", None, &a).await);
    assert_eq!(get(&st).await, (V0.to_vec(), v0));
}

#[tokio_test]
async fn delete_batch_keeps_repo() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, V0).await;
    put_conditional(&st, "a", V0, PutCondition::Create).await.unwrap();
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);

    let result = st
        .delete_batch(&ctx, "", vec![("repo".to_string(), 39), ("a".to_string(), 2)])
        .await
        .unwrap();
    assert_eq!(result, DeleteObjectsResult { deleted_objects: 1, deleted_bytes: 2 });
    assert_eq!(get(&st).await, (V0.to_vec(), v0));
    assert!(!dir.path().join("a").exists());
}

#[tokio_test]
async fn protocol_names_are_invalid_keys() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, V0).await;
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    let id = Uuid::new_v4();
    let before = tree(dir.path());

    for key in [
        "repo.d".to_string(),
        format!("repo.d/0.{}", etag(&v0)),
        "repo.d/x".to_string(),
        format!("repo.d.tmp.{id}/x"),
        format!("repo.legacy.{id}"),
        format!("repo.stub.{id}"),
    ] {
        let err = get_object(&st, &key).await.err().unwrap();
        assert!(matches!(err.kind, StorageErrorKind::Other(_)), "{key}: {err}");
        let err = st.get_object_last_modified(&ctx, &key).await.err().unwrap();
        assert!(matches!(err.kind, StorageErrorKind::Other(_)), "{key}: {err}");
        let err = st
            .put_object(
                &ctx,
                &key,
                V0.to_vec().into(),
                None,
                Vec::new(),
                WriteHint::Atomic,
            )
            .await
            .err()
            .unwrap();
        assert!(matches!(err.kind, StorageErrorKind::Other(_)), "{key}: {err}");
        assert_other_error(put_conditional(&st, &key, V0, PutCondition::Create).await);
        assert!(
            st.delete_batch(&ctx, "", vec![(key.clone(), 1)]).await.is_err(),
            "{key}"
        );
        assert!(st.list_objects(&ctx, &key).await.is_err(), "{key}");
    }
    assert_eq!(tree(dir.path()), before);
    // Only at the root: these are ordinary names further down.
    for key in ["a/repo.d", "b/repo.d/x", &format!("c/repo.legacy.{id}")] {
        put_conditional(&st, key, V0, PutCondition::Create).await.unwrap();
        assert_eq!(get_object(&st, key).await.unwrap(), V0, "{key}");
    }
}

#[tokio_test]
async fn a_legacy_repo_must_be_migrated() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    std::fs::write(dir.path().join("repo"), LEGACY).unwrap();

    let message = assert_other_error(try_get_conditional(&st, None).await);
    assert!(message.contains("migrate_legacy_repo"), "{message}");
    let message = assert_other_error(get_object(&st, "repo").await);
    assert!(message.contains("migrate_legacy_repo"), "{message}");
    assert_not_on_latest(&put_conditional(&st, "repo", V0, PutCondition::Create).await);
    assert_eq!(std::fs::read(dir.path().join("repo")).unwrap(), LEGACY);

    assert_eq!(st.migrate_legacy_repo().await.unwrap(), MigrateOutcome::Migrated);
    let (bytes, v0) = get(&st).await;
    assert_eq!(bytes, LEGACY);
    assert_eq!(generation(&v0), 0);
    assert_eq!(
        names(dir.path()),
        BTreeSet::from(["repo".to_string(), "repo.d".to_string()])
    );
    assert_eq!(st.migrate_legacy_repo().await.unwrap(), MigrateOutcome::AlreadyMigrated);

    let v1 = assert_updated(update(&st, b"v1", &v0).await);
    assert_eq!(get(&st).await, (b"v1".to_vec(), v1));
}

#[tokio_test]
async fn migrating_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    assert_eq!(st.migrate_legacy_repo().await.unwrap(), MigrateOutcome::NothingToMigrate);
    assert_eq!(names(dir.path()), BTreeSet::new());
}

#[tokio_test]
async fn an_interrupted_migration_must_be_rerun() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    std::fs::write(dir.path().join(format!("repo.legacy.{}", Uuid::new_v4())), LEGACY)
        .unwrap();

    let message = assert_other_error(try_get_conditional(&st, None).await);
    assert!(message.contains("rerun migrate_legacy_repo"), "{message}");
    assert_eq!(st.migrate_legacy_repo().await.unwrap(), MigrateOutcome::Migrated);
    assert_eq!(get(&st).await.0, LEGACY);
}

const DATA_DIRS: [&str; 5] =
    ["chunks", "manifests", "snapshots", "transactions", "overwritten"];

/// Makes `root/name` a directory that can't be opened, so fsyncing it fails.
/// `false` if this process opens it anyway (root): nothing to test.
fn unopenable_dir(root: &Path, name: &str) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).unwrap();
    File::open(&dir).is_err()
}

fn assert_permission_denied<T: std::fmt::Debug>(result: StorageResult<T>) {
    let err = result.err().unwrap();
    assert!(
        matches!(&err.kind, StorageErrorKind::IOError(err) if err.kind() == io::ErrorKind::PermissionDenied),
        "{err}"
    );
}

#[tokio_test]
async fn a_commit_syncs_every_data_dir_before_the_protocol() {
    for name in DATA_DIRS {
        let dir = tempfile::tempdir().unwrap();
        let st = storage(dir.path());
        let v0 = create(&st, V0).await;
        if !unopenable_dir(dir.path(), name) {
            return;
        }
        assert_permission_denied(update(&st, b"v1", &v0).await);
        assert_eq!(names(&dir.path().join("repo.d")), settled(&v0), "{name}");
    }
}

#[tokio_test]
async fn creation_syncs_the_data_dirs_before_the_protocol() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    if !unopenable_dir(dir.path(), "snapshots") {
        return;
    }
    assert_permission_denied(
        put_conditional(&st, "repo", V0, PutCondition::Create).await,
    );
    assert_eq!(names(dir.path()), BTreeSet::from(["snapshots".to_string()]));
}

#[tokio_test]
async fn migration_syncs_the_data_dirs_before_the_protocol() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    std::fs::write(dir.path().join("repo"), LEGACY).unwrap();
    if !unopenable_dir(dir.path(), "chunks") {
        return;
    }
    assert_permission_denied(st.migrate_legacy_repo().await);
    assert_eq!(
        names(dir.path()),
        BTreeSet::from(["chunks".to_string(), "repo".to_string()])
    );
    assert_eq!(std::fs::read(dir.path().join("repo")).unwrap(), LEGACY);
}

#[tokio_test]
async fn without_fsync_there_is_no_data_dir_barrier() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path()).with_fsync(false);
    std::fs::write(dir.path().join("repo"), LEGACY).unwrap();
    if !unopenable_dir(dir.path(), "chunks") {
        return;
    }
    assert_eq!(st.migrate_legacy_repo().await.unwrap(), MigrateOutcome::Migrated);
    let (_, v0) = get(&st).await;
    assert_updated(update(&st, b"v1", &v0).await);

    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path()).with_fsync(false);
    if !unopenable_dir(dir.path(), "snapshots") {
        return;
    }
    create(&st, V0).await;
}

#[tokio_test]
async fn a_frozen_repo_refuses_commits_until_unfrozen() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, V0).await;

    assert_eq!(st.freeze_repo().await.unwrap(), FreezeOutcome::Frozen(v0.clone()));
    assert_eq!(st.freeze_repo().await.unwrap(), FreezeOutcome::AlreadyFrozen);
    let err = update(&st, b"v1", &v0).await.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::RepositoryFrozen), "{err}");
    assert_eq!(get(&st).await, (V0.to_vec(), v0.clone()));
    assert_eq!(get_conditional(&st, Some(&v0)).await, None);

    let UnfreezeOutcome::Unfrozen(unfrozen) = st.unfreeze_repo().await.unwrap() else {
        panic!("the repo is frozen")
    };
    assert_eq!(unfrozen, version(&etag(&v0), Some("1")));
    assert_eq!(st.unfreeze_repo().await.unwrap(), UnfreezeOutcome::NotFrozen);
    assert_not_on_latest(&update(&st, b"v1", &v0).await);
    let (bytes, base) = get(&st).await;
    assert_eq!((bytes, &base), (V0.to_vec(), &unfrozen));
    let v2 = assert_updated(update(&st, b"v1", &base).await);
    assert_eq!(generation(&v2), 2);
    assert_eq!(get(&st).await, (b"v1".to_vec(), v2.clone()));
    assert_eq!(names(&dir.path().join("repo.d")), settled(&v2));
}

#[tokio_test]
async fn freezing_a_missing_repo_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    for err in
        [st.freeze_repo().await.err().unwrap(), st.unfreeze_repo().await.err().unwrap()]
    {
        assert!(
            matches!(&err.kind, StorageErrorKind::IOError(err) if err.kind() == io::ErrorKind::NotFound),
            "{err}"
        );
        assert!(err.to_string().contains("no repository"), "{err}");
    }
    assert_eq!(names(dir.path()), BTreeSet::new());
}

#[tokio_test]
async fn freezing_a_legacy_repo_names_the_remedy() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    std::fs::write(dir.path().join("repo"), LEGACY).unwrap();
    for message in [
        assert_other_error(st.freeze_repo().await),
        assert_other_error(st.unfreeze_repo().await),
    ] {
        assert!(message.contains("run migrate_legacy_repo once"), "{message}");
    }

    std::fs::rename(
        dir.path().join("repo"),
        dir.path().join(format!("repo.legacy.{}", Uuid::new_v4())),
    )
    .unwrap();
    for message in [
        assert_other_error(st.freeze_repo().await),
        assert_other_error(st.unfreeze_repo().await),
    ] {
        assert!(message.contains("rerun migrate_legacy_repo"), "{message}");
    }

    assert_eq!(st.migrate_legacy_repo().await.unwrap(), MigrateOutcome::Migrated);
    let (_, v0) = get(&st).await;
    assert_eq!(st.freeze_repo().await.unwrap(), FreezeOutcome::Frozen(v0));
}

#[tokio_test]
async fn a_new_instance_refuses_a_repo_overwritten_by_a_legacy_writer() {
    let dir = tempfile::tempdir().unwrap();
    let v0 = create(&storage(dir.path()), V0).await;
    std::fs::write(dir.path().join("repo"), LEGACY).unwrap();

    let st = storage(dir.path());
    let message = assert_other_error(try_get_conditional(&st, None).await);
    assert!(message.contains("manual recovery"), "{message}");
    // A failed check is not cached.
    let message = assert_other_error(update(&st, b"v1", &v0).await);
    assert!(message.contains("manual recovery"), "{message}");
    let message = assert_other_error(storage(dir.path()).freeze_repo().await);
    assert!(message.contains("manual recovery"), "{message}");
    assert_eq!(std::fs::read(dir.path().join("repo")).unwrap(), LEGACY);
}

/// Best effort: the check runs once per instance.
#[tokio_test]
async fn an_instance_in_use_misses_an_overwrite_by_a_legacy_writer() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, V0).await;
    assert_eq!(get(&st).await, (V0.to_vec(), v0.clone()));
    std::fs::write(dir.path().join("repo"), LEGACY).unwrap();

    assert_eq!(get(&st).await, (V0.to_vec(), v0.clone()));
    let v1 = assert_updated(update(&st, b"v1", &v0).await);
    assert_eq!(get(&st).await, (b"v1".to_vec(), v1));
}

#[tokio_test]
async fn a_missing_stub_next_to_repo_d_reads_fine() {
    let dir = tempfile::tempdir().unwrap();
    let v0 = create(&storage(dir.path()), V0).await;
    std::fs::remove_file(dir.path().join("repo")).unwrap();

    let st = storage(dir.path());
    assert_eq!(get(&st).await, (V0.to_vec(), v0.clone()));
    assert_updated(update(&st, b"v1", &v0).await);
}

/// A range longer than the ideal request size is not split into reads of
/// `repo` that could each see a different version.
#[tokio_test(flavor = "multi_thread", worker_threads = 4)]
async fn a_long_ranged_read_of_repo_sees_one_version() {
    const LEN: usize = 64 * 1024;
    const COMMITS: u8 = 100;
    let dir = tempfile::tempdir().unwrap();
    let st = Arc::new(storage(dir.path()));
    let v0 = create(&st, &[0; LEN]).await;

    let writer = {
        let st = Arc::clone(&st);
        tokio::spawn(async move {
            let mut base = v0;
            for i in 1..=COMMITS {
                base = assert_updated(update(&st, &[i; LEN], &base).await);
            }
        })
    };
    let settings = Settings {
        concurrency: Some(ConcurrencySettings {
            max_concurrent_requests_for_object: NonZeroU16::new(64),
            ideal_concurrent_request_size: NonZeroU64::new(1024),
        }),
        ..Settings::default()
    };
    let ctx = StorageContext::unattributed(&settings);
    // At least one read, however fast the writer is.
    loop {
        let mut buf = Vec::new();
        st.get_object(&ctx, "repo", Some(&(0..LEN as u64)))
            .await
            .unwrap()
            .read_to_end(&mut buf)
            .await
            .unwrap();
        assert_eq!(buf.len(), LEN);
        assert!(buf.iter().all(|byte| *byte == buf[0]), "a read mixed versions");
        if writer.is_finished() {
            break;
        }
    }
    writer.await.unwrap();
}

#[tokio_test]
async fn listing_the_root_shows_the_stub_as_repo() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, V0).await;
    let long = vec![b'x'; 1000];
    assert_updated(update(&st, &long, &v0).await);
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    st.put_object(
        &ctx,
        "chunks/x",
        V0.to_vec().into(),
        None,
        Vec::new(),
        WriteHint::Atomic,
    )
    .await
    .unwrap();
    let id = Uuid::new_v4();
    std::fs::create_dir_all(dir.path().join(format!("repo.d.tmp.{id}"))).unwrap();
    std::fs::write(dir.path().join(format!("repo.d.tmp.{id}/0.x")), V0).unwrap();
    std::fs::write(dir.path().join(format!("repo.legacy.{id}")), V0).unwrap();
    std::fs::write(dir.path().join(format!("repo.stub.{id}")), V0).unwrap();

    let mut listed: Vec<_> = st
        .list_objects(&ctx, "")
        .await
        .unwrap()
        .map_ok(|info| (info.id, info.size_bytes))
        .try_collect()
        .await
        .unwrap();
    listed.sort();
    let stub_size = std::fs::metadata(dir.path().join("repo")).unwrap().len();
    assert_ne!(stub_size, long.len() as u64);
    assert_eq!(
        listed,
        [("chunks/x".to_string(), V0.len() as u64), ("repo".to_string(), stub_size)]
    );
}

#[tokio_test]
async fn last_modified_of_repo() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    create(&st, V0).await;
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);

    let modified = st.get_object_last_modified(&ctx, "repo").await.unwrap();
    let age = chrono::Utc::now() - modified;
    assert!(age.abs() < chrono::TimeDelta::seconds(5), "{modified}");
}

#[tokio_test]
async fn commits_without_fsync() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path()).with_fsync(false);
    let v0 = create(&st, V0).await;
    let v1 = assert_updated(update(&st, b"v1", &v0).await);
    assert_eq!(get(&st).await, (b"v1".to_vec(), v1));
}

fn set_age(path: &Path, age: Duration) {
    File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(SystemTime::now() - age)
        .unwrap();
}

/// A repo with a 2-day-old and a 30-minute-old staging file in `repo.d`.
async fn sweep_fixture(dir: &Path) -> (PathBuf, PathBuf) {
    create(&storage(dir), V0).await;
    let old = dir.join(format!("repo.d/stage.{}", Uuid::new_v4()));
    let recent = dir.join(format!("repo.d/stage.{}", Uuid::new_v4()));
    std::fs::write(&old, b"old").unwrap();
    std::fs::write(&recent, b"recent").unwrap();
    set_age(&old, Duration::from_secs(2 * 24 * 60 * 60));
    set_age(&recent, Duration::from_secs(30 * 60));
    (old, recent)
}

async fn sweep(
    st: &FilesystemStorage,
    older_than: chrono::DateTime<chrono::Utc>,
    dry_run: bool,
) -> DeleteObjectsResult {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    st.sweep_leftovers(&ctx, older_than, dry_run).await.unwrap()
}

#[tokio_test]
async fn sweep_leftovers_respects_the_floor() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let (old, recent) = sweep_fixture(dir.path()).await;

    let result =
        sweep(&st, chrono::Utc::now() + chrono::TimeDelta::hours(1), false).await;
    assert_eq!(result, DeleteObjectsResult { deleted_objects: 1, deleted_bytes: 3 });
    assert!(!old.exists());
    assert!(recent.exists());
    assert_eq!(get(&st).await.0, V0);
}

#[tokio_test]
async fn sweep_leftovers_respects_older_than() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let (old, recent) = sweep_fixture(dir.path()).await;

    let result = sweep(&st, chrono::Utc::now() - chrono::TimeDelta::days(3), false).await;
    assert_eq!(result, DeleteObjectsResult::default());
    assert!(old.exists());
    assert!(recent.exists());
}

#[tokio_test]
async fn sweep_leftovers_dry_run() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let (old, recent) = sweep_fixture(dir.path()).await;

    let result = sweep(&st, chrono::Utc::now(), true).await;
    assert_eq!(result, DeleteObjectsResult { deleted_objects: 1, deleted_bytes: 3 });
    assert!(old.exists());
    assert!(recent.exists());
}

#[tokio_test]
async fn sweep_leftovers_deletes_dead_versions_whatever_their_age() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, V0).await;
    let v1 = assert_updated(update(&st, b"v1", &v0).await);
    let repo_d = dir.path().join("repo.d");
    let dead = [format!("0.{}", etag(&v0)), format!("1.{}", "0".repeat(64))];
    let in_flight = format!("2.{}", "0".repeat(64));
    for name in dead.iter().chain([&in_flight]) {
        std::fs::write(repo_d.join(name), b"x").unwrap();
    }

    let result = sweep(&st, chrono::Utc::now() - chrono::TimeDelta::days(3), false).await;
    assert_eq!(result, DeleteObjectsResult { deleted_objects: 2, deleted_bytes: 2 });
    let mut expected = settled(&v1);
    expected.insert(in_flight);
    assert_eq!(names(&repo_d), expected);
}
