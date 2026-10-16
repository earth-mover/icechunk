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
use icechunk_fs::{ConversionOutcome, FilesystemStorage, PublishOutcome};
use icechunk_macros::tokio_test;
use icechunk_storage::{
    ConcurrencySettings, DeleteObjectsResult, ETag, GcPhase, Generation,
    GetModifiedResult, PutCondition, Settings, Storage as _, StorageContext,
    StorageErrorKind, StorageResult, VersionInfo, VersionedUpdateResult, WriteHint,
};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;
use uuid::Uuid;

const UNDECODABLE: &[u8] = b"not a repo info";

fn now_ms() -> u64 {
    let since_epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    since_epoch.as_millis().try_into().unwrap()
}

#[path = "common/repo_infos.rs"]
#[expect(dead_code, reason = "shared with tests that use every helper")]
mod repo_infos;

use repo_infos::{changed_repo_info, featureless_repo_info, repo_info};

fn v0_bytes() -> Vec<u8> {
    repo_info(0)
}

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

/// `<N>.<h>`, the name of `version`'s file in `repo.d`.
fn version_file(version: &VersionInfo) -> String {
    format!("{}.{}", generation(version), etag(version))
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
    BTreeSet::from([version_file(version), marker(version)])
}

/// `latest.<K>.<N>.<h>`, the marker naming `version`.
fn marker(version: &VersionInfo) -> String {
    let n = generation(version);
    format!("latest.{:015}.{n}.{}", 999_999_999_999_999 - n, etag(version))
}

/// Makes `bytes` the current version on top of `base` with plain file
/// operations: how the conversion to the standard layout's change makes a
/// version without the feature, which changes through the storage refuse.
fn plant_version(dir: &Path, bytes: &[u8], base: &VersionInfo) -> VersionInfo {
    let new = version(
        &format!("{:x}", Sha256::digest(bytes)),
        Some(&(generation(base) + 1).to_string()),
    );
    let repo_d = dir.join("repo.d");
    std::fs::write(repo_d.join(version_file(&new)), bytes).unwrap();
    std::fs::rename(repo_d.join(marker(base)), repo_d.join(marker(&new))).unwrap();
    std::fs::remove_file(repo_d.join(version_file(base))).unwrap();
    new
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

    let before = now_ms();
    let v0 = create(&st, &v0_bytes()).await;
    assert!((before..=now_ms()).contains(&generation(&v0)), "{v0:?}");
    assert_eq!(get(&st).await, (v0_bytes(), v0.clone()));
    assert_eq!(std::fs::read(dir.path().join("repo")).unwrap(), v0_bytes());
    assert_eq!(get_object(&st, "repo").await.unwrap(), v0_bytes());
    assert_eq!(get_conditional(&st, Some(&v0)).await, None);

    let v1 = assert_updated(update(&st, &repo_info(1), &v0).await);
    assert_eq!(generation(&v1), generation(&v0) + 1);
    assert_ne!(etag(&v1), etag(&v0));
    assert_eq!(get_conditional(&st, Some(&v0)).await, Some((repo_info(1), v1.clone())));
    assert_eq!(get_conditional(&st, Some(&v1)).await, None);
    assert_eq!(get_object(&st, "repo").await.unwrap(), repo_info(1));
    assert_eq!(names(&dir.path().join("repo.d")), settled(&v1));
}

#[tokio_test]
async fn a_second_create_is_not_on_latest() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;

    for bytes in [v0_bytes(), repo_info(2)] {
        assert_not_on_latest(
            &put_conditional(&st, "repo", &bytes, PutCondition::Create).await,
        );
    }
    assert_eq!(get(&st).await, (v0_bytes(), v0));
}

#[tokio_test]
async fn a_stale_version_cannot_update_or_back_up() {
    let dir = tempfile::tempdir().unwrap();
    let a = storage(dir.path());
    let b = storage(dir.path());
    create(&a, &v0_bytes()).await;
    let (_, a_base) = get(&a).await;
    let (_, b_base) = get(&b).await;

    assert_updated(update(&a, &repo_info(10), &a_base).await);
    assert_not_on_latest(&update(&b, &repo_info(11), &b_base).await);
    assert_not_on_latest(&copy(&b, &b_base).await);

    assert_eq!(get(&a).await.0, repo_info(10));
    assert!(!dir.path().join("overwritten").exists());
}

#[tokio_test]
async fn copy_writes_the_bytes_of_its_version() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    let v1 = assert_updated(update(&st, &repo_info(1), &v0).await);

    assert_eq!(assert_updated(copy(&st, &v1).await), v1);
    assert_eq!(std::fs::read(dir.path().join("overwritten/x")).unwrap(), repo_info(1));
}

/// A listing that shows a marker above the version proves it stale, as a
/// copy-if-match on object storage would: nothing is written, even while
/// its file is still there.
#[tokio_test]
async fn copy_of_a_version_a_listing_shows_stale_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    assert_updated(update(&st, &repo_info(1), &v0).await);

    // A newer commit whose tidy-up hasn't deleted its base yet.
    let base = dir.path().join("repo.d").join(version_file(&v0));
    std::fs::write(&base, &v0_bytes()).unwrap();
    assert_not_on_latest(&copy(&st, &v0).await);
    assert!(!dir.path().join("overwritten").exists());

    // Once it has, the base is gone.
    std::fs::remove_file(&base).unwrap();
    assert_not_on_latest(&copy(&st, &v0).await);
    assert!(!dir.path().join("overwritten").exists());
}

/// Only a marker above the version proves it stale: a listing that shows an
/// older marker or none, as a stale or torn listing can, copies.
#[tokio_test]
async fn copy_without_a_later_marker_in_the_listing_copies() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    let v1 = assert_updated(update(&st, &repo_info(1), &v0).await);
    let repo_d = dir.path().join("repo.d");

    std::fs::rename(repo_d.join(marker(&v1)), repo_d.join(marker(&v0))).unwrap();
    assert_eq!(assert_updated(copy(&st, &v1).await), v1);
    assert_eq!(std::fs::read(dir.path().join("overwritten/x")).unwrap(), repo_info(1));

    std::fs::remove_file(dir.path().join("overwritten/x")).unwrap();
    std::fs::rename(repo_d.join(marker(&v0)), dir.path().join("hidden-marker")).unwrap();
    assert_eq!(assert_updated(copy(&st, &v1).await), v1);
    assert_eq!(std::fs::read(dir.path().join("overwritten/x")).unwrap(), repo_info(1));
}

#[tokio_test]
async fn copy_of_a_corrupt_version_file_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    std::fs::write(dir.path().join("repo.d").join(version_file(&v0)), b"x").unwrap();

    let message = assert_other_error(copy(&st, &v0).await);
    assert!(message.contains(&version_file(&v0)), "{message}");
    assert!(!dir.path().join("overwritten").exists());
}

#[tokio_test]
async fn a_version_without_etag_or_generation_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;

    for invalid in [
        VersionInfo::for_creation(),
        version(&etag(&v0), None),
        VersionInfo { etag: None, ..v0.clone() },
    ] {
        assert_other_error(update(&st, &repo_info(1), &invalid).await);
        assert_other_error(copy(&st, &invalid).await);
    }
    assert_eq!(get(&st).await, (v0_bytes(), v0));
    assert!(!dir.path().join("overwritten").exists());
}

#[tokio_test]
async fn an_invalid_version_is_an_error_and_touches_nothing() {
    let outer = tempfile::tempdir().unwrap();
    let root = outer.path().join("a").join("b").join("root");
    let st = storage(&root);
    let v0 = create(&st, &v0_bytes()).await;
    let before = tree(outer.path());

    for invalid in [
        version("../../escape", Some("0")),
        version(&etag(&v0).to_uppercase(), Some("0")),
        version(&etag(&v0), Some("../0")),
        version(&etag(&v0), Some("00")),
        version(&etag(&v0), Some("1000000000000000")),
    ] {
        assert_other_error(update(&st, &repo_info(1), &invalid).await);
        assert_other_error(copy(&st, &invalid).await);
    }

    assert_eq!(tree(outer.path()), before);
    assert_eq!(get(&st).await, (v0_bytes(), v0));
}

#[tokio_test]
async fn a_previous_version_that_does_not_parse_reads_in_full() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;

    for previous in [version(&etag(&v0), None), version("x", Some("0"))] {
        assert_eq!(
            get_conditional(&st, Some(&previous)).await,
            Some((v0_bytes(), v0.clone()))
        );
    }
}

#[tokio_test]
async fn updating_a_missing_repo_is_not_on_latest() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = version(&"0".repeat(64), Some("0"));

    assert_not_on_latest(&update(&st, &repo_info(1), &v0).await);
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
    create(&st, &v0_bytes()).await;
    let (_, base) = get(&st).await;

    let tasks: Vec<_> = (0..16)
        .map(|task: u32| {
            let st = storage(dir.path());
            let base = base.clone();
            tokio::spawn(async move {
                let content = repo_info(100 + task);
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
    let initial = create(&st, &v0_bytes()).await;

    let tasks: Vec<_> = (0..TASKS)
        .map(|task| {
            let st = storage(dir.path());
            tokio::spawn(async move {
                let mut updates = Vec::new();
                for i in 0..SUCCESSES {
                    let content = repo_info(u32::try_from(1000 + 100 * task + i).unwrap());
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
    let mut current = initial.clone();
    while let Some(new) = next.get(&etag(&current)) {
        assert!(visited.insert(etag(&current)), "the chain has a cycle");
        current = new.clone();
    }
    assert_eq!(visited.len(), updates.len(), "updates outside the chain");
    assert_eq!(current, get(&st).await.1);
    assert_eq!(generation(&current), generation(&initial) + (TASKS * SUCCESSES) as u64);
    assert_eq!(names(&dir.path().join("repo.d")), settled(&current));
}

#[tokio_test]
async fn if_version_on_another_key_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = assert_updated(
        put_conditional(&st, "a", &v0_bytes(), PutCondition::Create).await,
    );

    let message = assert_other_error(
        put_conditional(&st, "a", &repo_info(1), PutCondition::IfVersion(&v0)).await,
    );
    assert!(message.contains("only supported for `repo`"), "{message}");
    assert_eq!(get_object(&st, "a").await.unwrap(), v0_bytes());
}

#[tokio_test]
async fn unconditional_writes_of_repo_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);

    for hint in [WriteHint::Atomic, WriteHint::NewObject] {
        let message = assert_other_error(
            st.put_object(&ctx, "repo", repo_info(1).into(), None, Vec::new(), hint)
                .await,
        );
        assert!(message.contains("put_object_conditional"), "{message}");
    }
    put_conditional(&st, "a", &v0_bytes(), PutCondition::Create).await.unwrap();
    let a = version(&etag(&v0), None);
    assert_other_error(st.copy_object(&ctx, "a", "repo", None, &a).await);
    assert_eq!(get(&st).await, (v0_bytes(), v0));
}

#[tokio_test]
async fn delete_batch_keeps_repo() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    put_conditional(&st, "a", &v0_bytes(), PutCondition::Create).await.unwrap();
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);

    let result = st
        .delete_batch(&ctx, "", vec![("repo".to_string(), 39), ("a".to_string(), 2)])
        .await
        .unwrap();
    assert_eq!(result, DeleteObjectsResult { deleted_objects: 1, deleted_bytes: 2 });
    assert_eq!(get(&st).await, (v0_bytes(), v0));
    assert!(!dir.path().join("a").exists());
}

#[tokio_test]
async fn protocol_names_are_invalid_keys() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    let id = Uuid::new_v4();
    let before = tree(dir.path());

    for key in [
        "repo.d".to_string(),
        format!("repo.d/0.{}", etag(&v0)),
        "repo.d/x".to_string(),
        format!("repo.d.tmp.{id}/x"),
    ] {
        let err = get_object(&st, &key).await.err().unwrap();
        assert!(matches!(err.kind, StorageErrorKind::Other(_)), "{key}: {err}");
        let err = st.get_object_last_modified(&ctx, &key).await.err().unwrap();
        assert!(matches!(err.kind, StorageErrorKind::Other(_)), "{key}: {err}");
        let err = st
            .put_object(
                &ctx,
                &key,
                v0_bytes().into(),
                None,
                Vec::new(),
                WriteHint::Atomic,
            )
            .await
            .err()
            .unwrap();
        assert!(matches!(err.kind, StorageErrorKind::Other(_)), "{key}: {err}");
        assert_other_error(
            put_conditional(&st, &key, &v0_bytes(), PutCondition::Create).await,
        );
        assert!(
            st.delete_batch(&ctx, "", vec![(key.clone(), 1)]).await.is_err(),
            "{key}"
        );
        assert!(st.list_objects(&ctx, &key).await.is_err(), "{key}");
    }
    assert_eq!(tree(dir.path()), before);
    // Only at the root: these are ordinary names further down. Names that
    // are no longer protocol names are ordinary names.
    for key in [
        "a/repo.d",
        "b/repo.d/x",
        &format!("repo.legacy.{id}"),
        &format!("repo.stub.{id}"),
    ] {
        put_conditional(&st, key, &v0_bytes(), PutCondition::Create).await.unwrap();
        assert_eq!(get_object(&st, key).await.unwrap(), v0_bytes(), "{key}");
    }
}

#[tokio_test]
async fn a_standard_repo_must_be_converted() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let standard = featureless_repo_info(0);
    std::fs::write(dir.path().join("repo"), &standard).unwrap();

    let message = assert_other_error(try_get_conditional(&st, None).await);
    assert!(message.contains("standard layout"), "{message}");
    assert!(message.contains("convert_to_filesystem_layout()"), "{message}");
    let message = assert_other_error(get_object(&st, "repo").await);
    assert!(message.contains("convert_to_filesystem_layout()"), "{message}");
    let message = assert_other_error(
        put_conditional(&st, "repo", &v0_bytes(), PutCondition::Create).await,
    );
    assert!(message.contains("convert_to_filesystem_layout()"), "{message}");
    assert_eq!(std::fs::read(dir.path().join("repo")).unwrap(), standard);

    let before = now_ms();
    assert_eq!(
        st.convert_to_filesystem_layout().await.unwrap(),
        ConversionOutcome::Converted
    );
    let (bytes, v0) = get(&st).await;
    assert!((before..=now_ms()).contains(&generation(&v0)), "{v0:?}");
    assert!(icechunk_format::layout_v1::lists_feature(&bytes).unwrap());
    assert_eq!(std::fs::read(dir.path().join("repo")).unwrap(), bytes);
    assert_eq!(
        names(dir.path()),
        BTreeSet::from([
            "overwritten".to_string(),
            "repo".to_string(),
            "repo.d".to_string()
        ])
    );
    let [backup] =
        Vec::from_iter(names(&dir.path().join("overwritten"))).try_into().unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("overwritten").join(backup)).unwrap(),
        standard
    );
    assert_eq!(
        st.convert_to_filesystem_layout().await.unwrap(),
        ConversionOutcome::NothingToConvert
    );

    let v1 = assert_updated(update(&st, &repo_info(1), &v0).await);
    assert_eq!(get(&st).await, (repo_info(1), v1));
}

/// Names that are no longer protocol names, such as `repo.legacy.*` and
/// `repo.stub.*`, make no repository.
#[tokio_test]
async fn converting_nothing_is_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let id = Uuid::new_v4();
    for name in [format!("repo.legacy.{id}"), format!("repo.stub.{id}")] {
        std::fs::write(dir.path().join(name), featureless_repo_info(0)).unwrap();
    }
    let before = names(dir.path());
    let err = st.convert_to_filesystem_layout().await.unwrap_err();
    assert!(matches!(err.kind, StorageErrorKind::ObjectNotFound), "{err}");
    assert_eq!(names(dir.path()), before);
}

/// A repository listing writer features this storage doesn't implement is
/// refused before anything is written; that needs features only test builds
/// of `icechunk-format` write, so it is tested in `icechunk`. Here: an
/// interrupted conversion to the standard layout.
#[tokio_test]
async fn converting_during_a_conversion_to_the_standard_layout_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    plant_version(dir.path(), &featureless_repo_info(1), &v0);
    let before = tree(dir.path());
    let err = st.convert_to_filesystem_layout().await.unwrap_err();
    assert!(matches!(err.kind, StorageErrorKind::LayoutConversionInProgress(_)), "{err}");
    assert!(err.to_string().contains("convert_to_standard_layout()"), "{err}");
    assert_eq!(tree(dir.path()), before);
}

/// The current version without the feature becomes the standard `repo`, and
/// `repo.d` goes; a conversion back works.
#[tokio_test]
async fn a_repository_converts_to_the_standard_layout_and_back() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    let v1 = assert_updated(update(&st, &repo_info(1), &v0).await);
    let current =
        std::fs::read(dir.path().join("repo.d").join(version_file(&v1))).unwrap();

    assert_eq!(
        st.convert_to_standard_layout().await.unwrap(),
        ConversionOutcome::Converted
    );
    assert_eq!(
        names(dir.path()),
        BTreeSet::from(["overwritten".to_string(), "repo".to_string()])
    );
    let standard = mirror(dir.path());
    assert!(!icechunk_format::layout_v1::lists_feature(&standard).unwrap());
    let attempt = icechunk_format::layout_v1::attempt_id(&standard).unwrap().unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("overwritten").join(attempt)).unwrap(),
        current
    );
    let message =
        assert_other_error(try_get_conditional(&storage(dir.path()), None).await);
    assert!(message.contains("convert_to_filesystem_layout()"), "{message}");
    assert_eq!(
        st.convert_to_standard_layout().await.unwrap(),
        ConversionOutcome::NothingToConvert
    );
    assert_eq!(mirror(dir.path()), standard);

    let st = storage(dir.path());
    assert_eq!(
        st.convert_to_filesystem_layout().await.unwrap(),
        ConversionOutcome::Converted
    );
    let (bytes, _) = get(&st).await;
    assert!(icechunk_format::layout_v1::lists_feature(&bytes).unwrap());
}

#[tokio_test]
async fn converting_nothing_to_the_standard_layout_is_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let err = storage(dir.path()).convert_to_standard_layout().await.unwrap_err();
    assert!(matches!(err.kind, StorageErrorKind::ObjectNotFound), "{err}");
    assert_eq!(names(dir.path()), BTreeSet::new());
}

/// A conversion from the standard layout interrupted after its directory
/// rename leaves the standard repo info in `$ROOT/repo`, no version of
/// `repo.d`: converting to the standard layout instead of rerunning it stops,
/// naming the conversion to rerun, and changes nothing.
#[tokio_test]
async fn converting_to_standard_during_an_interrupted_conversion_from_it_is_a_fork() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let standard = featureless_repo_info(0);
    std::fs::write(dir.path().join("repo"), &standard).unwrap();
    assert_eq!(
        st.convert_to_filesystem_layout().await.unwrap(),
        ConversionOutcome::Converted
    );
    std::fs::write(dir.path().join("repo"), &standard).unwrap();
    let before = tree(dir.path());

    let err = st.convert_to_standard_layout().await.unwrap_err();
    assert!(matches!(err.kind, StorageErrorKind::RepositoryForked(_)), "{err}");
    assert!(
        err.to_string().contains("rerunning convert_to_filesystem_layout()"),
        "{err}"
    );
    assert_eq!(tree(dir.path()), before);
    assert_eq!(mirror(dir.path()), standard);
}

#[tokio_test]
async fn converting_to_standard_refuses_an_unreadable_mirror() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    create(&st, &v0_bytes()).await;
    plant_mirror(dir.path(), b"");
    let before = tree(dir.path());
    let err = st.convert_to_standard_layout().await.unwrap_err();
    assert!(matches!(err.kind, StorageErrorKind::MirrorUnreadable(_)), "{err}");
    assert_eq!(tree(dir.path()), before);
}

#[tokio_test]
async fn creation_refuses_a_repo_info_without_the_feature() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    for bytes in [featureless_repo_info(0), UNDECODABLE.to_vec()] {
        let message = assert_other_error(
            put_conditional(&st, "repo", &bytes, PutCondition::Create).await,
        );
        assert!(message.contains("filesystem-layout-v1"), "{message}");
    }
    assert_eq!(names(dir.path()), BTreeSet::new());
}

/// Without `repo.d`, opening classifies `$ROOT/repo` and never writes.
#[tokio_test]
async fn opening_without_repo_d_names_what_it_found() {
    let cases: [(Vec<u8>, &[&str]); 3] = [
        (changed_repo_info(1), &["orphan mirror", "Restore repo.d"]),
        (UNDECODABLE.to_vec(), &["repo can't be read or decoded"]),
        (Vec::new(), &["repo can't be read or decoded"]),
    ];
    for (planted, expected) in cases {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("repo"), &planted).unwrap();
        let st = storage(dir.path());
        let message = assert_other_error(try_get_conditional(&st, None).await);
        for part in expected {
            assert!(message.contains(part), "{message}");
        }
        assert!(message.contains(&dir.path().display().to_string()), "{message}");
        assert_not_on_latest(
            &put_conditional(&st, "repo", &v0_bytes(), PutCondition::Create).await,
        );
        assert_eq!(std::fs::read(dir.path().join("repo")).unwrap(), planted);
        assert_eq!(names(dir.path()), BTreeSet::from(["repo".to_string()]));
    }
}

/// An interrupted creation is no repository, and the next creation replaces
/// it.
#[tokio_test]
async fn an_interrupted_creation_is_not_found_and_is_resumed() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("repo"), repo_info(9)).unwrap();
    let st = storage(dir.path());
    let err = try_get_conditional(&st, None).await.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::ObjectNotFound), "{err}");

    let v0 = create(&st, &v0_bytes()).await;
    assert_eq!(get(&st).await, (v0_bytes(), v0.clone()));
    assert_eq!(mirror(dir.path()), v0_bytes());
    assert_eq!(names(&dir.path().join("repo.d")), settled(&v0));
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
        let v0 = create(&st, &v0_bytes()).await;
        if !unopenable_dir(dir.path(), name) {
            return;
        }
        assert_permission_denied(update(&st, &repo_info(1), &v0).await);
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
        put_conditional(&st, "repo", &v0_bytes(), PutCondition::Create).await,
    );
    assert_eq!(names(dir.path()), BTreeSet::from(["snapshots".to_string()]));
}

#[tokio_test]
async fn conversion_syncs_the_data_dirs_before_the_protocol() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    std::fs::write(dir.path().join("repo"), featureless_repo_info(0)).unwrap();
    if !unopenable_dir(dir.path(), "chunks") {
        return;
    }
    assert_permission_denied(st.convert_to_filesystem_layout().await);
    assert_eq!(
        names(dir.path()),
        BTreeSet::from(["chunks".to_string(), "repo".to_string()])
    );
    assert_eq!(std::fs::read(dir.path().join("repo")).unwrap(), featureless_repo_info(0));
}

#[tokio_test]
async fn without_fsync_there_is_no_data_dir_barrier() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path()).with_fsync(false);
    std::fs::write(dir.path().join("repo"), featureless_repo_info(0)).unwrap();
    if !unopenable_dir(dir.path(), "chunks") {
        return;
    }
    assert_eq!(
        st.convert_to_filesystem_layout().await.unwrap(),
        ConversionOutcome::Converted
    );
    let (_, v0) = get(&st).await;
    assert_updated(update(&st, &repo_info(1), &v0).await);

    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path()).with_fsync(false);
    if !unopenable_dir(dir.path(), "snapshots") {
        return;
    }
    create(&st, &v0_bytes()).await;
}

/// With `repo.d`, a new instance opens whatever `$ROOT/repo` holds: forks
/// and unreadable mirrors are for the operations that publish to find.
#[tokio_test]
async fn opening_with_repo_d_never_reads_the_mirror() {
    for planted in [Some(UNDECODABLE.to_vec()), Some(featureless_repo_info(1)), None] {
        let dir = tempfile::tempdir().unwrap();
        let v0 = create(&storage(dir.path()), &v0_bytes()).await;
        match &planted {
            Some(bytes) => std::fs::write(dir.path().join("repo"), bytes).unwrap(),
            None => std::fs::remove_file(dir.path().join("repo")).unwrap(),
        }

        let st = storage(dir.path());
        assert_eq!(get(&st).await, (v0_bytes(), v0.clone()), "{planted:?}");
        assert_eq!(get_object(&storage(dir.path()), "repo").await.unwrap(), v0_bytes());
    }
}

#[tokio_test]
async fn a_change_publishes_the_mirror_before_returning() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let mut base = create(&st, &v0_bytes()).await;
    for seq in 1..=3 {
        base = assert_updated(update(&st, &repo_info(seq), &base).await);
        assert_eq!(mirror(dir.path()), repo_info(seq));
        assert_eq!(names(&dir.path().join("repo.d")), settled(&base));
    }
    // A missing mirror is published too.
    std::fs::remove_file(dir.path().join("repo")).unwrap();
    assert_updated(update(&st, &repo_info(4), &base).await);
    assert_eq!(mirror(dir.path()), repo_info(4));
}

/// Bytes without the feature, or that don't decode, are refused before
/// anything is written.
#[tokio_test]
async fn a_change_refuses_bytes_without_the_feature() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    let before = tree(dir.path());
    let err = update(&st, &featureless_repo_info(1), &v0).await.unwrap_err();
    assert!(matches!(err.kind, StorageErrorKind::LayoutConversionInProgress(_)), "{err}");
    assert!(err.to_string().contains("convert_to_standard_layout()"), "{err}");
    let message = assert_other_error(update(&st, UNDECODABLE, &v0).await);
    assert!(message.contains("doesn't decode"), "{message}");
    assert_eq!(tree(dir.path()), before);
    assert_eq!(get(&st).await, (v0_bytes(), v0));
}

/// A fork found by the publication after a change: the change is durable,
/// and the fork is never overwritten.
#[tokio_test]
async fn a_fork_found_after_a_change_is_the_durable_error() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    plant_mirror(dir.path(), &featureless_repo_info(9));
    let err = update(&st, &repo_info(1), &v0).await.unwrap_err();
    let StorageErrorKind::RepositoryForkedAfterUpdate { new_version, detail } = &err.kind
    else {
        panic!("{err}")
    };
    assert_eq!(get(&st).await, (repo_info(1), (**new_version).clone()));
    assert!(detail.contains("nothing was published"), "{detail}");
    assert!(err.to_string().contains("durable"), "{err}");
    assert_eq!(mirror(dir.path()), featureless_repo_info(9));
}

/// A mirror that can't be read or decoded might be a fork: never
/// overwritten. The change landed, so it is `Updated`.
#[tokio_test]
async fn a_change_leaves_an_unreadable_mirror_alone() {
    for planted in [Vec::new(), UNDECODABLE.to_vec()] {
        let dir = tempfile::tempdir().unwrap();
        let st = storage(dir.path());
        let v0 = create(&st, &v0_bytes()).await;
        plant_mirror(dir.path(), &planted);
        let v1 = assert_updated(update(&st, &repo_info(1), &v0).await);
        assert_eq!(get(&st).await, (repo_info(1), v1));
        assert_eq!(mirror(dir.path()), planted);
    }
}

/// A range longer than the ideal request size is not split into reads of
/// `repo` that could each see a different version.
#[tokio_test(flavor = "multi_thread", worker_threads = 4)]
async fn a_long_ranged_read_of_repo_sees_one_version() {
    const COMMITS: u32 = 100;
    let dir = tempfile::tempdir().unwrap();
    let st = Arc::new(storage(dir.path()));
    let contents: Vec<Vec<u8>> = (0..=COMMITS).map(repo_info).collect();
    let len = contents.iter().map(Vec::len).min().unwrap();
    let v0 = create(&st, &contents[0]).await;

    let writer = {
        let st = Arc::clone(&st);
        let contents = contents.clone();
        tokio::spawn(async move {
            let mut base = v0;
            for content in &contents[1..] {
                base = assert_updated(update(&st, content, &base).await);
            }
        })
    };
    let settings = Settings {
        concurrency: Some(ConcurrencySettings {
            max_concurrent_requests_for_object: NonZeroU16::new(64),
            ideal_concurrent_request_size: NonZeroU64::new(16),
        }),
        ..Settings::default()
    };
    let ctx = StorageContext::unattributed(&settings);
    // At least one read, however fast the writer is.
    loop {
        let mut buf = Vec::new();
        st.get_object(&ctx, "repo", Some(&(0..len as u64)))
            .await
            .unwrap()
            .read_to_end(&mut buf)
            .await
            .unwrap();
        assert_eq!(buf.len(), len);
        assert!(
            contents.iter().any(|content| content.starts_with(&buf)),
            "a read mixed versions"
        );
        if writer.is_finished() {
            break;
        }
    }
    writer.await.unwrap();
}

/// `repo` is listed as the file `$ROOT/repo`, the mirror: no protocol read.
#[tokio_test]
async fn listing_the_root_shows_the_mirror_as_repo() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    // A mirror lagging behind a longer current version.
    let longer = changed_repo_info(1);
    assert!(longer.len() > v0_bytes().len());
    assert_updated(update(&st, &longer, &v0).await);
    plant_mirror(dir.path(), &v0_bytes());
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    st.put_object(
        &ctx,
        "chunks/x",
        v0_bytes().into(),
        None,
        Vec::new(),
        WriteHint::Atomic,
    )
    .await
    .unwrap();
    let id = Uuid::new_v4();
    std::fs::create_dir_all(dir.path().join(format!("repo.d.tmp.{id}"))).unwrap();
    std::fs::write(dir.path().join(format!("repo.d.tmp.{id}/0.x")), &v0_bytes()).unwrap();
    std::fs::write(dir.path().join(format!("repo.legacy.{id}")), &v0_bytes()).unwrap();
    std::fs::write(dir.path().join(format!("repo.stub.{id}")), &v0_bytes()).unwrap();
    std::fs::write(dir.path().join(format!("repo.staging.{id}")), &v0_bytes()).unwrap();

    let mut listed: Vec<_> = st
        .list_objects(&ctx, "")
        .await
        .unwrap()
        .map_ok(|info| (info.id, info.size_bytes))
        .try_collect()
        .await
        .unwrap();
    listed.sort();
    let size = v0_bytes().len() as u64;
    // Names that are no longer protocol names are listed like any object.
    assert_eq!(
        listed,
        [
            ("chunks/x".to_string(), size),
            ("repo".to_string(), size),
            (format!("repo.legacy.{id}"), size),
            (format!("repo.stub.{id}"), size),
        ]
    );
}

/// The current version's file, not the mirror, whose time can differ.
#[tokio_test]
async fn last_modified_of_repo_is_the_current_versions() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    let version_file = dir.path().join("repo.d").join(version_file(&v0));
    set_age(&version_file, Duration::from_secs(2 * 24 * 60 * 60));
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);

    let modified = st.get_object_last_modified(&ctx, "repo").await.unwrap();
    let expected: chrono::DateTime<chrono::Utc> =
        std::fs::metadata(&version_file).unwrap().modified().unwrap().into();
    assert_eq!(modified, expected);
    let mirror_modified: chrono::DateTime<chrono::Utc> =
        std::fs::metadata(dir.path().join("repo")).unwrap().modified().unwrap().into();
    assert_ne!(modified, mirror_modified);

    let empty = tempfile::tempdir().unwrap();
    let err = storage(empty.path()).get_object_last_modified(&ctx, "repo").await;
    assert!(matches!(err.err().unwrap().kind, StorageErrorKind::ObjectNotFound));
}

#[tokio_test]
async fn commits_without_fsync() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path()).with_fsync(false);
    let v0 = create(&st, &v0_bytes()).await;
    let v1 = assert_updated(update(&st, &repo_info(1), &v0).await);
    assert_eq!(get(&st).await, (repo_info(1), v1));
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
    create(&storage(dir), &v0_bytes()).await;
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
    assert_eq!(get(&st).await.0, v0_bytes());
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
    let v0 = create(&st, &v0_bytes()).await;
    let v1 = assert_updated(update(&st, &repo_info(1), &v0).await);
    let repo_d = dir.path().join("repo.d");
    let n = generation(&v0);
    let dead = [version_file(&v0), format!("{}.{}", n + 1, "0".repeat(64))];
    let in_flight = format!("{}.{}", n + 2, "0".repeat(64));
    for name in dead.iter().chain([&in_flight]) {
        std::fs::write(repo_d.join(name), b"x").unwrap();
    }

    let result = sweep(&st, chrono::Utc::now() - chrono::TimeDelta::days(3), false).await;
    assert_eq!(result, DeleteObjectsResult { deleted_objects: 2, deleted_bytes: 2 });
    let mut expected = settled(&v1);
    expected.insert(in_flight);
    assert_eq!(names(&repo_d), expected);
}

/// Writes the mirror `repo` directly.
fn plant_mirror(dir: &Path, bytes: &[u8]) {
    std::fs::write(dir.join("repo"), bytes).unwrap();
}

fn mirror(dir: &Path) -> Vec<u8> {
    std::fs::read(dir.join("repo")).unwrap()
}

#[tokio_test]
async fn publish_mirror_repairs_a_lagging_or_missing_mirror() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    let v1 = assert_updated(update(&st, &repo_info(1), &v0).await);

    plant_mirror(dir.path(), &v0_bytes());
    assert_eq!(st.publish_mirror().await.unwrap(), PublishOutcome::Current);
    assert_eq!(mirror(dir.path()), repo_info(1));
    assert_eq!(names(&dir.path().join("repo.d")), settled(&v1));

    std::fs::remove_file(dir.path().join("repo")).unwrap();
    assert_eq!(st.publish_mirror().await.unwrap(), PublishOutcome::Current);
    assert_eq!(mirror(dir.path()), repo_info(1));
}

#[tokio_test]
async fn publish_mirror_refuses_a_fork_or_an_unreadable_mirror() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;

    for planted in [featureless_repo_info(1), Vec::new()] {
        plant_mirror(dir.path(), &planted);
        let err = st.publish_mirror().await.err().unwrap();
        match &err.kind {
            StorageErrorKind::RepositoryForked(_) => assert!(!planted.is_empty()),
            StorageErrorKind::MirrorUnreadable(_) => assert!(planted.is_empty()),
            _ => panic!("{err}"),
        }
        assert_eq!(mirror(dir.path()), planted);
        assert_eq!(names(&dir.path().join("repo.d")), settled(&v0));
    }
}

#[tokio_test]
async fn publish_mirror_refuses_a_current_version_without_the_feature() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    // What an interrupted conversion to the standard layout leaves.
    let v1 = plant_version(dir.path(), &featureless_repo_info(1), &v0);
    let before = mirror(dir.path());

    let err = st.publish_mirror().await.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::LayoutConversionInProgress(_)), "{err}");
    assert!(err.to_string().contains("convert_to_standard_layout()"), "{err}");
    assert_eq!(mirror(dir.path()), before);
    assert_eq!(names(&dir.path().join("repo.d")), settled(&v1));
}

#[tokio_test]
async fn publish_mirror_without_a_repository_is_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let err = storage(dir.path()).publish_mirror().await.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::ObjectNotFound), "{err}");
    assert_eq!(names(dir.path()), BTreeSet::new());
}

async fn gc_hook(st: &FilesystemStorage, phase: GcPhase<'_>) -> StorageResult<()> {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    st.gc_hook(&ctx, phase).await
}

/// Before GC's deletes the hook publishes the version of GC's change; after
/// them it publishes again, repairing an older version renamed over the
/// mirror meanwhile, from the listed version or GC's, whichever is newer.
#[tokio_test]
async fn gc_hook_publishes_before_and_after_the_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    let gc_change = assert_updated(update(&st, &repo_info(1), &v0).await);

    plant_mirror(dir.path(), &v0_bytes());
    let changed = Some(&gc_change);
    gc_hook(&st, GcPhase::BeforeDeletes { changed }).await.unwrap();
    assert_eq!(mirror(dir.path()), repo_info(1));

    plant_mirror(dir.path(), &v0_bytes());
    gc_hook(&st, GcPhase::AfterDeletes { changed }).await.unwrap();
    assert_eq!(mirror(dir.path()), repo_info(1));

    let later = assert_updated(update(&st, &repo_info(2), &gc_change).await);
    plant_mirror(dir.path(), &v0_bytes());
    gc_hook(&st, GcPhase::AfterDeletes { changed }).await.unwrap();
    assert_eq!(mirror(dir.path()), repo_info(2));
    assert_eq!(names(&dir.path().join("repo.d")), settled(&later));
}

/// Before the deletes a fork or an unreadable mirror refuses GC, publishing
/// nothing; after them it is logged, and GC's result stands.
#[tokio_test]
async fn gc_hook_refuses_a_fork_or_an_unreadable_mirror_before_the_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    let changed = Some(&v0);
    for planted in [featureless_repo_info(9), Vec::new()] {
        plant_mirror(dir.path(), &planted);
        let err = gc_hook(&st, GcPhase::BeforeDeletes { changed }).await.unwrap_err();
        match &err.kind {
            StorageErrorKind::RepositoryForked(_) => assert!(!planted.is_empty()),
            StorageErrorKind::MirrorUnreadable(_) => assert!(planted.is_empty()),
            _ => panic!("{err}"),
        }
        assert!(err.to_string().contains("deleted nothing"), "{err}");
        assert_eq!(mirror(dir.path()), planted);
        gc_hook(&st, GcPhase::AfterDeletes { changed }).await.unwrap();
        assert_eq!(mirror(dir.path()), planted);
    }
}

/// GC on a repository in the filesystem layout always makes a change, so the
/// hook refuses to run without its version while `repo.d` exists.
#[tokio_test]
async fn gc_hook_without_a_change_is_refused_while_repo_d_exists() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    for phase in [
        GcPhase::BeforeDeletes { changed: None },
        GcPhase::AfterDeletes { changed: None },
    ] {
        gc_hook(&st, phase).await.unwrap();
    }

    let v0 = create(&st, &v0_bytes()).await;
    let message =
        assert_other_error(gc_hook(&st, GcPhase::BeforeDeletes { changed: None }).await);
    assert!(message.contains("only after GC's change"), "{message}");

    plant_version(dir.path(), &featureless_repo_info(1), &v0);
    let err = gc_hook(&st, GcPhase::BeforeDeletes { changed: None }).await.unwrap_err();
    assert!(matches!(err.kind, StorageErrorKind::LayoutConversionInProgress(_)), "{err}");
    assert!(err.to_string().contains("convert_to_standard_layout()"), "{err}");
    assert_eq!(mirror(dir.path()), v0_bytes());
}

/// A version without a generation: before the deletes GC fails; after them
/// the failure is only logged, GC's result stands.
#[tokio_test]
async fn gc_hook_with_an_invalid_version_fails_only_before_the_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let v0 = create(&st, &v0_bytes()).await;
    let invalid = version(&etag(&v0), None);
    let changed = Some(&invalid);
    assert_other_error(gc_hook(&st, GcPhase::BeforeDeletes { changed }).await);
    gc_hook(&st, GcPhase::AfterDeletes { changed }).await.unwrap();
    assert_eq!(mirror(dir.path()), v0_bytes());
}
