#![cfg(unix)]
#![expect(clippy::unwrap_used, clippy::panic)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use icechunk_fs::FilesystemStorage;
use icechunk_macros::tokio_test;
use icechunk_storage::{
    ETag, GetModifiedResult, PutCondition, Settings, Storage as _, StorageContext,
    StorageErrorKind, StorageResult, VersionInfo, VersionedUpdateResult,
};
use tokio::io::AsyncReadExt as _;

const V0: &[u8] = b"v0";

fn storage(dir: &Path) -> FilesystemStorage {
    FilesystemStorage::new(dir).unwrap()
}

async fn create(st: &FilesystemStorage, key: &str, content: &[u8]) -> VersionInfo {
    match put_conditional(st, key, content, PutCondition::Create).await.unwrap() {
        VersionedUpdateResult::Updated { new_version } => new_version,
        VersionedUpdateResult::NotOnLatestVersion => panic!("{key} already exists"),
    }
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

async fn update(
    st: &FilesystemStorage,
    key: &str,
    content: &[u8],
    base: &VersionInfo,
) -> StorageResult<VersionedUpdateResult> {
    put_conditional(st, key, content, PutCondition::IfVersion(base)).await
}

async fn copy(
    st: &FilesystemStorage,
    from: &str,
    to: &str,
    version: &VersionInfo,
) -> StorageResult<VersionedUpdateResult> {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    st.copy_object(&ctx, from, to, None, version).await
}

async fn get_conditional(
    st: &FilesystemStorage,
    key: &str,
    previous: Option<&VersionInfo>,
) -> Option<(Vec<u8>, VersionInfo)> {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    match st.get_object_conditional(&ctx, key, previous).await.unwrap() {
        GetModifiedResult::Modified { mut data, new_version } => {
            let mut buf = Vec::new();
            data.read_to_end(&mut buf).await.unwrap();
            Some((buf, new_version))
        }
        GetModifiedResult::OnLatestVersion => None,
    }
}

async fn get(st: &FilesystemStorage, key: &str) -> (Vec<u8>, VersionInfo) {
    get_conditional(st, key, None).await.unwrap()
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

fn assert_other_error(result: StorageResult<VersionedUpdateResult>) {
    let err = result.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::Other(_)), "{err}");
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

fn protocol_files(dir: &Path) -> BTreeSet<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.contains(".new_bytes.") || name.contains(".lockinfo."))
        .collect()
}

fn etag(version: &VersionInfo) -> String {
    version.etag().unwrap().clone()
}

#[tokio_test]
async fn a_stale_version_cannot_update_or_back_up() {
    let dir = tempfile::tempdir().unwrap();
    let a = storage(dir.path());
    let b = storage(dir.path());
    create(&a, "repo", V0).await;
    let (_, a_base) = get(&a, "repo").await;
    let (_, b_base) = get(&b, "repo").await;

    assert_updated(update(&a, "repo", b"a", &a_base).await);
    assert_not_on_latest(&update(&b, "repo", b"b", &b_base).await);
    assert_not_on_latest(&copy(&b, "repo", "overwritten/x", &b_base).await);

    assert_eq!(get(&a, "repo").await.0, b"a");
    assert!(!dir.path().join("overwritten").exists());
}

#[tokio_test]
async fn copy_writes_the_current_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    create(&st, "repo", V0).await;
    let (_, base) = get(&st, "repo").await;
    let v1 = assert_updated(update(&st, "repo", b"v1", &base).await);

    assert_eq!(assert_updated(copy(&st, "repo", "overwritten/x", &v1).await), v1);
    assert_eq!(std::fs::read(dir.path().join("overwritten/x")).unwrap(), b"v1");
    assert_eq!(get(&st, "repo").await.0, b"v1");
}

#[tokio_test]
async fn copy_from_a_missing_object_is_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let version = create(&st, "repo", V0).await;

    let err = copy(&st, "missing", "overwritten/x", &version).await.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::ObjectNotFound), "{err}");
    assert!(!dir.path().join("overwritten").exists());
}

#[tokio_test]
async fn the_updated_version_is_the_read_version() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    create(&st, "repo", V0).await;
    let (_, base) = get(&st, "repo").await;
    let v1 = assert_updated(update(&st, "repo", b"v1", &base).await);

    assert_eq!(get_conditional(&st, "repo", Some(&v1)).await, None);
    assert_eq!(get(&st, "repo").await, (b"v1".to_vec(), v1));
}

#[tokio_test]
async fn a_version_without_etag_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    create(&st, "repo", V0).await;
    let no_etag = VersionInfo::for_creation();

    assert_other_error(update(&st, "repo", b"v1", &no_etag).await);
    assert_other_error(copy(&st, "repo", "overwritten/x", &no_etag).await);
    assert_eq!(get(&st, "repo").await.0, V0);
    assert!(!dir.path().join("overwritten").exists());
}

#[tokio_test]
async fn an_invalid_etag_is_not_on_latest_and_touches_nothing() {
    let outer = tempfile::tempdir().unwrap();
    let root = outer.path().join("a").join("b").join("root");
    std::fs::create_dir_all(&root).unwrap();
    let st = storage(&root);
    let current = etag(&create(&st, "repo", V0).await);
    let before = tree(outer.path());

    for invalid in ["../../escape".to_string(), current.to_uppercase()] {
        let version = VersionInfo { etag: Some(ETag(invalid)), generation: None };
        assert_not_on_latest(&update(&st, "repo", b"v1", &version).await);
        assert_not_on_latest(&copy(&st, "repo", "overwritten/x", &version).await);
    }

    assert_eq!(tree(outer.path()), before);
    assert_eq!(get(&st, "repo").await.0, V0);
}

#[tokio_test]
async fn updating_a_missing_object_is_not_on_latest_and_leaves_at_most_a_lock() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let version = create(&st, "repo", V0).await;
    let before = tree(dir.path());

    assert_not_on_latest(&update(&st, "a/b/nothere", b"v1", &version).await);
    assert_eq!(tree(dir.path()), before);

    assert_not_on_latest(&update(&st, "nothere", b"v1", &version).await);
    // A lock taken on a non-current hash is left for GC, like any failed
    // attempt's lock.
    let added: Vec<_> = tree(dir.path()).difference(&before).cloned().collect();
    assert!(
        added.iter().all(|path| path.to_str().unwrap().starts_with("nothere.lock.")),
        "{added:?}"
    );
    assert!(!dir.path().join("nothere").exists());
    assert_eq!(protocol_files(dir.path()), BTreeSet::new());
}

#[tokio_test]
async fn repeated_content_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    create(&st, "repo", V0).await;
    let (_, base) = get(&st, "repo").await;

    assert_other_error(update(&st, "repo", V0, &base).await);
    assert_eq!(get(&st, "repo").await, (V0.to_vec(), base));
    assert_eq!(protocol_files(dir.path()), BTreeSet::new());
}

#[tokio_test]
async fn concurrent_updates_from_one_base_exactly_one_wins() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    create(&st, "repo", V0).await;
    let (_, base) = get(&st, "repo").await;

    let tasks: Vec<_> = (0..16)
        .map(|task| {
            let st = storage(dir.path());
            let base = base.clone();
            tokio::spawn(async move {
                let content = format!("writer {task}").into_bytes();
                let result = update(&st, "repo", &content, &base).await.unwrap();
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
    assert_eq!(get(&st, "repo").await, (content.clone(), version.clone()));
    // Losers may leave their locks behind: only tickets and lockinfos are
    // always cleaned up.
    assert_eq!(protocol_files(dir.path()), BTreeSet::new());
}

#[tokio_test]
async fn retrying_writers_lose_no_update() {
    const TASKS: usize = 8;
    const SUCCESSES: usize = 5;
    const MAX_ATTEMPTS: usize = 1000;

    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let initial = etag(&create(&st, "repo", V0).await);

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
                        let (_, base) = get(&st, "repo").await;
                        match update(&st, "repo", &content, &base).await.unwrap() {
                            VersionedUpdateResult::Updated { new_version } => {
                                updates.push((etag(&base), etag(&new_version)));
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

    let next: BTreeMap<_, _> = updates.iter().cloned().collect();
    assert_eq!(next.len(), updates.len(), "a base was replaced twice");
    let new_etags: BTreeSet<_> = updates.iter().map(|(_, new)| new).collect();
    assert_eq!(new_etags.len(), updates.len(), "a new etag repeats");

    let mut visited = BTreeSet::new();
    let mut current = initial;
    while let Some(new) = next.get(&current) {
        assert!(visited.insert(current.clone()), "the chain has a cycle");
        current = new.clone();
    }
    assert_eq!(visited.len(), updates.len(), "updates outside the chain");
    assert_eq!(current, etag(&get(&st, "repo").await.1));
    assert_eq!(protocol_files(dir.path()), BTreeSet::new());
}
