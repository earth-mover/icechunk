#![cfg(unix)]
#![expect(clippy::unwrap_used)]

use std::{
    collections::BTreeSet,
    os::unix::fs::MetadataExt as _,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures::{StreamExt as _, TryStreamExt as _};
use icechunk_format::writer_features::WriterFeature;
use icechunk_fs::{FilesystemStorage, new_filesystem_storage};
use icechunk_macros::tokio_test;
use icechunk_storage::{
    GcPhase, GetModifiedResult, ListInfo, PutCondition, Settings, Storage,
    StorageContext, StorageErrorKind, StorageResult, VersionInfo, VersionedUpdateResult,
    WriteHint,
};
use tokio::io::AsyncReadExt as _;

#[path = "common/repo_infos.rs"]
#[expect(dead_code, reason = "shared with tests that use every helper")]
mod repo_infos;

use repo_infos::repo_info;

const CONTENT: &[u8] = b"hello world";
const CONTENT_SHA256: &str =
    "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";

fn storage(dir: &Path) -> FilesystemStorage {
    FilesystemStorage::new(dir).unwrap()
}

fn write(dir: &Path, key: &str, content: &[u8]) {
    let path = dir.join(key);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

async fn get(
    st: &FilesystemStorage,
    key: &str,
    range: Option<&std::ops::Range<u64>>,
) -> Vec<u8> {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    let mut buf = Vec::new();
    st.get_object(&ctx, key, range).await.unwrap().read_to_end(&mut buf).await.unwrap();
    buf
}

async fn put(st: &FilesystemStorage, key: &str, content: &[u8], hint: WriteHint) {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    st.put_object(&ctx, key, content.to_vec().into(), None, Vec::new(), hint)
        .await
        .unwrap();
}

fn staging_leftovers(dir: &Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(dir)
        .unwrap()
        .flat_map(|entry| {
            let path = entry.unwrap().path();
            if path.is_dir() {
                staging_leftovers(&path)
            } else if path.file_name().unwrap().to_string_lossy().contains(".staging.") {
                vec![path]
            } else {
                Vec::new()
            }
        })
        .collect()
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

#[test]
fn serde_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let st: Arc<dyn Storage> = Arc::new(storage(dir.path()).with_fsync(false));
    let json = serde_json::to_string(&st).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(
        value,
        serde_json::json!({
            "type": "FilesystemStorage",
            "root": dir.path(),
            "fsync": false,
        })
    );

    let back: Arc<dyn Storage> = serde_json::from_str(&json).unwrap();
    assert_eq!(serde_json::to_string(&back).unwrap(), json);
}

#[tokio_test]
async fn reads() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    write(dir.path(), "a/b", b"0123456789");

    assert_eq!(get(&st, "a/b", None).await, b"0123456789");
    assert_eq!(get(&st, "a/b", Some(&(2..5))).await, b"234");
    assert_eq!(get(&st, "a/b", Some(&(3..3))).await, b"");
}

#[tokio_test]
async fn missing_object_is_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);

    let err = st.get_object(&ctx, "a/missing", None).await.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::ObjectNotFound), "{err}");
    let err = st.get_object_conditional(&ctx, "a/missing", None).await.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::ObjectNotFound), "{err}");
}

#[tokio_test]
async fn range_past_end_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    write(dir.path(), "a/b", b"0123456789");
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);

    for range in [5..11, 20..20] {
        let err = st.get_object(&ctx, "a/b", Some(&range)).await.err().unwrap();
        assert!(matches!(err.kind, StorageErrorKind::IOError(_)), "{range:?}: {err}");
    }
    // get_object would split this range before reaching the backend
    let err = st.get_object_range(&ctx, "a/b", Some(&(0..u64::MAX))).await.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::IOError(_)), "{err}");
}

#[tokio_test]
async fn path_traversal_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(&dir.path().join("root"));
    write(dir.path(), "x", CONTENT);
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);

    for key in ["../x", "/etc/passwd"] {
        let err = st.get_object(&ctx, key, None).await.err().unwrap();
        assert!(!matches!(err.kind, StorageErrorKind::ObjectNotFound), "{key}: {err}");
    }
}

#[tokio_test]
async fn conditional_get_versions_by_content() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    write(dir.path(), "a/b", CONTENT);

    let (data, version) = get_conditional(&st, "a/b", None).await.unwrap();
    assert_eq!(data, CONTENT);
    assert_eq!(version.etag().unwrap(), CONTENT_SHA256);
    assert_eq!(version.generation, None);

    assert!(get_conditional(&st, "a/b", Some(&version)).await.is_none());

    write(dir.path(), "a/b", b"something else");
    let (data, new_version) = get_conditional(&st, "a/b", Some(&version)).await.unwrap();
    assert_eq!(data, b"something else");
    assert_ne!(new_version, version);
}

#[tokio_test]
async fn relative_root_is_made_absolute() {
    let relative = Path::new("some/relative/dir");
    assert!(storage(relative).root().is_absolute());

    let st = new_filesystem_storage(relative).await.unwrap();
    let info = st.storage_info();
    assert_eq!(info.backend_type, "filesystem");
    assert!(info.fields.iter().any(|(k, v)| *k == "path" && Path::new(v).is_absolute()));
}

#[tokio_test]
async fn atomic_put_creates_parents() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());

    put(&st, "a/b/c", CONTENT, WriteHint::Atomic).await;
    assert_eq!(get(&st, "a/b/c", None).await, CONTENT);
    assert_eq!(std::fs::read(dir.path().join("a/b/c")).unwrap(), CONTENT);
    assert_eq!(staging_leftovers(dir.path()), Vec::<std::path::PathBuf>::new());
}

#[tokio_test]
async fn atomic_put_replaces() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());

    put(&st, "a/b", CONTENT, WriteHint::Atomic).await;
    put(&st, "a/b", b"replaced", WriteHint::Atomic).await;
    assert_eq!(get(&st, "a/b", None).await, b"replaced");
    assert_eq!(staging_leftovers(dir.path()), Vec::<std::path::PathBuf>::new());
}

#[tokio_test]
async fn new_object_put_creates() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());

    put(&st, "chunks/x", CONTENT, WriteHint::NewObject).await;
    assert_eq!(get(&st, "chunks/x", None).await, CONTENT);
    assert_eq!(staging_leftovers(dir.path()), Vec::<std::path::PathBuf>::new());
}

#[tokio_test]
async fn new_object_put_on_existing_name_falls_back_to_staging() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());

    put(&st, "chunks/x", b"a much longer first version", WriteHint::NewObject).await;
    let first_inode = std::fs::metadata(dir.path().join("chunks/x")).unwrap().ino();
    put(&st, "chunks/x", CONTENT, WriteHint::NewObject).await;
    assert_eq!(get(&st, "chunks/x", None).await, CONTENT);
    // a rename installs a new inode; writing in place would keep the old one
    assert_ne!(
        std::fs::metadata(dir.path().join("chunks/x")).unwrap().ino(),
        first_inode
    );
    assert_eq!(staging_leftovers(dir.path()), Vec::<std::path::PathBuf>::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readers_never_see_partial_atomic_writes() {
    const SIZE: usize = 256 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let st = Arc::new(storage(dir.path()).with_fsync(false));
    put(&st, "a/b", &[b'a'; SIZE], WriteHint::Atomic).await;

    let done = Arc::new(AtomicBool::new(false));
    let reader = {
        let (st, done) = (Arc::clone(&st), Arc::clone(&done));
        tokio::spawn(async move {
            let mut reads = 0;
            while !done.load(Ordering::Relaxed) {
                let data = get(&st, "a/b", None).await;
                assert_eq!(data.len(), SIZE);
                assert!(data.iter().all(|byte| *byte == data[0]));
                reads += 1;
            }
            reads
        })
    };
    for i in 0..200 {
        let byte = if i % 2 == 0 { b'b' } else { b'a' };
        put(&st, "a/b", &[byte; SIZE], WriteHint::Atomic).await;
    }
    done.store(true, Ordering::Relaxed);
    assert!(reader.await.unwrap() > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_atomic_writes_leave_one_whole_object() {
    let dir = tempfile::tempdir().unwrap();
    let st = Arc::new(storage(dir.path()));

    let contents: Vec<_> = (0..16u8).map(|i| vec![i; 4096]).collect();
    let tasks: Vec<_> = contents
        .iter()
        .cloned()
        .map(|content| {
            let st = Arc::clone(&st);
            tokio::spawn(
                async move { put(&st, "a/b", &content, WriteHint::Atomic).await },
            )
        })
        .collect();
    futures::future::try_join_all(tasks).await.unwrap();

    assert!(contents.contains(&get(&st, "a/b", None).await));
    assert_eq!(staging_leftovers(dir.path()), Vec::<std::path::PathBuf>::new());
}

#[tokio_test]
async fn failed_rename_removes_the_staging_file() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    write(dir.path(), "a/b/c", CONTENT);
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);

    // renaming a file over a non-empty directory fails
    let err = st
        .put_object(&ctx, "a/b", CONTENT.into(), None, Vec::new(), WriteHint::Atomic)
        .await
        .unwrap_err();
    assert!(matches!(err.kind, StorageErrorKind::IOError(_)), "{err}");
    assert_eq!(staging_leftovers(dir.path()), Vec::<std::path::PathBuf>::new());
    assert_eq!(get(&st, "a/b/c", None).await, CONTENT);
}

#[tokio_test]
async fn put_without_fsync() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path()).with_fsync(false);

    put(&st, "a/b", CONTENT, WriteHint::Atomic).await;
    put(&st, "a/c", b"other", WriteHint::NewObject).await;
    assert_eq!(get(&st, "a/b", None).await, CONTENT);
    assert_eq!(get(&st, "a/c", None).await, b"other");
}

async fn put_conditional(
    st: &FilesystemStorage,
    settings: &Settings,
    key: &str,
    content: &[u8],
    condition: PutCondition<'_>,
) -> StorageResult<VersionedUpdateResult> {
    let ctx = StorageContext::unattributed(settings);
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

#[tokio_test]
async fn create_location_creates_an_empty_root() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("a/repo");
    let st = storage(&root);

    st.create_location_if_needed().await.unwrap();
    assert!(root.is_dir());
    st.create_location_if_needed().await.unwrap();
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
}

#[tokio_test]
async fn conditional_create() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let settings = Settings::default();

    let VersionedUpdateResult::Updated { new_version } =
        put_conditional(&st, &settings, "config.yaml", CONTENT, PutCondition::Create)
            .await
            .unwrap()
    else {
        panic!("first create must succeed")
    };
    assert_eq!(new_version.etag().unwrap(), CONTENT_SHA256);
    assert_eq!(new_version.generation, None);
    assert_eq!(
        get_conditional(&st, "config.yaml", None).await.unwrap(),
        (CONTENT.to_vec(), new_version.clone())
    );
    assert_eq!(get(&st, "config.yaml", None).await, CONTENT);
    assert_eq!(staging_leftovers(dir.path()), Vec::<std::path::PathBuf>::new());

    let second =
        put_conditional(&st, &settings, "config.yaml", b"other", PutCondition::Create)
            .await
            .unwrap();
    assert!(matches!(second, VersionedUpdateResult::NotOnLatestVersion));
    assert_eq!(get(&st, "config.yaml", None).await, CONTENT);
    assert_eq!(staging_leftovers(dir.path()), Vec::<std::path::PathBuf>::new());

    assert!(get_conditional(&st, "config.yaml", Some(&new_version)).await.is_none());
}

#[tokio_test]
async fn conditional_create_of_identical_content_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let settings = Settings::default();
    write(dir.path(), "config.yaml", CONTENT);

    let VersionedUpdateResult::Updated { new_version } =
        put_conditional(&st, &settings, "config.yaml", CONTENT, PutCondition::Create)
            .await
            .unwrap()
    else {
        panic!("the object already holds this version")
    };
    assert_eq!(
        get_conditional(&st, "config.yaml", None).await.unwrap(),
        (CONTENT.to_vec(), new_version)
    );
    assert_eq!(staging_leftovers(dir.path()), Vec::<std::path::PathBuf>::new());
}

#[tokio_test]
async fn conditional_create_makes_parents() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path()).with_fsync(false);
    let settings = Settings::default();

    let result = put_conditional(&st, &settings, "a/b/c", CONTENT, PutCondition::Create)
        .await
        .unwrap();
    assert!(matches!(result, VersionedUpdateResult::Updated { .. }));
    assert_eq!(get(&st, "a/b/c", None).await, CONTENT);
    assert_eq!(staging_leftovers(dir.path()), Vec::<std::path::PathBuf>::new());
}

#[tokio_test]
async fn disabled_conditional_settings_are_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());

    for (key, settings) in [
        (
            "a",
            Settings { unsafe_use_conditional_update: Some(false), ..Default::default() },
        ),
        (
            "b",
            Settings { unsafe_use_conditional_create: Some(false), ..Default::default() },
        ),
    ] {
        let first = put_conditional(&st, &settings, key, CONTENT, PutCondition::Create)
            .await
            .unwrap();
        assert!(matches!(first, VersionedUpdateResult::Updated { .. }), "{key}");
        let second = put_conditional(&st, &settings, key, b"other", PutCondition::Create)
            .await
            .unwrap();
        assert!(matches!(second, VersionedUpdateResult::NotOnLatestVersion), "{key}");
        assert_eq!(get(&st, key, None).await, CONTENT);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_creates_have_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let st = Arc::new(storage(dir.path()));

    let tasks: Vec<_> = (0..16u8)
        .map(|i| {
            let st = Arc::clone(&st);
            tokio::spawn(async move {
                let content = repo_info(u32::from(i));
                let settings = Settings::default();
                let result = put_conditional(
                    &st,
                    &settings,
                    "repo",
                    &content,
                    PutCondition::Create,
                )
                .await
                .unwrap();
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
    assert_eq!(
        get_conditional(&st, "repo", None).await.unwrap(),
        (content.clone(), version.clone())
    );
    assert_eq!(std::fs::read(dir.path().join("repo")).unwrap(), *content);
    assert_eq!(staging_leftovers(dir.path()), Vec::<std::path::PathBuf>::new());
    let root: BTreeSet<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(root, BTreeSet::from(["repo".to_string(), "repo.d".to_string()]));
}

async fn list_infos(st: &FilesystemStorage, prefix: &str) -> Vec<ListInfo<String>> {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    let mut infos: Vec<_> =
        st.list_objects(&ctx, prefix).await.unwrap().try_collect().await.unwrap();
    infos.sort_by(|a, b| a.id.cmp(&b.id));
    infos
}

async fn list(st: &FilesystemStorage, prefix: &str) -> Vec<(String, u64)> {
    list_infos(st, prefix)
        .await
        .into_iter()
        .map(|info| (info.id, info.size_bytes))
        .collect()
}

#[tokio_test]
async fn list_missing_root_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(&dir.path().join("missing"));
    assert_eq!(list(&st, "").await, Vec::new());
}

#[tokio_test]
async fn implements_and_requires_the_filesystem_layout() {
    let dir = tempfile::tempdir().unwrap();
    let features = storage(dir.path()).writer_features().await.unwrap();
    assert_eq!(features, BTreeSet::from([WriterFeature::FilesystemLayoutV1]));
}

#[tokio_test]
async fn gc_hook_without_a_repository_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    for phase in [
        GcPhase::BeforeDeletes { changed: None },
        GcPhase::AfterDeletes { changed: None },
    ] {
        st.gc_hook(&ctx, phase).await.unwrap();
    }
    assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
}

#[tokio_test]
async fn root_is_clean() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);

    assert!(st.root_is_clean(&ctx).await.unwrap());
    put(&st, "a", CONTENT, WriteHint::Atomic).await;
    assert!(!st.root_is_clean(&ctx).await.unwrap());
}

#[tokio_test]
async fn nested_listing() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    put(&st, "p/a", CONTENT, WriteHint::Atomic).await;
    put(&st, "p/b/c", b"abc", WriteHint::NewObject).await;
    put(&st, "q/d", b"d", WriteHint::Atomic).await;

    let expected = vec![("a".to_string(), CONTENT.len() as u64), ("b/c".to_string(), 3)];
    assert_eq!(list(&st, "p").await, expected);
    assert_eq!(list(&st, "p/").await, expected);
    assert_eq!(list(&st, "missing").await, Vec::new());

    for info in list_infos(&st, "p").await {
        let mtime = std::fs::metadata(dir.path().join("p").join(&info.id))
            .unwrap()
            .modified()
            .unwrap();
        assert_eq!(info.created_at, chrono::DateTime::<chrono::Utc>::from(mtime));
    }
}

#[tokio_test]
async fn listing_a_file_prefix_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    put(&st, "p/a", CONTENT, WriteHint::Atomic).await;

    assert_eq!(list(&st, "p/a").await, Vec::new());
    assert_eq!(list(&st, "p/a/b").await, Vec::new());
}

fn write_many(dir: &Path, count: usize) -> Vec<String> {
    (0..count)
        .map(|i| {
            let key = format!("p/{}/{i}", i % 7);
            write(dir, &key, b"x");
            key
        })
        .collect()
}

#[tokio_test]
async fn listing_larger_than_a_batch() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let mut keys = write_many(dir.path(), 2500);
    keys.sort();

    let listed: Vec<String> = list(&st, "").await.into_iter().map(|(id, _)| id).collect();
    assert_eq!(listed, keys);
}

#[test]
fn dropping_a_listing_stops_the_walk() {
    let dir = tempfile::tempdir().unwrap();
    write_many(dir.path(), 5000);
    let st = storage(dir.path());

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let first = runtime.block_on(async {
            let settings = Settings::default();
            let ctx = StorageContext::unattributed(&settings);
            let mut stream = st.list_objects(&ctx, "").await.unwrap();
            stream.next().await.unwrap().unwrap()
        });
        // dropping the runtime waits for the blocking walker to return
        drop(runtime);
        done_tx.send(first.id).unwrap();
    });
    let first = done_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(first.starts_with("p/"), "{first}");
}

#[tokio_test]
async fn listing_skips_staging_files() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    put(&st, "p/a", CONTENT, WriteHint::Atomic).await;
    write(dir.path(), &format!("p/x.staging.{}", uuid::Uuid::new_v4()), b"partial");

    assert_eq!(list(&st, "p").await, vec![("a".to_string(), CONTENT.len() as u64)]);
}

#[tokio_test]
async fn list_objects_hides_protocol_files() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let id = uuid::Uuid::new_v4();
    for key in [
        "repo".to_string(),
        "chunks/ABC".to_string(),
        format!("repo.d/0.{CONTENT_SHA256}"),
        format!("repo.d/stage.{id}"),
        format!("repo.d.tmp.{id}/0.{CONTENT_SHA256}"),
        format!("repo.staging.{id}"),
        format!("chunks/ABC.staging.{id}"),
        // Root protocol names are only hidden at the root.
        "chunks/repo.d/x".to_string(),
        // Names that are no longer protocol names are ordinary names.
        format!("repo.legacy.{id}"),
        format!("repo.stub.{id}"),
    ] {
        write(dir.path(), &key, CONTENT);
    }

    let listed: Vec<String> = list(&st, "").await.into_iter().map(|(id, _)| id).collect();
    assert_eq!(
        listed,
        vec![
            "chunks/ABC".to_string(),
            "chunks/repo.d/x".to_string(),
            "repo".to_string(),
            format!("repo.legacy.{id}"),
            format!("repo.stub.{id}"),
        ]
    );
    let listed: Vec<String> =
        list(&st, "chunks").await.into_iter().map(|(id, _)| id).collect();
    assert_eq!(listed, vec!["ABC".to_string(), "repo.d/x".to_string()]);
}

#[tokio_test]
async fn root_with_only_leftovers_is_clean() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    let id = uuid::Uuid::new_v4();
    write(dir.path(), &format!("repo.d/stage.{id}"), b"");
    write(dir.path(), &format!("repo.d.tmp.{id}/0.{CONTENT_SHA256}"), b"");
    write(dir.path(), &format!("repo.staging.{id}"), b"");

    assert!(st.root_is_clean(&ctx).await.unwrap());

    // Names that are no longer protocol names are objects.
    write(dir.path(), &format!("repo.stub.{id}"), b"");
    assert!(!st.root_is_clean(&ctx).await.unwrap());
}

// APFS rejects non-UTF-8 names
#[cfg(target_os = "linux")]
#[tokio_test]
async fn non_utf8_names_are_skipped() {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt as _};

    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let chunks = dir.path().join("chunks");
    write(dir.path(), "chunks/ABC", CONTENT);
    std::fs::write(chunks.join(OsStr::from_bytes(b"\xff")), CONTENT).unwrap();
    let bad_dir = chunks.join(OsStr::from_bytes(b"\xfe"));
    std::fs::create_dir_all(&bad_dir).unwrap();
    std::fs::write(bad_dir.join("XYZ"), CONTENT).unwrap();

    assert_eq!(
        list(&st, "chunks").await,
        vec![("ABC".to_string(), CONTENT.len() as u64)]
    );
}

#[tokio_test]
async fn delete_batch_counts_missing_objects() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    put(&st, "p/a", CONTENT, WriteHint::Atomic).await;
    put(&st, "p/b", CONTENT, WriteHint::Atomic).await;
    let n = CONTENT.len() as u64;

    let result = st
        .delete_batch(&ctx, "p", vec![("a".to_string(), n), ("missing".to_string(), 7)])
        .await
        .unwrap();
    assert_eq!(result.deleted_objects, 2);
    assert_eq!(result.deleted_bytes, n + 7);
    assert!(!dir.path().join("p/a").exists());
    assert_eq!(list(&st, "").await, vec![("p/b".to_string(), n)]);
}

#[tokio_test]
async fn delete_batch_with_empty_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    put(&st, "p/a", CONTENT, WriteHint::Atomic).await;

    put(&st, "p/b", CONTENT, WriteHint::Atomic).await;

    let result = st.delete_batch(&ctx, "", vec![("p/a".to_string(), 1)]).await.unwrap();
    assert_eq!(result.deleted_objects, 1);
    assert_eq!(result.deleted_bytes, 1);
    assert!(!dir.path().join("p/a").exists());

    let result = st.delete_batch(&ctx, "p/", vec![("b".to_string(), 2)]).await.unwrap();
    assert_eq!(result.deleted_objects, 1);
    assert_eq!(result.deleted_bytes, 2);
    assert!(!dir.path().join("p/b").exists());
}

#[tokio_test]
async fn file_as_parent_component_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    put(&st, "a/b", CONTENT, WriteHint::Atomic).await;

    let err = st.get_object_last_modified(&ctx, "a/b/c").await.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::ObjectNotFound), "{err}");
    let err = st.get_object(&ctx, "a/b/c", None).await.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::ObjectNotFound), "{err}");

    let result = st.delete_batch(&ctx, "a/b", vec![("c".to_string(), 4)]).await.unwrap();
    assert_eq!(result.deleted_objects, 1);
    assert_eq!(result.deleted_bytes, 4);
    assert!(dir.path().join("a/b").is_file());
}

#[tokio_test]
async fn write_under_file_component_is_an_io_error() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    put(&st, "a/b", CONTENT, WriteHint::Atomic).await;

    for hint in [WriteHint::Atomic, WriteHint::NewObject] {
        let err = st
            .put_object(&ctx, "a/b/c/d", CONTENT.into(), None, Vec::new(), hint)
            .await
            .err()
            .unwrap();
        assert!(matches!(err.kind, StorageErrorKind::IOError(_)), "{err}");
    }
    let err = put_conditional(&st, &settings, "a/b/c/d", CONTENT, PutCondition::Create)
        .await
        .err()
        .unwrap();
    assert!(matches!(err.kind, StorageErrorKind::IOError(_)), "{err}");
}

#[tokio_test]
async fn delete_batch_rejects_invalid_keys() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(&dir.path().join("root"));
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    write(dir.path(), "x", CONTENT);

    let result = st.delete_batch(&ctx, "p", vec![("../../x".to_string(), 1)]).await;
    assert!(result.is_err());
    assert!(dir.path().join("x").exists());
}

#[tokio_test]
async fn last_modified() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    put(&st, "a/b", CONTENT, WriteHint::Atomic).await;

    let modified = st.get_object_last_modified(&ctx, "a/b").await.unwrap();
    let age = chrono::Utc::now() - modified;
    assert!(age.abs() < chrono::TimeDelta::seconds(5), "{modified}");

    let err = st.get_object_last_modified(&ctx, "a/missing").await.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::ObjectNotFound), "{err}");
    let err = st.get_object_last_modified(&ctx, "a").await.err().unwrap();
    assert!(matches!(err.kind, StorageErrorKind::ObjectNotFound), "{err}");
}

#[tokio_test]
async fn delete_batch_failures_are_not_counted() {
    let dir = tempfile::tempdir().unwrap();
    let st = storage(dir.path());
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    put(&st, "p/a", CONTENT, WriteHint::Atomic).await;
    std::fs::create_dir_all(dir.path().join("p/dir")).unwrap();

    let result = st
        .delete_batch(&ctx, "p", vec![("dir".to_string(), 5), ("a".to_string(), 3)])
        .await
        .unwrap();
    assert_eq!(result.deleted_objects, 1);
    assert_eq!(result.deleted_bytes, 3);
    assert!(dir.path().join("p/dir").is_dir());
}
