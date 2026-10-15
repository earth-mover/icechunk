#![cfg(all(unix, feature = "fault-injection"))]
#![expect(clippy::unwrap_used, clippy::panic)]

use std::{
    collections::HashSet,
    path::Path,
    sync::{Arc, Mutex},
};

use icechunk_fs::{
    FilesystemStorage,
    fault_injection::{Point, ProtocolHooks, Sabotage},
};
use icechunk_macros::tokio_test;
use icechunk_storage::{
    GetModifiedResult, PutCondition, Settings, Storage as _, StorageContext,
    StorageResult, VersionInfo, VersionedUpdateResult,
};
use tokio::io::AsyncReadExt as _;

/// Records every point reached and turns on `sabotages`.
#[derive(Debug, Default)]
struct Recorder {
    reached: Mutex<Vec<Point>>,
    sabotages: HashSet<Sabotage>,
}

impl ProtocolHooks for Recorder {
    fn at(&self, point: Point) {
        self.reached.lock().unwrap().push(point);
    }

    fn sabotaged(&self, sabotage: Sabotage) -> bool {
        self.sabotages.contains(&sabotage)
    }
}

impl Recorder {
    fn take(&self) -> Vec<Point> {
        std::mem::take(&mut *self.reached.lock().unwrap())
    }
}

fn with_hooks(dir: &Path, hooks: &Arc<Recorder>) -> FilesystemStorage {
    let hooks: Arc<dyn ProtocolHooks> = Arc::clone(hooks) as _;
    FilesystemStorage::new(dir).unwrap().with_protocol_hooks(hooks)
}

async fn put(
    st: &FilesystemStorage,
    content: &[u8],
    condition: PutCondition<'_>,
) -> StorageResult<VersionedUpdateResult> {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    st.put_object_conditional(
        &ctx,
        "repo",
        content.to_vec().into(),
        None,
        Vec::new(),
        condition,
    )
    .await
}

async fn latest(st: &FilesystemStorage) -> VersionInfo {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    match st.get_object_conditional(&ctx, "repo", None).await.unwrap() {
        GetModifiedResult::Modified { new_version, .. } => new_version,
        GetModifiedResult::OnLatestVersion => panic!("expected Modified"),
    }
}

async fn read(st: &FilesystemStorage) -> Vec<u8> {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    let mut buf = Vec::new();
    st.get_object(&ctx, "repo", None).await.unwrap().read_to_end(&mut buf).await.unwrap();
    buf
}

fn assert_updated(result: &StorageResult<VersionedUpdateResult>) {
    assert!(matches!(result, Ok(VersionedUpdateResult::Updated { .. })), "{result:?}");
}

/// Creates `repo` as `v1`, updates it to `v2` and returns the stale `v1` version.
async fn stale_v1(dir: &Path) -> VersionInfo {
    let st = FilesystemStorage::new(dir).unwrap();
    assert_updated(&put(&st, b"v1", PutCondition::Create).await);
    let v1 = latest(&st).await;
    assert_updated(&put(&st, b"v2", PutCondition::IfVersion(&v1)).await);
    v1
}

#[tokio_test]
async fn hooks_see_every_point_of_an_update() {
    let dir = tempfile::tempdir().unwrap();
    let hooks = Arc::new(Recorder::default());
    let st = with_hooks(dir.path(), &hooks);

    assert_updated(&put(&st, b"v1", PutCondition::Create).await);
    assert_eq!(hooks.take(), []);
    let v1 = latest(&st).await;
    assert_updated(&put(&st, b"v2", PutCondition::IfVersion(&v1)).await);
    assert_eq!(
        hooks.take(),
        [
            Point::Staged,
            Point::Linked,
            Point::BeforeRename,
            Point::Renamed,
            Point::BeforeTidyUp,
        ]
    );
    assert_eq!(read(&st).await, b"v2");
}

#[tokio_test]
async fn storage_without_hooks_is_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let st = FilesystemStorage::new(dir.path()).unwrap();
    assert_updated(&put(&st, b"v1", PutCondition::Create).await);
    let v1 = latest(&st).await;
    assert_updated(&put(&st, b"v2", PutCondition::IfVersion(&v1)).await);
    assert_eq!(read(&st).await, b"v2");
}

#[tokio_test]
async fn renaming_from_the_listed_marker_commits_a_stale_base() {
    let plain_dir = tempfile::tempdir().unwrap();
    let v1 = stale_v1(plain_dir.path()).await;
    let plain = FilesystemStorage::new(plain_dir.path()).unwrap();
    let refused = put(&plain, b"v3", PutCondition::IfVersion(&v1)).await;
    assert!(
        matches!(refused, Ok(VersionedUpdateResult::NotOnLatestVersion)),
        "{refused:?}"
    );
    assert_eq!(read(&plain).await, b"v2");

    let dir = tempfile::tempdir().unwrap();
    let v1 = stale_v1(dir.path()).await;
    let hooks = Arc::new(Recorder {
        sabotages: HashSet::from([Sabotage::RenameFromListedMarker]),
        ..Recorder::default()
    });
    let sabotaged = with_hooks(dir.path(), &hooks);
    assert_updated(&put(&sabotaged, b"v3", PutCondition::IfVersion(&v1)).await);
    assert_eq!(read(&sabotaged).await, b"v3");
}
