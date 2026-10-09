#![cfg(all(unix, feature = "fault-injection"))]
#![expect(clippy::unwrap_used, clippy::panic)]

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use icechunk_fs::{
    FilesystemStorage,
    fault_injection::{CasHooks, Point},
};
use icechunk_macros::tokio_test;
use icechunk_storage::{
    GetModifiedResult, PutCondition, Settings, Storage as _, StorageContext,
    StorageResult, VersionInfo, VersionedUpdateResult,
};

/// Records every point reached; `force_compare` overrides step 4's verdict.
#[derive(Debug, Default)]
struct Recorder {
    reached: Mutex<Vec<Point>>,
    force_compare: bool,
}

impl CasHooks for Recorder {
    fn at(&self, point: Point) {
        self.reached.lock().unwrap().push(point);
    }

    fn compared(&self, matches: bool) -> bool {
        matches || self.force_compare
    }
}

impl Recorder {
    fn take(&self) -> Vec<Point> {
        std::mem::take(&mut *self.reached.lock().unwrap())
    }
}

fn with_hooks(dir: &Path, hooks: &Arc<Recorder>) -> FilesystemStorage {
    let hooks: Arc<dyn CasHooks> = Arc::clone(hooks) as _;
    FilesystemStorage::new(dir).unwrap().with_cas_hooks(hooks)
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
            Point::LockInfoWritten,
            Point::TicketWritten,
            Point::LockTaken { generation: 0 },
            Point::Fenced,
            Point::Compared,
            Point::Renamed,
        ]
    );
    assert_eq!(std::fs::read(dir.path().join("repo")).unwrap(), b"v2");
}

#[tokio_test]
async fn storage_without_hooks_is_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let st = FilesystemStorage::new(dir.path()).unwrap();
    assert_updated(&put(&st, b"v1", PutCondition::Create).await);
    let v1 = latest(&st).await;
    assert_updated(&put(&st, b"v2", PutCondition::IfVersion(&v1)).await);
    assert_eq!(std::fs::read(dir.path().join("repo")).unwrap(), b"v2");
}

#[tokio_test]
async fn forced_compare_through_the_public_api() {
    // Separate directories: a refused update leaves its lock behind, which
    // would stop the next writer on that base before the compare.
    let plain_dir = tempfile::tempdir().unwrap();
    let v1 = stale_v1(plain_dir.path()).await;
    let plain = FilesystemStorage::new(plain_dir.path()).unwrap();
    let refused = put(&plain, b"v3", PutCondition::IfVersion(&v1)).await;
    assert!(
        matches!(refused, Ok(VersionedUpdateResult::NotOnLatestVersion)),
        "{refused:?}"
    );
    assert_eq!(std::fs::read(plain_dir.path().join("repo")).unwrap(), b"v2");

    let dir = tempfile::tempdir().unwrap();
    let v1 = stale_v1(dir.path()).await;
    let hooks = Arc::new(Recorder { force_compare: true, ..Recorder::default() });
    let sabotaged = with_hooks(dir.path(), &hooks);
    assert_updated(&put(&sabotaged, b"v3", PutCondition::IfVersion(&v1)).await);
    assert_eq!(std::fs::read(dir.path().join("repo")).unwrap(), b"v3");
}
