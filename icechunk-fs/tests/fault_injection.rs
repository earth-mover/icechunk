#![cfg(all(unix, feature = "fault-injection"))]
#![expect(clippy::unwrap_used, clippy::panic)]

use std::{
    collections::HashSet,
    fmt,
    path::Path,
    sync::{Arc, Mutex},
};

use icechunk_fs::{
    ConversionOutcome, FilesystemStorage,
    fault_injection::{
        ConversionStep, CreationStep, FromObjectStoreStep, Point, ProtocolHooks,
        Sabotage, ToObjectStoreStep,
    },
};
use icechunk_macros::tokio_test;
use icechunk_storage::{
    GetModifiedResult, PutCondition, Settings, Storage as _, StorageContext,
    StorageErrorKind, StorageResult, VersionInfo, VersionedUpdateResult,
};
use tokio::io::AsyncReadExt as _;

#[path = "common/repo_infos.rs"]
#[expect(dead_code, reason = "shared with tests that use every helper")]
mod repo_infos;

use repo_infos::{featureless_repo_info, repo_info};

type AtFn = Box<dyn Fn(Point) + Send + Sync>;

/// Records every point reached and turns on `sabotages`; `at` runs at every
/// point.
#[derive(Default)]
struct Recorder {
    reached: Mutex<Vec<Point>>,
    sabotages: HashSet<Sabotage>,
    at: Option<AtFn>,
}

impl fmt::Debug for Recorder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recorder").finish_non_exhaustive()
    }
}

impl ProtocolHooks for Recorder {
    fn at(&self, point: Point) {
        self.reached.lock().unwrap().push(point);
        if let Some(at) = &self.at {
            at(point);
        }
    }

    fn sabotaged(&self, sabotage: Sabotage) -> bool {
        self.sabotages.contains(&sabotage)
    }
}

fn sabotaged(sabotage: Sabotage) -> Arc<Recorder> {
    Arc::new(Recorder { sabotages: HashSet::from([sabotage]), ..Recorder::default() })
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

/// Creates `repo`, updates it to `v2` and returns the stale first version.
async fn stale_v1(dir: &Path) -> VersionInfo {
    let st = FilesystemStorage::new(dir).unwrap();
    assert_updated(&put(&st, &repo_info(1), PutCondition::Create).await);
    let v1 = latest(&st).await;
    assert_updated(&put(&st, &repo_info(2), PutCondition::IfVersion(&v1)).await);
    v1
}

fn root_repo_file(dir: &Path) -> Option<Vec<u8>> {
    std::fs::read(dir.join("repo")).ok()
}

#[tokio_test]
async fn hooks_see_every_point_of_a_creation_and_an_update() {
    let dir = tempfile::tempdir().unwrap();
    let hooks = Arc::new(Recorder::default());
    let st = with_hooks(dir.path(), &hooks);

    assert_updated(&put(&st, &repo_info(1), PutCondition::Create).await);
    assert_eq!(
        hooks.take(),
        [Point::Creation(CreationStep::Built), Point::Creation(CreationStep::DirRenamed)]
    );
    let v1 = latest(&st).await;
    assert_updated(&put(&st, &repo_info(2), PutCondition::IfVersion(&v1)).await);
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
    assert_eq!(read(&st).await, repo_info(2));
    assert_eq!(root_repo_file(dir.path()), None);
}

#[tokio_test]
async fn storage_without_hooks_is_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let st = FilesystemStorage::new(dir.path()).unwrap();
    assert_updated(&put(&st, &repo_info(1), PutCondition::Create).await);
    let v1 = latest(&st).await;
    assert_updated(&put(&st, &repo_info(2), PutCondition::IfVersion(&v1)).await);
    assert_eq!(read(&st).await, repo_info(2));
}

#[tokio_test]
async fn renaming_from_the_listed_marker_commits_a_stale_base() {
    let plain_dir = tempfile::tempdir().unwrap();
    let v1 = stale_v1(plain_dir.path()).await;
    let plain = FilesystemStorage::new(plain_dir.path()).unwrap();
    let refused = put(&plain, &repo_info(3), PutCondition::IfVersion(&v1)).await;
    assert!(
        matches!(refused, Ok(VersionedUpdateResult::NotOnLatestVersion)),
        "{refused:?}"
    );
    assert_eq!(read(&plain).await, repo_info(2));

    let dir = tempfile::tempdir().unwrap();
    let v1 = stale_v1(dir.path()).await;
    let hooks = sabotaged(Sabotage::RenameFromListedMarker);
    let sabotaged = with_hooks(dir.path(), &hooks);
    assert_updated(&put(&sabotaged, &repo_info(3), PutCondition::IfVersion(&v1)).await);
    assert_eq!(read(&sabotaged).await, repo_info(3));
}

#[tokio_test]
async fn hooks_see_every_point_of_a_conversion_from_the_object_store_layout() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("repo"), featureless_repo_info(0)).unwrap();
    let hooks = Arc::new(Recorder::default());
    let st = with_hooks(dir.path(), &hooks);

    assert_eq!(
        st.convert_to_filesystem_layout().await.unwrap(),
        ConversionOutcome::Converted
    );
    let step = |step| Point::Conversion(ConversionStep::FromObjectStore(step));
    assert_eq!(
        hooks.take(),
        [
            step(FromObjectStoreStep::Backup),
            step(FromObjectStoreStep::Build),
            step(FromObjectStoreStep::RenameDir),
            step(FromObjectStoreStep::RepoUnlinked),
        ]
    );
    assert!(icechunk_format::layout_v1::lists_feature(&read(&st).await).unwrap());
    assert_eq!(root_repo_file(dir.path()), None);
}

fn to_object_store(step: ToObjectStoreStep) -> Point {
    Point::Conversion(ConversionStep::ToObjectStore(step))
}

#[tokio_test]
async fn hooks_see_every_point_of_a_conversion_to_the_object_store_layout() {
    let dir = tempfile::tempdir().unwrap();
    let st = FilesystemStorage::new(dir.path()).unwrap();
    assert_updated(&put(&st, &repo_info(1), PutCondition::Create).await);
    let hooks = Arc::new(Recorder::default());
    let st = with_hooks(dir.path(), &hooks);

    assert_eq!(
        st.convert_to_object_store_layout().await.unwrap(),
        ConversionOutcome::Converted
    );
    assert_eq!(
        hooks.take(),
        [
            to_object_store(ToObjectStoreStep::BeforeChange),
            Point::Staged,
            Point::Linked,
            Point::BeforeRename,
            Point::Renamed,
            Point::BeforeTidyUp,
            to_object_store(ToObjectStoreStep::Change),
            to_object_store(ToObjectStoreStep::RepoWritten),
            to_object_store(ToObjectStoreStep::RenameDir),
            to_object_store(ToObjectStoreStep::DeleteDir),
        ]
    );
    assert!(!dir.path().join("repo.d").exists());
}

/// A conversion to the object store layout that crashes at every step in turn:
/// from its change until the rerun, writes through another
/// `FilesystemStorage` refuse, naming the conversion to rerun; the rerun
/// reaches the same end state.
#[tokio_test]
async fn a_crash_at_every_step_of_the_conversion_to_the_object_store_layout_then_a_rerun()
{
    for step in [
        ToObjectStoreStep::BeforeChange,
        ToObjectStoreStep::Change,
        ToObjectStoreStep::RepoWritten,
        ToObjectStoreStep::RenameDir,
        ToObjectStoreStep::DeleteDir,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let st = FilesystemStorage::new(dir.path()).unwrap();
        assert_updated(&put(&st, &repo_info(1), PutCondition::Create).await);
        let v1 = latest(&st).await;
        assert_updated(&put(&st, &repo_info(2), PutCondition::IfVersion(&v1)).await);
        let crash = to_object_store(step);
        let hooks = Arc::new(Recorder {
            at: Some(Box::new(move |point| assert_ne!(point, crash, "crash"))),
            ..Recorder::default()
        });
        let err = with_hooks(dir.path(), &hooks)
            .convert_to_object_store_layout()
            .await
            .unwrap_err();
        assert!(matches!(err.kind, StorageErrorKind::Other(_)), "{step:?}: {err}");

        let other = FilesystemStorage::new(dir.path()).unwrap();
        if matches!(step, ToObjectStoreStep::Change | ToObjectStoreStep::RepoWritten) {
            let current = latest(&other).await;
            let bytes = read(&other).await;
            assert!(!icechunk_format::layout_v1::lists_feature(&bytes).unwrap());
            // A writer's change derives from the current version.
            let err =
                put(&other, &bytes, PutCondition::IfVersion(&current)).await.unwrap_err();
            assert!(
                matches!(err.kind, StorageErrorKind::LayoutConversionInProgress(_)),
                "{step:?}: {err}"
            );
            assert!(
                err.to_string().contains("rerun convert_to_object_store_layout()"),
                "{err}"
            );
        }

        let expected = match step {
            ToObjectStoreStep::RenameDir | ToObjectStoreStep::DeleteDir => {
                ConversionOutcome::NothingToConvert
            }
            _ => ConversionOutcome::Converted,
        };
        assert_eq!(
            other.convert_to_object_store_layout().await.unwrap(),
            expected,
            "{step:?}"
        );
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert!(
            !names.iter().any(|name| name.starts_with("repo.d")),
            "{step:?}: {names:?}"
        );
        let object_store_info = root_repo_file(dir.path()).unwrap();
        let info = icechunk_format::layout_v1::change_info(&object_store_info).unwrap();
        assert!(!info.lists_feature);
        let attempt = info.attempt.unwrap().id;
        assert_eq!(
            std::fs::read(dir.path().join("overwritten").join(attempt)).unwrap(),
            repo_info(2),
            "{step:?}"
        );
    }
}
