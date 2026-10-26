#![cfg(all(unix, feature = "fault-injection"))]
#![expect(clippy::unwrap_used, clippy::panic)]

use std::{
    collections::HashSet,
    fmt, io,
    path::Path,
    sync::{Arc, Mutex},
};

use icechunk_fs::{
    ConversionOutcome, FilesystemStorage,
    fault_injection::{
        ConversionStep, CreationStep, FromStandardStep, Point, ProtocolHooks, Sabotage,
        ToStandardStep,
    },
};
use icechunk_macros::tokio_test;
use icechunk_storage::{
    GcPhase, GetModifiedResult, PutCondition, Settings, Storage as _, StorageContext,
    StorageErrorKind, StorageResult, VersionInfo, VersionedUpdateResult,
};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;

#[path = "common/repo_infos.rs"]
#[expect(dead_code, reason = "shared with tests that use every helper")]
mod repo_infos;

use repo_infos::{featureless_repo_info, repo_info};

type AtFn = Box<dyn Fn(Point) + Send + Sync>;
type RenameFn = Box<dyn Fn(&Path, &Path) -> io::Result<()> + Send + Sync>;

/// Records every point reached and turns on `sabotages`; `at` runs at every
/// point and `mirror_rename` replaces the renames onto `$ROOT/repo`.
#[derive(Default)]
struct Recorder {
    reached: Mutex<Vec<Point>>,
    sabotages: HashSet<Sabotage>,
    at: Option<AtFn>,
    mirror_rename: Option<RenameFn>,
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

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        match &self.mirror_rename {
            Some(rename) if to.ends_with("repo") => rename(from, to),
            _ => std::fs::rename(from, to),
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

fn mirror(dir: &Path) -> Vec<u8> {
    std::fs::read(dir.join("repo")).unwrap()
}

async fn gc_hook(st: &FilesystemStorage, phase: GcPhase<'_>) -> StorageResult<()> {
    let settings = Settings::default();
    let ctx = StorageContext::unattributed(&settings);
    st.gc_hook(&ctx, phase).await
}

/// `latest.<K>.<N>.<h>`.
fn marker_name(n: u64, h: &str) -> String {
    format!("latest.{:015}.{n}.{h}", 999_999_999_999_999 - n)
}

/// The `N` and `h` of the marker in `repo.d`.
fn current(dir: &Path) -> (u64, String) {
    let marker = std::fs::read_dir(dir.join("repo.d"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .find(|name| name.starts_with("latest."))
        .unwrap();
    let [_, _, n, h] = marker.splitn(4, '.').collect::<Vec<_>>().try_into().unwrap();
    (n.parse().unwrap(), h.to_string())
}

/// Writes `bytes` as version `n` of `repo.d`, without moving the marker.
fn write_version(dir: &Path, n: u64, bytes: &[u8]) -> VersionInfo {
    let h = format!("{:x}", Sha256::digest(bytes));
    std::fs::write(dir.join("repo.d").join(format!("{n}.{h}")), bytes).unwrap();
    VersionInfo {
        etag: Some(icechunk_storage::ETag(h)),
        generation: Some(icechunk_storage::Generation(n.to_string())),
    }
}

/// Another writer's change, with plain file operations: callable from a hook,
/// which runs on the protocol's blocking thread.
fn change_from_a_hook(dir: &Path, bytes: &[u8]) {
    let (n, h) = current(dir);
    let new = write_version(dir, n + 1, bytes);
    let repo_d = dir.join("repo.d");
    std::fs::rename(
        repo_d.join(marker_name(n, &h)),
        repo_d.join(marker_name(n + 1, new.etag().unwrap())),
    )
    .unwrap();
}

#[tokio_test]
async fn hooks_see_every_point_of_a_creation_and_an_update() {
    let dir = tempfile::tempdir().unwrap();
    let hooks = Arc::new(Recorder::default());
    let st = with_hooks(dir.path(), &hooks);

    assert_updated(&put(&st, &repo_info(1), PutCondition::Create).await);
    // The publication finds the mirror in place and renames nothing.
    assert_eq!(
        hooks.take(),
        [
            Point::Creation(CreationStep::Built),
            Point::Creation(CreationStep::Placed),
            Point::Creation(CreationStep::BeforeDirRename),
            Point::Creation(CreationStep::DirRenamed),
            Point::BeforePublish,
            Point::MirrorStaged,
            Point::BeforeMirrorRead,
        ]
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
            Point::BeforePublish,
            Point::MirrorStaged,
            Point::BeforeMirrorRead,
            Point::BeforeMirrorRename,
            Point::MirrorRenamed,
        ]
    );
    assert_eq!(read(&st).await, repo_info(2));
    assert_eq!(mirror(dir.path()), repo_info(2));
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
async fn hooks_see_every_point_of_a_conversion_from_the_standard_layout() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("repo"), featureless_repo_info(0)).unwrap();
    let hooks = Arc::new(Recorder::default());
    let st = with_hooks(dir.path(), &hooks);

    assert_eq!(
        st.convert_to_filesystem_layout().await.unwrap(),
        ConversionOutcome::Converted
    );
    let step = |step| Point::Conversion(ConversionStep::FromStandard(step));
    assert_eq!(
        hooks.take(),
        [
            step(FromStandardStep::Backup),
            step(FromStandardStep::Build),
            step(FromStandardStep::RenameDir),
            Point::BeforePublish,
            Point::MirrorStaged,
            Point::BeforeMirrorRead,
            Point::BeforeMirrorRename,
            Point::MirrorRenamed,
            step(FromStandardStep::Publish),
        ]
    );
    assert_eq!(read(&st).await, std::fs::read(dir.path().join("repo")).unwrap());
}

fn to_standard(step: ToStandardStep) -> Point {
    Point::Conversion(ConversionStep::ToStandard(step))
}

#[tokio_test]
async fn hooks_see_every_point_of_a_conversion_to_the_standard_layout() {
    let dir = tempfile::tempdir().unwrap();
    let st = FilesystemStorage::new(dir.path()).unwrap();
    assert_updated(&put(&st, &repo_info(1), PutCondition::Create).await);
    let hooks = Arc::new(Recorder::default());
    let st = with_hooks(dir.path(), &hooks);

    assert_eq!(
        st.convert_to_standard_layout().await.unwrap(),
        ConversionOutcome::Converted
    );
    assert_eq!(
        hooks.take(),
        [
            to_standard(ToStandardStep::ForkCheck),
            Point::BeforeMirrorRead,
            Point::Staged,
            Point::Linked,
            Point::BeforeRename,
            Point::Renamed,
            Point::BeforeTidyUp,
            to_standard(ToStandardStep::Change),
            Point::BeforePublish,
            Point::MirrorStaged,
            Point::BeforeMirrorRead,
            Point::BeforeMirrorRename,
            Point::MirrorRenamed,
            to_standard(ToStandardStep::Publish),
            to_standard(ToStandardStep::RenameDir),
            to_standard(ToStandardStep::DeleteDir),
        ]
    );
    assert!(!dir.path().join("repo.d").exists());
}

/// A conversion to the standard layout that crashes at every step in turn:
/// from its change until the rerun, writes and GC through another
/// `FilesystemStorage` refuse, naming the conversion to rerun; the rerun
/// reaches the same end state.
#[tokio_test]
async fn a_crash_at_every_step_of_the_conversion_to_the_standard_layout_then_a_rerun() {
    for step in [
        ToStandardStep::ForkCheck,
        ToStandardStep::Change,
        ToStandardStep::Publish,
        ToStandardStep::RenameDir,
        ToStandardStep::DeleteDir,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let st = FilesystemStorage::new(dir.path()).unwrap();
        assert_updated(&put(&st, &repo_info(1), PutCondition::Create).await);
        let v1 = latest(&st).await;
        assert_updated(&put(&st, &repo_info(2), PutCondition::IfVersion(&v1)).await);
        let crash = to_standard(step);
        let hooks = Arc::new(Recorder {
            at: Some(Box::new(move |point| assert_ne!(point, crash, "crash"))),
            ..Recorder::default()
        });
        let err = with_hooks(dir.path(), &hooks)
            .convert_to_standard_layout()
            .await
            .unwrap_err();
        assert!(matches!(err.kind, StorageErrorKind::Other(_)), "{step:?}: {err}");

        let other = FilesystemStorage::new(dir.path()).unwrap();
        if matches!(step, ToStandardStep::Change | ToStandardStep::Publish) {
            let current = latest(&other).await;
            let bytes = read(&other).await;
            assert!(!icechunk_format::layout_v1::lists_feature(&bytes).unwrap());
            // A writer's change derives from the current version.
            let refusals = [
                put(&other, &bytes, PutCondition::IfVersion(&current)).await.map(|_| ()),
                gc_hook(&other, GcPhase::BeforeDeletes { changed: None }).await,
                other.publish_mirror().await.map(|_| ()),
            ];
            for refused in refusals {
                let err = refused.unwrap_err();
                assert!(
                    matches!(err.kind, StorageErrorKind::LayoutConversionInProgress(_)),
                    "{step:?}: {err}"
                );
                assert!(
                    err.to_string().contains("rerun convert_to_standard_layout()"),
                    "{err}"
                );
            }
        }

        let expected = match step {
            ToStandardStep::RenameDir | ToStandardStep::DeleteDir => {
                ConversionOutcome::NothingToConvert
            }
            _ => ConversionOutcome::Converted,
        };
        assert_eq!(
            other.convert_to_standard_layout().await.unwrap(),
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
        let standard = mirror(dir.path());
        let info = icechunk_format::layout_v1::change_info(&standard).unwrap();
        assert!(!info.lists_feature);
        let attempt = info.attempt.unwrap().id;
        assert_eq!(
            std::fs::read(dir.path().join("overwritten").join(attempt)).unwrap(),
            repo_info(2),
            "{step:?}"
        );
    }
}

/// The change landed, so it is `Updated` although its publication failed;
/// the next change publishes.
#[tokio_test]
async fn a_failed_mirror_rename_is_repaired_by_the_next_change() {
    let dir = tempfile::tempdir().unwrap();
    let hooks = Arc::new(Recorder {
        mirror_rename: Some(Box::new(|_, _| {
            Err(io::Error::other("mirror rename failed"))
        })),
        ..Recorder::default()
    });
    let failing = with_hooks(dir.path(), &hooks);
    assert_updated(&put(&failing, &repo_info(1), PutCondition::Create).await);
    let v1 = latest(&failing).await;
    assert_updated(&put(&failing, &repo_info(2), PutCondition::IfVersion(&v1)).await);
    assert_eq!(read(&failing).await, repo_info(2));
    assert_eq!(mirror(dir.path()), repo_info(1));

    let st = FilesystemStorage::new(dir.path()).unwrap();
    let v2 = latest(&st).await;
    assert_updated(&put(&st, &repo_info(3), PutCondition::IfVersion(&v2)).await);
    assert_eq!(mirror(dir.path()), repo_info(3));
}

/// A publication before GC's deletes that gave up after renaming a version
/// at or above GC's lets GC go on; one that installed nothing fails GC.
#[tokio_test]
async fn gc_goes_on_only_if_its_publication_installed_its_version_or_a_later_one() {
    let dir = tempfile::tempdir().unwrap();
    let st = FilesystemStorage::new(dir.path()).unwrap();
    assert_updated(&put(&st, &repo_info(1), PutCondition::Create).await);
    let gc_change = latest(&st).await;
    let root = dir.path().to_path_buf();
    let changes = Mutex::new(0);
    let hooks = Arc::new(Recorder {
        // Another writer's change during every round.
        at: Some(Box::new(move |point| {
            let mut changes = changes.lock().unwrap();
            if point == Point::BeforeMirrorRename && *changes < 4 {
                *changes += 1;
                change_from_a_hook(&root, &repo_info(10 + *changes));
            }
        })),
        ..Recorder::default()
    });
    let contended = with_hooks(dir.path(), &hooks);
    std::fs::remove_file(dir.path().join("repo")).unwrap();
    gc_hook(&contended, GcPhase::BeforeDeletes { changed: Some(&gc_change) })
        .await
        .unwrap();
    // Round 4 renamed the third change; the fourth landed meanwhile.
    assert_eq!(mirror(dir.path()), repo_info(13));

    let dir = tempfile::tempdir().unwrap();
    let st = FilesystemStorage::new(dir.path()).unwrap();
    assert_updated(&put(&st, &repo_info(1), PutCondition::Create).await);
    let gc_change = latest(&st).await;
    let hooks = Arc::new(Recorder {
        // Every stage is swept before its rename.
        mirror_rename: Some(Box::new(|from, _| {
            std::fs::remove_file(from)?;
            Err(io::ErrorKind::NotFound.into())
        })),
        ..Recorder::default()
    });
    let installs_nothing = with_hooks(dir.path(), &hooks);
    std::fs::remove_file(dir.path().join("repo")).unwrap();
    let err =
        gc_hook(&installs_nothing, GcPhase::BeforeDeletes { changed: Some(&gc_change) })
            .await
            .unwrap_err();
    assert!(
        matches!(&err.kind, StorageErrorKind::Other(msg) if msg.contains("run GC again")),
        "{err}"
    );
    assert!(!dir.path().join("repo").exists());
}

#[tokio_test]
async fn sabotage_commit_without_publish_leaves_the_mirror_behind() {
    let dir = tempfile::tempdir().unwrap();
    let hooks = sabotaged(Sabotage::CommitWithoutPublish);
    let st = with_hooks(dir.path(), &hooks);
    assert_updated(&put(&st, &repo_info(1), PutCondition::Create).await);
    let v1 = latest(&st).await;
    assert_updated(&put(&st, &repo_info(2), PutCondition::IfVersion(&v1)).await);
    assert_eq!(read(&st).await, repo_info(2));
    assert_eq!(mirror(dir.path()), repo_info(1));
}

/// A fork planted after GC's change: the sabotaged hook lets GC delete.
#[tokio_test]
async fn sabotage_gc_without_fork_check_lets_gc_delete_under_a_fork() {
    let dir = tempfile::tempdir().unwrap();
    let st = FilesystemStorage::new(dir.path()).unwrap();
    assert_updated(&put(&st, &repo_info(1), PutCondition::Create).await);
    let gc_change = latest(&st).await;
    std::fs::write(dir.path().join("repo"), featureless_repo_info(9)).unwrap();
    let changed = Some(&gc_change);
    let err = gc_hook(&st, GcPhase::BeforeDeletes { changed }).await.unwrap_err();
    assert!(matches!(err.kind, StorageErrorKind::RepositoryForked(_)), "{err}");

    let sabotaged = with_hooks(dir.path(), &sabotaged(Sabotage::GcWithoutForkCheck));
    gc_hook(&sabotaged, GcPhase::BeforeDeletes { changed }).await.unwrap();
    assert_eq!(mirror(dir.path()), featureless_repo_info(9));
}

/// GC's change proved a version current that a lagging listing doesn't show
/// yet; here, a version file above the marker.
#[tokio_test]
async fn sabotage_gc_publishes_listed_publishes_another_version_than_gcs() {
    let dir = tempfile::tempdir().unwrap();
    let st = FilesystemStorage::new(dir.path()).unwrap();
    assert_updated(&put(&st, &repo_info(1), PutCondition::Create).await);
    let (n, _) = current(dir.path());
    let gc_change = write_version(dir.path(), n + 1, &repo_info(2));
    let changed = Some(&gc_change);

    let sabotaged = with_hooks(dir.path(), &sabotaged(Sabotage::GcPublishesListed));
    gc_hook(&sabotaged, GcPhase::BeforeDeletes { changed }).await.unwrap();
    assert_eq!(mirror(dir.path()), repo_info(1));

    gc_hook(&st, GcPhase::BeforeDeletes { changed }).await.unwrap();
    assert_eq!(mirror(dir.path()), repo_info(2));
}
