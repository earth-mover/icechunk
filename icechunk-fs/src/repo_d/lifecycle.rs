//! Creating, classifying and migrating a repository in the `repo.d` format.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use uuid::Uuid;

use super::{ProtocolError, RepoD, Step, discard_tmp_dir, remove_stage};
use crate::{
    names::{
        Hash, Marker, MarkerKind, REPO, RootName, Version, legacy_name, parse_root_name,
        stub_staging_name, tmp_dir_name,
    },
    nudge::nudge,
    read::read_all,
    storage::is_missing,
    stub::{is_stub, stub_bytes},
    write::{fsync_dir, write_fresh},
};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CreateOutcome {
    Created(Version),
    AlreadyExists,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenStatus {
    Ok,
    NotFound,
    LegacyFormat,
    MigrationInterrupted,
    MixedFormats,
}

/// What `migrate_legacy_repo` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrateOutcome {
    /// The single-file repository was converted.
    Migrated,
    /// The repository was already converted; what an interrupted migration
    /// left behind was cleaned up.
    AlreadyMigrated,
    /// There is no single-file repository here.
    NothingToMigrate,
}

/// What `root/repo` holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepoFile {
    Missing,
    Stub,
    /// Anything else: a single-file repository.
    Legacy,
}

enum StubOutcome {
    Written,
    /// `repo` exists and isn't the stub.
    LegacyRepo,
}

#[derive(Debug, PartialEq, Eq)]
enum Built {
    Created,
    /// `repo.d` exists, with its marker at this version.
    Exists(Version),
}

impl RepoD<'_> {
    /// Creates the repository with version 0 holding `bytes`. Creating it
    /// again with the same bytes is `Created`, the outcome after a lost reply.
    pub(crate) fn create(&self, bytes: &[u8]) -> Result<CreateOutcome, ProtocolError> {
        fs::create_dir_all(self.root)?;
        nudge(self.root);
        let repo = match self.repo_file() {
            Ok(repo @ (RepoFile::Missing | RepoFile::Stub)) => repo,
            // A single-file repo, which must be migrated.
            Ok(RepoFile::Legacy) => return Ok(CreateOutcome::AlreadyExists),
            // Something other than a regular file, such as a directory.
            Err(_) if self.repo_is_not_a_file() => {
                return Ok(CreateOutcome::AlreadyExists);
            }
            Err(err) => return Err(err.into()),
        };
        // Creating here would orphan the legacy file, which a later migration
        // deletes.
        if self.classify(repo)? == OpenStatus::MigrationInterrupted {
            return Err(ProtocolError::MigrationInterrupted);
        }
        if let StubOutcome::LegacyRepo = self.write_stub()? {
            return Ok(CreateOutcome::AlreadyExists);
        }
        let initial = Version::initial(Hash::of(bytes));
        match self.build_repo_d(&initial, bytes)? {
            Built::Created => Ok(CreateOutcome::Created(initial)),
            // Ours, after a lost reply.
            Built::Exists(current) if current == initial => {
                Ok(CreateOutcome::Created(initial))
            }
            Built::Exists(_) => Ok(CreateOutcome::AlreadyExists),
        }
    }

    pub(crate) fn open_status(&self) -> Result<OpenStatus, ProtocolError> {
        nudge(self.root);
        let repo = self.repo_file()?;
        self.classify(repo)
    }

    /// Whether `repo.d` exists, by the `open` classification. The states that
    /// need a remedy are errors naming it.
    pub(crate) fn opens(&self) -> Result<bool, ProtocolError> {
        match self.open_status()? {
            OpenStatus::Ok => Ok(true),
            OpenStatus::NotFound => Ok(false),
            OpenStatus::LegacyFormat => Err(ProtocolError::LegacyFormat),
            OpenStatus::MigrationInterrupted => Err(ProtocolError::MigrationInterrupted),
            OpenStatus::MixedFormats => Err(ProtocolError::MixedFormats),
        }
    }

    /// Converts a single-file repository, offline: no legacy writer may run
    /// once this starts, and `repo` is missing between its rename and the
    /// stub. Every step is idempotent, so a rerun completes an interrupted
    /// migration.
    pub(crate) fn migrate(&self) -> Result<MigrateOutcome, ProtocolError> {
        // The client looks `repo` up again before the rename.
        nudge(self.root);
        let legacy = match self.repo_file()? {
            RepoFile::Stub if self.repo_d_exists()? => {
                for legacy in self.legacy_files()? {
                    remove_if_exists(&legacy)?;
                }
                return Ok(MigrateOutcome::AlreadyMigrated);
            }
            // An interrupted create.
            RepoFile::Stub => return Ok(MigrateOutcome::NothingToMigrate),
            // A legacy writer overwrote the stub; renaming it aside would
            // hide its update behind a repo that opens.
            RepoFile::Legacy if self.repo_d_exists()? => {
                return Err(ProtocolError::MixedFormats);
            }
            RepoFile::Legacy => {
                // Use this name from now on; never list for it.
                let legacy = self.root.join(legacy_name(Uuid::new_v4()));
                fs::rename(self.root.join(REPO), &legacy)?;
                legacy
            }
            RepoFile::Missing => match self.legacy_files()?.as_slice() {
                [] if self.repo_d_exists()? => {
                    self.restore_stub()?;
                    return Ok(MigrateOutcome::AlreadyMigrated);
                }
                [] => return Ok(MigrateOutcome::NothingToMigrate),
                [legacy] => legacy.clone(),
                several => {
                    return Err(ProtocolError::ManualRecovery(format!(
                        "several legacy repo files, only one can be migrated: {}",
                        several
                            .iter()
                            .map(|path| path.display().to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
            },
        };
        // Fresh after the nudge, and immutable from now on.
        let bytes = read_all(&legacy)?;
        let initial = Version::initial(Hash::of(&bytes));
        match self.build_repo_d(&initial, &bytes)? {
            Built::Created => {}
            // A previous run got this far.
            Built::Exists(current) if current.hash() == initial.hash() => {}
            Built::Exists(current) => {
                return Err(ProtocolError::ManualRecovery(format!(
                    "repo.d holds other content than {legacy}: its current version is \
                     {current}. If an earlier migrate_legacy_repo run built repo.d \
                     (writers may have committed since), delete {legacy} and rerun \
                     migrate_legacy_repo to finish the migration",
                    legacy = legacy.display(),
                    current = current.file_name()
                )));
            }
        }
        self.restore_stub()?;
        remove_if_exists(&legacy)?;
        Ok(MigrateOutcome::Migrated)
    }

    /// `write_stub` during a migration, where a legacy `repo` can only come
    /// from a legacy writer.
    fn restore_stub(&self) -> Result<(), ProtocolError> {
        match self.write_stub()? {
            StubOutcome::Written => Ok(()),
            StubOutcome::LegacyRepo => Err(ProtocolError::ManualRecovery(
                "a legacy writer wrote repo during migration".to_string(),
            )),
        }
    }

    /// The `open` classification, given what `repo` holds.
    fn classify(&self, repo: RepoFile) -> Result<OpenStatus, ProtocolError> {
        Ok(if self.repo_d_exists()? {
            match repo {
                // A legacy writer overwrote the stub.
                RepoFile::Legacy => OpenStatus::MixedFormats,
                // A missing stub is harmless; migrate restores it.
                RepoFile::Stub | RepoFile::Missing => OpenStatus::Ok,
            }
        } else {
            match repo {
                // An interrupted create; create resumes it.
                RepoFile::Stub => OpenStatus::NotFound,
                RepoFile::Legacy => OpenStatus::LegacyFormat,
                RepoFile::Missing if !self.legacy_files()?.is_empty() => {
                    OpenStatus::MigrationInterrupted
                }
                RepoFile::Missing => OpenStatus::NotFound,
            }
        })
    }

    fn repo_file(&self) -> io::Result<RepoFile> {
        match read_all(&self.root.join(REPO)) {
            Ok(bytes) if is_stub(&bytes) => Ok(RepoFile::Stub),
            Ok(_) => Ok(RepoFile::Legacy),
            Err(err) if is_missing(&err) => Ok(RepoFile::Missing),
            Err(err) => Err(err),
        }
    }

    /// `repo` exists and is not a regular file. A symlink isn't followed.
    fn repo_is_not_a_file(&self) -> bool {
        fs::symlink_metadata(self.root.join(REPO))
            .is_ok_and(|metadata| !metadata.is_file())
    }

    fn repo_d_exists(&self) -> io::Result<bool> {
        match fs::symlink_metadata(self.dir()) {
            Ok(_) => Ok(true),
            Err(err) if is_missing(&err) => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Every `repo.legacy.*` in one listing of the root.
    fn legacy_files(&self) -> io::Result<Vec<PathBuf>> {
        let entries = match fs::read_dir(self.root) {
            Err(err) if is_missing(&err) => return Ok(Vec::new()),
            entries => entries?,
        };
        entries
            .filter_map(|entry| {
                entry
                    .map(|entry| {
                        let name = entry.file_name();
                        (name.to_str().and_then(parse_root_name)
                            == Some(RootName::Legacy))
                        .then(|| entry.path())
                    })
                    .transpose()
            })
            .collect()
    }

    /// Links the stub as `repo`.
    fn write_stub(&self) -> Result<StubOutcome, ProtocolError> {
        let staging = self.root.join(stub_staging_name(Uuid::new_v4()));
        write_fresh(&staging, &stub_bytes(), true)?;
        let linked = self.link_stub(&staging);
        remove_stage(&staging);
        let outcome = linked?;
        if let StubOutcome::Written = outcome {
            fsync_dir(self.root)?;
        }
        Ok(outcome)
    }

    fn link_stub(&self, staging: &Path) -> Result<StubOutcome, ProtocolError> {
        let repo = self.root.join(REPO);
        for _ in 0..2 {
            match fs::hard_link(staging, &repo) {
                Ok(()) => return Ok(StubOutcome::Written),
                // Ours after a lost reply, another creator's, or a legacy repo.
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                    match self.repo_file()? {
                        RepoFile::Stub => return Ok(StubOutcome::Written),
                        RepoFile::Legacy => return Ok(StubOutcome::LegacyRepo),
                        // Moved away since the link: link again.
                        RepoFile::Missing => {}
                    }
                }
                Err(err) => return Err(err.into()),
            }
        }
        Err(io::Error::other(
            "repo existed when linking the stub and was missing when read, twice: \
             another process is moving it",
        )
        .into())
    }

    /// Builds `repo.d` holding `initial` with `bytes` in a new directory, then
    /// renames it into place, which fails if `repo.d` exists and isn't empty.
    fn build_repo_d(
        &self,
        initial: &Version,
        bytes: &[u8],
    ) -> Result<Built, ProtocolError> {
        let tmp = self.root.join(tmp_dir_name(Uuid::new_v4()));
        fs::DirBuilder::new().create(&tmp)?;
        if let Err(err) = fill_repo_d(&tmp, initial, bytes) {
            self.discard(&tmp);
            return Err(err.into());
        }
        let renamed = fs::rename(&tmp, self.dir());
        self.after_dir_rename(renamed, &tmp, initial)
    }

    fn after_dir_rename(
        &self,
        renamed: io::Result<()>,
        tmp: &Path,
        initial: &Version,
    ) -> Result<Built, ProtocolError> {
        match renamed {
            Ok(()) => {
                fsync_dir(self.root)?;
                Ok(Built::Created)
            }
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::AlreadyExists | io::ErrorKind::DirectoryNotEmpty
                ) =>
            {
                self.discard(tmp);
                match self.listed_marker()? {
                    Some(marker) => Ok(Built::Exists(marker.version)),
                    None => Err(io::Error::other(
                        "repo.d disappeared after refusing a rename over it",
                    )
                    .into()),
                }
            }
            // Our rename may have landed and its reply been lost: `tmp` is gone.
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                match self.listed_marker()? {
                    Some(marker) if marker.version == *initial => Ok(Built::Created),
                    Some(marker) => Ok(Built::Exists(marker.version)),
                    None => Err(io::Error::other(format!(
                        "renaming {} to repo.d failed and repo.d doesn't exist: retry",
                        tmp.display()
                    ))
                    .into()),
                }
            }
            Err(err) => {
                self.discard(tmp);
                Err(err.into())
            }
        }
    }

    /// Liveness only: the sweep deletes a creation directory left behind.
    fn discard(&self, tmp: &Path) {
        if let Err(err) = discard_tmp_dir(self.root, tmp) {
            tracing::debug!(%err, path = %tmp.display(), "could not delete a creation directory");
        }
    }

    /// The marker of a listing of `repo.d`, listing again while it shows
    /// none. `None` if `repo.d` doesn't exist.
    fn listed_marker(&self) -> Result<Option<Marker>, ProtocolError> {
        self.with_listing(|marker| Ok(Step::Done(marker.cloned())))
    }
}

/// `<0>.<h>` and its marker, durably.
fn fill_repo_d(tmp: &Path, initial: &Version, bytes: &[u8]) -> io::Result<()> {
    write_fresh(&tmp.join(initial.file_name()), bytes, true)?;
    let marker = Marker { kind: MarkerKind::Latest, version: initial.clone() };
    write_fresh(&tmp.join(marker.file_name()), &[], true)?;
    fsync_dir(tmp)
}

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(err) if is_missing(&err) => Ok(()),
        removed => removed,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Barrier, thread};

    use uuid::Uuid;

    use super::{super::testing::*, CreateOutcome::*, *};
    use crate::{
        hooks::NoHooks,
        names::{legacy_name, stub_staging_name, tmp_dir_name},
        repo_d::{commit::UpdateOutcome, read::Current},
        stub::{is_stub, stub_bytes},
    };

    const LEGACY: &[u8] = b"a legacy repo info";

    fn repo(root: &Path) -> RepoD<'_> {
        RepoD { root, hooks: &NoHooks }
    }

    fn strings<const N: usize>(names: [&str; N]) -> BTreeSet<String> {
        names.into_iter().map(str::to_string).collect()
    }

    /// Writes `repo.d` holding version 0 with `bytes`, as `build_repo_d` does.
    fn repo_d_with(root: &Path, bytes: &[u8]) -> Version {
        let v0 = version(0, bytes);
        let dir = root.join("repo.d");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(v0.file_name()), bytes).unwrap();
        fs::write(dir.join(latest(&v0)), b"").unwrap();
        v0
    }

    fn write_stub(root: &Path) {
        fs::write(root.join("repo"), stub_bytes()).unwrap();
    }

    fn legacy_file(root: &Path) -> PathBuf {
        let path = root.join(legacy_name(Uuid::new_v4()));
        fs::write(&path, LEGACY).unwrap();
        path
    }

    /// Exactly the stub and `repo.d` holding version 0 with `bytes`.
    fn assert_created(root: &Path, bytes: &[u8]) {
        let v0 = version(0, bytes);
        assert_eq!(names(root), strings(["repo", "repo.d"]));
        assert!(is_stub(&fs::read(root.join("repo")).unwrap()));
        let dir = root.join("repo.d");
        assert_eq!(names(&dir), set([v0.file_name(), latest(&v0)]));
        assert_eq!(fs::read(dir.join(v0.file_name())).unwrap(), bytes);
        assert_eq!(fs::read(dir.join(latest(&v0))).unwrap(), b"");
    }

    // ── create ──────────────────────────────────────────────────────────────

    #[test]
    fn create_in_an_empty_root() {
        let root = tempfile::tempdir().unwrap();
        assert_eq!(repo(root.path()).create(V0).unwrap(), Created(version(0, V0)));
        assert_created(root.path(), V0);
    }

    #[test]
    fn create_makes_a_missing_root() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("a/b");
        assert_eq!(repo(&missing).create(V0).unwrap(), Created(version(0, V0)));
        assert_created(&missing, V0);
    }

    #[test]
    fn create_again_exists_unless_with_the_same_bytes() {
        let root = tempfile::tempdir().unwrap();
        let repo = repo(root.path());
        let v0 = version(0, V0);
        assert_eq!(repo.create(V0).unwrap(), Created(v0.clone()));
        assert_eq!(repo.create(b"other").unwrap(), AlreadyExists);
        // Our own create, after a lost reply.
        assert_eq!(repo.create(V0).unwrap(), Created(v0));
        assert_created(root.path(), V0);
    }

    #[test]
    fn create_after_a_commit_exists() {
        let root = tempfile::tempdir().unwrap();
        let repo = repo(root.path());
        let v0 = version(0, V0);
        assert_eq!(repo.create(V0).unwrap(), Created(v0.clone()));
        assert!(matches!(
            repo.conditional_update(b"v1", &v0).unwrap(),
            UpdateOutcome::Updated(_)
        ));
        assert_eq!(repo.create(V0).unwrap(), AlreadyExists);
    }

    #[test]
    fn create_over_a_legacy_repo_exists_and_creates_nothing() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("repo"), LEGACY).unwrap();
        assert_eq!(repo(root.path()).create(V0).unwrap(), AlreadyExists);
        assert_eq!(names(root.path()), strings(["repo"]));
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), LEGACY);
    }

    #[test]
    fn create_over_a_repo_that_is_not_a_file_exists() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir_all(root.path().join("repo")).unwrap();
        assert_eq!(repo(root.path()).create(V0).unwrap(), AlreadyExists);
        assert_eq!(names(root.path()), strings(["repo"]));
    }

    #[test]
    fn create_over_an_unreadable_repo_file_is_an_error() {
        use std::os::unix::fs::PermissionsExt as _;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("repo");
        fs::write(&path, LEGACY).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
        // Root reads it anyway: nothing to test.
        if fs::read(&path).is_ok() {
            return;
        }
        let err = repo(root.path()).create(V0).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Io(err) if err.kind() == io::ErrorKind::PermissionDenied),
            "{err:?}"
        );
        assert_eq!(names(root.path()), strings(["repo"]));
    }

    #[test]
    fn create_while_repo_keeps_vanishing_is_an_error() {
        let root = tempfile::tempdir().unwrap();
        // The link sees a name, the read finds nothing behind it.
        std::os::unix::fs::symlink("missing", root.path().join("repo")).unwrap();
        let err = repo(root.path()).create(V0).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Io(err) if err.to_string().contains("another process is moving it")),
            "{err:?}"
        );
        assert_eq!(names(root.path()), strings(["repo"]));
    }

    #[test]
    fn create_resumes_an_interrupted_create() {
        let root = tempfile::tempdir().unwrap();
        write_stub(root.path());
        assert_eq!(repo(root.path()).create(V0).unwrap(), Created(version(0, V0)));
        assert_created(root.path(), V0);
    }

    #[test]
    fn create_restores_a_missing_stub() {
        let root = tempfile::tempdir().unwrap();
        let v0 = repo_d_with(root.path(), V0);
        assert_eq!(repo(root.path()).create(V0).unwrap(), Created(v0));
        assert_created(root.path(), V0);
    }

    #[test]
    fn create_during_an_interrupted_migration_is_an_error() {
        let root = tempfile::tempdir().unwrap();
        let legacy = legacy_file(root.path());
        let err = repo(root.path()).create(V0).unwrap_err();
        assert!(matches!(err, ProtocolError::MigrationInterrupted), "{err:?}");
        assert_eq!(names(root.path()), set([legacy_name_of(&legacy)]));
    }

    fn legacy_name_of(path: &Path) -> String {
        path.file_name().unwrap().to_str().unwrap().to_string()
    }

    #[test]
    fn concurrent_creates_one_wins() {
        for _ in 0..20 {
            let root = tempfile::tempdir().unwrap();
            let repo = repo(root.path());
            let barrier = Barrier::new(2);
            let create = |bytes: &'static [u8]| {
                barrier.wait();
                (bytes, repo.create(bytes).unwrap())
            };
            let results = thread::scope(|scope| {
                let a = scope.spawn(|| create(b"a"));
                let b = scope.spawn(|| create(b"b"));
                [a.join().unwrap(), b.join().unwrap()]
            });
            let winners: Vec<_> = results
                .iter()
                .filter_map(|(bytes, outcome)| match outcome {
                    Created(v) => Some((*bytes, v.clone())),
                    AlreadyExists => None,
                })
                .collect();
            let [(bytes, v0)] = winners.as_slice() else {
                panic!("exactly one create wins: {results:?}")
            };
            assert_eq!(*v0, version(0, bytes));
            assert_eq!(
                repo.read_latest().unwrap(),
                Some(Current {
                    bytes: bytes::Bytes::from_static(bytes),
                    version: v0.clone()
                })
            );
            assert_created(root.path(), bytes);
        }
    }

    #[test]
    fn lost_reply_of_the_directory_rename() {
        let root = tempfile::tempdir().unwrap();
        let repo = repo(root.path());
        let ours = version(0, V0);
        let tmp = root.path().join(tmp_dir_name(Uuid::new_v4()));
        let lost = || Err(io::ErrorKind::NotFound.into());

        // No `repo.d`: the rename didn't land.
        let err = repo.after_dir_rename(lost(), &tmp, &ours).unwrap_err();
        assert!(matches!(err, ProtocolError::Io(_)), "{err:?}");

        // Ours landed.
        repo_d_with(root.path(), V0);
        assert_eq!(repo.after_dir_rename(lost(), &tmp, &ours).unwrap(), Built::Created);

        // Another creator's landed.
        let theirs = version(0, b"theirs");
        assert_eq!(
            repo.after_dir_rename(lost(), &tmp, &theirs).unwrap(),
            Built::Exists(ours)
        );
    }

    // ── open_status ─────────────────────────────────────────────────────────

    #[test]
    fn open_status_classifies_every_layout() {
        let status = |setup: &dyn Fn(&Path)| {
            let root = tempfile::tempdir().unwrap();
            setup(root.path());
            repo(root.path()).open_status().unwrap()
        };
        assert_eq!(status(&|_| {}), OpenStatus::NotFound);
        assert_eq!(
            status(&|root| {
                repo(root).create(V0).unwrap();
            }),
            OpenStatus::Ok
        );
        assert_eq!(
            status(&|root| fs::write(root.join("repo"), LEGACY).unwrap()),
            OpenStatus::LegacyFormat
        );
        assert_eq!(
            status(&|root| {
                legacy_file(root);
            }),
            OpenStatus::MigrationInterrupted
        );
        assert_eq!(
            status(&|root| {
                repo_d_with(root, V0);
                fs::write(root.join("repo"), LEGACY).unwrap();
            }),
            OpenStatus::MixedFormats
        );
        assert_eq!(status(&|root| write_stub(root)), OpenStatus::NotFound);
        assert_eq!(
            status(&|root| {
                repo_d_with(root, V0);
            }),
            OpenStatus::Ok
        );
        // Other protocol names alone are no repository.
        assert_eq!(
            status(&|root| {
                fs::create_dir_all(root.join(tmp_dir_name(Uuid::new_v4()))).unwrap();
                write_stub_staging(root);
            }),
            OpenStatus::NotFound
        );

        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing");
        assert_eq!(repo(&missing).open_status().unwrap(), OpenStatus::NotFound);
    }

    fn write_stub_staging(root: &Path) {
        fs::write(root.join(stub_staging_name(Uuid::new_v4())), stub_bytes()).unwrap();
    }

    // ── migrate ─────────────────────────────────────────────────────────────

    #[test]
    fn migrate_converts_a_legacy_repo() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("repo"), LEGACY).unwrap();
        let repo = repo(root.path());
        assert_eq!(repo.migrate().unwrap(), MigrateOutcome::Migrated);
        assert_created(root.path(), LEGACY);
        assert_eq!(
            repo.read_latest().unwrap(),
            Some(Current {
                bytes: bytes::Bytes::from_static(LEGACY),
                version: version(0, LEGACY)
            })
        );
        assert_eq!(repo.open_status().unwrap(), OpenStatus::Ok);

        assert_eq!(repo.migrate().unwrap(), MigrateOutcome::AlreadyMigrated);
        assert_created(root.path(), LEGACY);
    }

    #[test]
    fn migrate_a_committed_repo_again_changes_nothing() {
        let root = tempfile::tempdir().unwrap();
        let repo = repo(root.path());
        repo.create(V0).unwrap();
        repo.conditional_update(b"v1", &version(0, V0)).unwrap();
        let before = names(&root.path().join("repo.d"));
        assert_eq!(repo.migrate().unwrap(), MigrateOutcome::AlreadyMigrated);
        assert_eq!(names(&root.path().join("repo.d")), before);
    }

    #[test]
    fn migrate_without_a_legacy_repo_has_nothing_to_migrate() {
        let root = tempfile::tempdir().unwrap();
        assert_eq!(
            repo(root.path()).migrate().unwrap(),
            MigrateOutcome::NothingToMigrate
        );
        assert!(names(root.path()).is_empty());
        let missing = root.path().join("missing");
        assert_eq!(repo(&missing).migrate().unwrap(), MigrateOutcome::NothingToMigrate);

        // An interrupted create.
        write_stub(root.path());
        assert_eq!(
            repo(root.path()).migrate().unwrap(),
            MigrateOutcome::NothingToMigrate
        );
        assert_eq!(names(root.path()), strings(["repo"]));
    }

    #[test]
    fn migrate_resumes_after_a_crash_at_each_step() {
        // `repo` renamed aside; then `repo.d` built; then the stub linked.
        for (step, expected) in [
            MigrateOutcome::Migrated,
            MigrateOutcome::Migrated,
            MigrateOutcome::AlreadyMigrated,
        ]
        .into_iter()
        .enumerate()
        {
            let root = tempfile::tempdir().unwrap();
            legacy_file(root.path());
            if step >= 1 {
                repo_d_with(root.path(), LEGACY);
            }
            if step >= 2 {
                write_stub(root.path());
            }
            assert_eq!(repo(root.path()).migrate().unwrap(), expected, "step {step}");
            assert_created(root.path(), LEGACY);
        }
    }

    #[test]
    fn migrate_restores_a_missing_stub() {
        let root = tempfile::tempdir().unwrap();
        repo_d_with(root.path(), V0);
        assert_eq!(repo(root.path()).migrate().unwrap(), MigrateOutcome::AlreadyMigrated);
        assert_created(root.path(), V0);
    }

    #[test]
    fn migrate_with_several_legacy_files_needs_manual_recovery() {
        let root = tempfile::tempdir().unwrap();
        let first = legacy_file(root.path());
        let second = legacy_file(root.path());
        let before = names(root.path());
        let err = repo(root.path()).migrate().unwrap_err();
        assert!(
            matches!(&err, ProtocolError::ManualRecovery(msg)
                if msg.contains(&legacy_name_of(&first)) && msg.contains(&legacy_name_of(&second))),
            "{err:?}"
        );
        assert_eq!(names(root.path()), before);
    }

    #[test]
    fn migrate_onto_other_content_needs_manual_recovery() {
        let root = tempfile::tempdir().unwrap();
        let legacy = legacy_file(root.path());
        let v0 = repo_d_with(root.path(), V0);
        let err = repo(root.path()).migrate().unwrap_err();
        assert!(
            matches!(&err, ProtocolError::ManualRecovery(msg)
                if msg.contains("other content")
                    && msg.contains(&format!("delete {} and rerun", legacy.display()))),
            "{err:?}"
        );
        assert_eq!(
            names(root.path()),
            set([legacy_name_of(&legacy), "repo.d".to_string()])
        );
        assert_eq!(
            names(&root.path().join("repo.d")),
            set([v0.file_name(), latest(&v0)])
        );
    }

    #[test]
    fn migrate_of_mixed_formats_changes_nothing() {
        let root = tempfile::tempdir().unwrap();
        repo_d_with(root.path(), V0);
        fs::write(root.path().join("repo"), LEGACY).unwrap();
        let err = repo(root.path()).migrate().unwrap_err();
        assert!(matches!(err, ProtocolError::MixedFormats), "{err:?}");
        assert_eq!(names(root.path()), strings(["repo", "repo.d"]));
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), LEGACY);
    }
}
