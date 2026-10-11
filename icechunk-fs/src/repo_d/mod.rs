//! The `repo.d` protocol of design-docs/020-filesystem-storage.md: `repo` is
//! a directory of immutable versions and a marker whose name says which one
//! is current, moved only by `rename`.

pub(crate) mod commit;
pub(crate) mod freeze;
pub(crate) mod lifecycle;
pub(crate) mod read;
pub(crate) mod sweep;

use std::{
    fs, io, iter,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use uuid::Uuid;

use crate::{
    hooks::ProtocolHooks,
    names::{
        Marker, MarkerKind, REPO_D, RepoDName, Version, parse_repo_d_name, tmp_dir_name,
    },
    nudge::nudge,
    storage::is_missing,
};

/// The protocol on the repository at `root`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RepoD<'a> {
    pub(crate) root: &'a Path,
    pub(crate) hooks: &'a dyn ProtocolHooks,
}

#[derive(Debug)]
pub(crate) enum ProtocolError {
    /// The marker is `frozen.*`: no commit can succeed.
    Frozen,
    /// A single-file repo: run `migrate_legacy_repo`.
    LegacyFormat,
    /// `repo.legacy.*` without `repo`: rerun `migrate_legacy_repo`.
    MigrationInterrupted,
    /// `repo.d` and a `repo` that isn't the stub: a legacy writer ran, manual
    /// recovery.
    MixedFormats,
    /// Migration found several legacy files, or `repo.d` with other content.
    ManualRecovery(String),
    /// A version without both an etag and a generation.
    InvalidVersion(String),
    /// Ran out of tries; why the last one failed.
    NoConsistentView(String),
    /// A version file whose bytes don't hash to its name.
    Corrupt(String),
    Io(io::Error),
}

impl From<io::Error> for ProtocolError {
    fn from(err: io::Error) -> Self {
        ProtocolError::Io(err)
    }
}

#[derive(Debug)]
pub(crate) enum Listed {
    Markers(Vec<Marker>),
    /// `repo.d` doesn't exist.
    NotFound,
}

impl RepoD<'_> {
    /// `root/repo.d`
    pub(crate) fn dir(&self) -> PathBuf {
        self.root.join(REPO_D)
    }

    /// The markers of one listing of `repo.d`: none, one, or several when a
    /// rename landed during the listing.
    pub(crate) fn list_markers(&self) -> Result<Listed, ProtocolError> {
        let dir = self.dir();
        let markers = |names: Vec<RepoDName>| {
            Listed::Markers(
                names
                    .into_iter()
                    .filter_map(|name| match name {
                        RepoDName::Marker(marker) => Some(marker),
                        RepoDName::Content(_) | RepoDName::Stage(_) => None,
                    })
                    .collect(),
            )
        };
        match list_names(&dir) {
            // Possibly a cached negative entry: ask the server once.
            Err(err) if is_missing(&err) => {
                nudge(self.root);
                match list_names(&dir) {
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {
                        Ok(Listed::NotFound)
                    }
                    names => Ok(markers(names?)),
                }
            }
            names => Ok(markers(names?)),
        }
    }

    /// The marker of a listing: the newest state it witnessed. At equal `N`
    /// a `frozen` marker is newer than the `latest` one it was renamed from.
    pub(crate) fn highest(markers: &[Marker]) -> Option<&Marker> {
        markers
            .iter()
            .max_by_key(|marker| (marker.version.n(), marker.kind == MarkerKind::Frozen))
    }
}

/// The protocol names in one listing of `dir`.
fn list_names(dir: &Path) -> io::Result<Vec<RepoDName>> {
    fs::read_dir(dir)?
        .filter_map(|entry| {
            entry
                .map(|entry| entry.file_name().to_str().and_then(parse_repo_d_name))
                .transpose()
        })
        .collect()
}

const TRIES: usize = 8;
const FIRST_BACKOFF: Duration = Duration::from_millis(10);
const MAX_BACKOFF: Duration = Duration::from_millis(500);
/// Unit tests sleep this many times less.
const TIME_SCALE: u32 = if cfg!(test) { 1000 } else { 1 };

/// The pauses between tries.
fn backoffs() -> impl Iterator<Item = Duration> {
    iter::successors(Some(FIRST_BACKOFF), |pause| Some((*pause * 2).min(MAX_BACKOFF)))
        .take(TRIES - 1)
}

/// Calls `f` until it returns `Some`, at most `TRIES` times, sleeping with
/// backoff in between. It blocks: call it from a blocking thread.
pub(crate) fn with_retries<T>(mut f: impl FnMut() -> Option<T>) -> Option<T> {
    let mut pauses = backoffs();
    loop {
        if let Some(result) = f() {
            return Some(result);
        }
        thread::sleep(pauses.next()? / TIME_SCALE);
    }
}

const NO_MARKER: &str = "the listing of repo.d showed no marker";

/// What a step of [`RepoD::with_listing`] decided.
enum Step<T> {
    Done(T),
    /// List again; why.
    Retry(String),
}

impl<T> Step<T> {
    fn map<U>(self, f: impl FnOnce(T) -> U) -> Step<U> {
        match self {
            Step::Done(done) => Step::Done(f(done)),
            Step::Retry(why) => Step::Retry(why),
        }
    }
}

impl RepoD<'_> {
    /// Calls `step` with the marker of a listing of `repo.d` (`None` if
    /// `repo.d` doesn't exist) until it is done, at most [`TRIES`] times with
    /// backoff. A listing without a marker is a retry. After the last try,
    /// `NoConsistentView` says why it failed.
    fn with_listing<T>(
        &self,
        mut step: impl FnMut(Option<&Marker>) -> Result<Step<T>, ProtocolError>,
    ) -> Result<T, ProtocolError> {
        let dir = self.dir();
        let mut cause = String::new();
        with_retries(|| {
            nudge(&dir);
            let stepped = match self.list_markers() {
                Ok(Listed::Markers(markers)) => match Self::highest(&markers) {
                    Some(marker) => step(Some(marker)),
                    None => Ok(Step::Retry(NO_MARKER.to_string())),
                },
                Ok(Listed::NotFound) => step(None),
                Err(err) => Err(err),
            };
            match stepped {
                Ok(Step::Done(done)) => Some(Ok(done)),
                Ok(Step::Retry(why)) => {
                    cause = why;
                    None
                }
                Err(err) => Some(Err(err)),
            }
        })
        .unwrap_or_else(|| Err(ProtocolError::NoConsistentView(cause)))
    }
}

/// Invariant 3: a listing that shows the marker at `current` proves
/// `version` dead for good.
fn is_dead(version: &Version, current: &Version) -> bool {
    version.n() < current.n()
        || (version.n() == current.n() && version.hash() != current.hash())
}

/// Liveness only: the sweep deletes a staging file left behind.
fn remove_stage(stage: &Path) {
    if let Err(err) = fs::remove_file(stage) {
        tracing::debug!(%err, path = %stage.display(), "could not delete a staging file");
    }
}

/// Deletes the creation directory `tmp` in `root`, unless it has become
/// `repo.d`. It is first renamed to a fresh name: that fails on the server if
/// `tmp` is gone, whereas a deletion through `tmp` could follow a cached
/// entry into `repo.d`. `NotFound` means nothing was deleted.
fn discard_tmp_dir(root: &Path, tmp: &Path) -> io::Result<()> {
    let aside = root.join(tmp_dir_name(Uuid::new_v4()));
    fs::rename(tmp, &aside)?;
    fs::remove_dir_all(&aside)
}

#[cfg(test)]
pub(super) mod testing {
    //! Seeding and hooks for the protocol's unit tests.

    use std::{
        collections::{BTreeSet, HashSet},
        fmt, fs, io,
        path::Path,
        sync::{Mutex, mpsc},
        thread,
    };

    use crate::{
        hooks::{Point, ProtocolHooks, Sabotage},
        names::{Hash, Marker, MarkerKind, Version},
    };

    pub(crate) const V0: &[u8] = b"v0";

    pub(crate) fn version(n: u64, bytes: &[u8]) -> Version {
        Version::new(n, Hash::of(bytes)).unwrap()
    }

    pub(crate) fn latest(version: &Version) -> String {
        Marker { kind: MarkerKind::Latest, version: version.clone() }.file_name()
    }

    pub(crate) fn frozen(version: &Version) -> String {
        Marker { kind: MarkerKind::Frozen, version: version.clone() }.file_name()
    }

    /// Writes `repo.d/0.<h0>` and its marker for the bytes [`V0`].
    pub(crate) fn seed(root: &Path) -> Version {
        let v0 = version(0, V0);
        let dir = root.join("repo.d");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(v0.file_name()), V0).unwrap();
        fs::write(dir.join(latest(&v0)), b"").unwrap();
        v0
    }

    pub(crate) fn names(dir: &Path) -> BTreeSet<String> {
        fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect()
    }

    pub(crate) fn set<const N: usize>(names: [String; N]) -> BTreeSet<String> {
        names.into_iter().collect()
    }

    type AtFn = Box<dyn Fn(Point) + Send + Sync>;
    type PathFn = Box<dyn Fn(&Path, &Path) -> io::Result<()> + Send + Sync>;

    /// Records every point reached; each operation defaults to the real one.
    #[derive(Default)]
    pub(crate) struct TestHooks {
        reached: Mutex<Vec<Point>>,
        at: Option<AtFn>,
        link: Option<PathFn>,
        rename: Option<PathFn>,
        sabotages: HashSet<Sabotage>,
    }

    impl fmt::Debug for TestHooks {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("TestHooks").finish_non_exhaustive()
        }
    }

    impl ProtocolHooks for TestHooks {
        fn at(&self, point: Point) {
            self.reached.lock().unwrap().push(point);
            if let Some(at) = &self.at {
                at(point);
            }
        }

        fn link(&self, from: &Path, to: &Path) -> io::Result<()> {
            match &self.link {
                Some(link) => link(from, to),
                None => fs::hard_link(from, to),
            }
        }

        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            match &self.rename {
                Some(rename) => rename(from, to),
                None => fs::rename(from, to),
            }
        }

        fn sabotaged(&self, sabotage: Sabotage) -> bool {
            self.sabotages.contains(&sabotage)
        }
    }

    impl TestHooks {
        /// Runs `at` after the hooks' current `at`.
        pub(crate) fn at(self, at: impl Fn(Point) + Send + Sync + 'static) -> Self {
            let first = self.at;
            Self {
                at: Some(Box::new(move |point| {
                    if let Some(first) = &first {
                        first(point);
                    }
                    at(point);
                })),
                ..self
            }
        }

        pub(crate) fn link(
            self,
            link: impl Fn(&Path, &Path) -> io::Result<()> + Send + Sync + 'static,
        ) -> Self {
            Self { link: Some(Box::new(link)), ..self }
        }

        pub(crate) fn rename(
            self,
            rename: impl Fn(&Path, &Path) -> io::Result<()> + Send + Sync + 'static,
        ) -> Self {
            Self { rename: Some(Box::new(rename)), ..self }
        }

        pub(crate) fn sabotage(mut self, sabotage: Sabotage) -> Self {
            self.sabotages.insert(sabotage);
            self
        }

        pub(crate) fn reached(&self) -> Vec<Point> {
            self.reached.lock().unwrap().clone()
        }
    }

    /// Runs `writer` with `hooks` on its own thread, pauses it at `pause`,
    /// runs `while_paused`, then resumes it. Returns the writer's result and
    /// the points it reached.
    pub(crate) fn with_paused<T: Send>(
        hooks: TestHooks,
        pause: Point,
        writer: impl FnOnce(&TestHooks) -> T + Send,
        while_paused: impl FnOnce(),
    ) -> (T, Vec<Point>) {
        thread::scope(|scope| {
            let (reached_tx, reached_rx) = mpsc::channel();
            // Dropped if `while_paused` panics, which unblocks the writer.
            let (resume_tx, resume_rx) = mpsc::channel::<()>();
            let resume_rx = Mutex::new(resume_rx);
            let hooks = hooks.at(move |point| {
                if point == pause {
                    reached_tx.send(()).unwrap();
                    resume_rx.lock().unwrap().recv().unwrap();
                }
            });
            let writer = scope.spawn(move || {
                let result = writer(&hooks);
                (result, hooks.reached())
            });
            // Disconnected if the writer finished without reaching `pause`.
            reached_rx.recv().unwrap();
            while_paused();
            resume_tx.send(()).unwrap();
            writer.join().unwrap()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::{testing::*, *};
    use crate::hooks::NoHooks;

    fn repo(root: &Path) -> RepoD<'_> {
        RepoD { root, hooks: &NoHooks }
    }

    fn marker(kind: MarkerKind, version: Version) -> Marker {
        Marker { kind, version }
    }

    #[test]
    fn backoff_doubles_from_10_ms_capped_at_500_ms() {
        let pauses: Vec<u64> =
            backoffs().map(|pause| pause.as_millis().try_into().unwrap()).collect();
        assert_eq!(pauses, [10, 20, 40, 80, 160, 320, 500]);
    }

    #[test]
    fn with_retries_tries_8_times() {
        let calls = Cell::new(0);
        let result: Option<()> = with_retries(|| {
            calls.set(calls.get() + 1);
            None
        });
        assert_eq!((result, calls.get()), (None, 8));

        calls.set(0);
        let result = with_retries(|| {
            calls.set(calls.get() + 1);
            (calls.get() == 3).then_some("third")
        });
        assert_eq!((result, calls.get()), (Some("third"), 3));
    }

    #[test]
    fn list_markers_parses_both_kinds_and_ignores_the_rest() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let v1 = version(1, b"v1");
        let dir = root.path().join("repo.d");
        for name in [frozen(&v1), v1.file_name(), ".nfs123".to_string(), "x".to_string()]
        {
            fs::write(dir.join(name), b"").unwrap();
        }
        let Listed::Markers(mut markers) = repo(root.path()).list_markers().unwrap()
        else {
            panic!("repo.d exists")
        };
        markers.sort_by_key(|marker| marker.version.n());
        assert_eq!(
            markers,
            [marker(MarkerKind::Latest, v0), marker(MarkerKind::Frozen, v1)]
        );
    }

    #[test]
    fn list_markers_without_repo_d_is_not_found() {
        let root = tempfile::tempdir().unwrap();
        assert!(matches!(repo(root.path()).list_markers().unwrap(), Listed::NotFound));
        let missing = root.path().join("missing");
        assert!(matches!(repo(&missing).list_markers().unwrap(), Listed::NotFound));
    }

    #[test]
    fn list_markers_of_a_repo_d_file_is_an_error() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("repo.d"), b"").unwrap();
        let err = repo(root.path()).list_markers().unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Io(err) if err.kind() == io::ErrorKind::NotADirectory),
            "{err:?}"
        );
    }

    #[test]
    fn discard_tmp_dir_never_deletes_what_became_repo_d() {
        let root = tempfile::tempdir().unwrap();
        let tmp = root.path().join(tmp_dir_name(Uuid::new_v4()));
        fs::create_dir_all(&tmp).unwrap();
        fs::write(tmp.join("0.x"), b"x").unwrap();
        discard_tmp_dir(root.path(), &tmp).unwrap();
        assert!(names(root.path()).is_empty());

        fs::create_dir_all(&tmp).unwrap();
        fs::write(tmp.join("0.x"), b"x").unwrap();
        fs::rename(&tmp, root.path().join("repo.d")).unwrap();
        let err = discard_tmp_dir(root.path(), &tmp).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert_eq!(names(root.path()), set(["repo.d".to_string()]));
        assert_eq!(names(&root.path().join("repo.d")), set(["0.x".to_string()]));
    }

    #[test]
    fn highest_takes_the_highest_n_and_frozen_over_latest() {
        let v1 = version(1, b"v1");
        let v2 = version(2, b"v2");
        let latest1 = marker(MarkerKind::Latest, v1.clone());
        let frozen1 = marker(MarkerKind::Frozen, v1);
        let latest2 = marker(MarkerKind::Latest, v2);
        assert_eq!(RepoD::highest(&[]), None);
        assert_eq!(
            RepoD::highest(&[latest2.clone(), frozen1.clone(), latest1.clone()]),
            Some(&latest2)
        );
        assert_eq!(RepoD::highest(&[latest1.clone(), frozen1.clone()]), Some(&frozen1));
        assert_eq!(RepoD::highest(&[frozen1.clone(), latest1]), Some(&frozen1));
    }
}
