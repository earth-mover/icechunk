//! The `repo.d` protocol of design-docs/020-filesystem-storage.md: `repo` is
//! a directory of immutable versions and a marker whose name says which one
//! is current, moved only by `rename`.

pub(crate) mod commit;
pub(crate) mod convert;
pub(crate) mod lifecycle;
pub(crate) mod mirror;
pub(crate) mod read;
pub(crate) mod sweep;

use std::{
    collections::BTreeSet,
    fs, io, iter,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use icechunk_format::writer_features::{FeatureName, MinVersion};
use uuid::Uuid;

use crate::{
    hooks::ProtocolHooks,
    names::{Marker, REPO_D, RepoDName, Version, parse_repo_d_name, tmp_dir_name},
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
    /// The bytes of a creation don't list `filesystem-layout-v1`, or the
    /// bytes of a creation or a change don't decode.
    NotInLayout(String),
    /// `$ROOT/repo` doesn't list `filesystem-layout-v1` and `repo.d` doesn't
    /// exist: a repository in the standard layout, to convert.
    StandardLayout,
    /// `$ROOT/repo` lists `filesystem-layout-v1` with a longer history than
    /// a creation's and `repo.d` doesn't exist: manual recovery.
    OrphanMirror,
    /// `$ROOT/repo` can't be read or decoded and `repo.d` doesn't exist; why.
    RepoUnreadable(String),
    /// A rerun of the conversion from the standard layout can't attribute the
    /// `repo.d` it found to its own earlier run; what it found and the
    /// remedy.
    ManualRecovery(String),
    /// The repo info lists writer features this storage doesn't implement.
    MissingFeatures {
        missing: BTreeSet<FeatureName>,
        min_version: MinVersion,
    },
    /// A repo info that decodes but can't be changed: its shape is malformed,
    /// or it can't be converted; why.
    Malformed(String),
    /// An operation stopped before it finished; what happened and that
    /// rerunning it completes it.
    Incomplete(String),
    /// A version without both an etag and a generation.
    InvalidVersion(String),
    /// Ran out of tries; why the last one failed.
    NoConsistentView(String),
    /// A version file whose bytes don't hash to its name, or don't decode.
    Corrupt(String),
    /// The fork check found a fork; what it found.
    Forked(String),
    /// A change landed as `new`, and its publication found a fork; what it
    /// found.
    ForkedAfterUpdate {
        new: Version,
        detail: String,
    },
    /// GC's hook was called without the version of GC's change on a
    /// repository in the filesystem layout whose current version lists
    /// `filesystem-layout-v1`.
    GcWithoutChange(String),
    /// `$ROOT/repo` couldn't be read or decoded, so a fork can't be ruled out.
    MirrorUnreadable(String),
    /// The current version doesn't list `filesystem-layout-v1`: a conversion
    /// to the standard layout was interrupted.
    ConversionInProgress(String),
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
                        RepoDName::Content(_)
                        | RepoDName::Stage(_)
                        | RepoDName::Mirror(_) => None,
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

    /// The marker of a listing: the newest state it witnessed.
    pub(crate) fn highest(markers: &[Marker]) -> Option<&Marker> {
        markers.iter().max_by_key(|marker| marker.version.n())
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
/// The ops-log files the lost-reply check and the provenance check read at
/// most.
const OPS_LOG_FILES: usize = 3;
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
        fmt::{self, Write as _},
        fs, io,
        path::Path,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };

    use tracing::{
        Event, Level, Metadata, Subscriber,
        field::{Field, Visit},
        span,
    };

    use super::{RepoD, lifecycle::CreateOutcome::Created};
    pub(crate) use crate::repo_infos::{
        changed_repo_info, committed_repo_info, featureless_repo_info, repo_info,
    };
    use crate::{
        hooks::{Observed, Point, ProtocolHooks, Sabotage},
        names::{Hash, Marker, Version},
    };

    /// The bytes of the first version [`seed`] writes.
    pub(crate) fn v0_bytes() -> Vec<u8> {
        repo_info(0)
    }

    pub(crate) fn version(n: u64, bytes: &[u8]) -> Version {
        Version::new(n, Hash::of(bytes)).unwrap()
    }

    pub(crate) fn latest(version: &Version) -> String {
        Marker { version: version.clone() }.file_name()
    }

    /// Writes `repo.d/0.<h0>` and its marker for the bytes [`v0_bytes`].
    pub(crate) fn seed(root: &Path) -> Version {
        let v0 = version(0, &v0_bytes());
        let dir = root.join("repo.d");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(v0.file_name()), v0_bytes()).unwrap();
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

    pub(crate) const UNDECODABLE: &[u8] = b"not a repo info";

    pub(crate) fn strings<const N: usize>(names: [&str; N]) -> BTreeSet<String> {
        names.into_iter().map(str::to_string).collect()
    }

    pub(crate) fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .unwrap()
    }

    /// Creates the repository with `bytes`, checking that its first `N` is a
    /// Unix time in milliseconds within the call.
    pub(crate) fn create(
        root: &Path,
        hooks: &dyn ProtocolHooks,
        bytes: &[u8],
    ) -> Version {
        let before = now_ms();
        let outcome = RepoD { root, hooks }.create(bytes).unwrap();
        let after = now_ms();
        let Created(v0) = outcome else { panic!("{outcome:?}") };
        assert!(
            (before..=after).contains(&v0.n()),
            "{} not in {before}..={after}",
            v0.n()
        );
        assert_eq!(v0.hash(), &Hash::of(bytes));
        v0
    }

    /// Writes `repo.d` holding version 0 with `bytes`, without `$ROOT/repo`.
    pub(crate) fn repo_d_with(root: &Path, bytes: &[u8]) -> Version {
        let v0 = version(0, bytes);
        let dir = root.join("repo.d");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(v0.file_name()), bytes).unwrap();
        fs::write(dir.join(latest(&v0)), b"").unwrap();
        v0
    }

    pub(crate) fn plant(root: &Path, bytes: &[u8]) {
        fs::write(root.join("repo"), bytes).unwrap();
    }

    /// A rename of `from` to `to` that lands, then reports `ENOENT`, as after
    /// a lost reply, the first time its target is `name`.
    pub(crate) fn lose_rename_reply_onto(name: &'static str) -> TestHooks {
        let lost = AtomicBool::new(false);
        TestHooks::default().rename(move |from, to| {
            fs::rename(from, to)?;
            if to.file_name().is_some_and(|n| n == name)
                && !lost.swap(true, Ordering::SeqCst)
            {
                Err(io::ErrorKind::NotFound.into())
            } else {
                Ok(())
            }
        })
    }

    type AtFn = Box<dyn Fn(Point) + Send + Sync>;
    type PathFn = Box<dyn Fn(&Path, &Path) -> io::Result<()> + Send + Sync>;
    type ObservedFn = Box<dyn Fn(Observed) + Send + Sync>;

    /// Records every point reached; each operation defaults to the real one.
    #[derive(Default)]
    pub(crate) struct TestHooks {
        reached: Mutex<Vec<Point>>,
        observed: Mutex<Vec<Observed>>,
        at: Option<AtFn>,
        link: Option<PathFn>,
        rename: Option<PathFn>,
        when_observed: Option<ObservedFn>,
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

        fn observed(&self, what: Observed) {
            self.observed.lock().unwrap().push(what);
            if let Some(when_observed) = &self.when_observed {
                when_observed(what);
            }
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

        /// Runs `f` after recording each observation.
        pub(crate) fn when_observed(
            self,
            f: impl Fn(Observed) + Send + Sync + 'static,
        ) -> Self {
            Self { when_observed: Some(Box::new(f)), ..self }
        }

        pub(crate) fn sabotage(mut self, sabotage: Sabotage) -> Self {
            self.sabotages.insert(sabotage);
            self
        }

        pub(crate) fn reached(&self) -> Vec<Point> {
            self.reached.lock().unwrap().clone()
        }

        pub(crate) fn observed(&self) -> Vec<Observed> {
            self.observed.lock().unwrap().clone()
        }
    }

    /// The fields of the events at `level` logged while `f` runs on this
    /// thread.
    pub(crate) fn logged<T>(level: Level, f: impl FnOnce() -> T) -> (T, Vec<String>) {
        #[derive(Clone)]
        struct Collector(Level, Arc<Mutex<Vec<String>>>);
        struct Fields(String);
        impl Visit for Fields {
            fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
                write!(self.0, "{}={value:?} ", field.name()).unwrap();
            }
        }
        impl Subscriber for Collector {
            fn enabled(&self, _: &Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
                span::Id::from_u64(1)
            }
            fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
            fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
            fn event(&self, event: &Event<'_>) {
                if *event.metadata().level() == self.0 {
                    let mut fields = Fields(String::new());
                    event.record(&mut fields);
                    self.1.lock().unwrap().push(fields.0);
                }
            }
            fn enter(&self, _: &span::Id) {}
            fn exit(&self, _: &span::Id) {}
        }
        let collector = Collector(level, Arc::default());
        let result = tracing::subscriber::with_default(collector.clone(), f);
        let events = collector.1.lock().unwrap().clone();
        (result, events)
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

    fn marker(version: Version) -> Marker {
        Marker { version }
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
    fn list_markers_parses_markers_and_ignores_the_rest() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let v1 = version(1, b"v1");
        let dir = root.path().join("repo.d");
        let unknown_marker = latest(&v1).replacen("latest.", "frozen.", 1);
        for name in
            [unknown_marker, v1.file_name(), ".nfs123".to_string(), "x".to_string()]
        {
            fs::write(dir.join(name), b"").unwrap();
        }
        let Listed::Markers(markers) = repo(root.path()).list_markers().unwrap() else {
            panic!("repo.d exists")
        };
        assert_eq!(markers, [marker(v0)]);
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
    fn highest_takes_the_highest_n() {
        let latest1 = marker(version(1, b"v1"));
        let latest2 = marker(version(2, b"v2"));
        let latest3 = marker(version(3, b"v3"));
        assert_eq!(RepoD::highest(&[]), None);
        assert_eq!(RepoD::highest(std::slice::from_ref(&latest1)), Some(&latest1));
        assert_eq!(
            RepoD::highest(&[latest2.clone(), latest3.clone(), latest1.clone()]),
            Some(&latest3)
        );
        assert_eq!(RepoD::highest(&[latest3.clone(), latest1, latest2]), Some(&latest3));
    }
}
