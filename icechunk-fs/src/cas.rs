//! The lock-free conditional update of an object: [`conditional_update`].
//!
//! The protocol takes [`CasHooks`] so tests can interleave concurrent writers
//! deterministically and inject filesystem failures that real filesystems
//! don't produce on demand. Production uses [`NoHooks`].

use std::{
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    time::Duration,
};

use icechunk_storage::Generation;
use uuid::Uuid;

use crate::{
    lock::{LockReadError, ProtocolPaths, is_valid_hash, lock_is_stale, read_lock_owner},
    read::read_all,
    storage::is_missing,
    version::{generation_of, sha256_hex},
    write::{fsync_dir, write_exclusive},
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct CasOptions {
    pub(crate) fsync: bool,
    pub(crate) stale_after: Duration,
}

#[derive(Debug)]
pub(crate) enum CasOutcome {
    /// `generation` is that of the renamed ticket, if it could be observed.
    Updated {
        generation: Option<Generation>,
    },
    NotOnLatestVersion,
}

#[derive(Debug)]
pub(crate) enum CasError {
    /// The new content hashes to the base version.
    RepeatedContent,
    /// A lock file of the base's lock family doesn't hold an `attempt_id`.
    /// Needs manual recovery.
    CorruptLock {
        path: PathBuf,
    },
    Io(io::Error),
}

/// The protocol steps after which [`CasHooks::at`] is called.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Point {
    /// Our lockinfo exists; no lock or ticket yet.
    LockInfoWritten,
    /// Our ticket exists, before our lock is visible.
    TicketWritten,
    /// We hold the lock `<name>.lock.<base_hash>.<generation>`.
    LockTaken { generation: u64 },
    /// Every lower generation's ticket is deleted.
    Fenced,
    /// The fresh read matched the base; the next step is the commit rename.
    Compared,
    /// Our ticket was renamed over the target.
    Renamed,
}

/// Test seams in the protocol. The default methods do the real filesystem
/// operation, and the hooks never change the protocol's control flow.
///
/// Its correctness depends on how concurrent writers interleave and on how
/// name operations fail, and neither can be produced reliably from outside.
pub(crate) trait CasHooks: Sync {
    /// Called after each step. A test can block a writer here, run other
    /// writers (for example a stealer that fences and commits), then resume
    /// it, which checks interleavings deterministically, without sleeps.
    fn at(&self, _point: Point) {}

    /// Takes a lock. A test can link, then report `EEXIST`, as an NFS client
    /// does when it retries a `link` whose first reply was lost.
    fn link(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::hard_link(from, to)
    }

    /// The commit. A test can rename, then report `ENOENT`, as after a lost
    /// reply.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    /// The fence's unlink of an earlier holder's ticket. A test can fail it
    /// (`EIO`, `EACCES`) to check that the writer then doesn't commit.
    fn unlink(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }
}

/// The hooks used in production: every method does the real operation.
#[derive(Debug)]
pub(crate) struct NoHooks;

impl CasHooks for NoHooks {}

/// Replaces `target` with `new_bytes` if its content hashes to `base_hash`.
///
/// Implements the protocol specified in design-docs/020-filesystem-storage.md.
/// Change the code and the design doc together, and re-run the model in `tests/protocol_model.rs`.
pub(crate) fn conditional_update(
    target: &Path,
    new_bytes: &[u8],
    base_hash: &str,
    opts: &CasOptions,
    hooks: &dyn CasHooks,
) -> Result<CasOutcome, CasError> {
    // It goes into file names.
    if !is_valid_hash(base_hash) {
        return Err(CasError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid base version hash: {base_hash:?}"),
        )));
    }
    // Writing identical bytes would make `base_hash` current again after it
    // was replaced. Deleting lock files is only safe because that never
    // happens (see "Version identity and content uniqueness").
    let new_hash = sha256_hex(new_bytes);
    if new_hash == base_hash {
        return Err(CasError::RepeatedContent);
    }
    let attempt = Attempt {
        target,
        new_bytes,
        new_hash: &new_hash,
        base_hash,
        opts,
        hooks,
        paths: ProtocolPaths::new(target).map_err(CasError::Io)?,
        id: Uuid::new_v4(),
    };

    // `attempt.run()` has dropped its ticket fd by the time it returns
    // without committing: on NFS, unlinking a file the client has open
    // renames it instead.
    let ticket = match attempt.run() {
        Ok(Some(ticket)) => ticket,
        Ok(None) => {
            attempt.cleanup();
            return Ok(CasOutcome::NotOnLatestVersion);
        }
        Err(err) => {
            attempt.cleanup();
            return Err(err);
        }
    };

    // The rename is a change to the directory, not to any file, and fsyncing
    // the ticket doesn't make it durable. Without this, a power loss after we
    // report success can bring back the old `target`, silently undoing a
    // commit the caller already saw succeed.
    let synced = if opts.fsync { fsync_dir(attempt.paths.dir()) } else { Ok(()) };
    attempt.tidy_up();
    synced.map_err(CasError::Io)?;
    let generation = ticket.metadata().ok().map(|meta| generation_of(&meta));
    Ok(CasOutcome::Updated { generation })
}

struct Attempt<'a> {
    target: &'a Path,
    new_bytes: &'a [u8],
    new_hash: &'a str,
    base_hash: &'a str,
    opts: &'a CasOptions,
    hooks: &'a dyn CasHooks,
    paths: ProtocolPaths,
    id: Uuid,
}

impl Attempt<'_> {
    fn ticket(&self) -> PathBuf {
        self.paths.ticket(self.id)
    }

    fn lockinfo(&self) -> PathBuf {
        self.paths.lockinfo(self.id)
    }

    /// Steps 1 to 5. Returns a read-only fd on the ticket, now `target`, if
    /// the update committed, `None` on a conflict. The caller cleans up on
    /// anything but a commit.
    fn run(&self) -> Result<Option<File>, CasError> {
        let lockinfo = self.lockinfo();
        let ticket = self.ticket();

        // ── Step 1: stage the lock content and the new content ──────────────
        // Always fsynced: an empty lock file would block its lock family
        // until manual recovery.
        match write_exclusive(
            &lockinfo,
            self.id.hyphenated().to_string().as_bytes(),
            true,
        ) {
            Ok(()) => {}
            // The target's directory doesn't exist, so neither does the target.
            Err(err) if is_missing(&err) => return Ok(None),
            Err(err) => return Err(CasError::Io(err)),
        }
        self.hooks.at(Point::LockInfoWritten);

        // Early exit: if the gen-0 lock for our base exists and is not stale,
        // someone is committing on top of our base and we will lose anyway.
        let lock0 = self.paths.lock(self.base_hash, 0);
        match fs::symlink_metadata(&lock0) {
            Ok(_) => match lock_is_stale(&lock0, &lockinfo, self.opts.stale_after) {
                Ok(true) => {}
                Ok(false) => return Ok(None),
                Err(err) if is_missing(&err) => {}
                Err(err) => return Err(CasError::Io(err)),
            },
            Err(err) if is_missing(&err) => {}
            Err(err) => return Err(CasError::Io(err)),
        }

        // We create our commit ticket, critically, before our lock is visible.
        // If a stealer could see our lock before our ticket existed, its
        // unlink would hit ENOENT and we would still be able to commit later.
        write_exclusive(&ticket, self.new_bytes, self.opts.fsync)
            .map_err(CasError::Io)?;
        // Opened while nobody else knows the ticket's name, so after the
        // rename its `fstat` is of our commit, whatever replaces `target` later.
        let ticket_file = match File::open(&ticket) {
            Ok(file) => file,
            Err(err) if is_missing(&err) => return Ok(None),
            Err(err) => return Err(CasError::Io(err)),
        };
        self.hooks.at(Point::TicketWritten);

        // ── Step 2: take the lock for replacing `base_hash` ─────────────────
        let mut generation = 0;
        let my_generation = loop {
            let lock = self.paths.lock(self.base_hash, generation);
            match self.hooks.link(&lockinfo, &lock) {
                Ok(()) => break generation,
                // Our lockinfo was swept.
                Err(err) if is_missing(&err) => return Ok(None),
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                    match read_lock_owner(&lock) {
                        // A link retried after a lost reply reports EEXIST
                        // although the first try succeeded.
                        Ok(owner) if owner == self.id => break generation,
                        // Locks are only deleted once their hash is no longer
                        // current, so our base is outdated.
                        Err(LockReadError::NotFound) => return Ok(None),
                        // Without the holder's attempt_id we cannot delete its
                        // ticket, so we cannot steal.
                        Err(LockReadError::Unparseable) => {
                            return Err(CasError::CorruptLock { path: lock });
                        }
                        Err(LockReadError::Io(err)) => return Err(CasError::Io(err)),
                        Ok(_) => {
                            match lock_is_stale(&lock, &lockinfo, self.opts.stale_after) {
                                // Probably alive and about to change `target`.
                                Ok(false) => return Ok(None),
                                // Only one stealer can create the next generation.
                                Ok(true) => generation += 1,
                                Err(err) if is_missing(&err) => return Ok(None),
                                Err(err) => return Err(CasError::Io(err)),
                            }
                        }
                    }
                }
                Err(err) => return Err(CasError::Io(err)),
            }
        };
        self.hooks.at(Point::LockTaken { generation: my_generation });

        // ── Step 3: fence every earlier holder in this lock family ──────────
        for earlier in 0..my_generation {
            let lock = self.paths.lock(self.base_hash, earlier);
            match read_lock_owner(&lock) {
                Ok(owner) => match self.hooks.unlink(&self.paths.ticket(owner)) {
                    // NotFound: already committed or deleted.
                    Err(err) if err.kind() != io::ErrorKind::NotFound => {
                        return Err(CasError::Io(err));
                    }
                    _ => {}
                },
                // Deleted, so `base_hash` is no longer current and step 4 fails.
                Err(LockReadError::NotFound) => {}
                Err(LockReadError::Unparseable) => {
                    return Err(CasError::CorruptLock { path: lock });
                }
                Err(LockReadError::Io(err)) => return Err(CasError::Io(err)),
            }
        }
        self.hooks.at(Point::Fenced);

        // ── Step 4: compare ─────────────────────────────────────────────────
        if !self.target_hashes_to(self.base_hash)? {
            return Ok(None);
        }
        self.hooks.at(Point::Compared);

        // ── Step 5: commit point ────────────────────────────────────────────
        match self.hooks.rename(&ticket, self.target) {
            Ok(()) => {}
            // Either our ticket was deleted before the rename reached the
            // server, or the rename succeeded and a retry after a lost reply
            // saw ENOENT. Content is unique, so reading `target` tells us which.
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                if !self.target_hashes_to(self.new_hash)? {
                    return Ok(None);
                }
            }
            Err(err) => return Err(CasError::Io(err)),
        }
        self.hooks.at(Point::Renamed);
        Ok(Some(ticket_file))
    }

    /// Whether a fresh read of `target` hashes to `hash`; a missing target
    /// doesn't.
    fn target_hashes_to(&self, hash: &str) -> Result<bool, CasError> {
        match read_all(self.target) {
            Ok(bytes) => Ok(sha256_hex(&bytes) == hash),
            Err(err) if is_missing(&err) => Ok(false),
            Err(err) => Err(CasError::Io(err)),
        }
    }

    /// Step 6, after a commit: `base_hash` is no longer current, so its lock
    /// family can never be used again. A failure here is harmless.
    fn tidy_up(&self) {
        let lockinfo = self.lockinfo();
        if let Err(err) = fs::remove_file(&lockinfo) {
            tracing::warn!(path = %lockinfo.display(), error = %err, "Cannot delete lockinfo");
        }
        let entries = match fs::read_dir(self.paths.dir()) {
            Ok(entries) => entries,
            Err(err) => {
                tracing::warn!(
                    path = %self.paths.dir().display(),
                    error = %err,
                    "Cannot list directory to delete locks"
                );
                return;
            }
        };
        for entry in entries {
            match entry {
                Ok(entry)
                    if self.paths.is_lock_of(&entry.file_name(), self.base_hash) =>
                {
                    let lock = entry.path();
                    if let Err(err) = fs::remove_file(&lock) {
                        tracing::warn!(path = %lock.display(), error = %err, "Cannot delete lock");
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(
                        path = %self.paths.dir().display(),
                        error = %err,
                        "Cannot list directory to delete locks"
                    );
                }
            }
        }
    }

    // Called on every failure path. Deleting our ticket guarantees this
    // attempt can never commit later.
    //
    // It must NOT delete our lock file. A new writer could then take our freed
    // generation. It only deletes the tickets of *lower* generations, so it
    // would miss a stealer above us, and both could commit.
    fn cleanup(&self) {
        let _ = fs::remove_file(self.ticket());
        let _ = fs::remove_file(self.lockinfo());
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::{Mutex, mpsc},
        thread,
        time::SystemTime,
    };

    use tempfile::TempDir;

    use super::*;

    const V0: &[u8] = b"v0";
    const STALE_AFTER: Duration = Duration::from_secs(60);
    const OPTS: CasOptions = CasOptions { fsync: true, stale_after: STALE_AFTER };

    type AtFn = Box<dyn Fn(Point) + Send + Sync>;
    type PathFn = Box<dyn Fn(&Path, &Path) -> io::Result<()> + Send + Sync>;
    type UnlinkFn = Box<dyn Fn(&Path) -> io::Result<()> + Send + Sync>;

    /// Records every point reached; each behaviour defaults to the real one.
    #[derive(Default)]
    struct TestHooks {
        reached: Mutex<Vec<Point>>,
        at: Option<AtFn>,
        link: Option<PathFn>,
        rename: Option<PathFn>,
        unlink: Option<UnlinkFn>,
    }

    impl CasHooks for TestHooks {
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

        fn unlink(&self, path: &Path) -> io::Result<()> {
            match &self.unlink {
                Some(unlink) => unlink(path),
                None => fs::remove_file(path),
            }
        }
    }

    impl TestHooks {
        fn with_at(at: impl Fn(Point) + Send + Sync + 'static) -> Self {
            Self { at: Some(Box::new(at)), ..Self::default() }
        }

        fn reached(&self) -> Vec<Point> {
            self.reached.lock().unwrap().clone()
        }
    }

    /// Runs a writer of `new_bytes` on its own thread, pauses it at `pause`,
    /// runs `while_paused`, then resumes it. Returns its result and the points
    /// it reached.
    fn with_writer_paused_at(
        repo: &Path,
        new_bytes: &[u8],
        pause: Point,
        while_paused: impl FnOnce(),
    ) -> (Result<CasOutcome, CasError>, Vec<Point>) {
        thread::scope(|scope| {
            let (reached_tx, reached_rx) = mpsc::channel();
            // Dropped if `while_paused` panics, which unblocks the writer.
            let (resume_tx, resume_rx) = mpsc::channel::<()>();
            let resume_rx = Mutex::new(resume_rx);
            let hooks = TestHooks::with_at(move |point| {
                if point == pause {
                    reached_tx.send(()).unwrap();
                    resume_rx.lock().unwrap().recv().unwrap();
                }
            });
            let writer = scope.spawn(move || {
                let result = update(repo, new_bytes, &hooks);
                (result, hooks.reached())
            });
            // Disconnected if the writer finished without reaching `pause`.
            reached_rx.recv().unwrap();
            while_paused();
            resume_tx.send(()).unwrap();
            writer.join().unwrap()
        })
    }

    /// Hooks that make every lock look stale to this writer, by moving the
    /// mtime of its lockinfo into the future. `others` are the lockinfos
    /// present before it starts.
    fn sees_locks_as_stale(dir: &Path) -> TestHooks {
        let dir = dir.to_path_buf();
        let others = lockinfos(&dir);
        TestHooks::with_at(move |point| {
            if point == Point::LockInfoWritten {
                let ours: Vec<_> = lockinfos(&dir).difference(&others).cloned().collect();
                assert_eq!(ours.len(), 1, "{ours:?}");
                File::options()
                    .write(true)
                    .open(dir.join(&ours[0]))
                    .unwrap()
                    .set_modified(SystemTime::now() + 2 * STALE_AFTER)
                    .unwrap();
            }
        })
    }

    fn names(dir: &Path) -> BTreeSet<String> {
        fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect()
    }

    fn lockinfos(dir: &Path) -> BTreeSet<String> {
        names(dir).into_iter().filter(|name| name.starts_with("repo.lockinfo.")).collect()
    }

    fn tickets(dir: &Path) -> BTreeSet<String> {
        names(dir)
            .into_iter()
            .filter(|name| name.starts_with("repo.new_bytes."))
            .collect()
    }

    fn locks(dir: &Path) -> BTreeSet<String> {
        names(dir).into_iter().filter(|name| name.starts_with("repo.lock.")).collect()
    }

    fn lock_name(hash: &str, generation: u64) -> String {
        format!("repo.lock.{hash}.{generation}")
    }

    /// A directory with `repo` holding `V0`.
    fn setup() -> (TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        fs::write(&repo, V0).unwrap();
        (dir, repo)
    }

    fn update(
        repo: &Path,
        new_bytes: &[u8],
        hooks: &dyn CasHooks,
    ) -> Result<CasOutcome, CasError> {
        conditional_update(repo, new_bytes, &sha256_hex(V0), &OPTS, hooks)
    }

    fn assert_updated(result: Result<CasOutcome, CasError>) -> Option<Generation> {
        match result {
            Ok(CasOutcome::Updated { generation }) => generation,
            other => panic!("expected Updated, got {other:?}"),
        }
    }

    fn assert_not_on_latest(result: &Result<CasOutcome, CasError>) {
        assert!(matches!(result, Ok(CasOutcome::NotOnLatestVersion)), "{result:?}");
    }

    #[test]
    fn single_writer_updates() {
        let (dir, repo) = setup();
        let generation = assert_updated(update(&repo, b"v1", &NoHooks));
        assert_eq!(fs::read(&repo).unwrap(), b"v1");
        assert_eq!(names(dir.path()), BTreeSet::from(["repo".to_string()]));
        assert_eq!(generation, Some(generation_of(&fs::metadata(&repo).unwrap())));
    }

    #[test]
    fn repeated_content_is_an_error() {
        let (dir, repo) = setup();
        let hooks = TestHooks::default();
        assert!(matches!(update(&repo, V0, &hooks), Err(CasError::RepeatedContent)));
        assert!(hooks.reached().is_empty());
        assert_eq!(names(dir.path()), BTreeSet::from(["repo".to_string()]));
        assert_eq!(fs::read(&repo).unwrap(), V0);
    }

    #[test]
    fn stale_base_is_not_on_latest_and_keeps_the_lock() {
        let (dir, repo) = setup();
        fs::write(&repo, b"v1").unwrap();
        assert_not_on_latest(&update(&repo, b"v2", &NoHooks));
        assert_eq!(fs::read(&repo).unwrap(), b"v1");
        assert_eq!(
            names(dir.path()),
            BTreeSet::from(["repo".to_string(), lock_name(&sha256_hex(V0), 0)])
        );
    }

    #[test]
    fn two_writers_on_the_same_base_exactly_one_wins() {
        let (dir, repo) = setup();
        let (a_result, _) = with_writer_paused_at(
            &repo,
            b"a",
            Point::LockTaken { generation: 0 },
            || {
                let b_hooks = TestHooks::default();
                assert_not_on_latest(&update(&repo, b"b", &b_hooks));
                assert_eq!(b_hooks.reached(), [Point::LockInfoWritten]);
            },
        );
        assert_updated(a_result);
        assert_eq!(fs::read(&repo).unwrap(), b"a");
        assert_eq!(names(dir.path()), BTreeSet::from(["repo".to_string()]));
    }

    #[test]
    fn stealer_fences_a_paused_holder() {
        let (dir, repo) = setup();
        let (a_result, a_reached) =
            with_writer_paused_at(&repo, b"a", Point::Compared, || {
                let b_hooks = sees_locks_as_stale(dir.path());
                assert_updated(update(&repo, b"b", &b_hooks));
                assert!(b_hooks.reached().contains(&Point::LockTaken { generation: 1 }));
                assert_eq!(fs::read(&repo).unwrap(), b"b");
            });
        assert_not_on_latest(&a_result);
        assert!(!a_reached.contains(&Point::Renamed));
        assert_eq!(fs::read(&repo).unwrap(), b"b");
        assert_eq!(names(dir.path()), BTreeSet::from(["repo".to_string()]));
    }

    #[test]
    fn lost_reply_on_link_is_resolved_by_the_lock_content() {
        let (dir, repo) = setup();
        let hooks = TestHooks {
            link: Some(Box::new(|from, to| {
                fs::hard_link(from, to)?;
                Err(io::ErrorKind::AlreadyExists.into())
            })),
            ..TestHooks::default()
        };
        assert_updated(update(&repo, b"v1", &hooks));
        assert_eq!(fs::read(&repo).unwrap(), b"v1");
        assert_eq!(names(dir.path()), BTreeSet::from(["repo".to_string()]));
    }

    #[test]
    fn lost_reply_on_rename_is_resolved_by_the_content() {
        let (dir, repo) = setup();
        let hooks = TestHooks {
            rename: Some(Box::new(|from, to| {
                fs::rename(from, to)?;
                Err(io::ErrorKind::NotFound.into())
            })),
            ..TestHooks::default()
        };
        let generation = assert_updated(update(&repo, b"v1", &hooks));
        assert_eq!(fs::read(&repo).unwrap(), b"v1");
        assert_eq!(names(dir.path()), BTreeSet::from(["repo".to_string()]));
        assert_eq!(generation, Some(generation_of(&fs::metadata(&repo).unwrap())));
    }

    #[test]
    fn swept_lockinfo_is_not_on_latest() {
        let (dir, repo) = setup();
        let dir_path = dir.path().to_path_buf();
        let hooks = TestHooks::with_at(move |point| {
            if point == Point::TicketWritten {
                for lockinfo in lockinfos(&dir_path) {
                    fs::remove_file(dir_path.join(lockinfo)).unwrap();
                }
            }
        });
        assert_not_on_latest(&update(&repo, b"v1", &hooks));
        assert_eq!(fs::read(&repo).unwrap(), V0);
        assert_eq!(names(dir.path()), BTreeSet::from(["repo".to_string()]));
    }

    #[test]
    fn corrupt_lock_is_an_error() {
        let (dir, repo) = setup();
        let corrupt = dir.path().join(lock_name(&sha256_hex(V0), 0));
        fs::write(&corrupt, "garbage").unwrap();

        let hooks = sees_locks_as_stale(dir.path());
        match update(&repo, b"v1", &hooks) {
            Err(CasError::CorruptLock { path }) => assert_eq!(path, corrupt),
            other => panic!("expected CorruptLock, got {other:?}"),
        }
        assert_eq!(fs::read(&corrupt).unwrap(), b"garbage");
        assert_eq!(fs::read(&repo).unwrap(), V0);
        assert_eq!(tickets(dir.path()), BTreeSet::new());
        assert_eq!(lockinfos(dir.path()), BTreeSet::new());
        assert_eq!(locks(dir.path()), BTreeSet::from([lock_name(&sha256_hex(V0), 0)]));
    }

    #[test]
    fn missing_target_directory_is_not_on_latest() {
        let (dir, _) = setup();
        let target = dir.path().join("missing").join("repo");
        assert_not_on_latest(&update(&target, b"v1", &NoHooks));
        assert_eq!(names(dir.path()), BTreeSet::from(["repo".to_string()]));
    }

    #[test]
    fn missing_target_is_not_on_latest() {
        let (dir, repo) = setup();
        fs::remove_file(&repo).unwrap();
        assert_not_on_latest(&update(&repo, b"v1", &NoHooks));
        assert!(!repo.exists());
        assert_eq!(tickets(dir.path()), BTreeSet::new());
        assert_eq!(lockinfos(dir.path()), BTreeSet::new());
    }

    #[test]
    fn failed_fence_unlink_is_an_error() {
        for errno in [libc::EIO, libc::EACCES] {
            let (dir, repo) = setup();
            let base = sha256_hex(V0);
            let holder = Uuid::new_v4();
            let holder_ticket = format!("repo.new_bytes.{holder}");
            let holder_lockinfo = format!("repo.lockinfo.{holder}");
            fs::write(dir.path().join(&holder_ticket), b"a").unwrap();
            fs::write(dir.path().join(&holder_lockinfo), holder.hyphenated().to_string())
                .unwrap();
            fs::hard_link(
                dir.path().join(&holder_lockinfo),
                dir.path().join(lock_name(&base, 0)),
            )
            .unwrap();

            let hooks = TestHooks {
                unlink: Some(Box::new(move |_| Err(io::Error::from_raw_os_error(errno)))),
                ..sees_locks_as_stale(dir.path())
            };
            match update(&repo, b"b", &hooks) {
                Err(CasError::Io(err)) => assert_eq!(err.raw_os_error(), Some(errno)),
                other => panic!("expected Io, got {other:?}"),
            }
            let reached = hooks.reached();
            assert!(reached.contains(&Point::LockTaken { generation: 1 }), "{reached:?}");
            assert!(!reached.contains(&Point::Fenced), "{reached:?}");
            assert!(!reached.contains(&Point::Renamed), "{reached:?}");
            assert_eq!(fs::read(&repo).unwrap(), V0);
            assert_eq!(tickets(dir.path()), BTreeSet::from([holder_ticket]));
            assert_eq!(lockinfos(dir.path()), BTreeSet::from([holder_lockinfo]));
            assert_eq!(
                locks(dir.path()),
                BTreeSet::from([lock_name(&base, 0), lock_name(&base, 1)])
            );
        }
    }

    #[test]
    fn invalid_base_hash_is_an_error() {
        let (dir, repo) = setup();
        for base in ["../x", "", &sha256_hex(V0).to_uppercase()] {
            let hooks = TestHooks::default();
            match conditional_update(&repo, b"v1", base, &OPTS, &hooks) {
                Err(CasError::Io(err)) => {
                    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
                }
                other => panic!("expected InvalidInput, got {other:?}"),
            }
            assert!(hooks.reached().is_empty());
        }
        assert_eq!(names(dir.path()), BTreeSet::from(["repo".to_string()]));
        assert_eq!(fs::read(&repo).unwrap(), V0);
    }
}
