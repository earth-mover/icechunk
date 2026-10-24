//! What `protocol_model.rs` and `layout_model.rs` share: the files of a
//! `repo.d` directory and what its listings show, and the steps of a change
//! attempt from staging to its report.

use std::collections::BTreeSet;

/// (actor, attempt). Also the hash of the bytes that attempt writes.
pub(crate) type AttemptId = (u8, u8);
/// `(N, h)`.
pub(crate) type Version = (u8, AttemptId);

pub(crate) const INITIAL_HASH: AttemptId = (u8::MAX, 0);

/// A file in `repo.d`, other than the marker and the mirror's stages.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Name {
    /// `<N>.<h>`: the bytes of version `N`.
    Content(u8, AttemptId),
    /// `stage.<id>`: a change attempt's staged bytes.
    Stage(AttemptId),
}

/// `latest.<N>.<h>`.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Marker {
    pub(crate) n: u8,
    pub(crate) hash: AttemptId,
}

impl Marker {
    pub(crate) fn version(self) -> Version {
        (self.n, self.hash)
    }

    pub(crate) fn file(self) -> Name {
        Name::Content(self.n, self.hash)
    }
}

/// `repo.d`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Dir {
    pub(crate) files: BTreeSet<Name>,
    pub(crate) marker: Marker,
    /// Every marker this directory had. What a stale listing may show, and
    /// what a new marker name must not repeat.
    pub(crate) past: BTreeSet<Marker>,
}

/// A `repo.d` whose only version is `(n, hash)`.
pub(crate) fn fresh_dir(n: u8, hash: AttemptId) -> Dir {
    let marker = Marker { n, hash };
    Dir { files: BTreeSet::from([marker.file()]), marker, past: BTreeSet::from([marker]) }
}

impl Dir {
    /// The markers a listing may show: with stale listings, any marker this
    /// directory had, then any of `earlier` (markers of earlier directories at
    /// this path); otherwise only the current one.
    pub(crate) fn listing(
        &self,
        stale_listings: bool,
        earlier: &BTreeSet<Marker>,
    ) -> Vec<Marker> {
        if stale_listings {
            self.past.iter().chain(earlier).copied().collect()
        } else {
            vec![self.marker]
        }
    }

    /// Moves the marker to `to`. Returns false if that name existed before.
    pub(crate) fn move_marker(&mut self, to: Marker) -> bool {
        self.marker = to;
        self.past.insert(to)
    }

    /// After a successful rename proved the marker is at `(n, hash)`: delete
    /// every version file below `n`, and every other one at `n`.
    pub(crate) fn tidy_up(&mut self, n: u8, hash: AttemptId) {
        self.files.retain(|name| match name {
            Name::Content(i, h) => !(*i < n || (*i == n && *h != hash)),
            Name::Stage(_) => true,
        });
    }
}

/// The nondeterministic outcome of a step of a change attempt, decided by the
/// filesystem. Each step accepts only the choices that apply to it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ChangeChoice {
    /// The operation runs and its reply arrives.
    Normal,
    /// The operation runs on the server, but its reply is lost.
    LostReply,
    /// The listing's marker is this one.
    Seen(Marker),
    /// The listing is torn and shows no marker. In the lost-reply check: its
    /// reads stay stale, or its search of the ops log ends before the attempt.
    SeenNone,
}

/// The steps of a change attempt after `read_latest`.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ChangeStep {
    /// Write and fsync the staging file.
    Stage,
    /// `link(stage, <N+1>.<h'>)`, fsync the directory, unlink the staging file.
    Link,
    /// `rename(latest.<N>.<h>, latest.<N+1>.<h'>)`: the change point.
    Rename,
    /// The rename failed with `ENOENT`, or its reply was lost: list and decide.
    AfterFailedRename,
    /// Superseded or unproven: read the current version and search its ops log
    /// for the attempt. Only in a model whose [`ChangeAttempt::landed`] says.
    LostReplyCheck,
    /// The rename succeeded: delete every dead version file, report success.
    TidyUp,
}

/// A writer's change attempt in a model's state.
pub(crate) trait ChangeAttempt {
    /// `repo.d`, unless it is gone.
    fn dir(&self) -> Option<&Dir>;
    fn dir_mut(&mut self) -> Option<&mut Dir>;
    /// The hash of the attempt's bytes, and the version it replaces.
    fn attempt(&self) -> (AttemptId, Version);
    /// A reply was lost in this attempt, so a spurious conflict is allowed.
    fn set_lost_reply(&mut self);
    fn set_step(&mut self, step: ChangeStep);
    /// The rename from `source` to `target` landed.
    fn change(&mut self, source: Marker, target: Marker);
    /// `Updated`: the attempt's version must be in the chain.
    fn report_success(&mut self);
    /// `NotOnLatestVersion`. Spurious only after a lost reply.
    fn report_conflict(&mut self);
    /// Whether the attempt's rename landed, for the lost-reply check. `None`
    /// leaves the check out: a superseded or unproven attempt reports a
    /// conflict.
    fn landed(&self) -> Option<bool> {
        None
    }
}

/// One atomic step of a change attempt. Returns `None` when `choice` doesn't
/// apply.
pub(crate) fn step_change(
    attempt: &mut impl ChangeAttempt,
    step: ChangeStep,
    choice: ChangeChoice,
) -> Option<()> {
    let (hash, (base_n, base_hash)) = attempt.attempt();
    let content = Name::Content(base_n + 1, hash);
    let reachable = attempt.dir().is_some();
    match (step, choice) {
        (ChangeStep::Stage, ChangeChoice::Normal) => {
            let Some(dir) = attempt.dir_mut() else {
                attempt.report_conflict();
                return Some(());
            };
            dir.files.insert(Name::Stage(hash));
            attempt.set_step(ChangeStep::Link);
        }
        (ChangeStep::Link, ChangeChoice::Normal | ChangeChoice::LostReply) => {
            let staged = Name::Stage(hash);
            let Some(dir) = attempt.dir_mut().filter(|dir| dir.files.contains(&staged))
            else {
                // ENOENT: the staging file was swept, or `repo.d` is gone.
                attempt.report_conflict();
                return Some(());
            };
            // Ok, or EEXIST for our own earlier link: the name is ours.
            dir.files.insert(content);
            if choice == ChangeChoice::LostReply {
                // The client retries the link from the Link step.
                attempt.set_lost_reply();
            } else {
                dir.files.remove(&staged);
                attempt.set_step(ChangeStep::Rename);
            }
        }
        (ChangeStep::Rename, ChangeChoice::Normal | ChangeChoice::LostReply) => {
            let source = Marker { n: base_n, hash: base_hash };
            if attempt.dir().map(|dir| dir.marker) == Some(source) {
                let target = Marker { n: base_n + 1, hash };
                attempt.change(source, target);
                if choice == ChangeChoice::LostReply {
                    attempt.set_lost_reply();
                    attempt.set_step(ChangeStep::AfterFailedRename);
                } else {
                    attempt.set_step(ChangeStep::TidyUp);
                }
            } else {
                // ENOENT. A lost reply of a failed rename is just a failure.
                if choice == ChangeChoice::LostReply {
                    return None;
                }
                attempt.set_step(ChangeStep::AfterFailedRename);
            }
        }
        // No marker to list: `repo.d` is gone.
        (ChangeStep::AfterFailedRename, ChangeChoice::Normal) => {
            if reachable {
                return None;
            }
            attempt.report_conflict();
        }
        (ChangeStep::AfterFailedRename, ChangeChoice::Seen(seen))
            if seen.n == base_n + 1 =>
        {
            if seen.hash == hash {
                // Our rename landed and the reply was lost.
                tidy_up(attempt, base_n + 1, hash);
                attempt.report_success();
            } else {
                // Lost the race for N+1: our file is dead.
                remove(attempt, content);
                attempt.report_conflict();
            }
        }
        // Superseded, whether or not our rename landed: our file is dead.
        (ChangeStep::AfterFailedRename, ChangeChoice::Seen(seen)) if seen.n > base_n => {
            remove(attempt, content);
            lost_reply_check(attempt);
        }
        // Unproven: the listing is stale or torn. The content file stays for
        // the sweep, unless the check finds the marker at it.
        (
            ChangeStep::AfterFailedRename,
            ChangeChoice::Seen(_) | ChangeChoice::SeenNone,
        ) => {
            lost_reply_check(attempt);
        }
        // The current version's ops log names the attempt if it landed. Its
        // version file goes only if the check read it as current. Without
        // `repo.d` there is no current version: a conflict.
        (ChangeStep::LostReplyCheck, ChangeChoice::Normal) => {
            if reachable && attempt.landed() == Some(true) {
                let ours = Marker { n: base_n + 1, hash };
                if attempt.dir().map(|dir| dir.marker) == Some(ours) {
                    tidy_up(attempt, base_n + 1, hash);
                }
                attempt.report_success();
            } else {
                attempt.report_conflict();
            }
        }
        // Spurious after a lost reply, correct otherwise.
        (ChangeStep::LostReplyCheck, ChangeChoice::SeenNone) => attempt.report_conflict(),
        (ChangeStep::TidyUp, ChangeChoice::Normal) => {
            tidy_up(attempt, base_n + 1, hash);
            attempt.report_success();
        }
        _ => return None,
    }
    Some(())
}

/// Runs the lost-reply check next, or reports a conflict where it isn't
/// modelled.
fn lost_reply_check(attempt: &mut impl ChangeAttempt) {
    if attempt.landed().is_some() {
        attempt.set_step(ChangeStep::LostReplyCheck);
    } else {
        attempt.report_conflict();
    }
}

fn remove(attempt: &mut impl ChangeAttempt, name: Name) {
    if let Some(dir) = attempt.dir_mut() {
        dir.files.remove(&name);
    }
}

fn tidy_up(attempt: &mut impl ChangeAttempt, n: u8, hash: AttemptId) {
    if let Some(dir) = attempt.dir_mut() {
        dir.tidy_up(n, hash);
    }
}
