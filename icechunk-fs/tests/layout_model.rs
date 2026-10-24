//! Exhaustive model of the operations of the filesystem layout, version 1, around
//! the protocol for changes of the repo info that `protocol_model.rs` checks: the mirror at
//! `$ROOT/repo`, the `ReadOnly` status and consistent copies, garbage collection,
//! creation, and the conversions between layouts.
//!
//! The conversions are administrative: one starts only when no other actor is in
//! the middle of an operation, and no other actor steps until it finishes. A
//! crashed conversion leaves the floor; it resumes by running again, in isolation
//! again. Between the crash and the rerun other actors may act, and the design
//! must stop them from doing harm (a writer that finds a current version without
//! the feature refuses). Changes, their publications and GC run concurrently.
//!
//! The filesystem follows the rules of `protocol_model.rs`: name operations happen
//! on the server against the true state, and a stale listing of `repo.d` may show
//! any marker that directory ever had. Listings of `$ROOT` are fresh (callers nudge
//! first) unless `stale_root_listings` is set. Reads of `$ROOT/repo` are fresh too,
//! though a client may serve them from its cache: doc 020 lists the false fork
//! reports and mirror regressions this hides as accepted limits.
//!
//! Bounds and abstractions: the conversion to the standard layout attempts its
//! change twice per run, the code up to nine times (see
//! [`Config::conversion_attempts`]). The provenance check's reach in changes
//! depends on the ops-log page size, so beyond one change the model offers both
//! outcomes (see [`PROVENANCE_CHANGES`]).
//! Leftover files of attempts and publications that ended are dropped, see
//! [`drop_garbage`].
//!
//! Not modelled:
//! - GC's sweep of `repo.d.tmp.*` directories and of `.staging.<id>` names in
//!   `$ROOT` (liveness only);
//! - a `$ROOT/repo` that can't be read or decoded, which every fork check
//!   reports instead of overwriting it;
//! - a change's own publication failing with an I/O error, which the code logs
//!   before returning `Updated`. So GC's change always installs its version
//!   before its hook runs, the hook's gate never decides `GC_PUBLISHED`, and the
//!   code's `GcPublishesListed` sabotage has no counterpart here;
//! - a stale root listing that still shows `repo.d` after the conversion to the
//!   standard layout renamed it aside, which makes the code fail with
//!   `Incomplete` (liveness only).
//!
//! Repo info contents are abstract ([`Content`]): what matters is whether they
//! list `filesystem-layout-v1`, their status, and which repo info they derive
//! from ([`State::parents`]). Backups in `overwritten/` are not files of the
//! model: a version records what its writer backed up as its parent.
//!
//! Changing the status through the mirror with an old library is unsupported. The
//! optional old writer measures what the best-effort fork detection at publication
//! catches: those are `sometimes` properties named `metric: …`, never asserted
//! generically, and the pass/fail properties don't apply once it wrote. Each test
//! asserts the ones it documents and prints the shortest trace of every one it
//! finds.

use stateright::{Checker, Expectation, Model, Path, Property};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

#[path = "common/model.rs"]
mod model;

use model::{
    AttemptId, ChangeAttempt, ChangeChoice, ChangeStep, Dir, INITIAL_HASH, Marker, Name,
    Version, fresh_dir, step_change,
};

/// First versions written by creator `i` are `(CREATOR + i, attempt)`. Their ops
/// log holds only `RepoInitializedUpdate`.
const CREATOR: u8 = 100;
/// Versions the conversion to the standard layout makes: the only ones
/// that don't list the feature.
const TO_STANDARD: u8 = 150;
const STATUS: u8 = 160;
const GC: u8 = 170;
const EXPLICIT: u8 = 180;
/// First versions built by the conversion from the standard layout.
const FROM_STANDARD: u8 = 200;
/// The provenance check reads three ops-log files. At the smallest page, one
/// update per file, they reach the conversion's backup while at most one change
/// landed on its first version; at the default page, thousands. Within this
/// many changes the model's check finds the backup; beyond, it finds it or
/// misses it ([`Choice::ProvenanceMissed`]), which covers every page size.
const PROVENANCE_CHANGES: u8 = 1;
/// Stage ids of the publishers other than writers, which use their index.
const CREATOR_STAGE: u8 = 30;
const FROM_STANDARD_STAGE: u8 = 40;

fn is_creation(hash: AttemptId) -> bool {
    (CREATOR..TO_STANDARD).contains(&hash.0)
}

fn featureless(hash: AttemptId) -> bool {
    hash.0 == TO_STANDARD
}

/// The nondeterministic outcome of an actor's step, decided by the
/// filesystem. Each step accepts only the choices that apply to it.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Choice {
    /// The operation runs and its reply arrives.
    Normal,
    /// The operation runs on the server, but its reply is lost.
    LostReply,
    /// The listing's marker is this one.
    Seen(Marker),
    /// The listing is torn and shows no marker; a root listing misses
    /// `repo.d`.
    SeenNone,
    /// GC's sweep from a listing showing this marker also deletes the staged
    /// files, which it may consider old.
    SweepStages(Marker),
    /// The listing's marker is this one, and the provenance check's three
    /// ops-log files end before the conversion's backup (a small page).
    ProvenanceMissed(Marker),
}

impl Choice {
    fn change(self) -> Option<ChangeChoice> {
        match self {
            Choice::Normal => Some(ChangeChoice::Normal),
            Choice::LostReply => Some(ChangeChoice::LostReply),
            Choice::Seen(marker) => Some(ChangeChoice::Seen(marker)),
            Choice::SeenNone => Some(ChangeChoice::SeenNone),
            Choice::SweepStages(_) | Choice::ProvenanceMissed(_) => None,
        }
    }
}

/// A repo info file: `$ROOT/repo`, or what a version derives from.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Content {
    /// The bytes of the version with this hash. They list the feature unless the
    /// conversion to the standard layout made them.
    Version(AttemptId),
    /// Written by a library that took `repo` for a standard repository (an old
    /// library's `set_status`) from the version it read. It doesn't list the
    /// feature.
    Fork { from: AttemptId },
    /// A standard repo info updated `updates` times by standard writes: from
    /// the original repository in the standard layout (`from: None`), or from
    /// the featureless version `from` that a conversion to the standard layout
    /// left (`updates >= 1`).
    Standard { from: Option<AttemptId>, updates: u8 },
}

const ORIGINAL: Content = Content::Standard { from: None, updates: 0 };

impl Content {
    fn lists_feature(self) -> bool {
        matches!(self, Content::Version(hash) if !featureless(hash))
    }

    /// A standard repo info that standard writes update: the original, or what
    /// a conversion to the standard layout left, updated or not.
    fn takes_standard_writes(self) -> bool {
        match self {
            Content::Standard { .. } => true,
            Content::Version(hash) => featureless(hash),
            Content::Fork { .. } => false,
        }
    }

    /// A standard write of it.
    fn updated(self) -> Content {
        match self {
            Content::Standard { from, updates } => {
                Content::Standard { from, updates: updates + 1 }
            }
            Content::Version(hash) => Content::Standard { from: Some(hash), updates: 1 },
            other => other,
        }
    }
}

/// A `repo.d.tmp.<id>/` directory.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Tmp {
    /// Built by a creation or the conversion from the standard layout, around
    /// this first version.
    Build(AttemptId),
    /// `repo.d`, renamed aside by the conversion to the standard layout.
    Aside,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum PublishStep {
    /// Take the target's bytes (the caller's, or `repo.d/<N>.<h>`) and write
    /// `repo.d/mirror.<id>`.
    Stage,
    /// Read `$ROOT/repo`: the best-effort fork detection, and whether it holds
    /// the target already.
    ForkCheck,
    /// `rename(repo.d/mirror.<id>, repo)`.
    Rename,
    /// List `repo.d`: publish a newer version, or again after installing nothing.
    Verify,
}

/// A publication of the mirror, run synchronously by its owner.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Publish {
    step: PublishStep,
    /// `T`. Never goes backwards.
    target: Version,
    round: u8,
    /// The version a server-decided step of the owner proved current: the owner
    /// has its bytes.
    proved: Option<Version>,
    /// This round renamed the target over `repo`, or found it there.
    installed: bool,
    /// The version this publication left in `repo`: `GaveUp { left }`.
    left: Option<Version>,
    /// A repo info that the fork check accepts besides versions: the bytes the
    /// conversion from the standard layout backed up.
    accept: Option<Content>,
    /// The step this publisher was at when the old writer's write landed after
    /// its fork check.
    fork_landed_at: Option<PublishStep>,
}

const IDLE_PUBLISH: Publish = Publish {
    step: PublishStep::Stage,
    target: (0, INITIAL_HASH),
    round: 0,
    proved: None,
    installed: false,
    left: None,
    accept: None,
    fork_landed_at: None,
};

fn publish_of(
    target: Version,
    proved: Option<Version>,
    accept: Option<Content>,
) -> Publish {
    Publish { target, proved, accept, ..IDLE_PUBLISH }
}

enum Published {
    Running,
    Current,
    /// The rounds ran out: `GaveUp { left }`.
    GaveUp,
    Fork,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Role {
    /// Makes `Online` changes; refuses a `ReadOnly` or featureless current
    /// version.
    Data,
    /// Makes its change, a `GCRanUpdate` (version `changed`), which publishes
    /// it; then its hook publishes `changed` again and goes on only if that
    /// left `changed` or a later version in `repo` (`left`); sweeps, deletes
    /// and publishes once more. `mirror` is the highest `N` that `repo` held
    /// since the change landed.
    Gc { changed: Option<Version>, left: Option<Version>, mirror: Option<u8> },
    /// Sets the status `ReadOnly` with a change, waits for the copier, then
    /// (`online`) sets it back to `Online`.
    Status { online: bool },
    /// The conversion to the standard layout (administrative).
    /// `relists`: lists again after its fork check found a newer version, in
    /// this run.
    ToStandard { restarts: u8, relists: u8 },
    /// `publish_mirror()`: publishes the version its listing shows. A probe
    /// proving the listed marker current changes nothing: the listing has no
    /// other effect, so a stale one is the same as a fresh one taken earlier.
    Explicit,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    /// Idle: `read_latest` and start an attempt, or finish. For the conversion,
    /// a (re)run deciding from the files where to start.
    Start,
    /// The conversion's decision again, within a run.
    Recover,
    /// A step of a change attempt.
    Change(ChangeStep),
    /// The publication after a change; the conversion's step 3 too.
    Publish,
    /// GC's hook before its deletes: publish `changed`, without its bytes.
    PublishBefore,
    /// GC: sweep `repo.d` from one listing.
    Sweep,
    /// GC: delete what the repo info no longer references.
    DeleteData,
    /// GC: list `repo.d` for the publication after deleting.
    ListAfter,
    PublishAfter,
    /// The status is `ReadOnly`: the copier copies.
    WaitCopy,
    /// The conversion to standard reads `$ROOT/repo` before its change.
    ForkCheck,
    /// `rename(repo.d, repo.d.tmp.<id>)`.
    RenameDir,
    DeleteTmp,
    /// A rerun that finds `repo.d` gone deletes leftover tmp directories.
    DeleteLeftovers,
    Stopped,
    Done,
    Crashed,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Writer {
    /// The first half of its attempts' hashes.
    id: u8,
    role: Role,
    phase: Phase,
    /// Attempts in this run (this status change, for the status setter).
    attempts: u8,
    /// Attempts ever: the second half of the hash.
    serial: u8,
    attempt: AttemptId,
    base: Version,
    lost_reply: bool,
    publish: Publish,
}

/// Copies `repo.d` while the status is `ReadOnly`: the marker from a listing,
/// then the version file it names.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Copier {
    Waiting,
    /// The listed marker, if its name still existed when copied.
    Listed(Option<Marker>),
    Copied {
        marker: Option<Marker>,
        bytes: bool,
    },
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum FromStandardPhase {
    Start,
    Recover,
    /// Back up `repo`'s bytes to `overwritten/<B>`.
    Backup,
    /// Build `repo.d.tmp.<id>/` with the first version and its marker.
    Build,
    /// `rename(repo.d.tmp.<id>, repo.d)`, exclusive.
    RenameDir,
    Publish,
    Done,
    Stopped,
    Crashed,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct FromStandard {
    phase: FromStandardPhase,
    restarts: u8,
    serial: u8,
    hash: AttemptId,
    backup: Option<Content>,
    publish: Publish,
    /// This rerun's provenance check found the backup more than
    /// [`PROVENANCE_CHANGES`] changes back: it publishes.
    deep_provenance: bool,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum CreatorPhase {
    /// List `$ROOT`, read `repo`: refuse, create, or replace an interrupted
    /// creation.
    PreCheck,
    Build,
    /// `link(repo.staging.<id>, repo)`, exclusive.
    Link,
    /// `rename(repo.staging.<id>, repo)`: replaces an interrupted creation.
    Replace,
    RenameDir,
    Publish,
    /// Lost the directory rename after placing `repo`: list for the current
    /// version, publish it, then report "exists".
    ListCurrent,
    PublishCurrent,
    Created,
    Refused,
    Crashed,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Creator {
    index: u8,
    phase: CreatorPhase,
    restarts: u8,
    serial: u8,
    hash: AttemptId,
    replace: bool,
    placed: bool,
    publish: Publish,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Eraser {
    Writer,
    Gc,
    Conversion,
    Creator,
}

/// A fork overwritten by a publication.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Erasure {
    by: Eraser,
    /// Where the old writer's write landed in the publisher's window.
    landed_at: Option<PublishStep>,
    /// Nobody had detected the fork.
    silent: bool,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Fork {
    detected: bool,
    erased: Option<Erasure>,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Violation {
    /// A false acknowledgement, a spurious conflict, a reused marker name: the
    /// change protocol's own invariants.
    Protocol,
    /// The repository changed to a repo info that doesn't derive from the one
    /// before.
    Lost,
    GcDeletedUnpublished,
    ReadOnlyChanged,
    DeletedBeforePublished,
    StandardOverwritten,
    EndState,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct State {
    /// `$ROOT/repo`.
    repo: Option<Content>,
    /// `repo.d`. A configuration has at most one live: it is built under a
    /// `repo.d.tmp.<id>` name, see [`State::tmps`].
    dir: Option<Dir>,
    /// `repo.d/mirror.<id>`: the publishers' staged copies, by stage id. A
    /// publisher has at most one.
    mirrors: BTreeSet<u8>,
    tmps: BTreeSet<Tmp>,
    /// What each landed version derives from: its base, or for a first version
    /// of the conversion from the standard layout, the bytes it backed up.
    parents: BTreeMap<AttemptId, Content>,
    /// Versions that became current, in order.
    history: Vec<AttemptId>,
    /// The markers of earlier `repo.d` directories at this path, which a stale
    /// listing may still show.
    old_markers: BTreeSet<Marker>,
    /// Versions whose status is `ReadOnly`.
    readonly: BTreeSet<AttemptId>,
    acked: BTreeSet<Version>,
    writers: Vec<Writer>,
    copier: Option<Copier>,
    from_standard: Option<FromStandard>,
    creators: Vec<Creator>,
    old_writes: u8,
    standard_writes: u8,
    fork: Option<Fork>,
    gc_deleted_during_fork: bool,
    changed_during_fork: bool,
    /// A check found a lagging mirror whose version file is gone: a detection
    /// that only compares hashes with `repo.d` would call it a fork.
    hash_only_false_alarm: bool,
    false_fork_report: bool,
    /// A fork report for the standard repo info of an interrupted conversion
    /// from the standard layout.
    fork_report_interrupted: bool,
    /// A fork check found a standard write made outside the rules.
    divergence_detected: bool,
    /// A rerun of the conversion from the standard layout stopped at its
    /// provenance check.
    provenance_stopped: bool,
    /// The end-state violation is the accepted limit of a rerun of the
    /// conversion to the standard layout whose root listing missed `repo.d`.
    stale_root_end: bool,
    /// The conversion to standard's fork check found a newer version's bytes
    /// in `repo`: its listing was stale.
    relisted_after_fork_check: bool,
    /// The conversion to standard stopped after `relist_bound` relistings.
    relists_exhausted: bool,
    /// A fork check whose listing of `repo.d` was stale reported a false fork.
    false_fork_stale: bool,
    /// The conversion to the standard layout changed the repo info, then met a fork.
    stuck_on_fork: bool,
    /// While GC deleted, `repo` held a version older than the one GC's
    /// publication left there.
    gc_mirror_regressed: bool,
    /// `publish_mirror()` renamed a version older than the one in `repo`.
    explicit_regressed: bool,
    /// `N` of the newest version a GC publication left in `repo`.
    gc_published_n: Option<u8>,
    violation: Option<(Violation, String)>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Actor {
    Writer(usize),
    Copier,
    FromStandard,
    Creator(usize),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Owner {
    Writer(usize),
    Creator(usize),
    FromStandard,
}

#[derive(Clone, Debug, PartialEq)]
enum Action {
    Step(Actor, Choice),
    Crash(Actor),
    /// An old library's `set_status` through `$ROOT/repo`: unsupported.
    OldWrite,
    /// An update of a standard `$ROOT/repo` by any library.
    StandardWrite,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum StatusOps {
    ReadOnly,
    ReadOnlyThenOnline,
}

/// The code's sabotages that the model mirrors: each breaks one rule.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Sabotage {
    /// A change returns without publishing the mirror.
    CommitWithoutPublish,
    /// The conversion to the standard layout renames `repo.d` aside before
    /// publishing its version.
    DeleteDirBeforePublish,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    writers: u8,
    max_attempts: u8,
    /// How many rounds a publication runs before giving up; 0 for no limit.
    publish_rounds: u8,
    gc: bool,
    status: Option<StatusOps>,
    copier: bool,
    to_standard: bool,
    /// Attempts at its change per run of the conversion to the standard
    /// layout: the first, and one after each conflict, which a stale listing
    /// causes. The code makes up to `RELISTS + 1` (9). The retry loop is
    /// uniform: each attempt starts from a new `read_latest`, fork check and
    /// change, and carries nothing from the one before but the counter and its
    /// leftovers, which [`drop_garbage`] drops. So a second attempt reaches every
    /// interleaving a later one does, and exhaustion stops the same way at any
    /// bound: 2 is the model's bound.
    conversion_attempts: u8,
    from_standard: bool,
    /// A rerun of the conversion from the standard layout publishes only over
    /// the bytes its first version backed up.
    provenance_check: bool,
    /// A publication whose fork check finds the bytes of a version newer than
    /// its target in `repo` adopts it instead of renaming an older one over it.
    mirror_never_backwards: bool,
    /// How many times the conversion to standard lists again after its fork
    /// check found a newer version, in one run, before it stops with a
    /// retryable error.
    relist_bound: u8,
    /// Every `repo.d` starts at `N = 0` instead of above the `N` of the
    /// earlier ones.
    first_n_zero: bool,
    creators: u8,
    /// `publish_mirror()` runs once, at any time.
    explicit_publish: bool,
    old_writes: u8,
    standard_writes: u8,
    crashes: bool,
    /// How many times a crashed conversion or creation runs again.
    max_restarts: u8,
    lost_replies: bool,
    stale_listings: bool,
    stale_root_listings: bool,
    sabotage: Option<Sabotage>,
}

const NOTHING: Config = Config {
    writers: 0,
    max_attempts: 1,
    publish_rounds: 0,
    gc: false,
    status: None,
    copier: false,
    to_standard: false,
    conversion_attempts: 2,
    from_standard: false,
    provenance_check: true,
    first_n_zero: false,
    relist_bound: 8,
    mirror_never_backwards: true,
    creators: 0,
    explicit_publish: false,
    old_writes: 0,
    standard_writes: 0,
    crashes: false,
    max_restarts: 1,
    lost_replies: false,
    stale_listings: false,
    stale_root_listings: false,
    sabotage: None,
};

struct LayoutModel {
    config: Config,
}

fn violate(state: &mut State, violation: Violation, detail: String) {
    if state.violation.is_none() {
        state.violation = Some((violation, detail));
    }
}

impl State {
    fn marker(&self) -> Option<Marker> {
        self.dir.as_ref().map(|dir| dir.marker)
    }

    /// Whether `repo.d/<name>` resolves.
    fn reachable(&self) -> bool {
        self.dir.is_some()
    }

    fn has_file(&self, name: Name) -> bool {
        self.dir.as_ref().is_some_and(|dir| dir.files.contains(&name))
    }

    /// `N` of a version file `<N>.<hash>` in `repo.d`: a `$ROOT/repo` with these
    /// bytes is a mirror, not a fork.
    fn version_file_n(&self, hash: AttemptId) -> Option<u8> {
        let dir = self.dir.as_ref()?;
        dir.files.iter().find_map(|name| match name {
            Name::Content(n, h) if *h == hash => Some(*n),
            _ => None,
        })
    }

    fn lacks_feature(&self) -> bool {
        self.repo.is_some_and(|repo| !repo.lists_feature())
    }

    /// The repository's state of record: the current version while `repo.d`
    /// has a marker, otherwise `$ROOT/repo`, unless that is a creation's first
    /// version without `repo.d` (opening reports `NotFound`).
    fn truth(&self) -> Option<Content> {
        if let Some(marker) = self.marker() {
            return Some(Content::Version(marker.hash));
        }
        match self.repo {
            Some(Content::Version(hash)) if is_creation(hash) && self.dir.is_none() => {
                None
            }
            repo => repo,
        }
    }

    /// The repo info that `content` was made from.
    fn parent_of(&self, content: Content) -> Option<Content> {
        match content {
            Content::Version(hash) => self.parents.get(&hash).copied(),
            Content::Fork { from } => Some(Content::Version(from)),
            Content::Standard { from: Some(hash), updates: 1 } => {
                Some(Content::Version(hash))
            }
            Content::Standard { from, updates } => {
                Some(Content::Standard { from, updates: updates.checked_sub(1)? })
            }
        }
    }

    /// Whether version `hash` derives from `content`, or is it.
    fn derives(&self, hash: AttemptId, content: Content) -> bool {
        self.content_derives(Content::Version(hash), content)
    }

    /// Whether `from` derives from `content`, or is it.
    fn content_derives(&self, from: Content, content: Content) -> bool {
        let mut current = Some(from);
        while let Some(this) = current {
            if this == content {
                return true;
            }
            current = self.parent_of(this);
        }
        false
    }

    /// How many changes version `hash` is past the first version of a
    /// conversion from the standard layout that backed up `repo`, if it derives
    /// from one: how far the provenance check must read.
    fn changes_since_backup(&self, hash: AttemptId, repo: Content) -> Option<u8> {
        let (mut hash, mut changes) = (hash, 0);
        loop {
            let parent = *self.parents.get(&hash)?;
            if hash.0 == FROM_STANDARD {
                return (parent == repo).then_some(changes);
            }
            let Content::Version(next) = parent else { return None };
            (hash, changes) = (next, changes + 1);
        }
    }

    /// What the conversion from the standard layout backed up for the first
    /// version that `hash` derives from.
    fn origin(&self, hash: AttemptId) -> Option<Content> {
        let mut hash = hash;
        loop {
            let parent = *self.parents.get(&hash)?;
            if hash.0 == FROM_STANDARD {
                return Some(parent);
            }
            let Content::Version(next) = parent else { return None };
            hash = next;
        }
    }

    fn publish(&self, owner: Owner) -> Publish {
        match owner {
            Owner::Writer(index) => self.writers[index].publish,
            Owner::Creator(index) => self.creators[index].publish,
            Owner::FromStandard => self.from_standard.map_or(IDLE_PUBLISH, |c| c.publish),
        }
    }

    fn set_publish(&mut self, owner: Owner, publish: Publish) {
        match owner {
            Owner::Writer(index) => self.writers[index].publish = publish,
            Owner::Creator(index) => self.creators[index].publish = publish,
            Owner::FromStandard => {
                if let Some(conv) = &mut self.from_standard {
                    conv.publish = publish;
                }
            }
        }
    }

    /// The publishers in the middle of a publication.
    fn publishing(&self) -> Vec<Owner> {
        let writers = (0..self.writers.len())
            .filter(|i| {
                matches!(
                    self.writers[*i].phase,
                    Phase::Publish | Phase::PublishBefore | Phase::PublishAfter
                )
            })
            .map(Owner::Writer);
        let creators = (0..self.creators.len())
            .filter(|i| {
                matches!(
                    self.creators[*i].phase,
                    CreatorPhase::Publish | CreatorPhase::PublishCurrent
                )
            })
            .map(Owner::Creator);
        let conversion = self
            .from_standard
            .filter(|conv| conv.phase == FromStandardPhase::Publish)
            .map(|_| Owner::FromStandard);
        writers.chain(creators).chain(conversion).collect()
    }

    fn stage_id(&self, owner: Owner) -> u8 {
        match owner {
            Owner::Writer(index) => index as u8,
            Owner::Creator(index) => CREATOR_STAGE + index as u8,
            Owner::FromStandard => FROM_STANDARD_STAGE,
        }
    }

    fn eraser(&self, owner: Owner) -> Eraser {
        match owner {
            Owner::Writer(index) => match self.writers[index].role {
                Role::Data | Role::Status { .. } | Role::Explicit => Eraser::Writer,
                Role::Gc { .. } => Eraser::Gc,
                Role::ToStandard { .. } => Eraser::Conversion,
            },
            Owner::Creator(_) => Eraser::Creator,
            Owner::FromStandard => Eraser::Conversion,
        }
    }

    fn status_waiting(&self) -> bool {
        self.writers.iter().any(|w| w.phase == Phase::WaitCopy)
    }

    /// A conversion is running: nobody else may step.
    fn admin_running(&self) -> bool {
        use FromStandardPhase as F;
        self.writers.iter().any(|w| {
            matches!(w.role, Role::ToStandard { .. })
                && !matches!(
                    w.phase,
                    Phase::Start | Phase::Done | Phase::Stopped | Phase::Crashed
                )
        }) || self.from_standard.is_some_and(|conv| {
            !matches!(conv.phase, F::Start | F::Done | F::Stopped | F::Crashed)
        })
    }

    /// No actor other than the conversions is in the middle of an operation: a
    /// conversion may start.
    fn others_idle(&self) -> bool {
        self.writers.iter().all(|w| {
            matches!(w.role, Role::ToStandard { .. })
                || matches!(
                    w.phase,
                    Phase::Start | Phase::Done | Phase::Crashed | Phase::WaitCopy
                )
        }) && !matches!(self.copier, Some(Copier::Listed(_)))
            && self.creators.iter().all(|c| {
                matches!(
                    c.phase,
                    CreatorPhase::PreCheck
                        | CreatorPhase::Created
                        | CreatorPhase::Refused
                        | CreatorPhase::Crashed
                )
            })
    }

    /// Every actor has finished, so only the old writer may still act.
    fn finished(&self) -> bool {
        self.writers
            .iter()
            .all(|w| matches!(w.phase, Phase::Done | Phase::Stopped | Phase::Crashed))
            && self.from_standard.is_none_or(|conv| {
                matches!(
                    conv.phase,
                    FromStandardPhase::Done
                        | FromStandardPhase::Stopped
                        | FromStandardPhase::Crashed
                )
            })
            && self.creators.iter().all(|c| {
                matches!(
                    c.phase,
                    CreatorPhase::Created | CreatorPhase::Refused | CreatorPhase::Crashed
                )
            })
    }

    /// `N` of the version in `repo`.
    fn mirror_n(&self) -> Option<u8> {
        let Some(Content::Version(hash)) = self.repo else { return None };
        let dir = self.dir.as_ref()?;
        dir.past.iter().filter(|m| m.hash == hash).map(|m| m.n).max()
    }

    /// The phases of the actors, for traces.
    fn summary(&self) -> String {
        let publish = |phase: String, publish: Publish, active: bool| {
            if active {
                format!(
                    "{phase}/{:?}#{}({:?})",
                    publish.step, publish.round, publish.target
                )
            } else {
                phase
            }
        };
        let mut parts: Vec<String> = self
            .writers
            .iter()
            .map(|w| {
                let active = matches!(
                    w.phase,
                    Phase::Publish | Phase::PublishBefore | Phase::PublishAfter
                );
                publish(format!("w{}:{:?}", w.id, w.phase), w.publish, active)
            })
            .collect();
        if let Some(c) = self.copier {
            parts.push(format!("copier:{c:?}"));
        }
        if let Some(c) = self.from_standard {
            let active = c.phase == FromStandardPhase::Publish;
            parts.push(publish(format!("from:{:?}", c.phase), c.publish, active));
        }
        parts.extend(self.creators.iter().map(|c| {
            let active =
                matches!(c.phase, CreatorPhase::Publish | CreatorPhase::PublishCurrent);
            publish(format!("c{}:{:?}", c.index, c.phase), c.publish, active)
        }));
        if let Some(dir) = &self.dir {
            let files = dir.files.iter().map(|f| format!("{f:?}"));
            let mirrors = self.mirrors.iter().map(|id| format!("Mirror({id})"));
            parts.push(format!(
                "repo.d[{}]",
                files.chain(mirrors).collect::<Vec<_>>().join(" ")
            ));
        }
        if !self.tmps.is_empty() {
            parts.push(format!("tmps {:?}", self.tmps));
        }
        format!("  | {}", parts.join(" "))
    }
}

/// A fork check found in `repo` a repo info without the feature that is not one
/// of the versions it knows. That is a fork when the old library wrote it; a
/// divergence when a standard write updated a standard repo info before the rerun
/// of an interrupted conversion (outside the rules); and expected when it is the
/// standard repo info of a conversion from the standard layout interrupted before
/// it published, met before the rerun (outside the rules). Anything else is a
/// false report: an accepted one when the check's own listing of `repo.d` was
/// stale and missed the version file with `repo`'s bytes.
fn detect_fork(state: &mut State, stale_listing: bool) {
    let interrupted = state.marker().and_then(|m| state.origin(m.hash));
    match (state.repo, &mut state.fork) {
        (Some(Content::Fork { .. }), Some(fork)) => fork.detected = true,
        (Some(repo), _) if interrupted == Some(repo) => {
            state.fork_report_interrupted = true;
        }
        (Some(Content::Standard { .. }), _) => state.divergence_detected = true,
        _ if stale_listing => state.false_fork_stale = true,
        _ => state.false_fork_report = true,
    }
}

/// Replaces `$ROOT/repo` with the bytes of version `new`: a publication, or a
/// creation placing its first version.
fn overwrite_repo(
    state: &mut State,
    new: AttemptId,
    by: Eraser,
    landed_at: Option<PublishStep>,
) {
    match state.repo {
        Some(Content::Fork { .. }) => {
            if let Some(fork) = &mut state.fork
                && fork.erased.is_none()
            {
                fork.erased = Some(Erasure { by, landed_at, silent: !fork.detected });
            }
        }
        Some(old) if !old.lists_feature() && !state.derives(new, old) => violate(
            state,
            Violation::StandardOverwritten,
            format!("{old:?} replaced by version {new:?}, which doesn't derive from it"),
        ),
        _ => {}
    }
    state.repo = Some(Content::Version(new));
}

/// GC is about to delete: its change and the publication of it come first.
fn gc_deletes(state: &mut State, index: usize) {
    state.gc_deleted_during_fork |= matches!(state.repo, Some(Content::Fork { .. }));
    let Role::Gc { changed, left, mirror } = state.writers[index].role else {
        unreachable!("only GC deletes");
    };
    match (changed, mirror) {
        (Some(changed), Some(mirror)) if mirror >= changed.0 => {
            state.gc_mirror_regressed |=
                state.mirror_n().is_some_and(|n| left.is_some_and(|l| n < l.0));
        }
        _ => violate(
            state,
            Violation::GcDeletedUnpublished,
            format!("GC deleted after change {changed:?}, repo held at most {mirror:?}"),
        ),
    }
}

impl Model for LayoutModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let config = self.config;
        let writer = |id, role| Writer {
            id,
            role,
            phase: Phase::Start,
            attempts: 0,
            serial: 0,
            attempt: INITIAL_HASH,
            base: (0, INITIAL_HASH),
            lost_reply: false,
            publish: IDLE_PUBLISH,
        };
        let mut writers: Vec<Writer> =
            (0..config.writers).map(|id| writer(id, Role::Data)).collect();
        if config.gc {
            writers
                .push(writer(GC, Role::Gc { changed: None, left: None, mirror: None }));
        }
        if config.status.is_some() {
            writers.push(writer(STATUS, Role::Status { online: false }));
        }
        if config.to_standard {
            writers
                .push(writer(TO_STANDARD, Role::ToStandard { restarts: 0, relists: 0 }));
        }
        if config.explicit_publish {
            writers.push(writer(EXPLICIT, Role::Explicit));
        }
        let creators = (0..config.creators)
            .map(|index| Creator {
                index,
                phase: CreatorPhase::PreCheck,
                restarts: 0,
                serial: 0,
                hash: INITIAL_HASH,
                replace: false,
                placed: false,
                publish: IDLE_PUBLISH,
            })
            .collect();
        let base = State {
            repo: None,
            dir: None,
            mirrors: BTreeSet::new(),
            tmps: BTreeSet::new(),
            parents: BTreeMap::new(),
            history: vec![],
            old_markers: BTreeSet::new(),
            readonly: BTreeSet::new(),
            acked: BTreeSet::new(),
            writers,
            copier: config.copier.then_some(Copier::Waiting),
            from_standard: config.from_standard.then_some(FromStandard {
                phase: FromStandardPhase::Start,
                restarts: 0,
                serial: 0,
                hash: INITIAL_HASH,
                backup: None,
                publish: IDLE_PUBLISH,
                deep_provenance: false,
            }),
            creators,
            old_writes: config.old_writes,
            standard_writes: config.standard_writes,
            fork: None,
            gc_deleted_during_fork: false,
            changed_during_fork: false,
            hash_only_false_alarm: false,
            false_fork_report: false,
            fork_report_interrupted: false,
            divergence_detected: false,
            provenance_stopped: false,
            stale_root_end: false,
            relisted_after_fork_check: false,
            relists_exhausted: false,
            false_fork_stale: false,
            stuck_on_fork: false,
            gc_mirror_regressed: false,
            explicit_regressed: false,
            gc_published_n: None,
            violation: None,
        };
        if config.from_standard && !config.to_standard {
            return vec![State { repo: Some(ORIGINAL), ..base }];
        }
        if config.creators > 0 {
            return vec![base];
        }
        vec![State {
            repo: Some(Content::Version(INITIAL_HASH)),
            dir: Some(fresh_dir(0, INITIAL_HASH)),
            history: vec![INITIAL_HASH],
            ..base
        }]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        if state.violation.is_some() {
            return;
        }
        let config = self.config;
        let admin = state.admin_running();
        let admin_may_start = !admin && state.others_idle();
        let mut alive = vec![];

        for (index, writer) in state.writers.iter().enumerate() {
            let actor = Actor::Writer(index);
            let conversion = matches!(writer.role, Role::ToStandard { .. });
            let running = !matches!(
                writer.phase,
                Phase::Start | Phase::Done | Phase::Stopped | Phase::Crashed
            );
            if (admin && !(conversion && running))
                || matches!(writer.phase, Phase::Done | Phase::Stopped | Phase::Crashed)
            {
                continue;
            }
            match writer.phase {
                Phase::Start if conversion => {
                    if !admin_may_start {
                        continue;
                    }
                    self.push_seen(state, actor, actions);
                    // The root listing misses `repo.d`.
                    if config.stale_root_listings && state.reachable() {
                        actions.push(Action::Step(actor, Choice::SeenNone));
                    }
                }
                Phase::Start => {
                    self.push_normal(actor, actions);
                    if writer.attempts < self.budget(writer.role) && state.reachable() {
                        for marker in self.listing(state) {
                            actions.push(Action::Step(actor, Choice::Seen(marker)));
                        }
                    }
                }
                Phase::Recover | Phase::ListAfter => {
                    self.push_seen(state, actor, actions);
                }
                Phase::Change(ChangeStep::Stage | ChangeStep::TidyUp)
                | Phase::DeleteData
                | Phase::DeleteTmp
                | Phase::DeleteLeftovers => self.push_normal(actor, actions),
                Phase::ForkCheck => self.push_fork_check(state, actor, actions),
                Phase::Change(ChangeStep::Link | ChangeStep::Rename)
                | Phase::RenameDir => {
                    self.push_lost_reply(actor, actions);
                }
                Phase::Change(ChangeStep::AfterFailedRename) => {
                    self.push_seen(state, actor, actions);
                    if state.reachable() && config.stale_listings {
                        actions.push(Action::Step(actor, Choice::SeenNone));
                    }
                }
                // `SeenNone`: the spurious conflict.
                Phase::Change(ChangeStep::LostReplyCheck) => {
                    self.push_normal(actor, actions);
                    actions.push(Action::Step(actor, Choice::SeenNone));
                }
                Phase::Publish | Phase::PublishBefore | Phase::PublishAfter => {
                    self.push_publish(state, actor, writer.publish, actions);
                }
                Phase::Sweep => {
                    self.push_seen(state, actor, actions);
                    for marker in self.listing(state) {
                        actions.push(Action::Step(actor, Choice::SweepStages(marker)));
                    }
                }
                Phase::WaitCopy => {
                    if !matches!(state.copier, Some(Copier::Waiting | Copier::Listed(_)))
                    {
                        self.push_normal(actor, actions);
                    }
                }
                Phase::Done | Phase::Stopped | Phase::Crashed => {}
            }
            if !conversion || running {
                alive.push(actor);
            }
        }

        if let Some(copier) = state.copier
            && !admin
            && state.status_waiting()
        {
            match copier {
                Copier::Waiting => self.push_seen(state, Actor::Copier, actions),
                Copier::Listed(_) => self.push_normal(Actor::Copier, actions),
                Copier::Copied { .. } => {}
            }
        }

        if let Some(conv) = state.from_standard {
            use FromStandardPhase as P;
            let actor = Actor::FromStandard;
            match conv.phase {
                P::Start | P::Recover if conv.phase == P::Recover || admin_may_start => {
                    self.push_seen(state, actor, actions);
                    if let Some(repo) = state.repo.filter(|repo| !repo.lists_feature())
                        && config.provenance_check
                    {
                        let missed = self.listing(state).into_iter().filter(|m| {
                            !featureless(m.hash)
                                && state
                                    .changes_since_backup(m.hash, repo)
                                    .is_some_and(|changes| changes > PROVENANCE_CHANGES)
                        });
                        actions.extend(
                            missed.map(|m| {
                                Action::Step(actor, Choice::ProvenanceMissed(m))
                            }),
                        );
                    }
                }
                P::Backup | P::Build => self.push_normal(actor, actions),
                P::RenameDir => self.push_lost_reply(actor, actions),
                P::Publish => self.push_publish(state, actor, conv.publish, actions),
                _ => {}
            }
            if !matches!(conv.phase, P::Start | P::Done | P::Stopped | P::Crashed) {
                alive.push(actor);
            }
        }

        for (index, creator) in state.creators.iter().enumerate() {
            use CreatorPhase as P;
            let actor = Actor::Creator(index);
            if admin {
                continue;
            }
            match creator.phase {
                P::PreCheck => {
                    self.push_normal(actor, actions);
                    if config.stale_root_listings {
                        actions.push(Action::Step(actor, Choice::SeenNone));
                    }
                }
                // Lost replies on the link, the replacing rename and the
                // directory rename are resolved by reading `repo` or listing
                // `repo.d`, which show our bytes or marker: the same outcome.
                P::Build | P::Link | P::Replace | P::RenameDir => {
                    self.push_normal(actor, actions);
                }
                P::ListCurrent => self.push_seen(state, actor, actions),
                P::Publish | P::PublishCurrent => {
                    self.push_publish(state, actor, creator.publish, actions);
                }
                P::Created | P::Refused | P::Crashed => continue,
            }
            alive.push(actor);
        }

        if state.old_writes > 0
            && !admin
            && state.reachable()
            && matches!(state.repo, Some(Content::Version(_)))
        {
            actions.push(Action::OldWrite);
        }
        if state.standard_writes > 0
            && !admin
            && state.repo.is_some_and(Content::takes_standard_writes)
        {
            actions.push(Action::StandardWrite);
        }

        if config.crashes {
            actions.extend(alive.into_iter().map(Action::Crash));
        }
    }

    fn next_state(&self, last: &State, action: Action) -> Option<State> {
        let mut state = last.clone();
        let missed = matches!(action, Action::Step(_, Choice::ProvenanceMissed(_)));
        match action {
            Action::Step(actor, choice) => match actor {
                Actor::Writer(index) => self.step_writer(&mut state, index, choice)?,
                Actor::Copier => step_copier(&mut state, choice)?,
                Actor::FromStandard => self.step_from_standard(&mut state, choice)?,
                Actor::Creator(index) => self.step_creator(&mut state, index, choice)?,
            },
            Action::Crash(actor) => self.crash(&mut state, actor),
            Action::OldWrite => old_write(&mut state),
            Action::StandardWrite => {
                state.standard_writes -= 1;
                state.repo = state.repo.map(Content::updated);
            }
        }
        check_forward(last, &mut state);
        self.check_stop(last, &mut state, missed);
        watch_gc_mirror(&mut state);
        normalize(&mut state);
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let config = self.config;
        let mut properties = vec![
            Property::always("the change protocol holds", |_, s: &State| {
                clean(s, Violation::Protocol)
            }),
            Property::always(MARKER_BYTES, |_, state: &State| {
                state
                    .dir
                    .as_ref()
                    .is_none_or(|dir| dir.files.contains(&dir.marker.file()))
            }),
            Property::always(NO_ACK_LOST, |_, s: &State| clean(s, Violation::Lost)),
            Property::always(MIRROR_CURRENT, |_, state: &State| {
                mirror_was_current(state)
            }),
            Property::always(GC_PUBLISHED, |_, s: &State| {
                clean(s, Violation::GcDeletedUnpublished)
            }),
            Property::always(READ_ONLY, |_, s: &State| {
                clean(s, Violation::ReadOnlyChanged)
            }),
            Property::always(COPY_CONSISTENT, |_, state: &State| match state.copier {
                Some(Copier::Copied { marker: Some(marker), bytes }) => {
                    bytes && state.readonly.contains(&marker.hash)
                }
                _ => true,
            }),
            Property::always(DELETED_PUBLISHED, |_, s: &State| {
                clean(s, Violation::DeletedBeforePublished)
            }),
            Property::always(STANDARD_KEPT, |_, s: &State| {
                clean(s, Violation::StandardOverwritten)
            }),
            Property::always(SAME_END_STATE, |_, s: &State| {
                clean(s, Violation::EndState)
            }),
            Property::always(ONLY_STALE_ROOT_END, |_, s: &State| {
                clean(s, Violation::EndState) || s.stale_root_end
            }),
            // Metrics of the best-effort fork detection.
            Property::sometimes(ERASED_BY_WRITER, |_, state: &State| {
                erased_silently_by(state, Eraser::Writer)
            }),
            Property::sometimes(ERASED_BY_GC, |_, state: &State| {
                erased_silently_by(state, Eraser::Gc)
            }),
            Property::sometimes(DETECTED, |_, state: &State| {
                state.fork.is_some_and(|fork| fork.detected)
            }),
            Property::sometimes(UNDETECTED, |_, state: &State| {
                state.finished()
                    && state
                        .fork
                        .is_some_and(|fork| !fork.detected && fork.erased.is_none())
            }),
            Property::sometimes(CHANGE_DURING_FORK, |_, state: &State| {
                state.changed_during_fork
            }),
            Property::sometimes(GC_DELETES_DURING_FORK, |_, state: &State| {
                state.gc_deleted_during_fork
            }),
            Property::sometimes(FORK_REPORT_INTERRUPTED, |_, state: &State| {
                state.fork_report_interrupted
            }),
            Property::sometimes(DIVERGENCE_DETECTED, |_, state: &State| {
                state.divergence_detected
            }),
            Property::sometimes(PROVENANCE_STOPPED, |_, state: &State| {
                state.provenance_stopped
            }),
            Property::sometimes(FALSE_FORK_STALE, |_, state: &State| {
                state.false_fork_stale
            }),
            Property::sometimes(RELISTS_EXHAUSTED, |_, state: &State| {
                state.relists_exhausted
            }),
            Property::sometimes(FROM_DONE_AFTER_REFUSAL, |_, state: &State| {
                state.fork_report_interrupted
                    && state
                        .from_standard
                        .is_some_and(|conv| conv.phase == FromStandardPhase::Done)
            }),
            Property::sometimes(STUCK_ON_FORK, |_, state: &State| state.stuck_on_fork),
            Property::sometimes(HASH_ONLY_FALSE_ALARM, |_, state: &State| {
                state.hash_only_false_alarm
            }),
            // Metrics of the mirror around GC.
            Property::sometimes(GC_MIRROR_REGRESSED, |_, state: &State| {
                state.gc_mirror_regressed
            }),
            Property::sometimes(GC_LEFT_REGRESSED, |_, state: &State| {
                state.finished()
                    && state.fork.is_none()
                    && state.mirror_n().is_some_and(|n| Some(n) < state.gc_published_n)
            }),
            Property::sometimes(EXPLICIT_REGRESSED, |_, state: &State| {
                state.explicit_regressed
            }),
            Property::sometimes(COPY_MISSED_MARKER, |_, state: &State| {
                matches!(state.copier, Some(Copier::Copied { marker: None, .. }))
            }),
            Property::sometimes(CREATOR_PUBLISHES_FEATURELESS, |_, state: &State| {
                state.creators.iter().any(|c| {
                    c.phase == CreatorPhase::PublishCurrent
                        && featureless(c.publish.target.1)
                })
            }),
        ];
        // Reachability of the steps the properties above judge: each config
        // asserts the ones it must reach.
        properties.extend([
            Property::sometimes(REACH_TO_STANDARD_DONE, |_, state: &State| {
                state.dir.is_none()
                    && state.writers.iter().any(|w| {
                        matches!(w.role, Role::ToStandard { .. })
                            && w.phase == Phase::Done
                    })
            }),
            Property::sometimes(REACH_TO_STANDARD_STEP_3, |_, state: &State| {
                state.writers.iter().any(|w| {
                    matches!(w.role, Role::ToStandard { .. })
                        && w.phase == Phase::Publish
                        && w.publish.proved.is_none()
                })
            }),
            Property::sometimes(REACH_TO_STANDARD_AFTER_RENAME, |_, state: &State| {
                state.dir.is_none()
                    && state.tmps.contains(&Tmp::Aside)
                    && state.writers.iter().any(|w| {
                        matches!(w.role, Role::ToStandard { .. })
                            && w.phase == Phase::Start
                    })
            }),
            Property::sometimes(REACH_TO_STANDARD_RERUN_RENAMES, |_, state: &State| {
                state.writers.iter().any(|w| {
                    matches!(w.role, Role::ToStandard { restarts, .. } if restarts > 0)
                        && w.phase == Phase::RenameDir
                        && w.attempts == 0
                })
            }),
            Property::sometimes(REACH_TO_STANDARD_LEFTOVERS, |_, state: &State| {
                state.writers.iter().any(|w| w.phase == Phase::DeleteLeftovers)
            }),
            Property::sometimes(REACH_TO_STANDARD_RELIST, |_, state: &State| {
                state.relisted_after_fork_check
            }),
            Property::sometimes(REACH_FROM_STANDARD_RERUN, |_, state: &State| {
                state.from_standard.is_some_and(|conv| {
                    conv.phase == FromStandardPhase::Publish
                        && conv.publish.proved.is_none()
                        && conv.publish.accept.is_some()
                })
            }),
            Property::sometimes(REACH_FROM_STANDARD_RERUN_DEEP, |_, state: &State| {
                state.from_standard.is_some_and(|conv| {
                    conv.phase == FromStandardPhase::Publish && conv.deep_provenance
                })
            }),
            Property::sometimes(REACH_STANDARD_UPDATES_CARRIED, |_, state: &State| {
                state.marker().is_some_and(|m| {
                    m.hash.0 == FROM_STANDARD
                        && matches!(
                            state.parents.get(&m.hash),
                            Some(Content::Standard { from: Some(_), .. })
                        )
                })
            }),
            Property::sometimes(REACH_FROM_AFTER_TO, |_, state: &State| {
                state.marker().is_some_and(|m| m.hash.0 == FROM_STANDARD)
                    && state.history.iter().any(|h| featureless(*h))
            }),
            Property::sometimes(REACH_GC_DELETES, |_, state: &State| {
                state.writers.iter().any(|w| w.phase == Phase::DeleteData)
            }),
            Property::sometimes(REACH_CREATOR_REPUBLISHES, |_, state: &State| {
                state.creators.iter().any(|c| c.phase == CreatorPhase::PublishCurrent)
            }),
            Property::sometimes(REACH_TO_STANDARD_READ_ONLY, |_, state: &State| {
                state.dir.is_none()
                    && state.writers.iter().any(|w| {
                        matches!(w.role, Role::ToStandard { .. })
                            && w.phase == Phase::Done
                    })
                    && state.history.last().is_some_and(|last| {
                        featureless(*last) && state.readonly.contains(last)
                    })
            }),
        ]);
        properties.push(Property::always(NO_FALSE_FORK, |_, state: &State| {
            !state.false_fork_report
        }));
        // Progress, in configurations without faults or old writers, where
        // every actor terminates.
        let faultless = !config.crashes
            && !config.lost_replies
            && !config.stale_listings
            && !config.stale_root_listings
            && config.old_writes == 0
            && config.standard_writes == 0;
        if faultless {
            properties.push(Property::eventually(
                CONVERGES,
                |model: &LayoutModel, state| {
                    state.violation.is_some() || model.converged(state)
                },
            ));
        }
        properties
    }
}

/// No violation of this kind.
fn clean(state: &State, violation: Violation) -> bool {
    !matches!(state.violation, Some((v, _)) if v == violation)
}

fn erased_silently_by(state: &State, by: Eraser) -> bool {
    state
        .fork
        .and_then(|fork| fork.erased)
        .is_some_and(|erasure| erasure.silent && erasure.by == by)
}

/// `$ROOT/repo` holds a version that was current, a standard repo info of
/// record, or a creation's first version before its `repo.d` exists.
fn mirror_was_current(state: &State) -> bool {
    if state.fork.is_some() {
        return true;
    }
    match state.repo {
        Some(Content::Version(hash)) => {
            state.history.contains(&hash) || (is_creation(hash) && state.dir.is_none())
        }
        _ => true,
    }
}

/// Every change of the repository's state of record derives from the state
/// before: no change is lost. Not checked once the old writer forked.
fn check_forward(last: &State, state: &mut State) {
    if state.fork.is_some() {
        return;
    }
    let (before, after) = (last.truth(), state.truth());
    if before == after {
        return;
    }
    let forward = match (before, after) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(before), Some(after)) => state.parent_of(after) == Some(before),
    };
    if !forward {
        violate(
            state,
            Violation::Lost,
            format!("the repository went from {before:?} to {after:?}"),
        );
    }
}

/// The version of GC's change, at GC's steps after it.
fn gc_changed(role: Role) -> Version {
    match role {
        Role::Gc { changed: Some(changed), .. } => changed,
        _ => unreachable!("GC's steps after its change"),
    }
}

/// Records, for a GC whose change landed, the highest `N` that `repo` held since.
fn watch_gc_mirror(state: &mut State) {
    let Some(n) = state.mirror_n() else { return };
    for writer in &mut state.writers {
        if let Role::Gc { changed: Some(changed), left, mirror } = writer.role {
            writer.role =
                Role::Gc { changed: Some(changed), left, mirror: mirror.max(Some(n)) };
        }
    }
}

/// The 2.3 `set_status` through `$ROOT/repo`: read it, write it back without the
/// feature.
fn old_write(state: &mut State) {
    state.old_writes -= 1;
    let Some(Content::Version(from)) = state.repo else {
        unreachable!("the old writer acts only on a version in repo");
    };
    state.repo = Some(Content::Fork { from });
    state.fork = Some(Fork { detected: false, erased: None });
    for owner in state.publishing() {
        let mut publish = state.publish(owner);
        publish.fork_landed_at = Some(publish.step);
        state.set_publish(owner, publish);
    }
}

fn step_copier(state: &mut State, choice: Choice) -> Option<()> {
    state.copier = Some(match (state.copier?, choice) {
        // Copying a listed name that no longer exists fails.
        (Copier::Waiting, Choice::Seen(seen)) => {
            Copier::Listed((state.marker() == Some(seen)).then_some(seen))
        }
        (Copier::Waiting, Choice::Normal) => Copier::Listed(None),
        (Copier::Listed(marker), Choice::Normal) => Copier::Copied {
            marker,
            bytes: marker.is_some_and(|marker| state.has_file(marker.file())),
        },
        _ => return None,
    });
    Some(())
}

/// Drops leftovers that nothing reads again, which otherwise multiply the
/// states. Soundness, file by file (the readers are every `files`, `has_file`,
/// `version_file_n` and `mirrors` use in this file and `common/model.rs`):
///
/// - `repo.d/<N>.<h>` whose attempt never landed (`h` not in `history`) and is
///   not in flight (no writer in `Phase::Change` with attempt `h`). Its readers
///   are `has_file` of a listed marker (`read_latest` of writers,
///   `publish_mirror()`, both conversions, the copier) and `MARKER_BYTES`: markers
///   only ever name versions that landed. The publication's `Stage`: its target
///   is a marker's version, a proved one, one adopted from `repo` or GC's own,
///   all landed. `version_file_n` of `repo`'s hash: `repo` holds landed versions,
///   forks, standard repo infos or a creator's first version, whose file enters
///   `repo.d` only with that `repo.d`, as it lands. The attempt's own `Link`:
///   not in flight any more, and no other attempt has its hash, so its name
///   never recurs. Tidy-up and the sweep only delete. No property, metric or
///   reach condition reads other files: they read the marker, `repo`,
///   `history`, `parents`, `readonly`, `acked` and the copier's recorded bytes.
/// - `repo.d/stage.<id>` of an attempt not in flight: its only reader is that
///   attempt's own `Link`; the sweep only deletes.
/// - `repo.d/mirror.<id>` of a publisher not publishing (it crashed): its only
///   reader is its owner's `Rename`, reached only after the same round's
///   `Stage` wrote it again; the sweep only deletes.
fn drop_garbage(state: &mut State) {
    let in_flight: BTreeSet<AttemptId> = state
        .writers
        .iter()
        .filter(|w| matches!(w.phase, Phase::Change(_)))
        .map(|w| w.attempt)
        .collect();
    let publishing: BTreeSet<u8> =
        state.publishing().into_iter().map(|owner| state.stage_id(owner)).collect();
    let history = &state.history;
    if let Some(dir) = &mut state.dir {
        dir.files.retain(|name| match name {
            Name::Content(_, hash) => history.contains(hash) || in_flight.contains(hash),
            Name::Stage(hash) => in_flight.contains(hash),
        });
    }
    state.mirrors.retain(|stage| publishing.contains(stage));
}

/// Resets fields that are dead in the current phase, so equivalent states
/// compare equal.
fn normalize(state: &mut State) {
    drop_garbage(state);
    for writer in &mut state.writers {
        if !matches!(writer.phase, Phase::ForkCheck | Phase::Change(_)) {
            writer.attempt = INITIAL_HASH;
            writer.base = (0, INITIAL_HASH);
            writer.lost_reply = false;
        }
        if !matches!(
            writer.phase,
            Phase::Publish | Phase::PublishBefore | Phase::PublishAfter
        ) {
            writer.publish = IDLE_PUBLISH;
        }
        if matches!(writer.phase, Phase::Done | Phase::Stopped | Phase::Crashed) {
            writer.attempts = 0;
            writer.serial = 0;
            writer.role = match writer.role {
                Role::Gc { .. } => Role::Gc { changed: None, left: None, mirror: None },
                Role::ToStandard { .. } => Role::ToStandard { restarts: 0, relists: 0 },
                role => role,
            };
        }
    }
    for creator in &mut state.creators {
        use CreatorPhase as P;
        if !matches!(creator.phase, P::Publish | P::PublishCurrent) {
            creator.publish = IDLE_PUBLISH;
        }
        if matches!(creator.phase, P::PreCheck | P::Created | P::Refused | P::Crashed) {
            creator.hash = INITIAL_HASH;
            creator.placed = false;
            creator.replace = false;
        }
        if matches!(creator.phase, P::Created | P::Refused | P::Crashed) {
            creator.serial = 0;
            creator.restarts = 0;
        }
    }
    if let Some(conv) = &mut state.from_standard {
        use FromStandardPhase as P;
        if conv.phase != P::Publish {
            conv.publish = IDLE_PUBLISH;
            conv.deep_provenance = false;
        }
        if !matches!(conv.phase, P::Build | P::RenameDir) {
            conv.backup = None;
        }
        if !matches!(conv.phase, P::RenameDir) {
            conv.hash = INITIAL_HASH;
        }
        if matches!(conv.phase, P::Done | P::Stopped | P::Crashed) {
            conv.serial = 0;
            conv.restarts = 0;
        }
    }
}

impl LayoutModel {
    fn budget(&self, role: Role) -> u8 {
        match role {
            Role::ToStandard { .. } => self.config.conversion_attempts,
            _ => self.config.max_attempts,
        }
    }

    /// `N` of a new incarnation's first version: the Unix time in milliseconds,
    /// which exceeds every `N` an earlier `repo.d` at this path reached.
    fn first_n(&self, state: &State) -> u8 {
        if self.config.first_n_zero {
            return 0;
        }
        state.old_markers.iter().map(|m| m.n + 1).max().unwrap_or(0)
    }

    /// The markers a listing of `repo.d` may show: stale, any marker of this or
    /// an earlier `repo.d` at this path.
    fn listing(&self, state: &State) -> Vec<Marker> {
        let Some(dir) = &state.dir else { return vec![] };
        dir.listing(self.config.stale_listings, &state.old_markers)
    }

    fn push_normal(&self, actor: Actor, actions: &mut Vec<Action>) {
        actions.push(Action::Step(actor, Choice::Normal));
    }

    fn push_lost_reply(&self, actor: Actor, actions: &mut Vec<Action>) {
        actions.push(Action::Step(actor, Choice::Normal));
        if self.config.lost_replies {
            actions.push(Action::Step(actor, Choice::LostReply));
        }
    }

    /// A listing of `repo.d`: one choice per marker it may show, or `Normal`
    /// when there is none.
    fn push_seen(&self, state: &State, actor: Actor, actions: &mut Vec<Action>) {
        let listing = self.listing(state);
        if listing.is_empty() {
            actions.push(Action::Step(actor, Choice::Normal));
        }
        actions.extend(listing.into_iter().map(|m| Action::Step(actor, Choice::Seen(m))));
    }

    /// A fork check: its listing of `repo.d` may be stale and miss the version
    /// file with `repo`'s bytes.
    fn push_fork_check(&self, state: &State, actor: Actor, actions: &mut Vec<Action>) {
        actions.push(Action::Step(actor, Choice::Normal));
        let mirror = matches!(
            state.repo,
            Some(Content::Version(hash)) if state.version_file_n(hash).is_some()
        );
        if self.config.stale_listings && mirror {
            actions.push(Action::Step(actor, Choice::SeenNone));
        }
    }

    fn push_publish(
        &self,
        state: &State,
        actor: Actor,
        publish: Publish,
        actions: &mut Vec<Action>,
    ) {
        match publish.step {
            PublishStep::Stage => self.push_normal(actor, actions),
            PublishStep::ForkCheck => self.push_fork_check(state, actor, actions),
            PublishStep::Rename => self.push_lost_reply(actor, actions),
            PublishStep::Verify => self.push_seen(state, actor, actions),
        }
    }

    /// Everyone finished, the conversions completed, and the mirror is current.
    fn converged(&self, state: &State) -> bool {
        let conversions_done = state.writers.iter().all(|w| {
            !matches!(w.role, Role::ToStandard { .. }) || w.phase == Phase::Done
        }) && state
            .from_standard
            .is_none_or(|conv| conv.phase == FromStandardPhase::Done);
        state.finished()
            && conversions_done
            && state
                .marker()
                .is_none_or(|marker| state.repo == Some(Content::Version(marker.hash)))
    }

    /// A conversion stops only for a reason the configuration provides: stale
    /// listings (attempts exhausted, a to-standard root listing that misses
    /// `repo.d`), the old writer or standard writes (forks, provenance), the
    /// other conversion (a conversion in progress), or bounded publication rounds
    /// (giving up); or for the accepted limit of a provenance check whose
    /// ops-log files end before the backup (`missed`), after changes made before
    /// the rerun. Otherwise it must finish, rerun or not.
    fn check_stop(&self, last: &State, state: &mut State, missed: bool) {
        let config = self.config;
        let stopped = |s: &State| {
            s.writers.iter().any(|w| w.phase == Phase::Stopped)
                || s.from_standard.is_some_and(|c| c.phase == FromStandardPhase::Stopped)
        };
        let reasons = config.stale_listings
            || (config.stale_root_listings && config.to_standard)
            || config.old_writes > 0
            || config.standard_writes > 0
            || (config.to_standard && config.from_standard)
            || config.publish_rounds != 0
            || missed;
        if !reasons && stopped(state) && !stopped(last) {
            violate(state, Violation::EndState, "a conversion stopped".into());
        }
    }

    fn crash(&self, state: &mut State, actor: Actor) {
        let restart = |restarts: &mut u8| {
            let again = *restarts < self.config.max_restarts;
            if again {
                *restarts += 1;
            }
            again
        };
        match actor {
            Actor::Writer(index) => {
                let writer = &mut state.writers[index];
                writer.phase = Phase::Crashed;
                if let Role::ToStandard { restarts, relists } = &mut writer.role
                    && restart(restarts)
                {
                    *relists = 0;
                    writer.attempts = 0;
                    writer.phase = Phase::Start;
                }
            }
            Actor::Copier => {}
            Actor::FromStandard => {
                if let Some(conv) = &mut state.from_standard {
                    conv.phase = if restart(&mut conv.restarts) {
                        FromStandardPhase::Start
                    } else {
                        FromStandardPhase::Crashed
                    };
                }
            }
            Actor::Creator(index) => {
                let creator = &mut state.creators[index];
                creator.phase = if restart(&mut creator.restarts) {
                    CreatorPhase::PreCheck
                } else {
                    CreatorPhase::Crashed
                };
            }
        }
    }

    // ── writers, GC, the status setter, the conversion to standard ──────────

    fn step_writer(&self, state: &mut State, index: usize, choice: Choice) -> Option<()> {
        let before = state.writers[index];
        let (base_n, base_hash) = before.base;
        let reachable = state.reachable();
        let conversion = matches!(before.role, Role::ToStandard { .. });
        match (before.phase, choice) {
            (Phase::Start | Phase::Recover, _) if conversion => {
                self.to_standard_decide(state, index, choice)?;
            }
            // `read_latest`: the listed version and its bytes. A version
            // without the feature is refused: a conversion is in progress.
            (Phase::Start, Choice::Seen(seen)) if before.role == Role::Explicit => {
                if !state.has_file(seen.file()) {
                    return None;
                }
                let me = &mut state.writers[index];
                if featureless(seen.hash) {
                    me.phase = Phase::Done;
                } else {
                    me.publish = publish_of(seen.version(), Some(seen.version()), None);
                    me.phase = Phase::Publish;
                }
            }
            (Phase::Start, Choice::Normal) => {
                if before.attempts < self.budget(before.role) && reachable {
                    return None;
                }
                state.writers[index].phase = Phase::Done;
            }
            (Phase::Start, Choice::Seen(seen)) => {
                if before.attempts >= self.budget(before.role)
                    || !state.has_file(seen.file())
                {
                    return None;
                }
                // A featureless current version: a conversion to the standard
                // layout is in progress. `ReadOnly`: only a status change goes on.
                let read_only = state.readonly.contains(&seen.hash)
                    && !matches!(before.role, Role::Status { .. });
                if featureless(seen.hash) || read_only {
                    state.writers[index].phase = Phase::Done;
                } else {
                    begin_attempt(state, index, seen);
                }
            }
            (Phase::Change(step), choice) => {
                let attempt = &mut WriterAttempt { model: self, state, index };
                step_change(attempt, step, choice.change()?)?;
            }
            (Phase::Publish | Phase::PublishBefore | Phase::PublishAfter, choice) => {
                match self.step_publish(state, Owner::Writer(index), choice)? {
                    Published::Running => {}
                    Published::Current => self.after_publish(state, index, true),
                    Published::GaveUp => self.after_publish(state, index, false),
                    Published::Fork => {
                        state.writers[index].phase = if conversion {
                            state.stuck_on_fork |=
                                state.marker().is_some_and(|m| featureless(m.hash));
                            Phase::Stopped
                        } else {
                            Phase::Done
                        };
                    }
                }
            }
            (Phase::Sweep | Phase::ListAfter, Choice::Normal) => {
                if reachable {
                    return None;
                }
                state.writers[index].phase = Phase::Done;
            }
            (Phase::Sweep, Choice::Seen(seen) | Choice::SweepStages(seen)) => {
                gc_deletes(state, index);
                let stages = matches!(choice, Choice::SweepStages(_));
                if let Some(dir) = &mut state.dir {
                    dir.files.retain(|name| match name {
                        Name::Content(i, x) => {
                            !(*i < seen.n || (*i == seen.n && *x != seen.hash))
                        }
                        Name::Stage(_) => !stages,
                    });
                }
                if stages {
                    state.mirrors.clear();
                }
                state.writers[index].phase = Phase::DeleteData;
            }
            (Phase::DeleteData, Choice::Normal) => {
                gc_deletes(state, index);
                state.writers[index].phase = Phase::ListAfter;
            }
            (Phase::ListAfter, Choice::Seen(seen)) => {
                let changed = gc_changed(before.role);
                // From the listed version, or from GC's own if the listing lags,
                // without its bytes.
                let target = if seen.n > changed.0 { seen.version() } else { changed };
                let me = &mut state.writers[index];
                me.publish = publish_of(target, None, None);
                me.phase = Phase::PublishAfter;
            }
            (Phase::WaitCopy, Choice::Normal) => {
                let me = &mut state.writers[index];
                me.phase = if self.config.status == Some(StatusOps::ReadOnlyThenOnline) {
                    me.role = Role::Status { online: true };
                    me.attempts = 0;
                    Phase::Start
                } else {
                    Phase::Done
                };
            }
            // Before its change: a `repo` without the feature that is not the
            // listed version is a fork, or a conversion from the standard layout
            // to rerun first. Refuse, changing nothing.
            // `SeenNone`: its stale listing of `repo.d` misses the version file
            // with `repo`'s bytes.
            (Phase::ForkCheck, Choice::Normal | Choice::SeenNone) => {
                let files_seen = choice == Choice::Normal;
                let base = Marker { n: base_n, hash: base_hash };
                let mirror = match state.repo {
                    Some(Content::Version(hash)) if files_seen => {
                        state.version_file_n(hash).map(|n| (n, hash))
                    }
                    _ => None,
                };
                match (state.repo, mirror) {
                    // The bytes of a newer version, or of a featureless one: the
                    // listing was stale. List again.
                    (_, Some((n, hash)))
                        if n > base_n
                            || (featureless(hash) && !featureless(base_hash)) =>
                    {
                        state.relisted_after_fork_check = true;
                        let Role::ToStandard { restarts, relists } = before.role else {
                            unreachable!("only the conversion to standard has this step");
                        };
                        let writer = &mut state.writers[index];
                        if relists >= self.config.relist_bound {
                            // A retryable error, not a fork report.
                            state.relists_exhausted = true;
                            writer.phase = Phase::Stopped;
                        } else {
                            writer.role =
                                Role::ToStandard { restarts, relists: relists + 1 };
                            writer.phase = Phase::Recover;
                        }
                    }
                    (_, Some(_)) => begin_attempt(state, index, base),
                    (Some(repo), None) if !repo.lists_feature() => {
                        detect_fork(state, !files_seen);
                        state.writers[index].phase = Phase::Stopped;
                    }
                    _ => begin_attempt(state, index, base),
                }
            }
            (Phase::RenameDir, Choice::Normal | Choice::LostReply) => {
                if !reachable {
                    if choice == Choice::LostReply {
                        return None;
                    }
                    state.writers[index].phase = Phase::Recover;
                    return Some(());
                }
                // The property: `$ROOT/repo` holds the featureless current
                // version before `repo.d` goes.
                let current = state.marker().map(|m| m.hash);
                let published = current.is_some_and(|hash| {
                    featureless(hash) && state.repo == Some(Content::Version(hash))
                });
                if !published && state.fork.is_none() {
                    violate(
                        state,
                        Violation::DeletedBeforePublished,
                        format!(
                            "repo.d at {current:?} renamed aside, repo {:?}",
                            state.repo
                        ),
                    );
                }
                if let Some(dir) = state.dir.take() {
                    state.old_markers.extend(dir.past);
                }
                state.mirrors.clear();
                state.tmps.insert(Tmp::Aside);
                state.writers[index].phase = if choice == Choice::LostReply {
                    Phase::Recover
                } else {
                    Phase::DeleteTmp
                };
            }
            (Phase::DeleteTmp | Phase::DeleteLeftovers, Choice::Normal) => {
                state.tmps.clear();
                check_to_standard_end(state);
                state.writers[index].phase = Phase::Done;
            }
            _ => return None,
        }
        Some(())
    }

    /// The conversion's (re)start: from a listing of `$ROOT`, stale only with
    /// `stale_root_listings`, and a possibly stale one of `repo.d`.
    fn to_standard_decide(
        &self,
        state: &mut State,
        index: usize,
        choice: Choice,
    ) -> Option<()> {
        let me = state.writers[index];
        match choice {
            // No `repo.d`, or (`SeenNone`) a root listing that misses it:
            // `$ROOT/repo` decides.
            Choice::Normal | Choice::SeenNone => {
                if choice == Choice::Normal && state.reachable() {
                    return None;
                }
                // No repository yet: the code fails with `ObjectNotFound`,
                // changing nothing. The model runs the conversion once one
                // exists.
                state.truth()?;
                // Finished, but for deleting leftover tmp directories, if `repo`
                // is the converted standard repo info. Otherwise an orphan
                // mirror, or `NotFound` for an interrupted creation: an error.
                state.writers[index].phase = if state.lacks_feature() {
                    Phase::DeleteLeftovers
                } else {
                    Phase::Stopped
                };
            }
            Choice::Seen(seen) => {
                if !state.has_file(seen.file()) {
                    return None;
                }
                let writer = &mut state.writers[index];
                if featureless(seen.hash) {
                    // Made by an earlier run: step 3.
                    writer.publish = publish_of(seen.version(), None, None);
                    writer.phase = Phase::Publish;
                } else {
                    assert!(
                        me.attempts < self.budget(me.role),
                        "a conflict of the last attempt stops the run"
                    );
                    writer.base = seen.version();
                    writer.phase = Phase::ForkCheck;
                }
            }
            _ => return None,
        }
        Some(())
    }

    /// The rename from `source` to `target` landed.
    fn change(&self, state: &mut State, index: usize, source: Marker, target: Marker) {
        let role = state.writers[index].role;
        let base_read_only = state.readonly.contains(&source.hash);
        if base_read_only
            && !matches!(role, Role::Status { .. } | Role::ToStandard { .. })
        {
            violate(
                state,
                Violation::ReadOnlyChanged,
                format!("{role:?} changed to {target:?} from a ReadOnly version"),
            );
        }
        let read_only = match role {
            Role::Status { online } => !online,
            _ => base_read_only,
        };
        if read_only {
            state.readonly.insert(target.hash);
        }
        let Some(dir) = state.dir.as_mut() else {
            unreachable!("the rename landed in repo.d");
        };
        if !dir.move_marker(target) {
            violate(
                state,
                Violation::Protocol,
                format!("marker name used twice: {target:?}"),
            );
        }
        state.parents.insert(target.hash, Content::Version(source.hash));
        state.history.push(target.hash);
        if let Role::Gc { .. } = role {
            state.writers[index].role =
                Role::Gc { changed: Some(target.version()), left: None, mirror: None };
        }
        state.changed_during_fork |= matches!(state.repo, Some(Content::Fork { .. }));
    }

    /// The change landed: its version must be in the chain. The writer publishes
    /// before it returns; a crash before then still leaves the change durable,
    /// so the model records the acknowledgement here.
    fn report_success(&self, state: &mut State, index: usize) {
        let me = state.writers[index];
        let version = (me.base.0 + 1, me.attempt);
        if !state.history.contains(&version.1) {
            violate(
                state,
                Violation::Protocol,
                format!("writer {} acked {version:?}, which never landed", me.id),
            );
        }
        state.acked.insert(version);
        let writer = &mut state.writers[index];
        writer.phase = Phase::Publish;
        writer.publish = publish_of(version, Some(version), None);
        let conversion = matches!(me.role, Role::ToStandard { .. });
        match self.config.sabotage {
            // The conversion calls the change's steps without its publication.
            Some(Sabotage::CommitWithoutPublish) if !conversion => {
                self.after_publish(state, index, true);
            }
            Some(Sabotage::DeleteDirBeforePublish) if conversion => {
                state.writers[index].phase = Phase::RenameDir;
            }
            _ => {}
        }
    }

    /// `NotOnLatestVersion`. Spurious only after a lost reply.
    fn report_conflict(&self, state: &mut State, index: usize) {
        let me = state.writers[index];
        let version = (me.base.0 + 1, me.attempt);
        if state.history.contains(&version.1) && !me.lost_reply {
            violate(
                state,
                Violation::Protocol,
                format!(
                    "writer {} reported a conflict for {version:?}, which landed",
                    me.id
                ),
            );
        }
        state.writers[index].phase = match me.role {
            // After its last attempt the conversion stops with a retryable
            // error; otherwise it lists again.
            Role::ToStandard { .. } if me.attempts >= self.budget(me.role) => {
                Phase::Stopped
            }
            Role::ToStandard { .. } => Phase::Recover,
            _ => Phase::Start,
        };
    }

    /// A publication ended: `Current`, or (`current` false) `GaveUp`.
    fn after_publish(&self, state: &mut State, index: usize, current: bool) {
        let me = state.writers[index];
        let next = match me.role {
            Role::Data => Phase::Start,
            Role::Explicit => Phase::Done,
            // GC's change published its version: now its hook before deleting.
            Role::Gc { changed, .. } if me.phase == Phase::Publish => {
                let Some(changed) = changed else { unreachable!("GC's change landed") };
                state.writers[index].publish = publish_of(changed, None, None);
                Phase::PublishBefore
            }
            Role::Gc { changed, mirror, .. } if me.phase == Phase::PublishBefore => {
                let Some(changed) = changed else { unreachable!("GC's change landed") };
                let left = me.publish.left;
                state.writers[index].role =
                    Role::Gc { changed: Some(changed), left, mirror };
                state.gc_published_n = state.gc_published_n.max(left.map(|v| v.0));
                // Otherwise GC fails before deleting anything.
                if current || left.is_some_and(|left| left.0 >= changed.0) {
                    Phase::Sweep
                } else {
                    Phase::Done
                }
            }
            Role::Gc { .. } => Phase::Done,
            Role::Status { online: false } if self.config.copier => Phase::WaitCopy,
            Role::Status { online: false }
                if self.config.status == Some(StatusOps::ReadOnlyThenOnline) =>
            {
                state.writers[index].role = Role::Status { online: true };
                state.writers[index].attempts = 0;
                Phase::Start
            }
            Role::Status { .. } => Phase::Done,
            // `Current` proves step 3; `GaveUp` fails the run, to be rerun.
            Role::ToStandard { .. } if current => Phase::RenameDir,
            Role::ToStandard { .. } => Phase::Stopped,
        };
        state.writers[index].phase = next;
    }

    // ── publication ─────────────────────────────────────────────────────────

    /// Only GC's hook acts on `GaveUp { left }`; the others log it.
    fn tracks_left(&self, state: &State, owner: Owner) -> bool {
        matches!(owner, Owner::Writer(index) if state.writers[index].phase == Phase::PublishBefore)
    }

    fn explicit(&self, state: &State, owner: Owner) -> bool {
        matches!(owner, Owner::Writer(index) if state.writers[index].role == Role::Explicit)
    }

    fn next_round(&self, publish: &mut Publish) -> Published {
        let rounds = self.config.publish_rounds;
        if rounds != 0 {
            if publish.round + 1 >= rounds {
                return Published::GaveUp;
            }
            publish.round += 1;
        }
        publish.step = PublishStep::Stage;
        publish.installed = false;
        Published::Running
    }

    fn step_publish(
        &self,
        state: &mut State,
        owner: Owner,
        choice: Choice,
    ) -> Option<Published> {
        let mut publish = state.publish(owner);
        let reachable = state.reachable();
        let stage = state.stage_id(owner);
        let (target_n, target_hash) = publish.target;
        let outcome = match (publish.step, choice) {
            // Without `repo.d`, an I/O error: `GaveUp`, keeping any earlier
            // `left`. Unreachable: `repo.d` goes only by the conversion's rename
            // aside, which runs alone.
            (PublishStep::Stage, Choice::Normal) => {
                if !reachable {
                    Published::GaveUp
                } else {
                    let bytes = publish.proved == Some(publish.target)
                        || state.has_file(Name::Content(target_n, target_hash));
                    if bytes {
                        state.mirrors.insert(stage);
                        publish.step = PublishStep::ForkCheck;
                    } else {
                        // The version file is gone: the marker moved on.
                        publish.step = PublishStep::Verify;
                    }
                    Published::Running
                }
            }
            // `SeenNone`: its stale listing of `repo.d` misses the version file
            // with `repo`'s bytes.
            (PublishStep::ForkCheck, Choice::Normal | Choice::SeenNone) => {
                let files_seen = choice == Choice::Normal;
                // `N` of the version file with `repo`'s bytes, as the listing shows.
                let repo_file_n = match state.repo {
                    Some(Content::Version(hash)) if files_seen => {
                        state.version_file_n(hash)
                    }
                    _ => None,
                };
                publish.fork_landed_at = None;
                // Known: the target, the proved version, and every version file
                // in `repo.d`.
                let read = |hash| {
                    hash == target_hash || publish.proved.map(|v| v.1) == Some(hash)
                };
                let known_repo = match state.repo {
                    Some(Content::Version(hash)) => read(hash) || repo_file_n.is_some(),
                    _ => false,
                };
                if matches!(state.repo, Some(Content::Version(_))) && !known_repo {
                    state.hash_only_false_alarm = true;
                }
                // `repo` holds the bytes of a newer version: never go backwards
                // over it. It becomes the target, found in place.
                let newer = match state.repo {
                    Some(Content::Version(hash)) => repo_file_n
                        .filter(|n| *n > target_n && self.config.mirror_never_backwards)
                        .map(|n| (n, hash)),
                    _ => None,
                };
                if let Some(newer) = newer {
                    publish.target = newer;
                }
                match state.repo {
                    Some(Content::Version(hash)) if hash == publish.target.1 => {
                        state.mirrors.remove(&stage);
                        publish.installed = true;
                        if self.tracks_left(state, owner) {
                            publish.left = Some(publish.target);
                        }
                        publish.step = PublishStep::Verify;
                        Published::Running
                    }
                    None => {
                        publish.step = PublishStep::Rename;
                        Published::Running
                    }
                    Some(repo)
                        if repo.lists_feature()
                            || known_repo
                            || Some(repo) == publish.accept =>
                    {
                        publish.step = PublishStep::Rename;
                        Published::Running
                    }
                    Some(_) => {
                        state.mirrors.remove(&stage);
                        detect_fork(state, !files_seen);
                        Published::Fork
                    }
                }
            }
            (PublishStep::Rename, Choice::Normal | Choice::LostReply) => {
                if reachable && state.mirrors.contains(&stage) {
                    state.mirrors.remove(&stage);
                    let by = state.eraser(owner);
                    let older = state.mirror_n().is_some_and(|n| n > target_n);
                    state.explicit_regressed |= older && self.explicit(state, owner);
                    overwrite_repo(state, target_hash, by, publish.fork_landed_at);
                    // After a lost reply the round's listing decides, and the
                    // version left in `repo` is unknown.
                    publish.installed = choice == Choice::Normal;
                    if publish.installed && self.tracks_left(state, owner) {
                        publish.left = Some(publish.target);
                    }
                } else {
                    if choice == Choice::LostReply {
                        return None;
                    }
                    // ENOENT: GC swept the stage as old.
                    publish.installed = false;
                }
                publish.step = PublishStep::Verify;
                Published::Running
            }
            (PublishStep::Verify, Choice::Normal) => {
                if reachable && state.marker().is_some() {
                    return None;
                }
                Published::GaveUp
            }
            (PublishStep::Verify, Choice::Seen(seen)) => {
                if seen.n > target_n {
                    publish.target = seen.version();
                    self.next_round(&mut publish)
                } else if publish.installed {
                    Published::Current
                } else {
                    self.next_round(&mut publish)
                }
            }
            _ => return None,
        };
        state.set_publish(owner, publish);
        Some(outcome)
    }

    // ── conversion from the standard layout ────────────────────────────────

    fn step_from_standard(&self, state: &mut State, choice: Choice) -> Option<()> {
        use FromStandardPhase as P;
        let conv = state.from_standard?;
        let mut next = conv;
        let lost = choice == Choice::LostReply;
        match (conv.phase, choice) {
            (P::Start | P::Recover, Choice::Normal) => {
                if state.reachable() {
                    return None;
                }
                next.phase = match state.repo {
                    Some(repo) if !repo.lists_feature() => P::Backup,
                    _ => P::Stopped,
                };
            }
            (P::Start | P::Recover, Choice::Seen(seen)) => {
                if !state.has_file(seen.file()) {
                    return None;
                }
                next.phase = match state.repo {
                    // A conversion to the standard layout is in progress.
                    _ if featureless(seen.hash) => P::Stopped,
                    // Converted already, or nothing to convert: then step 8.
                    Some(repo) if repo.lists_feature() => {
                        state.tmps.clear();
                        if state.origin(seen.hash).is_some() {
                            check_from_standard_end(state);
                        }
                        P::Done
                    }
                    None => {
                        next.publish = publish_of(seen.version(), None, None);
                        P::Publish
                    }
                    Some(repo) => {
                        let changes = state.changes_since_backup(seen.hash, repo);
                        if changes.is_some() || !self.config.provenance_check {
                            next.deep_provenance = changes
                                .is_some_and(|changes| changes > PROVENANCE_CHANGES);
                            next.publish = publish_of(seen.version(), None, Some(repo));
                            P::Publish
                        } else {
                            state.provenance_stopped = true;
                            P::Stopped
                        }
                    }
                };
            }
            (P::Start | P::Recover, Choice::ProvenanceMissed(seen)) => {
                if !state.has_file(seen.file()) {
                    return None;
                }
                state.provenance_stopped = true;
                next.phase = P::Stopped;
            }
            (P::Backup, Choice::Normal) => {
                next.backup = state.repo;
                next.phase = P::Build;
            }
            (P::Build, Choice::Normal) => {
                next.hash = (FROM_STANDARD, conv.serial);
                next.serial += 1;
                state.tmps.insert(Tmp::Build(next.hash));
                let Some(backup) = conv.backup else { unreachable!("backed up") };
                state.parents.insert(next.hash, backup);
                next.phase = P::RenameDir;
            }
            (P::RenameDir, Choice::Normal | Choice::LostReply) => {
                state.tmps.remove(&Tmp::Build(conv.hash));
                if state.dir.is_none() {
                    let first = (self.first_n(state), conv.hash);
                    state.dir = Some(fresh_dir(first.0, conv.hash));
                    state.history.push(conv.hash);
                    next.publish = publish_of(first, Some(first), conv.backup);
                    next.phase = if lost { P::Recover } else { P::Publish };
                } else {
                    if lost {
                        return None;
                    }
                    // `EEXIST`: the tmp directory is discarded (removed above),
                    // and the rerun's decision says whose `repo.d` it is.
                    next.phase = P::Recover;
                }
            }
            (P::Publish, choice) => {
                state.from_standard = Some(next);
                next.phase =
                    match self.step_publish(state, Owner::FromStandard, choice)? {
                        Published::Running => P::Publish,
                        // Leftover tmp directories of crashed runs go too.
                        Published::Current => {
                            state.tmps.clear();
                            check_from_standard_end(state);
                            P::Done
                        }
                        // An error that says to rerun the conversion.
                        Published::GaveUp | Published::Fork => P::Stopped,
                    };
                next.publish = state.publish(Owner::FromStandard);
            }
            _ => return None,
        }
        state.from_standard = Some(next);
        Some(())
    }

    // ── creation ────────────────────────────────────────────────────────────

    fn step_creator(
        &self,
        state: &mut State,
        index: usize,
        choice: Choice,
    ) -> Option<()> {
        use CreatorPhase as P;
        let creator = state.creators[index];
        let mut next = creator;
        match (creator.phase, choice) {
            (P::PreCheck, Choice::Normal | Choice::SeenNone) => {
                let sees_dir = state.dir.is_some() && choice != Choice::SeenNone;
                next.phase = match state.repo {
                    _ if sees_dir => P::Refused,
                    None => P::Build,
                    // An interrupted creation, as far as the listing tells.
                    Some(Content::Version(hash)) if is_creation(hash) => {
                        next.replace = true;
                        P::Build
                    }
                    Some(_) => P::Refused,
                };
            }
            (P::Build, Choice::Normal) => {
                next.hash = (CREATOR + creator.index, creator.serial);
                next.serial += 1;
                state.tmps.insert(Tmp::Build(next.hash));
                next.phase = if creator.replace { P::Replace } else { P::Link };
            }
            (P::Link, Choice::Normal) => {
                next.phase = match state.repo {
                    None => {
                        overwrite_repo(state, creator.hash, Eraser::Creator, None);
                        next.placed = true;
                        P::RenameDir
                    }
                    // `EEXIST`: another creator's; classified once more.
                    Some(Content::Version(hash))
                        if is_creation(hash) && state.dir.is_none() =>
                    {
                        P::Replace
                    }
                    Some(_) => {
                        state.tmps.remove(&Tmp::Build(creator.hash));
                        P::Refused
                    }
                };
            }
            (P::Replace, Choice::Normal) => {
                overwrite_repo(state, creator.hash, Eraser::Creator, None);
                next.placed = true;
                next.phase = P::RenameDir;
            }
            (P::RenameDir, Choice::Normal) => {
                state.tmps.remove(&Tmp::Build(creator.hash));
                next.phase = if state.dir.is_none() {
                    let first = (self.first_n(state), creator.hash);
                    state.dir = Some(fresh_dir(first.0, creator.hash));
                    state.history.push(creator.hash);
                    next.publish = publish_of(first, Some(first), None);
                    P::Publish
                } else if creator.placed {
                    P::ListCurrent
                } else {
                    P::Refused
                };
            }
            (P::ListCurrent, Choice::Normal) => {
                if state.reachable() {
                    return None;
                }
                next.phase = P::Refused;
            }
            (P::ListCurrent, Choice::Seen(seen)) => {
                next.publish = publish_of(seen.version(), None, None);
                next.phase = P::PublishCurrent;
            }
            (P::Publish | P::PublishCurrent, choice) => {
                state.creators[index] = next;
                let outcome = self.step_publish(state, Owner::Creator(index), choice)?;
                next.publish = state.creators[index].publish;
                if !matches!(outcome, Published::Running) {
                    next.phase =
                        if creator.phase == P::Publish { P::Created } else { P::Refused };
                }
            }
            _ => return None,
        }
        state.creators[index] = next;
        Some(())
    }
}

fn begin_attempt(state: &mut State, index: usize, seen: Marker) {
    let me = &mut state.writers[index];
    me.attempt = (me.id, me.serial);
    me.serial += 1;
    me.attempts += 1;
    me.base = seen.version();
    me.lost_reply = false;
    me.phase = Phase::Change(ChangeStep::Stage);
    if let Role::Gc { .. } = me.role {
        me.role = Role::Gc { changed: None, left: None, mirror: None };
    }
}

/// Writer `index`'s change attempt in `state`.
struct WriterAttempt<'a> {
    model: &'a LayoutModel,
    state: &'a mut State,
    index: usize,
}

impl ChangeAttempt for WriterAttempt<'_> {
    fn dir(&self) -> Option<&Dir> {
        self.state.dir.as_ref()
    }

    fn dir_mut(&mut self) -> Option<&mut Dir> {
        self.state.dir.as_mut()
    }

    fn attempt(&self) -> (AttemptId, Version) {
        let me = &self.state.writers[self.index];
        (me.attempt, me.base)
    }

    fn set_lost_reply(&mut self) {
        self.state.writers[self.index].lost_reply = true;
    }

    fn set_step(&mut self, step: ChangeStep) {
        self.state.writers[self.index].phase = Phase::Change(step);
    }

    fn change(&mut self, source: Marker, target: Marker) {
        self.model.change(self.state, self.index, source, target);
    }

    fn report_success(&mut self) {
        self.model.report_success(self.state, self.index);
    }

    fn report_conflict(&mut self) {
        self.model.report_conflict(self.state, self.index);
    }

    fn landed(&self) -> Option<bool> {
        Some(self.state.history.contains(&self.state.writers[self.index].attempt))
    }
}

/// An uninterrupted conversion to the standard layout leaves `repo` holding the
/// version it made last, without the feature, and nothing else.
fn check_to_standard_end(state: &mut State) {
    if state.fork.is_some() {
        return;
    }
    // Standard writes may update the standard repo info once `repo.d` is gone.
    let last = state.history.last().copied();
    let expected = last.is_some_and(|last| {
        featureless(last)
            && state
                .repo
                .is_some_and(|repo| state.content_derives(repo, Content::Version(last)))
    });
    if !expected || state.dir.is_some() || !state.tmps.is_empty() {
        // The accepted limit: a rerun whose root listing missed `repo.d` found
        // the published version without the feature and reported nothing to
        // convert. `repo.d` stays, its current version that one.
        state.stale_root_end = state.violation.is_none()
            && state.tmps.is_empty()
            && state.marker().is_some_and(|marker| {
                featureless(marker.hash)
                    && last == Some(marker.hash)
                    && state.repo == Some(Content::Version(marker.hash))
            });
        violate(
            state,
            Violation::EndState,
            format!(
                "to standard ended with repo.d at {:?}, repo {:?}, tmps {:?}",
                state.marker(),
                state.repo,
                state.tmps
            ),
        );
    }
}

/// The conversion from the standard layout leaves `repo.d`, and `repo` holding a
/// version that was current and derives from one of its first versions.
fn check_from_standard_end(state: &mut State) {
    let expected = match state.repo {
        Some(Content::Version(hash)) => {
            state.history.contains(&hash) && state.origin(hash).is_some()
        }
        _ => false,
    };
    if !expected || state.dir.is_none() || !state.tmps.is_empty() {
        violate(
            state,
            Violation::EndState,
            format!(
                "from standard ended with repo {:?}, tmps {:?}",
                state.repo, state.tmps
            ),
        );
    }
}

// ── checking ────────────────────────────────────────────────────────────────

/// Prints each action of a trace, with the marker, `repo` and the actors after
/// it.
fn print_trace(path: Path<State, Action>) {
    let steps = path.into_vec();
    for (index, (_, action)) in steps.iter().enumerate() {
        let (Some(action), Some((after, _))) = (action, steps.get(index + 1)) else {
            continue;
        };
        println!(
            "  {action:?}  ->  marker {:?}, repo {:?}{}",
            after.marker(),
            after.repo,
            after.summary()
        );
    }
}

/// Checks `config` breadth-first, for the shortest traces, and prints the state
/// count, the time and the trace of every discovery. Asserts that the `always`
/// and `eventually` properties named in `violated` have a counterexample and that
/// the others have none, and that every `metric: …` property in `metrics` has an
/// example.
fn check(name: &str, config: Config, violated: &[&str], metrics: &[&str]) {
    let model = LayoutModel { config };
    let properties = model.properties();
    let start = Instant::now();
    let checker = model.checker().spawn_bfs().join();
    report(name, &properties, &checker, start, violated, metrics);
}

/// [`check`] depth-first on all cores, for configurations too large to keep a
/// breadth-first frontier. Traces are not the shortest.
fn check_large(name: &str, config: Config, violated: &[&str], metrics: &[&str]) {
    let model = LayoutModel { config };
    let properties = model.properties();
    let threads = std::thread::available_parallelism().map_or(1, usize::from);
    let start = Instant::now();
    let checker = model.checker().threads(threads).spawn_dfs().join();
    report(name, &properties, &checker, start, violated, metrics);
}

fn report(
    name: &str,
    properties: &[Property<LayoutModel>],
    checker: &impl Checker<LayoutModel>,
    start: Instant,
    violated: &[&str],
    metrics: &[&str],
) {
    println!(
        "{name}: {} unique states, max depth {}, {:.2?}",
        checker.unique_state_count(),
        checker.max_depth(),
        start.elapsed()
    );
    let discoveries = checker.discoveries();
    for property in properties {
        if let Some(path) = discoveries.get(property.name) {
            let steps = path.clone().into_actions().len();
            println!("{name}: \"{}\", {steps} steps:", property.name);
            print_trace(path.clone());
        }
    }
    for absent in violated.iter().chain(metrics).filter(|name| name.starts_with('!')) {
        assert!(
            properties.iter().any(|property| {
                format!("!{}", property.name) == *absent
                    && property.expectation == Expectation::Sometimes
            }),
            "{name}: \"{absent}\" names no metric that may be absent"
        );
    }
    for expected in violated.iter().chain(metrics) {
        let expected = expected.trim_start_matches('!');
        assert!(
            properties.iter().any(|property| property.name == expected),
            "{name}: no property \"{expected}\""
        );
    }
    for property in properties {
        let found = discoveries.contains_key(property.name);
        match property.expectation {
            Expectation::Always | Expectation::Eventually => {
                if violated.contains(&property.name) {
                    assert!(
                        found,
                        "{name}: expected a counterexample of \"{}\"",
                        property.name
                    );
                } else {
                    assert!(
                        !found,
                        "{name}: unexpected counterexample of \"{}\"",
                        property.name
                    );
                }
            }
            Expectation::Sometimes => {
                let absent = format!("!{}", property.name);
                assert!(
                    !(found && metrics.contains(&absent.as_str())),
                    "{name}: unexpected example of \"{}\"",
                    property.name
                );
                if metrics.contains(&property.name) {
                    assert!(
                        found,
                        "{name}: expected an example of \"{}\"",
                        property.name
                    );
                }
            }
        }
    }
    assert!(checker.is_done(), "{name}: checking is incomplete");
}

/// Every distinct way a fork disappears without being detected in `config`.
fn silent_erasures(config: Config) -> BTreeSet<(Eraser, Option<PublishStep>)> {
    let found = Arc::new(Mutex::new(BTreeSet::new()));
    let sink = Arc::clone(&found);
    LayoutModel { config }
        .checker()
        .visitor(move |path: Path<State, Action>| {
            if let Some(erasure) = path.last_state().fork.and_then(|fork| fork.erased)
                && erasure.silent
            {
                sink.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert((erasure.by, erasure.landed_at));
            }
        })
        .spawn_bfs()
        .join();
    found.lock().unwrap_or_else(PoisonError::into_inner).clone()
}

// ── configurations ──────────────────────────────────────────────────────────

const REACH_TO_STANDARD_DONE: &str = "reach: the conversion to standard completes";
const REACH_TO_STANDARD_STEP_3: &str =
    "reach: a rerun of the conversion to standard publishes its featureless version";
const REACH_TO_STANDARD_AFTER_RENAME: &str =
    "reach: the conversion to standard crashes after renaming repo.d aside";
const REACH_TO_STANDARD_RERUN_RENAMES: &str = "reach: a rerun of the conversion to standard publishes its featureless version and renames repo.d aside";
const REACH_TO_STANDARD_LEFTOVERS: &str = "reach: a rerun of the conversion to standard finds repo.d gone and deletes leftovers";
const REACH_TO_STANDARD_RELIST: &str = "reach: the conversion to standard's fork check finds a newer version's bytes and lists again";
const REACH_FROM_STANDARD_RERUN: &str =
    "reach: a rerun of the conversion from standard publishes after its provenance check";
const REACH_FROM_STANDARD_RERUN_DEEP: &str = "reach: a rerun of the conversion from standard publishes after its provenance check, two changes or more past the backup";
const REACH_STANDARD_UPDATES_CARRIED: &str = "reach: a conversion from standard carries standard updates made after a conversion to standard";
const REACH_FROM_AFTER_TO: &str =
    "reach: a conversion from standard follows a conversion to standard";
const REACH_GC_DELETES: &str = "reach: GC deletes";
const REACH_CREATOR_REPUBLISHES: &str =
    "reach: a creator that lost the directory rename publishes the current version";
const REACH_TO_STANDARD_READ_ONLY: &str =
    "reach: the conversion to standard completes from a ReadOnly version, which it keeps";
const MARKER_BYTES: &str = "the current marker has bytes";
const NO_ACK_LOST: &str = "no acknowledged change lost";
const MIRROR_CURRENT: &str = "the mirror is a version that was current";
const GC_PUBLISHED: &str = "GC deletes only after its change and publication";
const READ_ONLY: &str =
    "while ReadOnly, only a status change or a conversion changes the repo info";
const COPY_CONSISTENT: &str = "a copy taken while ReadOnly holds the ReadOnly version";
const DELETED_PUBLISHED: &str =
    "repo.d is deleted only once repo holds its current version without the feature";
const STANDARD_KEPT: &str = "a standard repository is never overwritten";
const SAME_END_STATE: &str = "a conversion ends as an uninterrupted run";
const ONLY_STALE_ROOT_END: &str = "a conversion ends as an uninterrupted run, or as a rerun to standard whose root listing missed repo.d";
const NO_FALSE_FORK: &str = "no fork report without a fork";
const CONVERGES: &str = "everyone finishes, with the mirror current";
const ERASED_BY_WRITER: &str = "metric: a writer's publication erases a fork";
const ERASED_BY_GC: &str = "metric: GC's publication erases a fork";
const DETECTED: &str = "metric: a check detects a fork";
const UNDETECTED: &str = "metric: everyone finishes with a fork nobody detected";
const CHANGE_DURING_FORK: &str = "metric: a change lands with a fork in repo";
const GC_DELETES_DURING_FORK: &str = "metric: GC deletes data while a fork is in repo";
const FORK_REPORT_INTERRUPTED: &str = "metric: a fork check before the rerun finds the standard repo of an interrupted conversion from standard";
const FALSE_FORK_STALE: &str = "metric: a fork check whose listing of repo.d is stale reports a false fork (accepted)";
const RELISTS_EXHAUSTED: &str =
    "metric: the conversion to standard stops after relisting relist_bound times";
const FROM_DONE_AFTER_REFUSAL: &str = "reach: the conversion from standard finishes after a fork check found its standard repo";
const DIVERGENCE_DETECTED: &str =
    "metric: a fork check finds a standard write made before the rerun of a conversion";
const PROVENANCE_STOPPED: &str =
    "metric: a rerun of the conversion from standard stops at its provenance check";
const STUCK_ON_FORK: &str =
    "metric: the conversion to standard changes the repo info, then meets a fork";
const HASH_ONLY_FALSE_ALARM: &str =
    "metric: a hash-only check would call a lagging mirror a fork";
const GC_MIRROR_REGRESSED: &str =
    "metric: repo is older than GC's publication while GC deletes";
const GC_LEFT_REGRESSED: &str =
    "metric: everyone finishes with repo older than GC's publication";
const EXPLICIT_REGRESSED: &str =
    "metric: publish_mirror() renames a version older than the one in repo";
const COPY_MISSED_MARKER: &str =
    "metric: a stale listing leaves a copy without its marker";
const CREATOR_PUBLISHES_FEATURELESS: &str = "metric: a creator that lost the directory rename publishes a current version without the feature";

/// The doc's relisting bound is 8; quick configs use 2, which reaches the same
/// exhaustion with fewer states. The large configs keep 8.
const QUICK_RELISTS: u8 = 2;

const WRITERS: Config = Config { writers: 2, ..NOTHING };

const CRASHES: Config =
    Config { crashes: true, lost_replies: true, max_restarts: 2, ..NOTHING };

const FAULTS: Config = Config { stale_listings: true, ..CRASHES };

/// Changes, publications and GC without faults: everyone finishes and the mirror
/// ends at the current version.
#[test]
fn mirror_plain() {
    check(
        "mirror_plain",
        Config { gc: true, ..WRITERS },
        &[],
        &[GC_MIRROR_REGRESSED, REACH_GC_DELETES],
    );
}

/// With crashes, a publisher whose late rename lands an older version and that
/// crashes before verifying leaves `repo` older than GC's publication, while GC
/// deletes and after everyone finished.
#[test]
fn mirror_gc_crashes() {
    let config = Config { gc: true, writers: 2, ..CRASHES };
    check("mirror_gc_crashes", config, &[], &[GC_MIRROR_REGRESSED, GC_LEFT_REGRESSED]);
}

/// Changes and publications with every fault: the mirror is always a version
/// that was current.
#[test]
fn mirror_faults() {
    check("mirror_faults", Config { writers: 2, ..FAULTS }, &[], &[]);
}

#[test]
fn mirror_gc_faults() {
    check("mirror_gc_faults", Config { writers: 1, gc: true, ..FAULTS }, &[], &[]);
}

/// A publisher that gives up after a fixed number of rounds can be the last to
/// rename, with a version that a change superseded during its last round:
/// exhausting the rounds is like crashing there.
#[test]
fn mirror_bounded_rounds() {
    let config = Config { gc: true, publish_rounds: 2, ..WRITERS };
    check("mirror_bounded_rounds", config, &[CONVERGES], &[]);
}

/// The design's four rounds, with GC and every fault: exhausting them is like
/// crashing there, and GC deletes only after a publication that left its version
/// or a later one in `repo`.
#[test]
fn mirror_four_rounds() {
    let config = Config { writers: 1, gc: true, publish_rounds: 4, ..FAULTS };
    check("mirror_four_rounds", config, &[], &[]);
}

/// Sabotage: a change that returns without publishing leaves the mirror behind.
#[test]
fn mirror_commit_without_publish() {
    let config = Config { sabotage: Some(Sabotage::CommitWithoutPublish), ..WRITERS };
    check("mirror_commit_without_publish", config, &[CONVERGES], &[]);
}

/// `publish_mirror()` with fresh listings: like any slow publisher, it can rename
/// a version that a change superseded after its listing.
#[test]
fn publish_mirror() {
    let config = Config { writers: 2, explicit_publish: true, ..CRASHES };
    check("publish_mirror", config, &[], &[EXPLICIT_REGRESSED]);
}

/// With stale listings its verifying listing can lag too, and nothing sends it
/// round again.
#[test]
fn publish_mirror_stale_listings() {
    let config = Config { writers: 2, explicit_publish: true, ..FAULTS };
    check("publish_mirror_stale_listings", config, &[], &[EXPLICIT_REGRESSED]);
}

/// The status set `ReadOnly` by a change stops every other change, GC's included,
/// so a copy taken meanwhile holds the `ReadOnly` version with its bytes.
#[test]
fn read_only_copy() {
    let config = Config {
        writers: 1,
        gc: true,
        status: Some(StatusOps::ReadOnly),
        copier: true,
        ..CRASHES
    };
    check("read_only_copy", config, &[], &[REACH_GC_DELETES]);
}

/// The same with stale listings: a change from a stale `Online` base fails at
/// its rename. A copy from a stale listing misses the marker: an incomplete copy,
/// which a reader of the copy finds without a current version, never a wrong one.
#[test]
fn read_only_copy_stale_listings() {
    let config = Config {
        writers: 1,
        gc: true,
        status: Some(StatusOps::ReadOnly),
        copier: true,
        ..FAULTS
    };
    check("read_only_copy_stale_listings", config, &[], &[COPY_MISSED_MARKER]);
}

/// Best-effort detection: the old library forks the repository through the
/// mirror (unsupported). Publications check `repo` and detect forks, but one
/// whose check ran before the old library's write erases the fork, changes land
/// on top of it, and it can stay undetected.
#[test]
fn fork_detection_writers() {
    let config = Config { old_writes: 1, ..WRITERS };
    check(
        "fork_detection_writers",
        config,
        &[],
        &[
            ERASED_BY_WRITER,
            DETECTED,
            UNDETECTED,
            CHANGE_DURING_FORK,
            HASH_ONLY_FALSE_ALARM,
        ],
    );
}

#[test]
fn fork_detection_gc() {
    let config = Config { writers: 1, gc: true, old_writes: 1, ..NOTHING };
    check(
        "fork_detection_gc",
        config,
        &[],
        &[ERASED_BY_WRITER, ERASED_BY_GC, DETECTED, UNDETECTED, GC_DELETES_DURING_FORK],
    );
}

/// The best-effort detection, quantified: a fork disappears undetected only when
/// the old library's write lands between a publisher's fork check and its rename.
#[test]
fn fork_windows() {
    let config = Config { writers: 1, gc: true, old_writes: 1, ..NOTHING };
    let found = silent_erasures(config);
    println!("fork_windows: {found:?}");
    let expected = BTreeSet::from([
        (Eraser::Writer, Some(PublishStep::Rename)),
        (Eraser::Gc, Some(PublishStep::Rename)),
    ]);
    assert_eq!(found, expected);
}

const TO_STANDARD_ONLY: Config = Config { writers: 1, to_standard: true, ..NOTHING };

#[test]
fn to_standard_plain() {
    check("to_standard_plain", TO_STANDARD_ONLY, &[], &[REACH_TO_STANDARD_DONE]);
}

/// The conversion to the standard layout crashing and losing replies at every
/// step, after writers that may have crashed before publishing, and next to
/// writers and GC between a crash and the rerun: every rerun ends as an
/// uninterrupted run, and `repo.d` goes only once `repo` holds its featureless
/// current version.
#[test]
fn to_standard_crashes() {
    let config = Config { writers: 2, to_standard: true, ..CRASHES };
    check(
        "to_standard_crashes",
        config,
        &[],
        &[
            REACH_TO_STANDARD_DONE,
            REACH_TO_STANDARD_STEP_3,
            REACH_TO_STANDARD_AFTER_RENAME,
            REACH_TO_STANDARD_RERUN_RENAMES,
            REACH_TO_STANDARD_LEFTOVERS,
        ],
    );
}

#[test]
fn to_standard_gc_crashes() {
    let config = Config { writers: 1, gc: true, to_standard: true, ..CRASHES };
    check(
        "to_standard_gc_crashes",
        config,
        &[],
        &[REACH_TO_STANDARD_DONE, REACH_GC_DELETES],
    );
}

/// With stale listings: the conversion's change from a stale base fails at its
/// rename, so the version it converts is the current one.
#[test]
fn to_standard_stale_listings() {
    let config =
        Config { writers: 2, to_standard: true, relist_bound: QUICK_RELISTS, ..FAULTS };
    check(
        "to_standard_stale_listings",
        config,
        &[],
        &[
            REACH_TO_STANDARD_DONE,
            REACH_TO_STANDARD_STEP_3,
            REACH_TO_STANDARD_AFTER_RENAME,
            REACH_TO_STANDARD_RELIST,
            FALSE_FORK_STALE,
            RELISTS_EXHAUSTED,
        ],
    );
}

/// `publish_mirror()` between a crash of the conversion to standard after its
/// step 3 and the rerun, with stale listings: its target can be an older version
/// while `repo` holds the published featureless one. Its fork check adopts that
/// newer version as found in place (it never goes backwards), so it neither
/// reports a fork nor overwrites it; only a fork check whose own listing is stale
/// reports a false fork (accepted).
#[test]
fn to_standard_publish_mirror() {
    let config = Config {
        writers: 1,
        to_standard: true,
        explicit_publish: true,
        relist_bound: QUICK_RELISTS,
        ..FAULTS
    };
    check("to_standard_publish_mirror", config, &[], &[FALSE_FORK_STALE]);
}

/// Without the rule that a publication never renames an older version over the
/// bytes of a newer one in `repo`, it replaces the published featureless version,
/// the standard repo info of record, with an older version.
#[test]
fn to_standard_publish_mirror_backwards() {
    let config = Config {
        writers: 1,
        to_standard: true,
        explicit_publish: true,
        mirror_never_backwards: false,
        relist_bound: QUICK_RELISTS,
        ..FAULTS
    };
    check("to_standard_publish_mirror_backwards", config, &[STANDARD_KEPT], &[]);
}

/// A `ReadOnly` repository converts too, and stays `ReadOnly`.
#[test]
fn to_standard_read_only() {
    let config = Config {
        writers: 1,
        status: Some(StatusOps::ReadOnly),
        to_standard: true,
        relist_bound: QUICK_RELISTS,
        ..FAULTS
    };
    check(
        "to_standard_read_only",
        config,
        &[],
        &[REACH_TO_STANDARD_DONE, REACH_TO_STANDARD_READ_ONLY],
    );
}

/// Sabotage: the conversion renames `repo.d` aside before it publishes its
/// version without the feature.
#[test]
fn to_standard_delete_dir_before_publish() {
    let config =
        Config { sabotage: Some(Sabotage::DeleteDirBeforePublish), ..TO_STANDARD_ONLY };
    check("to_standard_delete_dir_before_publish", config, &[DELETED_PUBLISHED], &[]);
}

/// A rerun whose stale listing of `$ROOT` misses `repo.d`, after an earlier
/// run published its version without the feature, finds that standard `repo`
/// and reports nothing to convert, leaving `repo.d`: an accepted limit, cured by
/// running the conversion again once listings refresh. Every end-state
/// violation is that one (`ONLY_STALE_ROOT_END` holds). A run whose root listing
/// misses `repo.d` before that stops on the orphan mirror, changing nothing.
#[test]
fn to_standard_stale_root_listing() {
    let config =
        Config { writers: 1, to_standard: true, stale_root_listings: true, ..CRASHES };
    check("to_standard_stale_root_listing", config, &[SAME_END_STATE], &[]);
}

/// The old library writes `repo` between a crash and the rerun (unsupported): the
/// conversion's publication detects the fork and stops, possibly after its
/// change, which leaves the repository refusing writers.
#[test]
fn to_standard_fork_before_rerun() {
    let config = Config { writers: 1, to_standard: true, old_writes: 1, ..CRASHES };
    check("to_standard_fork_before_rerun", config, &[], &[DETECTED, STUCK_ON_FORK]);
}

const FROM_STANDARD_ONLY: Config = Config { from_standard: true, ..NOTHING };

#[test]
fn from_standard_plain() {
    check("from_standard_plain", FROM_STANDARD_ONLY, &[], &[]);
}

/// A rerun after a crash at any step ends as an uninterrupted run.
#[test]
fn from_standard_crashes() {
    check(
        "from_standard_crashes",
        Config { from_standard: true, ..CRASHES },
        &[],
        &[REACH_FROM_STANDARD_RERUN],
    );
}

/// A standard update of `repo` between a crash and the rerun: the provenance
/// check stops the rerun instead of publishing over it.
#[test]
fn from_standard_standard_writes() {
    let config = Config { from_standard: true, standard_writes: 1, ..CRASHES };
    check("from_standard_standard_writes", config, &[], &[PROVENANCE_STOPPED]);
}

/// Without the provenance check, the rerun publishes the first version over the
/// updated standard `repo`.
#[test]
fn from_standard_without_provenance() {
    let config = Config {
        from_standard: true,
        standard_writes: 1,
        provenance_check: false,
        ..CRASHES
    };
    check("from_standard_without_provenance", config, &[STANDARD_KEPT], &[]);
}

/// Writers between a crash and the rerun, with stale listings.
#[test]
fn from_standard_writers() {
    let config = Config { from_standard: true, writers: 1, ..FAULTS };
    check("from_standard_writers", config, &[], &[]);
}

/// Creations never overwrite the standard repository, even with a stale root
/// listing.
#[test]
fn from_standard_and_creator() {
    let config =
        Config { from_standard: true, creators: 1, stale_root_listings: true, ..CRASHES };
    check("from_standard_and_creator", config, &[], &[]);
}

/// To the standard layout and back, either conversion crashing at any step, with
/// a writer.
#[test]
fn round_trip() {
    let config = Config {
        writers: 1,
        to_standard: true,
        from_standard: true,
        standard_writes: 1,
        ..CRASHES
    };
    check(
        "round_trip",
        config,
        &[],
        &[
            FORK_REPORT_INTERRUPTED,
            REACH_TO_STANDARD_DONE,
            REACH_TO_STANDARD_RERUN_RENAMES,
            REACH_TO_STANDARD_LEFTOVERS,
            REACH_FROM_AFTER_TO,
            REACH_FROM_STANDARD_RERUN,
            REACH_STANDARD_UPDATES_CARRIED,
        ],
    );
}

/// A writer that runs between a crash of the conversion from the standard layout
/// after its directory rename and the rerun (outside the rules) makes a change on
/// the first version and reports the standard `repo` as a fork when it
/// publishes. Nothing is lost; the rerun publishes through its provenance check.
#[test]
fn from_standard_writers_crashes() {
    let config = Config { from_standard: true, writers: 1, ..CRASHES };
    check("from_standard_writers_crashes", config, &[], &[FORK_REPORT_INTERRUPTED]);
}

/// Two writers between a crash of the conversion from the standard layout after
/// its directory rename and the rerun (outside the rules): the rerun's provenance
/// check, two changes past the conversion's backup, finds it (a large page) and
/// publishes, or misses it (a small page) and stops, changing nothing. The
/// writers' publications meet the standard `repo` before the rerun and report a
/// fork (`FORK_REPORT_INTERRUPTED`), as in `from_standard_writers_crashes`.
#[test]
fn from_standard_two_writers_crashes() {
    let config = Config { from_standard: true, writers: 2, ..CRASHES };
    check(
        "from_standard_two_writers_crashes",
        config,
        &[],
        &[PROVENANCE_STOPPED, REACH_FROM_STANDARD_RERUN_DEEP, FORK_REPORT_INTERRUPTED],
    );
}

/// A standard write and a writer between a crash of the conversion from the
/// standard layout and its rerun (outside the rules): the writer's publication
/// finds the updated standard `repo`, a real divergence, and leaves it alone; the
/// rerun's provenance check stops.
#[test]
fn from_standard_writers_standard_writes() {
    let config =
        Config { from_standard: true, writers: 1, standard_writes: 1, ..CRASHES };
    check(
        "from_standard_writers_standard_writes",
        config,
        &[],
        &[DIVERGENCE_DETECTED, PROVENANCE_STOPPED],
    );
}

/// A `repo.d` that starts again at `N = 0` after a round trip: a stale listing
/// still shows a marker of the earlier `repo.d` with a higher `N`, and GC's
/// sweep from it deletes the current version. Hence the first `N` is the Unix
/// time in milliseconds, above every `N` the earlier `repo.d` reached.
#[test]
fn round_trip_first_n_zero() {
    let config = Config {
        gc: true,
        to_standard: true,
        from_standard: true,
        first_n_zero: true,
        relist_bound: QUICK_RELISTS,
        ..FAULTS
    };
    check("round_trip_first_n_zero", config, &[MARKER_BYTES], &[]);
}

/// A conversion to the standard layout run after an interrupted conversion from
/// the standard layout, instead of its rerun (outside the rules): its fork check
/// before its change finds the old standard `repo` and refuses, changing nothing,
/// so the rerun of the conversion from the standard layout can still finish.
#[test]
fn round_trip_mixed_conversions() {
    let config = Config { writers: 0, to_standard: true, from_standard: true, ..CRASHES };
    let stuck = format!("!{STUCK_ON_FORK}");
    check(
        "round_trip_mixed_conversions",
        config,
        &[],
        &[
            FORK_REPORT_INTERRUPTED,
            &stuck,
            REACH_FROM_STANDARD_RERUN,
            REACH_FROM_AFTER_TO,
            FROM_DONE_AFTER_REFUSAL,
        ],
    );
}

#[test]
fn round_trip_gc() {
    let config = Config {
        gc: true,
        to_standard: true,
        from_standard: true,
        standard_writes: 1,
        relist_bound: QUICK_RELISTS,
        ..FAULTS
    };
    check(
        "round_trip_gc",
        config,
        &[],
        &[REACH_FROM_AFTER_TO, REACH_GC_DELETES, REACH_STANDARD_UPDATES_CARRIED],
    );
}

/// Creation that crashes and runs again: the strict interrupted-creation rule
/// replaces the `repo` of the interrupted run.
#[test]
fn creator_crashes() {
    check("creator_crashes", Config { creators: 1, ..CRASHES }, &[], &[]);
}

/// Two creators: one replaces the other's `repo` as an interrupted creation
/// while the other wins the directory rename. The loser publishes the current
/// version before it reports "exists", so the mirror holds a version that was
/// never current only until then.
#[test]
fn two_creators() {
    check(
        "two_creators",
        Config { creators: 2, ..NOTHING },
        &[MIRROR_CURRENT],
        &[REACH_CREATOR_REPUBLISHES],
    );
}

/// Two creators, then a conversion to the standard layout, which waits for a
/// repository (on an empty root it fails with `ObjectNotFound`).
#[test]
fn creators_to_standard() {
    let config = Config { creators: 2, to_standard: true, ..NOTHING };
    check(
        "creators_to_standard",
        config,
        &[MIRROR_CURRENT],
        &[REACH_CREATOR_REPUBLISHES, REACH_TO_STANDARD_DONE],
    );
}

/// A creator that crashed after its directory rename runs again with a root
/// listing that misses `repo.d`, during an interrupted conversion to the
/// standard layout (outside the rules): it replaces `repo` as an interrupted
/// creation, loses the directory rename and publishes the current version,
/// without the feature, which the conversion's rerun publishes anyway. The
/// rerun with a stale root listing leaves `repo.d`, as in
/// `to_standard_stale_root_listing`, and every end-state violation is that one.
#[test]
fn creator_to_standard_stale_root_listings() {
    let config =
        Config { creators: 1, to_standard: true, stale_root_listings: true, ..CRASHES };
    check(
        "creator_to_standard_stale_root_listings",
        config,
        &[MIRROR_CURRENT, SAME_END_STATE],
        &[CREATOR_PUBLISHES_FEATURELESS, REACH_TO_STANDARD_DONE],
    );
}

// ── large configurations, `just fs-model-check` ─────────────────────────────

const ROUND_TRIP_REACHES: &[&str] = &[
    REACH_TO_STANDARD_DONE,
    REACH_FROM_AFTER_TO,
    REACH_FROM_STANDARD_RERUN,
    REACH_STANDARD_UPDATES_CARRIED,
];

/// Two writers, GC, the status set `ReadOnly` and back around a copy, crashes and
/// lost replies.
#[test]
#[ignore = "large state space; run `just fs-model-check`"]
fn slow_layout_read_only_copy() {
    let config = Config {
        writers: 2,
        gc: true,
        status: Some(StatusOps::ReadOnlyThenOnline),
        copier: true,
        ..CRASHES
    };
    check_large("slow_layout_read_only_copy", config, &[], &[REACH_GC_DELETES]);
}

/// The same with one writer and every fault.
#[test]
#[ignore = "large state space; run `just fs-model-check`"]
fn slow_layout_read_only_copy_stale_listings() {
    let config = Config {
        writers: 1,
        gc: true,
        status: Some(StatusOps::ReadOnlyThenOnline),
        copier: true,
        ..FAULTS
    };
    check_large(
        "slow_layout_read_only_copy_stale_listings",
        config,
        &[],
        &[REACH_GC_DELETES, COPY_MISSED_MARKER],
    );
}

/// Two writers and GC, every fault.
#[test]
#[ignore = "large state space; run `just fs-model-check`"]
fn slow_layout_mirror_faults() {
    let config = Config { writers: 2, gc: true, ..FAULTS };
    check_large("slow_layout_mirror_faults", config, &[], &[REACH_GC_DELETES]);
}

/// Two writers, GC and the old library: what the best-effort detection catches.
#[test]
#[ignore = "large state space; run `just fs-model-check`"]
fn slow_layout_fork_detection() {
    let config = Config { writers: 2, gc: true, old_writes: 1, ..CRASHES };
    check_large(
        "slow_layout_fork_detection",
        config,
        &[],
        &[ERASED_BY_WRITER, ERASED_BY_GC, DETECTED, REACH_GC_DELETES],
    );
}

/// A conversion to the standard layout next to two writers and GC, every fault.
#[test]
#[ignore = "large state space; run `just fs-model-check`"]
fn slow_layout_to_standard_everything() {
    let config = Config { writers: 2, gc: true, to_standard: true, ..FAULTS };
    check_large(
        "slow_layout_to_standard_everything",
        config,
        &[],
        &[
            REACH_TO_STANDARD_DONE,
            REACH_TO_STANDARD_STEP_3,
            REACH_TO_STANDARD_AFTER_RENAME,
            REACH_TO_STANDARD_RELIST,
            REACH_GC_DELETES,
        ],
    );
}

/// To the standard layout and back, next to two writers, every fault.
#[test]
#[ignore = "large state space; run `just fs-model-check`"]
fn slow_layout_round_trip_writers() {
    let config = Config {
        writers: 2,
        to_standard: true,
        from_standard: true,
        standard_writes: 1,
        ..FAULTS
    };
    check_large("slow_layout_round_trip_writers", config, &[], ROUND_TRIP_REACHES);
}

/// To the standard layout and back, next to a writer and GC, every fault.
#[test]
#[ignore = "large state space; run `just fs-model-check`"]
fn slow_layout_round_trip_gc() {
    let config = Config {
        writers: 1,
        gc: true,
        to_standard: true,
        from_standard: true,
        standard_writes: 1,
        ..FAULTS
    };
    check_large(
        "slow_layout_round_trip_gc",
        config,
        &[],
        &[ROUND_TRIP_REACHES, &[REACH_GC_DELETES]].concat(),
    );
}
