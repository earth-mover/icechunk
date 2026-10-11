//! Exhaustive model of the versioned repo-info directory protocol in
//! design-docs/020-filesystem-storage.md ("Algorithm").
//!
//! The filesystem is modelled by the spec's "Rules": a successful
//! `link`, `rename` or `unlink` happens on the server against the true state;
//! a listing may return any marker that ever existed (stronger than a real
//! client, whose listing is a mix of a few snapshots); reads of version files
//! see the true file state, because their names are never rewritten.

use stateright::{Checker, Model, Property, report::WriteReporter};
use std::collections::BTreeSet;
use std::io::Write;

/// (writer, attempt). Also the hash of the bytes that attempt writes.
type AttemptId = (u8, u8);
const INITIAL_HASH: AttemptId = (u8::MAX, 0);

/// A file in `repo.d`, other than the marker.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Name {
    /// `<N>.<h>`: the bytes of version `N`.
    Content(u8, AttemptId),
    /// `stage.<id>`: an attempt's staged bytes.
    Stage(AttemptId),
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Latest,
    Frozen,
}

/// `latest.<N>.<h>` or `frozen.<N>.<h>`.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Marker {
    kind: Kind,
    n: u8,
    hash: AttemptId,
}

/// The writer's next step.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    /// `read_latest`: list, take the marker, read its bytes; or finish. A
    /// writer keeps attempting after it commits.
    Start,
    /// Write and fsync the staging file.
    Stage,
    /// `link(stage, <N+1>.<h'>)`, fsync the directory, unlink the staging file.
    Link,
    /// `rename(latest.<N>.<h>, latest.<N+1>.<h'>)`: the commit point.
    Rename,
    /// The rename failed with `ENOENT`, or its reply was lost: list and decide.
    AfterFailedRename,
    /// The rename succeeded: delete every dead version file, report success.
    TidyUp,
    /// Out of attempts.
    Done,
    /// Stopped forever, leaving its files behind.
    Crashed,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Writer {
    id: u8,
    phase: Phase,
    attempts: u8,
    /// The hash of this attempt's bytes.
    attempt: AttemptId,
    /// The version this attempt replaces.
    base: (u8, AttemptId),
    /// A reply was lost in this attempt, so a spurious conflict is allowed.
    lost_reply: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct State {
    files: BTreeSet<Name>,
    marker: Marker,
    /// Every marker name that ever existed. What a stale listing may show,
    /// and what a new marker name must not repeat.
    past_markers: BTreeSet<Marker>,
    writers: Vec<Writer>,
    /// Versions the marker named, in order, after the initial one.
    history: Vec<(u8, AttemptId)>,
    /// Versions whose writer reported success.
    acked: BTreeSet<(u8, AttemptId)>,
    /// The marker GC's last listing showed, if it is sweeping.
    gc_marker: Option<Marker>,
    freezes: u8,
    violation: Option<String>,
}

/// The nondeterministic outcome of a writer's step, decided by the
/// filesystem. Each phase accepts only the choices that apply to it.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Choice {
    /// The operation runs and its reply arrives.
    Normal,
    /// The operation runs on the server, but its reply is lost.
    LostReply,
    /// The listing's marker is this one.
    Seen(Marker),
    /// The listing is torn and shows no marker.
    SeenNone,
}

#[derive(Clone, Debug, PartialEq)]
enum Action {
    /// Writer `.0` takes its next step, with outcome `.1`.
    Step(usize, Choice),
    /// Writer `.0` crashes.
    Crash(usize),
    /// GC lists the directory and sees this marker.
    GcList(Marker),
    /// GC deletes a file its listing proved dead, or a staging file.
    GcDelete(Name),
    /// An operator freezes the repo.
    Freeze,
    /// An operator unfreezes the repo.
    Unfreeze,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    writers: u8,
    max_attempts: u8,
    lost_replies: bool,
    crashes: bool,
    gc: bool,
    /// How many times the repo may be frozen.
    freezes: u8,
    /// Listings may show any past marker; otherwise only the current one.
    stale_listings: bool,
}

struct ProtocolModel {
    config: Config,
}

/// Resets fields that are dead in the writer's current phase, so equivalent
/// states compare equal.
fn normalize(state: &mut State) {
    for writer in &mut state.writers {
        if matches!(writer.phase, Phase::Start | Phase::Done | Phase::Crashed) {
            writer.attempt = INITIAL_HASH;
            writer.base = (0, INITIAL_HASH);
            writer.lost_reply = false;
        }
        if matches!(writer.phase, Phase::Done | Phase::Crashed) {
            writer.attempts = 0;
        }
    }
}

/// Writers are interchangeable: the representative of a state is the least
/// state reachable by permuting writer ids.
fn representative(state: &State) -> State {
    let ids: Vec<u8> = state.writers.iter().map(|writer| writer.id).collect();
    permutations(&ids)
        .iter()
        .map(|permutation| rename_writers(state, permutation))
        .min()
        .unwrap_or_else(|| state.clone())
}

fn permutations(ids: &[u8]) -> Vec<Vec<u8>> {
    let Some((last, rest)) = ids.split_last() else {
        return vec![vec![]];
    };
    permutations(rest)
        .into_iter()
        .flat_map(|shorter| {
            (0..=shorter.len()).map(move |position| {
                let mut longer = shorter.clone();
                longer.insert(position, *last);
                longer
            })
        })
        .collect()
}

/// Renames writer `id` to `permutation[id]` everywhere in `state`.
fn rename_writers(state: &State, permutation: &[u8]) -> State {
    let hash = |(writer, number): AttemptId| {
        if writer == INITIAL_HASH.0 {
            (writer, number)
        } else {
            (permutation[usize::from(writer)], number)
        }
    };
    let version = |(n, h): (u8, AttemptId)| (n, hash(h));
    let marker = |marker: &Marker| Marker { hash: hash(marker.hash), ..*marker };
    let name = |name: &Name| match name {
        Name::Content(n, h) => Name::Content(*n, hash(*h)),
        Name::Stage(id) => Name::Stage(hash(*id)),
    };
    let mut writers = state.writers.clone();
    for writer in &state.writers {
        let id = permutation[usize::from(writer.id)];
        writers[usize::from(id)] = Writer {
            id,
            attempt: hash(writer.attempt),
            base: version(writer.base),
            ..writer.clone()
        };
    }
    State {
        files: state.files.iter().map(name).collect(),
        marker: marker(&state.marker),
        past_markers: state.past_markers.iter().map(marker).collect(),
        writers,
        history: state.history.iter().copied().map(version).collect(),
        acked: state.acked.iter().copied().map(version).collect(),
        gc_marker: state.gc_marker.as_ref().map(marker),
        freezes: state.freezes,
        violation: state.violation.clone(),
    }
}

impl Model for ProtocolModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let writers = (0..self.config.writers)
            .map(|id| Writer {
                id,
                phase: Phase::Start,
                attempts: 0,
                attempt: INITIAL_HASH,
                base: (0, INITIAL_HASH),
                lost_reply: false,
            })
            .collect();
        let marker = Marker { kind: Kind::Latest, n: 0, hash: INITIAL_HASH };
        vec![State {
            files: BTreeSet::from([Name::Content(0, INITIAL_HASH)]),
            marker,
            past_markers: BTreeSet::from([marker]),
            writers,
            history: vec![],
            acked: BTreeSet::new(),
            gc_marker: None,
            freezes: 0,
            violation: None,
        }]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        if state.violation.is_some() {
            return;
        }
        for (index, writer) in state.writers.iter().enumerate() {
            match writer.phase {
                Phase::Done | Phase::Crashed => continue,
                Phase::Start => {
                    actions.push(Action::Step(index, Choice::Normal));
                    if writer.attempts < self.config.max_attempts {
                        for marker in self.visible_markers(state) {
                            actions.push(Action::Step(index, Choice::Seen(marker)));
                        }
                    }
                }
                Phase::Stage | Phase::TidyUp => {
                    actions.push(Action::Step(index, Choice::Normal));
                }
                Phase::Link | Phase::Rename => {
                    actions.push(Action::Step(index, Choice::Normal));
                    if self.config.lost_replies {
                        actions.push(Action::Step(index, Choice::LostReply));
                    }
                }
                Phase::AfterFailedRename => {
                    for marker in self.visible_markers(state) {
                        actions.push(Action::Step(index, Choice::Seen(marker)));
                    }
                    // A torn listing is a form of staleness.
                    if self.config.stale_listings {
                        actions.push(Action::Step(index, Choice::SeenNone));
                    }
                }
            }
            if self.config.crashes {
                actions.push(Action::Crash(index));
            }
        }
        if self.config.gc {
            for marker in self.visible_markers(state) {
                actions.push(Action::GcList(marker));
            }
            for name in &state.files {
                let dead = match (name, &state.gc_marker) {
                    (Name::Stage(_), _) => true,
                    (Name::Content(n, h), Some(seen)) => {
                        *n < seen.n || (*n == seen.n && *h != seen.hash)
                    }
                    (Name::Content(..), None) => false,
                };
                if dead {
                    actions.push(Action::GcDelete(name.clone()));
                }
            }
        }
        match state.marker.kind {
            Kind::Latest if state.freezes < self.config.freezes => {
                actions.push(Action::Freeze);
            }
            Kind::Frozen => actions.push(Action::Unfreeze),
            Kind::Latest => {}
        }
    }

    fn next_state(&self, last: &State, action: Action) -> Option<State> {
        let mut state = last.clone();
        match action {
            Action::Crash(writer) => state.writers[writer].phase = Phase::Crashed,
            Action::GcList(marker) => state.gc_marker = Some(marker),
            Action::GcDelete(name) => {
                state.files.remove(&name);
            }
            Action::Freeze => {
                let Marker { n, hash, .. } = state.marker;
                state.freezes += 1;
                move_marker(&mut state, Marker { kind: Kind::Frozen, n, hash });
            }
            Action::Unfreeze => {
                let Marker { n, hash, .. } = state.marker;
                // link(<n>.<h>, <n+1>.<h>): Ok, or EEXIST after a lost reply.
                state.files.insert(Name::Content(n + 1, hash));
                move_marker(&mut state, Marker { kind: Kind::Latest, n: n + 1, hash });
            }
            Action::Step(writer, choice) => self.step(&mut state, writer, choice)?,
        }
        normalize(&mut state);
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always("no violation", |_, state: &State| {
                state.violation.is_none()
            }),
            Property::always("the current marker has bytes", |_, state: &State| {
                state.files.contains(&Name::Content(state.marker.n, state.marker.hash))
            }),
            Property::always(
                "versions form a chain and acks are in it",
                |_, state: &State| {
                    let chain_ok = state
                        .history
                        .iter()
                        .enumerate()
                        .all(|(position, (n, _))| usize::from(*n) == position + 1);
                    chain_ok
                        && state.acked.iter().all(|acked| state.history.contains(acked))
                },
            ),
        ];
        // Stale listings can starve a writer forever, crashes can stop every
        // writer, and GC and freezing are always enabled, which `eventually`
        // can't handle.
        let config = self.config;
        if !config.stale_listings && !config.crashes && !config.gc && config.freezes == 0
        {
            properties
                .push(Property::eventually("some writer commits", |_, state: &State| {
                    !state.acked.is_empty()
                }));
        }
        properties
    }
}

/// Moves the marker, recording the name. A name used twice is a violation.
fn move_marker(state: &mut State, to: Marker) {
    if !state.past_markers.insert(to) {
        state.violation = Some(format!("marker name used twice: {to:?}"));
    }
    if to.kind == Kind::Latest {
        state.history.push((to.n, to.hash));
    }
    state.marker = to;
}

impl ProtocolModel {
    /// The markers a listing may show.
    fn visible_markers(&self, state: &State) -> Vec<Marker> {
        if self.config.stale_listings {
            state.past_markers.iter().copied().collect()
        } else {
            vec![state.marker]
        }
    }

    /// One atomic step of `writer`. Returns `None` when `choice` doesn't apply.
    fn step(&self, state: &mut State, writer: usize, choice: Choice) -> Option<()> {
        let before = state.writers[writer].clone();
        let (base_n, base_hash) = before.base;
        let content = Name::Content(base_n + 1, before.attempt);
        let me = &mut state.writers[writer];
        match (before.phase, choice) {
            // ── read_latest ─────────────────────────────────────────────────
            (Phase::Start, Choice::Normal) => {
                if before.attempts < self.config.max_attempts {
                    return None;
                }
                me.phase = Phase::Done;
            }
            (Phase::Start, Choice::Seen(seen)) => {
                if before.attempts >= self.config.max_attempts {
                    return None;
                }
                // A deleted version file makes read_latest list again.
                if !state.files.contains(&Name::Content(seen.n, seen.hash)) {
                    return None;
                }
                me.attempt = (before.id, before.attempts);
                me.attempts += 1;
                me.base = (seen.n, seen.hash);
                me.lost_reply = false;
                me.phase = Phase::Stage;
            }

            // ── stage ───────────────────────────────────────────────────────
            (Phase::Stage, Choice::Normal) => {
                state.files.insert(Name::Stage(before.attempt));
                me.phase = Phase::Link;
            }

            // ── link(stage, <N+1>.<h'>) ─────────────────────────────────────
            (Phase::Link, Choice::Normal | Choice::LostReply) => {
                if !state.files.contains(&Name::Stage(before.attempt)) {
                    // ENOENT: the staging file was swept.
                    report_conflict(state, writer);
                    return Some(());
                }
                // Ok, or EEXIST for our own earlier link: the name is ours.
                state.files.insert(content);
                if choice == Choice::LostReply {
                    // The client retries the link from the Link phase.
                    state.writers[writer].lost_reply = true;
                } else {
                    state.files.remove(&Name::Stage(before.attempt));
                    state.writers[writer].phase = Phase::Rename;
                }
            }

            // ── rename(latest.<N>.<h>, latest.<N+1>.<h'>) ───────────────────
            (Phase::Rename, Choice::Normal | Choice::LostReply) => {
                let source = Marker { kind: Kind::Latest, n: base_n, hash: base_hash };
                if state.marker == source {
                    let target = Marker {
                        kind: Kind::Latest,
                        n: base_n + 1,
                        hash: before.attempt,
                    };
                    move_marker(state, target);
                    if choice == Choice::LostReply {
                        state.writers[writer].lost_reply = true;
                        state.writers[writer].phase = Phase::AfterFailedRename;
                    } else {
                        state.writers[writer].phase = Phase::TidyUp;
                    }
                } else {
                    // ENOENT. A lost reply of a failed rename is just a failure.
                    if choice == Choice::LostReply {
                        return None;
                    }
                    state.writers[writer].phase = Phase::AfterFailedRename;
                }
            }

            // ── after_failed_rename ─────────────────────────────────────────
            (Phase::AfterFailedRename, Choice::Seen(seen)) => {
                if seen.n == base_n + 1 && seen.hash == before.attempt {
                    // Our rename landed and the reply was lost.
                    tidy_up(state, base_n + 1, before.attempt);
                    report_success(state, writer);
                } else if seen.n > base_n {
                    // Lost the race for N+1, or superseded: our file is dead.
                    state.files.remove(&content);
                    report_conflict(state, writer);
                } else if seen.kind == Kind::Frozen
                    && seen.n == base_n
                    && seen.hash == base_hash
                {
                    report_frozen(state, writer);
                } else {
                    // Ambiguous: the content file stays for the sweep.
                    report_conflict(state, writer);
                }
            }
            (Phase::AfterFailedRename, Choice::SeenNone) => {
                report_conflict(state, writer);
            }

            // ── tidy_up ─────────────────────────────────────────────────────
            (Phase::TidyUp, Choice::Normal) => {
                tidy_up(state, base_n + 1, before.attempt);
                report_success(state, writer);
            }

            _ => return None,
        }
        Some(())
    }
}

/// After a successful rename proved the marker is at `(n, hash)`: delete
/// every version file below `n`, and every other one at `n`.
fn tidy_up(state: &mut State, n: u8, hash: AttemptId) {
    state.files.retain(|name| match name {
        Name::Content(i, h) => !(*i < n || (*i == n && *h != hash)),
        Name::Stage(_) => true,
    });
}

/// `Updated`: the attempt's version must be in the chain.
fn report_success(state: &mut State, writer: usize) {
    let me = &state.writers[writer];
    let version = (me.base.0 + 1, me.attempt);
    if !state.history.contains(&version) {
        state.violation =
            Some(format!("writer {} acked {version:?}, which never landed", me.id));
    }
    state.acked.insert(version);
    state.writers[writer].phase = Phase::Start;
}

/// `NotOnLatestVersion`. Spurious only after a lost reply.
fn report_conflict(state: &mut State, writer: usize) {
    let me = &state.writers[writer];
    let version = (me.base.0 + 1, me.attempt);
    if state.history.contains(&version) && !me.lost_reply {
        state.violation = Some(format!(
            "writer {} reported a conflict for {version:?}, which landed",
            me.id
        ));
    }
    state.writers[writer].phase = Phase::Start;
}

/// `RepositoryFrozen`. Never for a commit that landed.
fn report_frozen(state: &mut State, writer: usize) {
    let me = &state.writers[writer];
    let version = (me.base.0 + 1, me.attempt);
    if state.history.contains(&version) {
        state.violation = Some(format!(
            "writer {} reported frozen for {version:?}, which landed",
            me.id
        ));
    }
    state.writers[writer].phase = Phase::Start;
}

/// Breadth-first for small configurations, for the shortest counterexamples.
/// Larger ones don't fit in memory that way, so they run depth-first, on all
/// cores, with symmetry reduction (only the depth-first checker supports it)
/// and progress reports.
fn check(config: Config) {
    let builder = ProtocolModel { config }.checker();
    if config.writers <= 2 {
        assert_checked(config, &builder.spawn_bfs().join());
    } else {
        let threads = std::thread::available_parallelism().map_or(1, usize::from);
        let checker = builder
            .threads(threads)
            .symmetry_fn(representative)
            .spawn_dfs()
            .join_and_report(&mut WriteReporter::new(&mut CapturedStdout));
        assert_checked(config, &checker);
    }
}

/// Writes through `print!`, so the test harness captures progress reports.
struct CapturedStdout;

impl Write for CapturedStdout {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        print!("{}", String::from_utf8_lossy(buf));
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn assert_checked(config: Config, checker: &impl Checker<ProtocolModel>) {
    println!(
        "{config:?}: {} unique states, max depth {}",
        checker.unique_state_count(),
        checker.max_depth()
    );
    checker.assert_properties();
}

const PLAIN: Config = Config {
    writers: 2,
    max_attempts: 2,
    lost_replies: false,
    crashes: false,
    gc: false,
    freezes: 0,
    stale_listings: false,
};

#[test]
fn two_writers_plain() {
    check(PLAIN);
}

#[test]
fn two_writers_lost_replies() {
    check(Config { lost_replies: true, ..PLAIN });
}

#[test]
fn two_writers_stale_listings() {
    check(Config { stale_listings: true, ..PLAIN });
}

#[test]
fn two_writers_freeze() {
    check(Config { freezes: 1, stale_listings: true, lost_replies: true, ..PLAIN });
}

/// Every fault with one attempt per writer: the quick form of
/// `two_writers_everything` for `just test`.
#[test]
fn two_writers_everything_one_attempt() {
    check(Config {
        max_attempts: 1,
        lost_replies: true,
        crashes: true,
        gc: true,
        freezes: 1,
        stale_listings: true,
        ..PLAIN
    });
}

/// Every fault but stale listings, with two attempts per writer so that GC
/// sees versions of earlier attempts: the other quick form of
/// `two_writers_everything` for `just test`.
#[test]
fn two_writers_everything_but_stale_listings() {
    check(Config {
        lost_replies: true,
        crashes: true,
        gc: true,
        freezes: 1,
        stale_listings: false,
        ..PLAIN
    });
}

#[test]
#[ignore = "about 47 s; run `just fs-model-check` before merging protocol changes"]
fn two_writers_everything() {
    check(Config {
        lost_replies: true,
        crashes: true,
        gc: true,
        freezes: 1,
        stale_listings: true,
        ..PLAIN
    });
}

#[test]
fn three_writers_one_attempt() {
    check(Config {
        writers: 3,
        max_attempts: 1,
        lost_replies: true,
        crashes: true,
        gc: true,
        freezes: 1,
        stale_listings: true,
    });
}

/// Everything but freezing, which `three_writers_one_attempt` and
/// `two_writers_everything` cover: the full configuration below is too large
/// to finish.
#[test]
#[ignore = "large state space; run `just fs-model-check` before merging protocol changes"]
fn three_writers_no_freeze() {
    check(Config {
        writers: 3,
        max_attempts: 2,
        lost_replies: true,
        crashes: true,
        gc: true,
        freezes: 0,
        stale_listings: true,
    });
}

#[test]
#[ignore = "too large to finish; kept to document the full configuration"]
fn three_writers_everything() {
    check(Config {
        writers: 3,
        max_attempts: 2,
        lost_replies: true,
        crashes: true,
        gc: true,
        freezes: 1,
        stale_listings: true,
    });
}
