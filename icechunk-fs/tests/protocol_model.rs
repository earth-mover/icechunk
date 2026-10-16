//! Exhaustive model of the versioned repo-info directory protocol in
//! design-docs/020-filesystem-storage.md ("Algorithm").
//!
//! The filesystem is modelled by the spec's "Rules": a successful
//! `link`, `rename` or `unlink` happens on the server against the true state;
//! a listing may return any marker that ever existed (stronger than a real
//! client, whose listing is a mix of a few snapshots); reads of version files
//! see the true file state, because their names are never rewritten.
//!
//! A superseded or unproven attempt reports a conflict here: `layout_model.rs`
//! models the lost-reply check that can acknowledge it.

use stateright::{Checker, Model, Property, report::WriteReporter};
use std::collections::BTreeSet;
use std::io::Write;

#[path = "common/model.rs"]
mod model;

use model::{
    AttemptId, ChangeAttempt, ChangeChoice, ChangeStep, Dir, INITIAL_HASH, Marker, Name,
    Version, fresh_dir, step_change,
};

/// The writer's next step.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    /// `read_latest`: list, take the marker, read its bytes; or finish. A
    /// writer keeps attempting after it commits.
    Start,
    /// A step of a change attempt.
    Change(ChangeStep),
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
    base: Version,
    /// A reply was lost in this attempt, so a spurious conflict is allowed.
    lost_reply: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct State {
    dir: Dir,
    writers: Vec<Writer>,
    /// Versions the marker named, in order, after the initial one.
    history: Vec<Version>,
    /// Versions whose writer reported success.
    acked: BTreeSet<Version>,
    /// The marker GC's last listing showed, if it is sweeping.
    gc_marker: Option<Marker>,
    violation: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
enum Action {
    /// Writer `.0` takes its next step, with outcome `.1`.
    Step(usize, ChangeChoice),
    /// Writer `.0` crashes.
    Crash(usize),
    /// GC lists the directory and sees this marker.
    GcList(Marker),
    /// GC deletes a file its listing proved dead, or a staging file.
    GcDelete(Name),
}

#[derive(Clone, Copy, Debug)]
struct Config {
    writers: u8,
    max_attempts: u8,
    lost_replies: bool,
    crashes: bool,
    gc: bool,
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
    let version = |(n, h): Version| (n, hash(h));
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
        dir: Dir {
            files: state.dir.files.iter().map(name).collect(),
            marker: marker(&state.dir.marker),
            past: state.dir.past.iter().map(marker).collect(),
        },
        writers,
        history: state.history.iter().copied().map(version).collect(),
        acked: state.acked.iter().copied().map(version).collect(),
        gc_marker: state.gc_marker.as_ref().map(marker),
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
        vec![State {
            dir: fresh_dir(0, INITIAL_HASH),
            writers,
            history: vec![],
            acked: BTreeSet::new(),
            gc_marker: None,
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
                    actions.push(Action::Step(index, ChangeChoice::Normal));
                    if writer.attempts < self.config.max_attempts {
                        for marker in self.visible_markers(state) {
                            actions.push(Action::Step(index, ChangeChoice::Seen(marker)));
                        }
                    }
                }
                Phase::Change(ChangeStep::Stage | ChangeStep::TidyUp) => {
                    actions.push(Action::Step(index, ChangeChoice::Normal));
                }
                Phase::Change(ChangeStep::Link | ChangeStep::Rename) => {
                    actions.push(Action::Step(index, ChangeChoice::Normal));
                    if self.config.lost_replies {
                        actions.push(Action::Step(index, ChangeChoice::LostReply));
                    }
                }
                Phase::Change(ChangeStep::LostReplyCheck) => {
                    unreachable!("the protocol model leaves the lost-reply check out")
                }
                Phase::Change(ChangeStep::AfterFailedRename) => {
                    for marker in self.visible_markers(state) {
                        actions.push(Action::Step(index, ChangeChoice::Seen(marker)));
                    }
                    // A torn listing is a form of staleness.
                    if self.config.stale_listings {
                        actions.push(Action::Step(index, ChangeChoice::SeenNone));
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
            for name in &state.dir.files {
                let dead = match (name, &state.gc_marker) {
                    (Name::Stage(_), _) => true,
                    (Name::Content(n, h), Some(seen)) => {
                        *n < seen.n || (*n == seen.n && *h != seen.hash)
                    }
                    (Name::Content(..), None) => false,
                };
                if dead {
                    actions.push(Action::GcDelete(*name));
                }
            }
        }
    }

    fn next_state(&self, last: &State, action: Action) -> Option<State> {
        let mut state = last.clone();
        match action {
            Action::Crash(writer) => state.writers[writer].phase = Phase::Crashed,
            Action::GcList(marker) => state.gc_marker = Some(marker),
            Action::GcDelete(name) => {
                state.dir.files.remove(&name);
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
                state.dir.files.contains(&state.dir.marker.file())
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
        // writer, and GC is always enabled, which `eventually` can't handle.
        let config = self.config;
        if !config.stale_listings && !config.crashes && !config.gc {
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
    if !state.dir.move_marker(to) {
        state.violation = Some(format!("marker name used twice: {to:?}"));
    }
    state.history.push((to.n, to.hash));
}

impl ProtocolModel {
    /// The markers a listing may show.
    fn visible_markers(&self, state: &State) -> Vec<Marker> {
        state.dir.listing(self.config.stale_listings, &BTreeSet::new())
    }

    /// One atomic step of `writer`. Returns `None` when `choice` doesn't apply.
    fn step(&self, state: &mut State, writer: usize, choice: ChangeChoice) -> Option<()> {
        let before = state.writers[writer].clone();
        let me = &mut state.writers[writer];
        match (before.phase, choice) {
            // ── read_latest ─────────────────────────────────────────────────
            (Phase::Start, ChangeChoice::Normal) => {
                if before.attempts < self.config.max_attempts {
                    return None;
                }
                me.phase = Phase::Done;
            }
            (Phase::Start, ChangeChoice::Seen(seen)) => {
                if before.attempts >= self.config.max_attempts {
                    return None;
                }
                // A deleted version file makes read_latest list again.
                if !state.dir.files.contains(&seen.file()) {
                    return None;
                }
                me.attempt = (before.id, before.attempts);
                me.attempts += 1;
                me.base = seen.version();
                me.lost_reply = false;
                me.phase = Phase::Change(ChangeStep::Stage);
            }
            (Phase::Change(step), choice) => {
                return step_change(&mut WriterAttempt { state, writer }, step, choice);
            }
            _ => return None,
        }
        Some(())
    }
}

/// Writer `writer`'s change attempt in `state`. `repo.d` never goes away.
struct WriterAttempt<'a> {
    state: &'a mut State,
    writer: usize,
}

impl ChangeAttempt for WriterAttempt<'_> {
    fn dir(&self) -> Option<&Dir> {
        Some(&self.state.dir)
    }

    fn dir_mut(&mut self) -> Option<&mut Dir> {
        Some(&mut self.state.dir)
    }

    fn attempt(&self) -> (AttemptId, Version) {
        let me = &self.state.writers[self.writer];
        (me.attempt, me.base)
    }

    fn set_lost_reply(&mut self) {
        self.state.writers[self.writer].lost_reply = true;
    }

    fn set_step(&mut self, step: ChangeStep) {
        self.state.writers[self.writer].phase = Phase::Change(step);
    }

    fn change(&mut self, _source: Marker, target: Marker) {
        move_marker(self.state, target);
    }

    fn report_success(&mut self) {
        report_success(self.state, self.writer);
    }

    fn report_conflict(&mut self) {
        report_conflict(self.state, self.writer);
    }
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

/// Every fault with one attempt per writer: the quick form of
/// `two_writers_everything` for `just test`.
#[test]
fn two_writers_everything_one_attempt() {
    check(Config {
        max_attempts: 1,
        lost_replies: true,
        crashes: true,
        gc: true,
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
        stale_listings: false,
        ..PLAIN
    });
}

#[test]
#[ignore = "591,476 states, about 1 s in release; run `just fs-model-check` before merging protocol changes"]
fn two_writers_everything() {
    check(Config {
        lost_replies: true,
        crashes: true,
        gc: true,
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
        stale_listings: true,
    });
}

#[test]
#[ignore = "large state space; run `just fs-model-check` before merging protocol changes"]
fn three_writers_everything() {
    check(Config {
        writers: 3,
        max_attempts: 2,
        lost_replies: true,
        crashes: true,
        gc: true,
        stale_listings: true,
    });
}
