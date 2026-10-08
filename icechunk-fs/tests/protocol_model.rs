//! Exhaustive model of the conditional-update protocol in
//! design-docs/020-filesystem-storage.md ("Algorithm detail", "Sweeping").

use stateright::{Checker, Model, Property, report::WriteReporter};
use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use std::io::Write;

/// (writer, attempt). Also the id of the content that attempt writes.
type AttemptId = (u8, u8);
const INITIAL_VERSION: AttemptId = (u8::MAX, 0);

/// A file in the repo root.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Name {
    /// `repo`, the repo info file.
    Repo,
    /// `repo.new_bytes.<attempt_id>`, the attempt's commit ticket.
    Ticket(AttemptId),
    /// `repo.lockinfo.<attempt_id>`, linked as the attempt's lock.
    LockInfo(AttemptId),
    /// `repo.lock.<h>.<gen>`: the hash of the base version, and the generation.
    Lock(AttemptId, u8),
}

/// The writer's next step.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    /// Read `repo` to get the base of a new attempt, or finish. A writer
    /// keeps attempting after it commits, so it can join the lock family of
    /// its own version.
    Start,
    /// `copy_object`: fresh read of `repo` and compare with the base.
    Backup,
    /// Step 1: write the lockinfo file.
    WriteLockInfo,
    /// Step 1: give up if the generation 0 lock exists and isn't stale.
    EarlyExit,
    /// Step 1: write the commit ticket.
    StageTicket,
    /// Step 2: link the lockinfo file to the lock of the current generation.
    Link,
    /// Step 2: the client's retry of a link whose reply was lost.
    LinkRetry,
    /// Step 2: the link failed with `EEXIST`; read the lock's owner.
    LinkEexist,
    /// Step 3: delete the ticket of the next lower generation's holder.
    Fence,
    /// Step 4: fresh read of `repo` and compare with the base.
    Compare,
    /// Step 5: rename the ticket over `repo`.
    Rename,
    /// Step 5: the rename failed with `ENOENT`; read `repo` to learn whether
    /// it landed.
    RenameEnoent,
    /// Step 6: delete the lockinfo file and the lock family, and report success.
    Tidy,
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
    attempt: AttemptId,
    base: AttemptId,
    generation: u8,
    held_generation: u8,
    next_to_fence: u8,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct State {
    /// File -> content: a version id, or the owner's attempt id for lock files.
    files: BTreeMap<Name, AttemptId>,
    writers: Vec<Writer>,
    /// Versions installed in `repo`, in order.
    history: Vec<AttemptId>,
    /// Version -> the base it was computed from.
    parent: BTreeMap<AttemptId, AttemptId>,
    /// Versions whose writer reported success.
    acked: BTreeSet<AttemptId>,
    /// Lock files (name, owner) in GC's latest listing whose hash differs from
    /// the `repo` it read next, and that still exist.
    gc_deletable: BTreeSet<(Name, AttemptId)>,
    violation: Option<String>,
}

/// The nondeterministic outcome of a writer's step that the filesystem or the
/// clock decides. Each phase accepts only the choices that apply to it.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Choice {
    /// The operation runs and its reply arrives.
    Normal,
    /// The operation runs on the server, but its reply is lost.
    LostReply,
    /// The existing lock looks alive: younger than `STALE_AFTER`.
    LockYoung,
    /// The existing lock looks dead: older than `STALE_AFTER`.
    LockStale,
}

#[derive(Clone, Debug, PartialEq)]
enum Action {
    /// Writer `.0` takes its next step, with outcome `.1`.
    Step(usize, Choice),
    /// Writer `.0` crashes.
    Crash(usize),
    /// GC deletes a file.
    GcDelete(Name),
    /// GC lists the lock files, then reads `repo`. Both happen in this one
    /// step, which allows every interleaving of "list, then read" and more.
    GcReadRepo,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    writers: u8,
    max_attempts: u8,
    lost_replies: bool,
    crashes: bool,
    gc: bool,
}

struct ProtocolModel {
    config: Config,
}

fn cleanup_and_restart(state: &mut State, writer: usize) {
    let attempt = state.writers[writer].attempt;
    state.files.remove(&Name::Ticket(attempt));
    state.files.remove(&Name::LockInfo(attempt));
    // Never deletes a lock file (spec: cleanup()).
    state.writers[writer].phase = Phase::Start;
}

/// Resets fields that are dead in the writer's current phase, so equivalent
/// states compare equal. Also drops lock files that are gone from GC's
/// listing: a lock file recreated under the same name is a new file, which
/// that listing doesn't include.
fn normalize(state: &mut State) {
    for writer in &mut state.writers {
        if matches!(writer.phase, Phase::Start | Phase::Done | Phase::Crashed) {
            writer.attempt = INITIAL_VERSION;
            writer.base = INITIAL_VERSION;
        }
        if !matches!(writer.phase, Phase::Link | Phase::LinkRetry | Phase::LinkEexist) {
            writer.generation = 0;
        }
        if writer.phase != Phase::Fence {
            writer.held_generation = 0;
            writer.next_to_fence = 0;
        }
        if matches!(writer.phase, Phase::Done | Phase::Crashed) {
            writer.attempts = 0;
        }
    }
    let files = &state.files;
    state.gc_deletable.retain(|(name, owner)| files.get(name) == Some(owner));
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
    let attempt = |(writer, number): AttemptId| {
        if writer == INITIAL_VERSION.0 {
            (writer, number)
        } else {
            (permutation[usize::from(writer)], number)
        }
    };
    let name = |name: &Name| match name {
        Name::Repo => Name::Repo,
        Name::Ticket(id) => Name::Ticket(attempt(*id)),
        Name::LockInfo(id) => Name::LockInfo(attempt(*id)),
        Name::Lock(hash, generation) => Name::Lock(attempt(*hash), *generation),
    };
    let mut writers = state.writers.clone();
    for writer in &state.writers {
        let id = permutation[usize::from(writer.id)];
        writers[usize::from(id)] = Writer {
            id,
            attempt: attempt(writer.attempt),
            base: attempt(writer.base),
            ..writer.clone()
        };
    }
    State {
        files: state
            .files
            .iter()
            .map(|(file, content)| (name(file), attempt(*content)))
            .collect(),
        writers,
        history: state.history.iter().copied().map(attempt).collect(),
        parent: state
            .parent
            .iter()
            .map(|(version, base)| (attempt(*version), attempt(*base)))
            .collect(),
        acked: state.acked.iter().copied().map(attempt).collect(),
        gc_deletable: state
            .gc_deletable
            .iter()
            .map(|(lock, owner)| (name(lock), attempt(*owner)))
            .collect(),
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
                attempt: INITIAL_VERSION,
                base: INITIAL_VERSION,
                generation: 0,
                held_generation: 0,
                next_to_fence: 0,
            })
            .collect();
        vec![State {
            files: BTreeMap::from([(Name::Repo, INITIAL_VERSION)]),
            writers,
            history: vec![],
            parent: BTreeMap::new(),
            acked: BTreeSet::new(),
            gc_deletable: BTreeSet::new(),
            violation: None,
        }]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        if state.violation.is_some() {
            return;
        }
        for (index, writer) in state.writers.iter().enumerate() {
            if matches!(writer.phase, Phase::Done | Phase::Crashed) {
                continue;
            }
            for choice in
                [Choice::Normal, Choice::LostReply, Choice::LockYoung, Choice::LockStale]
            {
                actions.push(Action::Step(index, choice));
            }
            if self.config.crashes {
                actions.push(Action::Crash(index));
            }
        }
        if self.config.gc {
            for (name, content) in &state.files {
                match name {
                    // Deleted at any age (stronger than the spec's GC cutoff).
                    Name::Ticket(_) | Name::LockInfo(_) => {
                        actions.push(Action::GcDelete(name.clone()));
                    }
                    // Only locks GC listed before its read of `repo`, with a hash
                    // other than the one it read.
                    Name::Lock(..)
                        if state.gc_deletable.contains(&(name.clone(), *content)) =>
                    {
                        actions.push(Action::GcDelete(name.clone()));
                    }
                    _ => {}
                }
            }
            actions.push(Action::GcReadRepo);
        }
    }

    fn next_state(&self, last: &State, action: Action) -> Option<State> {
        let mut state = last.clone();
        match action {
            Action::Crash(writer) => state.writers[writer].phase = Phase::Crashed,
            Action::GcReadRepo => {
                let seen = state.files[&Name::Repo];
                state.gc_deletable = state
                    .files
                    .iter()
                    .filter(
                        |(name, _)| matches!(name, Name::Lock(hash, _) if *hash != seen),
                    )
                    .map(|(name, owner)| (name.clone(), *owner))
                    .collect();
            }
            Action::GcDelete(name) => {
                state.files.remove(&name);
            }
            Action::Step(writer, choice) => self.step(&mut state, writer, choice)?,
        }
        normalize(&mut state);
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always("no lost update", |_, state: &State| {
                state.violation.is_none()
            }),
            // Without power loss in the model, this is "no lost update" plus
            // `acked` ⊆ `history`.
            Property::always(
                "acknowledged commits are never undone",
                |_, state: &State| {
                    let chain_ok =
                        state.history.iter().enumerate().all(|(position, version)| {
                            let expected_base = if position == 0 {
                                INITIAL_VERSION
                            } else {
                                state.history[position - 1]
                            };
                            state.parent.get(version) == Some(&expected_base)
                        });
                    chain_ok
                        && state.acked.iter().all(|acked| state.history.contains(acked))
                },
            ),
        ];
        // With GC there are no terminal states (`GcReadRepo` is always
        // enabled), which `eventually` needs.
        if !self.config.crashes && !self.config.gc {
            properties
                .push(Property::eventually("some writer commits", |_, state: &State| {
                    !state.history.is_empty()
                }));
        }
        properties
    }
}

impl ProtocolModel {
    /// One atomic step of `writer`. Returns `None` when `choice` doesn't apply.
    fn step(&self, state: &mut State, writer: usize, choice: Choice) -> Option<()> {
        let before = state.writers[writer].clone();
        let lost = choice == Choice::LostReply;
        if lost && !self.config.lost_replies {
            return None;
        }
        let only_normal = |choice: Choice| (choice == Choice::Normal).then_some(());
        match before.phase {
            Phase::Start => {
                only_normal(choice)?;
                if before.attempts == self.config.max_attempts {
                    state.writers[writer].phase = Phase::Done;
                } else {
                    let attempts = before.attempts + 1;
                    let base = state.files[&Name::Repo];
                    let current = &mut state.writers[writer];
                    current.attempts = attempts;
                    current.attempt = (before.id, attempts);
                    current.base = base;
                    current.phase = Phase::Backup;
                }
            }
            Phase::Backup => {
                only_normal(choice)?;
                if state.files[&Name::Repo] == before.base {
                    state.writers[writer].phase = Phase::WriteLockInfo;
                } else {
                    cleanup_and_restart(state, writer);
                }
            }
            Phase::WriteLockInfo => {
                only_normal(choice)?;
                state.files.insert(Name::LockInfo(before.attempt), before.attempt);
                state.writers[writer].phase = Phase::EarlyExit;
            }
            Phase::EarlyExit => {
                let first_lock = Name::Lock(before.base, 0);
                match (state.files.contains_key(&first_lock), choice) {
                    (true, Choice::LockYoung) => cleanup_and_restart(state, writer),
                    (true, Choice::LockStale) | (false, Choice::Normal) => {
                        state.writers[writer].phase = Phase::StageTicket;
                    }
                    _ => return None,
                }
            }
            Phase::StageTicket => {
                only_normal(choice)?;
                state.files.insert(Name::Ticket(before.attempt), before.attempt);
                let current = &mut state.writers[writer];
                current.generation = 0;
                current.phase = Phase::Link;
            }
            // A lost reply means the link landed and the client retries it later
            // (`LinkRetry`), which re-executes on the server.
            Phase::Link | Phase::LinkRetry => {
                if !matches!(choice, Choice::Normal | Choice::LostReply) {
                    return None;
                }
                let lock = Name::Lock(before.base, before.generation);
                if !state.files.contains_key(&Name::LockInfo(before.attempt)) {
                    only_normal(choice)?;
                    cleanup_and_restart(state, writer); // our lockinfo was swept
                } else if let Entry::Vacant(slot) = state.files.entry(lock) {
                    slot.insert(before.attempt);
                    let current = &mut state.writers[writer];
                    if lost {
                        current.phase = Phase::LinkRetry;
                    } else {
                        current.held_generation = before.generation;
                        current.next_to_fence = 0;
                        current.phase = Phase::Fence;
                    }
                } else {
                    only_normal(choice)?;
                    state.writers[writer].phase = Phase::LinkEexist;
                }
            }
            Phase::LinkEexist => {
                let owner =
                    state.files.get(&Name::Lock(before.base, before.generation)).copied();
                match (owner, choice) {
                    (None, Choice::Normal) => cleanup_and_restart(state, writer),
                    (Some(owner), Choice::Normal) if owner == before.attempt => {
                        let current = &mut state.writers[writer];
                        current.held_generation = before.generation;
                        current.next_to_fence = 0;
                        current.phase = Phase::Fence;
                    }
                    (Some(owner), Choice::LockYoung) if owner != before.attempt => {
                        cleanup_and_restart(state, writer);
                    }
                    (Some(owner), Choice::LockStale) if owner != before.attempt => {
                        let current = &mut state.writers[writer];
                        current.generation += 1;
                        current.phase = Phase::Link;
                    }
                    _ => return None,
                }
            }
            // Reading the owner and unlinking its ticket are one step: lock
            // content never changes and ticket names are never reused.
            Phase::Fence => {
                only_normal(choice)?;
                if before.next_to_fence < before.held_generation {
                    let lower = Name::Lock(before.base, before.next_to_fence);
                    if let Some(owner) = state.files.get(&lower).copied() {
                        state.files.remove(&Name::Ticket(owner));
                    }
                    state.writers[writer].next_to_fence += 1;
                } else {
                    state.writers[writer].phase = Phase::Compare;
                }
            }
            Phase::Compare => {
                only_normal(choice)?;
                if state.files[&Name::Repo] == before.base {
                    state.writers[writer].phase = Phase::Rename;
                } else {
                    cleanup_and_restart(state, writer);
                }
            }
            Phase::Rename => {
                if !matches!(choice, Choice::Normal | Choice::LostReply) {
                    return None;
                }
                if let Some(content) = state.files.remove(&Name::Ticket(before.attempt)) {
                    let replaced = state.files[&Name::Repo];
                    if replaced != before.base {
                        state.violation = Some(format!(
                            "writer {} replaced {replaced:?}, base was {:?}",
                            before.id, before.base
                        ));
                    }
                    state.files.insert(Name::Repo, content);
                    state.history.push(content);
                    state.parent.insert(content, before.base);
                    // The retry after a lost reply always sees ENOENT: nothing
                    // recreates our ticket.
                    state.writers[writer].phase =
                        if lost { Phase::RenameEnoent } else { Phase::Tidy };
                } else {
                    only_normal(choice)?;
                    state.writers[writer].phase = Phase::RenameEnoent;
                }
            }
            Phase::RenameEnoent => {
                only_normal(choice)?;
                if state.files[&Name::Repo] == before.attempt {
                    state.writers[writer].phase = Phase::Tidy;
                } else {
                    cleanup_and_restart(state, writer);
                }
            }
            Phase::Tidy => {
                only_normal(choice)?;
                state.files.remove(&Name::LockInfo(before.attempt));
                state
                    .files
                    .retain(|name, _| !matches!(name, Name::Lock(hash, _) if *hash == before.base));
                state.acked.insert(before.attempt);
                state.writers[writer].phase = Phase::Start;
            }
            Phase::Done | Phase::Crashed => return None,
        }
        Some(())
    }
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

#[test]
fn two_writers_plain() {
    check(Config {
        writers: 2,
        max_attempts: 2,
        lost_replies: false,
        crashes: false,
        gc: false,
    });
}

#[test]
fn two_writers_lost_replies() {
    check(Config {
        writers: 2,
        max_attempts: 2,
        lost_replies: true,
        crashes: false,
        gc: false,
    });
}

#[test]
fn two_writers_everything() {
    check(Config {
        writers: 2,
        max_attempts: 2,
        lost_replies: true,
        crashes: true,
        gc: true,
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
    });
}

/// Takes over 10 minutes and about 19 GB of RAM (12m46s on 32 threads, release).
#[test]
#[ignore = "large state space; run `just fs-model-check` before merging protocol changes"]
fn three_writers_everything() {
    check(Config {
        writers: 3,
        max_attempts: 2,
        lost_replies: true,
        crashes: true,
        gc: true,
    });
}
