#![cfg(unix)]
#![expect(clippy::unwrap_used, clippy::panic, reason = "test helpers")]

use std::{
    collections::BTreeSet,
    fs,
    path::Path,
    time::{Duration, Instant, SystemTime},
};

use chrono::{DateTime, Utc};
use icechunk_fs_stress::{
    config::{Mode, RunConfig, WorkerId},
    control::{Control, WaitOutcome, register, wait_for_go, write_control},
    env_info::{HostDone, HostInfo, host_info},
    layout::RunRoot,
    log::{LogWriter, Op, OpResult, Record, read_log},
    oracle::Violation,
    recovery::recovery_worker,
    report::Report,
    storage_mode, supervisor,
    verify::verify,
};

const POLL: Duration = Duration::from_millis(10);

fn no_faults(mode: Mode, hosts: u32) -> RunConfig {
    let mut config = RunConfig::defaults(mode, hosts);
    config.faults.kill_probability = 0.0;
    config.faults.pause_probability = 0.0;
    config.external_kill_every = None;
    config
}

fn unix_ms(t: SystemTime) -> i64 {
    DateTime::<Utc>::from(t).timestamp_millis()
}

fn write_done(root: &RunRoot, tag: &str, go_seen_at: SystemTime, stopped_at: SystemTime) {
    let done = HostDone {
        tag: tag.to_owned(),
        go_seen_at_ms: unix_ms(go_seen_at),
        stopped_at_ms: unix_ms(stopped_at),
        workers: 1,
        incarnations_spawned: 1,
        external_kills: 0,
        stragglers: 0,
        sampler_error: None,
    };
    fs::write(root.host_done(tag), serde_json::to_vec(&done).unwrap()).unwrap();
}

fn harness_details(report: &Report) -> Vec<&str> {
    report
        .verdict
        .violations
        .iter()
        .map(|violation| match violation {
            Violation::Harness { detail } => detail.as_str(),
            other => panic!("unexpected violation {other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn missing_done_entries_are_harness_errors_and_leave_the_repository_alone() {
    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().join("run"));
    supervisor::init(&root, &no_faults(Mode::Storage, 2)).await.unwrap();
    for tag in ["a", "b"] {
        register(&root, &host_info(tag, &root.storage())).unwrap();
    }
    fs::write(root.go(), "a\nb\n").unwrap();
    let participants = BTreeSet::from(["a".to_owned(), "b".to_owned()]);
    write_control(&root, &Control::Go { participants }).unwrap();
    // One change by a's worker: verify requires a landed operation.
    {
        use std::sync::Arc;

        use icechunk_fs_stress::storage_mode::{Attempt, Updater};
        let config = no_faults(Mode::Storage, 2);
        let who = WorkerId { tag: "a".to_owned(), slot: "0".to_owned(), incarnation: 0 };
        let log = LogWriter::create(&root.worker_log("a", "0", 0)).unwrap();
        let storage = storage_mode::storage(&root, &config, None).await.unwrap();
        let mut updater =
            Updater::new(Arc::clone(&storage), &who, &config).await.unwrap();
        let attempt = updater.attempt(&log).await.unwrap();
        assert!(matches!(attempt, Attempt::Logged(OpResult::Ack { .. })), "{attempt:?}");
    }
    let now = SystemTime::now();
    write_done(&root, "a", now, now);
    // A live writer's staging file, as far as verify can tell.
    let stage = root.storage().join(format!("repo.d/stage.{}", uuid::Uuid::new_v4()));
    fs::write(&stage, b"t").unwrap();

    let started = Instant::now();
    let report = verify(&root, Duration::from_secs(1)).await.unwrap();
    assert!(started.elapsed() >= Duration::from_secs(1));
    let details = harness_details(&report);
    assert!(
        matches!(&details[..], [detail] if detail.contains("done/") && detail.ends_with(": b")),
        "{details:?}"
    );
    assert_eq!(report.exit_code(), 3);
    assert_eq!(report.done.len(), 1);
    assert_eq!(report.hosts.len(), 2);
    assert_eq!(report.participants.tags.len(), 2);
    assert!(report.summary().contains(" 2 host(s)"), "{}", report.summary());
    assert_eq!(report.recovery_ms, None);
    assert!(
        report.summary().contains("skipped: hosts still missing: b"),
        "{}",
        report.summary()
    );
    assert_eq!(Report::load(&root).unwrap(), report);
    let who = recovery_worker();
    assert!(!root.worker_log(&who.tag, &who.slot, who.incarnation).exists());
    let age = fs::metadata(&stage).unwrap().modified().unwrap().elapsed().unwrap();
    assert!(age < Duration::from_secs(3600), "{age:?}");

    // Once the late host finished, the run verifies in full.
    write_done(&root, "b", now, now);
    let report = verify(&root, Duration::from_secs(1)).await.unwrap();
    assert!(report.passed, "{}", report.summary());
    assert!(report.recovery_ms.is_some());
    assert!(!stage.exists());
}

#[tokio::test]
async fn violations_with_hosts_missing_are_provisional() {
    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().join("run"));
    supervisor::init(&root, &no_faults(Mode::Storage, 2)).await.unwrap();
    for tag in ["a", "b"] {
        register(&root, &host_info(tag, &root.storage())).unwrap();
    }
    fs::write(root.go(), "a\nb\n").unwrap();
    let participants = BTreeSet::from(["a".to_owned(), "b".to_owned()]);
    write_control(&root, &Control::Go { participants }).unwrap();
    let now = SystemTime::now();
    write_done(&root, "a", now, now);
    // An ack that isn't on the chain, as a still-running host could log one
    // after verify read `repo`.
    let log = LogWriter::create(&root.worker_log("b", "0", 0)).unwrap();
    let id = uuid::Uuid::new_v4();
    let op = Op::Cas {
        base: "e0".to_owned(),
        new: "e1".to_owned(),
        backup: "repo.1.X".to_owned(),
    };
    log.append(&Record::Intent { t_ms: unix_ms(now), id, op }).unwrap();
    let ack = OpResult::Ack { snapshot: None };
    log.append(&Record::Outcome { t_ms: unix_ms(now), id, result: ack }).unwrap();

    let report = verify(&root, Duration::from_secs(1)).await.unwrap();
    assert!(
        report
            .verdict
            .violations
            .iter()
            .any(|v| matches!(v, Violation::LostUpdate { .. })),
        "{}",
        report.summary()
    );
    assert_eq!(report.participants.missing, ["b"]);
    assert_eq!(report.exit_code(), 3, "{}", report.summary());
    assert!(
        report.summary().contains(
            "provisional: hosts still missing (b); verify again once they finished"
        ),
        "{}",
        report.summary()
    );

    write_done(&root, "b", now, now);
    let report = verify(&root, Duration::from_secs(1)).await.unwrap();
    assert_eq!(report.participants.missing, Vec::<String>::new());
    assert_eq!(report.exit_code(), 1, "{}", report.summary());
    assert!(!report.summary().contains("provisional"), "{}", report.summary());
}

#[tokio::test]
async fn a_missing_repo_is_broken_history_in_a_written_report() {
    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().join("run"));
    supervisor::init(&root, &no_faults(Mode::Storage, 2)).await.unwrap();
    for tag in ["a", "b"] {
        register(&root, &host_info(tag, &root.storage())).unwrap();
    }
    fs::write(root.go(), "a\nb\n").unwrap();
    let participants = BTreeSet::from(["a".to_owned(), "b".to_owned()]);
    write_control(&root, &Control::Go { participants }).unwrap();
    let now = SystemTime::now();
    // `b` stays missing, so verify skips the recovery write, which would wait
    // out its whole deadline without a `repo`.
    write_done(&root, "a", now, now);
    fs::remove_dir_all(root.storage().join("repo.d")).unwrap();

    let report = verify(&root, Duration::from_millis(100)).await.unwrap();
    assert!(
        report.verdict.violations.iter().any(|v| matches!(
            v,
            Violation::BrokenHistory { detail } if detail.contains("repo")
        )),
        "{}",
        report.summary()
    );
    assert_eq!(Report::load(&root).unwrap(), report);
}

#[tokio::test]
async fn a_run_that_never_started_is_a_harness_error() {
    for stopped in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().join("run"));
        supervisor::init(&root, &no_faults(Mode::Storage, 1)).await.unwrap();
        if stopped {
            write_control(&root, &Control::Stop).unwrap();
        }
        let report = verify(&root, Duration::from_secs(600)).await.unwrap();
        let details = harness_details(&report);
        assert!(
            matches!(&details[..], [detail] if detail.contains("never started")),
            "{details:?}"
        );
        assert_eq!(report.recovery_ms, None);
        assert_eq!(Report::load(&root).unwrap(), report);
    }
}

#[tokio::test]
async fn verify_needs_an_initialized_run() {
    let dir = tempfile::tempdir().unwrap();
    assert!(verify(&RunRoot::new(dir.path().to_path_buf()), POLL).await.is_err());
}

fn has_ack(root: &RunRoot, who: &WorkerId) -> bool {
    read_log(&root.worker_log(&who.tag, &who.slot, who.incarnation)).is_ok_and(
        |records| {
            records.iter().any(|r| {
                matches!(r, Record::Outcome { result: OpResult::Ack { .. }, .. })
            })
        },
    )
}

/// Every file below `dir`, with `hosts/*.json`'s mount line blanked.
fn file_contents(dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            file_contents(&path, out);
            continue;
        }
        let mut bytes = fs::read(&path).unwrap();
        if path.parent().unwrap().ends_with("hosts") {
            let mut info: HostInfo = serde_json::from_slice(&bytes).unwrap();
            info.mount_line = None;
            bytes = serde_json::to_vec(&info).unwrap();
        }
        out.push((path.display().to_string(), bytes));
    }
}

#[tokio::test]
async fn a_run_root_reached_through_an_alias_path_verifies() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real");
    let alias = dir.path().join("alias");
    let config = no_faults(Mode::Storage, 1);
    supervisor::init(&RunRoot::new(real.clone()), &config).await.unwrap();
    std::os::unix::fs::symlink(&real, &alias).unwrap();

    let root = RunRoot::new(alias);
    let WaitOutcome::Participant { go_seen_at, .. } =
        wait_for_go(&root, "a", 1, POLL).unwrap()
    else {
        panic!("not a participant");
    };
    let who = WorkerId { tag: "a".to_owned(), slot: "0".to_owned(), incarnation: 0 };
    let worker = tokio::spawn({
        let (root, config, who) = (root.clone(), config.clone(), who.clone());
        async move {
            storage_mode::worker(&root, &config, &who, go_seen_at + config.duration).await
        }
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    while !has_ack(&root, &who) {
        assert!(Instant::now() < deadline, "no ack within 60 s");
        tokio::time::sleep(POLL).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    write_control(&root, &Control::Stop).unwrap();
    worker.await.unwrap().unwrap();
    write_done(&root, "a", go_seen_at, SystemTime::now());

    let report = verify(&root, Duration::from_secs(5)).await.unwrap();
    assert!(report.passed, "{}", report.summary());
    assert_eq!(report.exit_code(), 0);
    assert!(report.verdict.stats.acks > 0, "{}", report.summary());
    assert!(report.recovery_ms.is_some());

    let mut files = Vec::new();
    file_contents(&real, &mut files);
    let dir_path = dir.path().display().to_string();
    for (path, bytes) in &files {
        let content = String::from_utf8_lossy(bytes);
        assert!(!content.contains(&dir_path), "{path} contains {dir_path}");
    }

    let err = verify(&root, Duration::from_secs(5)).await.unwrap_err();
    assert!(format!("{err:#}").contains("already verified"), "{err:#}");
}

/// Rule M2: verify publishes the mirror before checking it, so a mirror that
/// a publisher left behind is repaired, and the publication is logged.
#[tokio::test]
async fn verify_publishes_a_lagging_mirror_before_checking_it() {
    use std::sync::Arc;

    use icechunk_fs_stress::storage_mode::{Attempt, Updater};

    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().join("run"));
    let config = no_faults(Mode::Storage, 1);
    supervisor::init(&root, &config).await.unwrap();
    register(&root, &host_info("a", &root.storage())).unwrap();
    fs::write(root.go(), "a\n").unwrap();
    let participants = BTreeSet::from(["a".to_owned()]);
    write_control(&root, &Control::Go { participants }).unwrap();
    let mirror = root.storage().join(storage_mode::REPO_KEY);
    let initial = fs::read(&mirror).unwrap();

    let who = WorkerId { tag: "a".to_owned(), slot: "0".to_owned(), incarnation: 0 };
    let log = LogWriter::create(&root.worker_log("a", "0", 0)).unwrap();
    let storage = storage_mode::storage(&root, &config, None).await.unwrap();
    let mut updater = Updater::new(Arc::clone(&storage), &who, &config).await.unwrap();
    let attempt = updater.attempt(&log).await.unwrap();
    assert!(matches!(attempt, Attempt::Logged(OpResult::Ack { .. })), "{attempt:?}");
    // As a publisher that died after its change, then a slow one, leave it.
    fs::write(&mirror, &initial).unwrap();
    let now = SystemTime::now();
    write_done(&root, "a", now, now);

    let report = verify(&root, Duration::from_secs(5)).await.unwrap();
    assert!(report.passed, "{}", report.summary());
    assert_ne!(fs::read(&mirror).unwrap(), initial);
    let published = read_log(&root.worker_log("verify", "publish", 0)).unwrap();
    assert!(
        published.iter().any(|record| matches!(record, Record::MirrorRename { .. })),
        "{published:?}"
    );
}

/// Registers host `a` and starts a one-host run.
fn start(root: &RunRoot) {
    register(root, &host_info("a", &root.storage())).unwrap();
    fs::write(root.go(), "a\n").unwrap();
    let participants = BTreeSet::from(["a".to_owned()]);
    write_control(root, &Control::Go { participants }).unwrap();
}

/// A verify that stopped after its publication, before its recovery write,
/// runs again with its next publication under its own log; one that made its
/// recovery write was the run's full check.
#[tokio::test]
async fn a_verify_interrupted_before_its_recovery_write_runs_again() {
    use std::sync::Arc;

    use icechunk_fs_stress::storage_mode::{Attempt, Updater};

    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().join("run"));
    let config = no_faults(Mode::Storage, 1);
    supervisor::init(&root, &config).await.unwrap();
    start(&root);
    let mirror = root.storage().join(storage_mode::REPO_KEY);
    let initial = fs::read(&mirror).unwrap();
    let who = WorkerId { tag: "a".to_owned(), slot: "0".to_owned(), incarnation: 0 };
    let log = LogWriter::create(&root.worker_log("a", "0", 0)).unwrap();
    let storage = storage_mode::storage(&root, &config, None).await.unwrap();
    let mut updater = Updater::new(Arc::clone(&storage), &who, &config).await.unwrap();
    let attempt = updater.attempt(&log).await.unwrap();
    assert!(matches!(attempt, Attempt::Logged(OpResult::Ack { .. })), "{attempt:?}");
    let now = SystemTime::now();
    write_done(&root, "a", now, now);
    // What the interrupted verify left: its publication's log, which
    // renamed nothing, so the mirror still lags.
    LogWriter::create(&root.worker_log("verify", "publish", 0)).unwrap();
    fs::write(&mirror, &initial).unwrap();

    let report = verify(&root, Duration::from_secs(5)).await.unwrap();
    assert!(report.passed, "{}", report.summary());
    assert!(report.recovery_ms.is_some());
    let published = read_log(&root.worker_log("verify", "publish", 1)).unwrap();
    assert!(
        published.iter().any(|record| matches!(record, Record::MirrorRename { .. })),
        "{published:?}"
    );

    let err = verify(&root, Duration::from_secs(5)).await.unwrap_err();
    assert!(format!("{err:#}").contains("already verified"), "{err:#}");
}

/// A repository-mode run with deletions whose status toggler died during a
/// hold: verify sets the status `Online` and the run passes in full, M7,
/// the publication, the recovery write, the canaries and GC included.
#[tokio::test]
async fn a_repository_run_left_read_only_is_set_online_and_verified() {
    use std::sync::Arc;

    use chrono::Utc;
    use icechunk::format::repo_info::{RepoAvailability, RepoStatus};
    use icechunk_fs_stress::{
        config::{DEFAULT_READONLY_HOLD, ReadOnlyConfig},
        log::start_worker_log,
        repo_mode::{RepoWorker, open_repo},
        toggler::TOGGLER_REASON,
    };

    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().join("run"));
    let mut config = no_faults(Mode::Repository, 1);
    config.readonly = Some(ReadOnlyConfig {
        every: Duration::from_secs(10),
        hold: DEFAULT_READONLY_HOLD,
    });
    supervisor::init(&root, &config).await.unwrap();
    start(&root);
    let who = WorkerId { tag: "a".to_owned(), slot: "0".to_owned(), incarnation: 0 };
    let log = Arc::new(start_worker_log(&root, &who).unwrap());
    let repo = open_repo(&root, &config, None).await.unwrap();
    let mut worker = RepoWorker::new(repo, log, &who, config.chunk_bytes).unwrap();
    let acked = |result: Option<OpResult>| matches!(result, Some(OpResult::Ack { .. }));
    assert!(worker.ensure_branch().await.unwrap());
    assert!(acked(worker.branch_commit().await.unwrap()));
    assert!(acked(worker.tag().await.unwrap()));
    assert!(acked(worker.branch_commit().await.unwrap()));
    assert!(acked(worker.delete_tag().await.unwrap()));
    assert!(acked(Some(worker.delete_branch().await.unwrap())));
    assert!(worker.ensure_branch().await.unwrap());
    assert!(acked(worker.branch_commit().await.unwrap()));
    assert!(acked(worker.main_commit().await.unwrap()));
    let read_only = RepoStatus {
        availability: RepoAvailability::ReadOnly,
        set_at: Utc::now(),
        limited_availability_reason: Some(TOGGLER_REASON.to_owned()),
    };
    let repo = open_repo(&root, &config, None).await.unwrap();
    repo.set_status(&read_only).await.unwrap();
    let now = SystemTime::now();
    write_done(&root, "a", now, now);

    let report = verify(&root, Duration::from_secs(5)).await.unwrap();
    assert!(report.passed, "{}", report.summary());
    assert!(report.recovery_ms.is_some());
    assert!(
        report
            .notes
            .iter()
            .any(|note| note.contains("ReadOnly") && note.contains("Online")),
        "{:?}",
        report.notes
    );
    let repo = open_repo(&root, &config, None).await.unwrap();
    assert_eq!(repo.get_status().await.unwrap().availability, RepoAvailability::Online);
}

/// A one-host run with the fork actor configured, after one change.
async fn forked_run(mode: Mode) -> (tempfile::TempDir, RunRoot, RunConfig) {
    use std::sync::Arc;

    use icechunk_fs_stress::{
        config::ForkConfig,
        log::start_worker_log,
        repo_mode::{RepoWorker, open_repo},
        storage_mode::{Attempt, Updater},
    };

    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().join("run"));
    let mut config = no_faults(mode, 1);
    config.fork =
        Some(ForkConfig { at: Duration::from_secs(1), repeat: false, during_gc: false });
    supervisor::init(&root, &config).await.unwrap();
    start(&root);
    let who = WorkerId { tag: "a".to_owned(), slot: "0".to_owned(), incarnation: 0 };
    let log = Arc::new(start_worker_log(&root, &who).unwrap());
    match mode {
        Mode::Storage => {
            let storage = storage_mode::storage(&root, &config, None).await.unwrap();
            let mut updater = Updater::new(storage, &who, &config).await.unwrap();
            let attempt = updater.attempt(&log).await.unwrap();
            assert!(
                matches!(attempt, Attempt::Logged(OpResult::Ack { .. })),
                "{attempt:?}"
            );
        }
        Mode::Repository => {
            let repo = open_repo(&root, &config, None).await.unwrap();
            let mut worker =
                RepoWorker::new(repo, log, &who, config.chunk_bytes).unwrap();
            let result = worker.main_commit().await.unwrap();
            assert!(matches!(result, Some(OpResult::Ack { .. })), "{result:?}");
        }
    }
    (dir, root, config)
}

/// Writes a fork as the fork actor does, logging it in `a.fork.0.log`.
fn fork(root: &RunRoot) -> icechunk_fs_stress::fork::Fork {
    use icechunk_fs_stress::fork::{FORK_SLOT, announce_backup, write_fork};
    let log = LogWriter::create(&root.worker_log("a", FORK_SLOT, 0)).unwrap();
    let written = write_fork(&root.storage(), announce_backup(&log).unwrap()).unwrap();
    log.append(&written.record(None)).unwrap();
    written
}

/// A run whose fork persisted: F5 passes, the publication refuses the fork,
/// the recovery write lands and reports it, and nothing is swept.
#[tokio::test]
async fn a_persisted_fork_is_checked_on_both_sides_and_reported() {
    for mode in [Mode::Storage, Mode::Repository] {
        let (_dir, root, _) = forked_run(mode).await;
        let written = fork(&root);
        let mirror = root.storage().join(storage_mode::REPO_KEY);
        let forked = fs::read(&mirror).unwrap();
        let now = SystemTime::now();
        write_done(&root, "a", now, now);

        let report = verify(&root, Duration::from_secs(5)).await.unwrap();
        assert!(report.passed, "{mode:?}: {}", report.summary());
        assert!(
            report
                .notes
                .iter()
                .any(|note| note.contains(&written.fork_hash)
                    && note.contains("persisted")),
            "{:?}",
            report.notes
        );
        assert_eq!(fs::read(&mirror).unwrap(), forked, "{mode:?}");
        let who = recovery_worker();
        let recovery =
            read_log(&root.worker_log(&who.tag, &who.slot, who.incarnation)).unwrap();
        assert!(
            recovery.iter().any(|record| matches!(
                record,
                Record::Outcome { result: OpResult::DurableButForked { .. }, .. }
            )),
            "{recovery:?}"
        );
        let published = read_log(&root.worker_log("verify", "publish", 0)).unwrap();
        assert!(
            !published.iter().any(|record| matches!(record, Record::MirrorRename { .. }))
        );
    }
}

/// A fork whose bytes left `$ROOT/repo` after the fork actor stopped
/// watching, by a write outside any publication: verify logs the erasure and
/// F4 finds it unexplained.
#[tokio::test]
async fn a_fork_erased_after_the_actor_stopped_is_seen_by_verify() {
    let (_dir, root, _) = forked_run(Mode::Storage).await;
    let mirror = root.storage().join(storage_mode::REPO_KEY);
    let version = fs::read(&mirror).unwrap();
    let written = fork(&root);
    fs::write(&mirror, &version).unwrap();
    let now = SystemTime::now();
    write_done(&root, "a", now, now);

    let report = verify(&root, Duration::from_secs(5)).await.unwrap();
    assert!(
        report.verdict.violations.iter().any(|v| matches!(
            v,
            Violation::SilentForkLoss { fork_hash, .. } if *fork_hash == written.fork_hash
        )),
        "{}",
        report.summary()
    );
    let observed = read_log(&root.worker_log("verify", "fork", 0)).unwrap();
    assert!(
        observed.iter().any(|record| matches!(record, Record::ForkGone { .. })),
        "{observed:?}"
    );
}

/// Under the sabotage, verify's own publication erases a persisted fork: F4.
#[tokio::test]
async fn verify_reports_its_own_publication_erasing_a_fork() {
    use icechunk_fs_stress::config::Sabotage;

    let (_dir, root, mut config) = forked_run(Mode::Storage).await;
    config.sabotage = Some(Sabotage::PublishWithoutForkCheck);
    fs::remove_file(root.run_json()).unwrap();
    config.write_new(&root).unwrap();
    let written = fork(&root);
    let now = SystemTime::now();
    write_done(&root, "a", now, now);

    let report = verify(&root, Duration::from_secs(5)).await.unwrap();
    assert!(
        report.verdict.violations.iter().any(|v| matches!(
            v,
            Violation::SilentForkLoss { fork_hash, erased_by }
                if *fork_hash == written.fork_hash && erased_by.contains("publish_mirror()")
        )),
        "{}",
        report.summary()
    );
}

type Actors = Vec<tokio::task::JoinHandle<anyhow::Result<()>>>;

/// Runs `actors` (each a worker slot's future) on a one-host run until they
/// end, writes `done/` and verifies.
async fn run_in_process(root: &RunRoot, actors: Actors, go: SystemTime) -> Report {
    for actor in actors {
        actor.await.unwrap().unwrap();
    }
    write_done(root, "a", go, SystemTime::now());
    verify(root, Duration::from_secs(5)).await.unwrap()
}

fn worker_id(slot: &str) -> WorkerId {
    WorkerId { tag: "a".to_owned(), slot: slot.to_owned(), incarnation: 0 }
}

/// A short in-process run with `sabotage`, its fault floor off: whether
/// selftest's judge detects it, and whether the run without it passes.
async fn judged(
    config: &RunConfig,
    sabotage: icechunk_fs_stress::config::Sabotage,
    actors: fn(&RunRoot, &RunConfig, SystemTime) -> Actors,
) -> (icechunk_fs_stress::selftest::CaseOutcome, Report) {
    use icechunk_fs_stress::{config::FaultFloor, selftest::judge};

    let mut sabotaged = config.clone();
    sabotaged.fault_floor = FaultFloor::ZERO;
    sabotaged.sabotage = Some(sabotage);
    let mut control = sabotaged.clone();
    control.sabotage = None;
    let mut reports = Vec::new();
    for config in [sabotaged, control] {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().join("run"));
        supervisor::init(&root, &config).await.unwrap();
        start(&root);
        let go = SystemTime::now();
        let until = go + Duration::from_secs(2);
        reports.push(run_in_process(&root, actors(&root, &config, until), go).await);
    }
    let control = reports.pop().unwrap();
    (judge(&reports[0], sabotage), control)
}

/// `PublishWithoutForkCheck`: the fork actor's forks are erased by
/// publications that never checked: F4.
#[tokio::test]
async fn a_publication_without_its_fork_check_is_caught_by_f4() {
    use icechunk_fs_stress::{
        config::{ForkConfig, Sabotage},
        fork::{FORK_SLOT, fork_actor},
        selftest::CaseOutcome,
    };

    let mut config = no_faults(Mode::Storage, 1);
    config.fork =
        Some(ForkConfig { at: Duration::from_millis(1), repeat: true, during_gc: false });
    let (outcome, control) =
        judged(&config, Sabotage::PublishWithoutForkCheck, |root, config, until| {
            let worker = {
                let (root, config) = (root.clone(), config.clone());
                tokio::spawn(async move {
                    storage_mode::worker(&root, &config, &worker_id("0"), until).await
                })
            };
            let fork = {
                let (root, config) = (root.clone(), config.clone());
                tokio::spawn(async move {
                    fork_actor(&root, &config, &worker_id(FORK_SLOT), until).await
                })
            };
            vec![worker, fork]
        })
        .await;
    assert!(
        matches!(&outcome, CaseOutcome::Detected(d) if d.contains("SilentForkLoss")),
        "{outcome:?}"
    );
    assert!(control.passed, "{}", control.summary());
}

/// `GcWithoutForkCheck`: a fork written after GC's change was published, as
/// with `fork.during_gc`, is left in place by GC's hook, and GC acknowledged:
/// F3.
#[tokio::test]
async fn a_gc_that_deletes_over_a_fork_is_caught_by_f3() {
    use icechunk_fs_stress::{
        config::{ForkConfig, GcConfig, Sabotage},
        repo_mode::gc_worker,
        selftest::CaseOutcome,
        supervisor::GC_SLOT,
    };

    let mut config = no_faults(Mode::Repository, 1);
    config.gc = Some(GcConfig {
        every: Duration::from_millis(200),
        ..GcConfig::defaults(config.faults.pause)
    });
    config.fork =
        Some(ForkConfig { at: Duration::from_millis(1), repeat: false, during_gc: true });
    let (outcome, control) =
        judged(&config, Sabotage::GcWithoutForkCheck, |root, config, until| {
            let (root, config) = (root.clone(), config.clone());
            vec![tokio::spawn(async move {
                gc_worker(&root, &config, &worker_id(GC_SLOT), until).await
            })]
        })
        .await;
    assert!(
        matches!(&outcome, CaseOutcome::Detected(d) if d.contains("GcAfterFork")),
        "{outcome:?}"
    );
    assert!(control.passed, "{}", control.summary());
    assert!(
        control.notes.iter().any(|note| note.contains("persisted")),
        "{:?}",
        control.notes
    );
}

/// `CommitWithoutPublish`: the status toggler's own `ReadOnly` change, a
/// change like any other, returns without publishing: M4.
#[tokio::test]
async fn a_change_without_its_publication_is_caught_by_m4() {
    use icechunk_fs_stress::{
        config::{ReadOnlyConfig, Sabotage},
        selftest::CaseOutcome,
        toggler::{TOGGLER_SLOT, toggler},
    };

    let mut config = no_faults(Mode::Repository, 1);
    config.readonly = Some(ReadOnlyConfig {
        every: Duration::from_millis(300),
        hold: Duration::from_millis(100),
    });
    let (outcome, control) =
        judged(&config, Sabotage::CommitWithoutPublish, |root, config, until| {
            let (root, config) = (root.clone(), config.clone());
            vec![tokio::spawn(async move {
                toggler(&root, &config, &worker_id(TOGGLER_SLOT), until).await
            })]
        })
        .await;
    assert!(
        matches!(&outcome, CaseOutcome::Detected(d) if d.contains("ReadOnlyMirrorMismatch")),
        "{outcome:?}"
    );
    assert!(control.passed, "{}", control.summary());
    assert!(control.verdict.stats.readonly_holds > 0, "{}", control.summary());
}

/// `PublishNonCurrent`: with a killed writer's version file above the
/// marker, verify's own publication installs it: M1 and M2.
#[tokio::test]
async fn a_publication_of_a_version_above_the_marker_is_caught_by_m1_and_m2() {
    use icechunk_fs_stress::{config::Sabotage, selftest::CaseOutcome};

    let config = no_faults(Mode::Storage, 1);
    let (outcome, control) =
        judged(&config, Sabotage::PublishNonCurrent, |root, config, _| {
            let (root, config) = (root.clone(), config.clone());
            vec![tokio::spawn(async move {
                use std::sync::Arc;

                use icechunk_fs_stress::storage_mode::{Attempt, Updater};
                // One acknowledged change, then what a writer killed after
                // linking its next version leaves.
                let who = worker_id("0");
                let log = LogWriter::create(&root.worker_log("a", "0", 0))?;
                let storage = storage_mode::storage(&root, &config, None).await?;
                let mut updater =
                    Updater::new(Arc::clone(&storage), &who, &config).await?;
                let attempt = updater.attempt(&log).await?;
                anyhow::ensure!(
                    matches!(attempt, Attempt::Logged(OpResult::Ack { .. })),
                    "{attempt:?}"
                );
                let repo_d = root.storage().join("repo.d");
                let marker = fs::read_dir(&repo_d)?
                    .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                    .find(|name| name.starts_with("latest."))
                    .unwrap();
                let n: u64 = marker.split('.').nth(2).unwrap().parse()?;
                let base = storage_mode::read_repo(&*storage).await?;
                let decoded = storage_mode::decode(&base)?;
                let payload = storage_mode::Payload {
                    id: uuid::Uuid::new_v4(),
                    parent: storage_mode::payload_etag(&base),
                    host: "a".to_owned(),
                    slot: "0".to_owned(),
                    incarnation: 0,
                    seq: 1,
                    padding: String::new(),
                };
                let bytes =
                    storage_mode::next_repo_info(&decoded.info, &payload, "repo.1.X")?;
                let name = format!("{}.{}", n + 1, storage_mode::payload_etag(&bytes));
                fs::write(repo_d.join(name), &bytes)?;
                Ok(())
            })]
        })
        .await;
    assert!(
        matches!(&outcome, CaseOutcome::Detected(d) if d.contains("MirrorNotAVersion") && d.contains("MirrorStale")),
        "{outcome:?}"
    );
    assert!(control.passed, "{}", control.summary());
}

/// A verify pointed at a storage root without a repository fails loudly.
#[tokio::test]
async fn a_run_without_a_repository_is_a_harness_failure() {
    for (case, damage) in [
        (
            "doesn't exist",
            (|storage: &Path| fs::remove_dir_all(storage).unwrap()) as fn(&Path),
        ),
        ("holds no repository", |storage: &Path| {
            fs::remove_dir_all(storage).unwrap();
            #[expect(
                clippy::create_dir,
                reason = "plants a new directory in an existing parent"
            )]
            fs::create_dir(storage).unwrap();
        }),
    ] {
        let (_dir, root, _) = forked_run(Mode::Storage).await;
        damage(&root.storage());
        let now = SystemTime::now();
        write_done(&root, "a", now, now);
        let report = verify(&root, Duration::from_secs(5)).await.unwrap();
        let details = harness_details(&report);
        assert!(
            matches!(&details[..], [detail] if detail.contains(case)),
            "{case}: {details:?}"
        );
        assert_eq!(report.exit_code(), 3);
        assert_eq!(report.recovery_ms, None);
    }
}

/// A run whose workers ran but changed nothing, as when they used another
/// storage root, fails loudly.
#[tokio::test]
async fn a_run_in_which_no_worker_operation_landed_is_a_harness_failure() {
    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().join("run"));
    supervisor::init(&root, &no_faults(Mode::Storage, 1)).await.unwrap();
    start(&root);
    let log = LogWriter::create(&root.worker_log("a", "0", 0)).unwrap();
    let id = uuid::Uuid::new_v4();
    let op = Op::Cas {
        base: "e0".to_owned(),
        new: "e1".to_owned(),
        backup: "repo.1.X".to_owned(),
    };
    log.append(&Record::Intent { t_ms: 1, id, op }).unwrap();
    log.append(&Record::Outcome { t_ms: 2, id, result: OpResult::Conflict }).unwrap();
    let now = SystemTime::now();
    write_done(&root, "a", now, now);
    let report = verify(&root, Duration::from_secs(5)).await.unwrap();
    let details = harness_details(&report);
    assert!(
        matches!(&details[..], [detail] if detail.contains("no operation of the 1 workers landed")),
        "{details:?}"
    );
}
