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
    let op = Op::Cas { base: "e0".to_owned(), new: "e1".to_owned() };
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
