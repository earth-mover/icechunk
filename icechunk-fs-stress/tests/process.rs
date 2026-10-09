#![cfg(unix)]
#![expect(clippy::unwrap_used, clippy::panic, reason = "test helpers")]

use std::{
    fs,
    io::Read as _,
    os::unix::process::ExitStatusExt as _,
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use icechunk_fs_stress::{
    config::{Mode, PointName, RunConfig},
    control::{Control, read_control},
    env_info::HostDone,
    layout::RunRoot,
    log::{FaultAction, Op, Record, read_all_logs, read_log},
    oracle::{History, Verdict, repo::check_repo},
    repo_mode, storage_mode,
    supervisor::GC_SLOT,
};

fn fs_stress() -> Command {
    Command::new(env!("CARGO_BIN_EXE_fs-stress"))
}

/// Waits for every child, killing them all and failing if any is still
/// running after `timeout`.
fn wait_all(mut children: Vec<Child>, timeout: Duration) -> Vec<ExitStatus> {
    let deadline = Instant::now() + timeout;
    let mut statuses = vec![None; children.len()];
    while statuses.iter().any(Option::is_none) {
        for (child, status) in children.iter_mut().zip(&mut statuses) {
            if status.is_none() {
                *status = child.try_wait().unwrap();
            }
        }
        if Instant::now() >= deadline && statuses.iter().any(Option::is_none) {
            for child in &mut children {
                let _ = child.kill();
                let _ = child.wait();
            }
            panic!("fs-stress processes still running after {timeout:?}: {statuses:?}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    statuses.into_iter().flatten().collect()
}

fn wait_one(child: Child, timeout: Duration) -> ExitStatus {
    wait_all(vec![child], timeout).remove(0)
}

const TIMEOUT: Duration = Duration::from_secs(120);

#[tokio::test]
#[ignore = "spawns stress processes; run with just fs-stress-process-tests"]
async fn kill_fault_kills_the_worker_process_after_logging_it() {
    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().to_path_buf());
    let mut config = RunConfig::defaults(Mode::Storage, 1);
    config.faults.points = vec![PointName::LockTaken];
    config.faults.kill_probability = 1.0;
    config.faults.pause_probability = 0.0;
    config.write_new(&root).unwrap();
    fs::create_dir_all(root.logs_dir()).unwrap();
    storage_mode::init(&root, &config).await.unwrap();

    let until = SystemTime::now() + Duration::from_secs(60);
    let until_ms = until.duration_since(UNIX_EPOCH).unwrap().as_millis().to_string();
    let worker = fs_stress()
        .args(["worker", "--root"])
        .arg(root.root())
        .args(["--tag", "h", "--slot", "0", "--incarnation", "0", "--until-ms"])
        .arg(until_ms)
        .spawn()
        .unwrap();
    let status = wait_one(worker, TIMEOUT);
    assert_eq!(status.signal(), Some(libc::SIGKILL), "{status:?}");

    let records = read_log(&root.worker_log("h", "0", 0)).unwrap();
    assert!(matches!(records.first(), Some(Record::Start { .. })), "{records:?}");
    assert!(
        matches!(
            records.last(),
            Some(Record::Fault {
                action: FaultAction::Kill,
                point: PointName::LockTaken,
                ..
            })
        ),
        "{records:?}"
    );
}

#[test]
#[ignore = "spawns stress processes; run with just fs-stress-process-tests"]
fn two_hosts_run_and_a_late_host_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().join("run"));
    let init = fs_stress()
        .args(["init", "--root"])
        .arg(root.root())
        .args(["--mode", "storage", "--hosts", "2", "--duration", "3s"])
        .args(["--stale-after", "1s", "--kill-p", "0", "--pause-p", "0"])
        .args(["--external-kill-every", "1s", "--workers", "2"])
        .spawn()
        .unwrap();
    let status = wait_one(init, TIMEOUT);
    assert!(status.success(), "{status:?}");
    assert_eq!(read_control(&root).unwrap(), Control::Wait);

    let hosts: Vec<_> = ["a", "b"]
        .into_iter()
        .map(|tag| {
            fs_stress()
                .args(["workers", "--root"])
                .arg(root.root())
                .args(["--tag", tag])
                .spawn()
                .unwrap()
        })
        .collect();
    for status in wait_all(hosts, TIMEOUT) {
        assert!(status.success(), "{status:?}");
    }

    for tag in ["a", "b"] {
        let done: HostDone =
            serde_json::from_slice(&fs::read(root.host_done(tag)).unwrap()).unwrap();
        assert_eq!(done.tag, tag);
        assert_eq!(done.workers, 2);
        assert_eq!(done.stragglers, 0);
        assert!(done.external_kills >= 2, "{done:?}");
        assert!(done.stopped_at_ms >= done.go_seen_at_ms + 3000, "{done:?}");

        let records = read_log(&root.supervisor_log(tag)).unwrap();
        let spawns = records.iter().filter(|r| matches!(r, Record::Spawn { .. })).count();
        let kills =
            records.iter().filter(|r| matches!(r, Record::ExternalKill { .. })).count();
        assert_eq!(spawns as u64, done.incarnations_spawned, "{records:?}");
        assert_eq!(kills as u64, done.external_kills, "{records:?}");
        assert!(done.incarnations_spawned >= 2 + done.external_kills, "{done:?}");
    }
    assert!(matches!(read_control(&root).unwrap(), Control::Go { .. }));

    let mut late = fs_stress()
        .args(["workers", "--root"])
        .arg(root.root())
        .args(["--tag", "c"])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stderr_pipe = late.stderr.take().unwrap();
    let status = wait_one(late, TIMEOUT);
    let mut stderr = String::new();
    stderr_pipe.read_to_string(&mut stderr).unwrap();
    assert_eq!(status.code(), Some(2), "{status:?}: {stderr}");
    assert!(stderr.contains("registered after the run started"), "{stderr}");
    assert!(!root.host_info("c").exists());
    assert!(!root.host_done("c").exists());
}

#[tokio::test]
#[ignore = "spawns stress processes; run with just fs-stress-process-tests"]
async fn repository_run_with_gc_passes_the_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().join("run"));
    let init = fs_stress()
        .args(["init", "--root"])
        .arg(root.root())
        .args(["--mode", "repository", "--gc", "--duration", "5s", "--hosts", "1"])
        .args(["--kill-p", "0", "--pause-p", "0", "--external-kill-every", "off"])
        .spawn()
        .unwrap();
    let status = wait_one(init, TIMEOUT);
    assert!(status.success(), "{status:?}");

    let workers = fs_stress()
        .args(["workers", "--root"])
        .arg(root.root())
        .args(["--tag", "a", "--workers", "4"])
        .spawn()
        .unwrap();
    let status = wait_one(workers, TIMEOUT);
    assert!(status.success(), "{status:?}");

    let config = RunConfig::load(&root).unwrap();
    let repo = repo_mode::open_repo(&root, &config, None).await.unwrap();
    let history = History { logs: read_all_logs(&root).unwrap(), ..History::default() };
    let mut verdict = Verdict::default();
    check_repo(&repo, &history, &config, false, &mut verdict).await.unwrap();
    assert_eq!(verdict.violations, [], "{verdict:?}");
    assert_eq!(verdict.warnings, [], "{verdict:?}");
    assert!(verdict.stats.chain_len > 0, "{verdict:?}");

    // Intents, not acks: GC can fail every time against concurrent commits,
    // since it updates the repo info with max_tries 1 and doesn't retry
    // RepoUpdateAttemptsLimit (a known icechunk GC retry bug).
    let gc_intents = history
        .worker_records()
        .filter(|(name, _)| name.slot == GC_SLOT)
        .filter(|(_, record)| matches!(record, Record::Intent { op: Op::Gc, .. }))
        .count();
    assert!(gc_intents >= 1, "{:?}", history.logs.keys().collect::<Vec<_>>());
    let ops = |matches: fn(&Op) -> bool| {
        history
            .worker_records()
            .filter(
                |(_, record)| matches!(record, Record::Intent { op, .. } if matches(op)),
            )
            .count()
    };
    assert!(ops(|op| matches!(op, Op::BranchCommit { .. })) > 0);
    assert!(ops(|op| matches!(op, Op::MainCommit)) > 0);
    assert!(ops(|op| matches!(op, Op::Tag { .. })) > 0);
}

/// Runs `fs-stress` with `args`, its output in files under `dir`, and returns
/// its exit status, stdout and stderr.
fn run_logged(
    dir: &std::path::Path,
    args: &[&str],
    timeout: Duration,
) -> (ExitStatus, String, String) {
    let stdout = dir.join("stdout.txt");
    let stderr = dir.join("stderr.txt");
    let child = fs_stress()
        .args(args)
        .stdin(Stdio::null())
        .stdout(fs::File::create(&stdout).unwrap())
        .stderr(fs::File::create(&stderr).unwrap())
        .spawn()
        .unwrap();
    let status = wait_one(child, timeout);
    (status, fs::read_to_string(stdout).unwrap(), fs::read_to_string(stderr).unwrap())
}

#[test]
#[ignore = "spawns stress processes; run with just fs-stress-process-tests"]
fn storage_run_with_default_faults_passes() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("run");
    let (status, stdout, stderr) = run_logged(
        dir.path(),
        &[
            "run",
            "--root",
            root.to_str().unwrap(),
            "--mode",
            "storage",
            // Each pause and kill holds up every writer for about a second, so
            // shorter runs miss the fault floor at some point too often.
            "--duration",
            "30s",
            "--stale-after",
            "1s",
            "--pause",
            "2s",
            "--external-kill-every",
            "1s",
            "--progress-every",
            "2s",
        ],
        Duration::from_secs(300),
    );
    assert_eq!(status.code(), Some(0), "{stdout}\n{stderr}");
    assert!(stdout.starts_with("PASS"), "{stdout}");
    for phase in [
        "estimated data volume: at most",
        "waiting for go (1/1",
        "s/30s  workers ",
        "deadline reached",
        "waiting for done/ (1/1)",
        "recovery write took",
        "aging and sweeping",
    ] {
        assert!(stderr.contains(phase), "{phase:?} missing from {stderr}");
    }
    let report: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("report.json")).unwrap()).unwrap();
    assert_eq!(report["passed"], true);
}

#[test]
#[ignore = "spawns stress processes; run with just fs-stress-process-tests"]
fn selftest_detects_every_case_in_both_modes() {
    let dir = tempfile::tempdir().unwrap();
    let parent = dir.path().join("cases");
    let (status, stdout, stderr) = run_logged(
        dir.path(),
        &[
            "selftest",
            "--root-parent",
            parent.to_str().unwrap(),
            "--duration",
            "15s",
            "--mode",
            "both",
        ],
        Duration::from_secs(900),
    );
    assert_eq!(status.code(), Some(0), "{stdout}\n{stderr}");
    let lines: Vec<_> = stdout.lines().collect();
    let cases = ["skip-fence", "skip-compare", "trust-rename-enoent", "legacy-local"];
    let expected: Vec<_> = ["storage", "repository"]
        .iter()
        .flat_map(|mode| cases.iter().map(move |case| format!("{mode} {case}: detected")))
        .collect();
    assert_eq!(lines.len(), expected.len() + 1, "{stdout}");
    for (line, prefix) in lines.iter().zip(&expected) {
        assert!(line.starts_with(prefix), "{prefix:?} expected in {stdout}");
    }
    assert_eq!(lines[expected.len()], "PASS");
    for mode in ["storage", "repository"] {
        let legacy = format!("{mode} legacy-local: detected: ");
        let line = lines.iter().find(|line| line.starts_with(&legacy)).unwrap();
        assert!(line.contains("LostUpdate"), "{line}");
    }
}
