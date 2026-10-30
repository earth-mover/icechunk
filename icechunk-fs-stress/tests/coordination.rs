#![cfg(unix)]
#![expect(clippy::unwrap_used, reason = "test helpers")]

use std::{
    collections::BTreeSet,
    fs, thread,
    time::{Duration, Instant},
};

use icechunk_fs_stress::{
    config::{Mode, RunConfig},
    control::{
        Control, WaitOutcome, read_control, read_go, registered, wait_for_go,
        write_control,
    },
    layout::RunRoot,
    log::{Record, read_all_logs},
    repo_mode,
    storage_mode::REPO_KEY,
    supervisor,
};

const POLL: Duration = Duration::from_millis(10);

async fn initialized() -> (tempfile::TempDir, RunRoot) {
    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().join("run"));
    supervisor::init(&root, &RunConfig::defaults(Mode::Storage, 2)).await.unwrap();
    (dir, root)
}

fn tags(tags: &[&str]) -> BTreeSet<String> {
    tags.iter().map(|&tag| tag.to_owned()).collect()
}

fn wait_until(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        thread::sleep(POLL);
    }
}

#[tokio::test]
async fn expected_hosts_all_become_participants() {
    let (_dir, root) = initialized().await;
    let outcomes: Vec<WaitOutcome> = thread::scope(|s| {
        let handles: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|tag| {
                let root = &root;
                s.spawn(move || wait_for_go(root, tag, 2, POLL).unwrap())
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    for outcome in outcomes {
        let WaitOutcome::Participant { participants, .. } = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(participants, tags(&["a", "b"]));
    }
    assert_eq!(
        read_control(&root).unwrap(),
        Control::Go { participants: tags(&["a", "b"]) }
    );
    assert_eq!(registered(&root).unwrap(), tags(&["a", "b"]));
    assert_eq!(read_go(&root).unwrap(), tags(&["a", "b"]));

    // A host starting after the go never registers.
    assert_eq!(wait_for_go(&root, "c", 2, POLL).unwrap(), WaitOutcome::Late);
    assert_eq!(registered(&root).unwrap(), tags(&["a", "b"]));
    assert!(!root.host_info("c").exists());
}

#[tokio::test]
async fn host_left_out_of_go_unregisters() {
    let (_dir, root) = initialized().await;
    thread::scope(|s| {
        let waiter = s.spawn(|| wait_for_go(&root, "a", 2, POLL).unwrap());
        wait_until("a registers", || root.host_info("a").exists());
        write_control(&root, &Control::Go { participants: tags(&["other"]) }).unwrap();
        assert_eq!(waiter.join().unwrap(), WaitOutcome::Late);
    });
    assert!(!root.host_info("a").exists());
    assert_eq!(registered(&root).unwrap(), BTreeSet::new());
}

#[tokio::test]
async fn stop_before_go_makes_waiters_late() {
    let (_dir, root) = initialized().await;
    thread::scope(|s| {
        let waiter = s.spawn(|| wait_for_go(&root, "a", 2, POLL).unwrap());
        wait_until("a registers", || root.host_info("a").exists());
        write_control(&root, &Control::Stop).unwrap();
        assert_eq!(waiter.join().unwrap(), WaitOutcome::Late);
    });
    assert!(!root.host_info("a").exists());
    assert_eq!(wait_for_go(&root, "b", 2, POLL).unwrap(), WaitOutcome::Late);
    assert!(!root.host_info("b").exists());
}

#[tokio::test]
async fn registering_a_tag_twice_is_an_error() {
    let (_dir, root) = initialized().await;
    thread::scope(|s| {
        let waiter = s.spawn(|| wait_for_go(&root, "a", 2, POLL));
        wait_until("a registers", || root.host_info("a").exists());
        assert!(wait_for_go(&root, "a", 2, POLL).is_err());
        write_control(&root, &Control::Stop).unwrap();
        assert_eq!(waiter.join().unwrap().unwrap(), WaitOutcome::Late);
    });
}

#[tokio::test]
async fn init_writes_the_layout() {
    let (_dir, root) = initialized().await;
    assert_eq!(read_control(&root).unwrap(), Control::Wait);
    assert_eq!(RunConfig::load(&root).unwrap(), RunConfig::defaults(Mode::Storage, 2));
    for dir in [root.hosts_dir(), root.done_dir()] {
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0, "{}", dir.display());
    }
    // Only `init`'s log, with the first version's number.
    let logs = read_all_logs(&root).unwrap();
    let records: Vec<&Record> = logs.values().flatten().collect();
    assert!(
        matches!(&records[..], [Record::Start { host, .. }, Record::InitVersion { .. }]
            if host == "init"),
        "{logs:?}"
    );
    assert!(root.storage().join(REPO_KEY).is_file());
    let mut names: Vec<_> = fs::read_dir(root.root())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, ["control", "done", "hosts", "logs", "run.json", "storage"]);
}

#[tokio::test]
async fn init_accepts_an_empty_root_and_refuses_a_non_empty_one() {
    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().to_path_buf());
    let config = RunConfig::defaults(Mode::Storage, 1);
    supervisor::init(&root, &config).await.unwrap();
    assert_eq!(read_control(&root).unwrap(), Control::Wait);

    assert!(supervisor::init(&root, &config).await.is_err());

    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("something"), "").unwrap();
    let root = RunRoot::new(dir.path().to_path_buf());
    assert!(supervisor::init(&root, &config).await.is_err());
    let names: Vec<_> = fs::read_dir(dir.path()).unwrap().collect();
    assert_eq!(names.len(), 1);
}

#[tokio::test]
async fn init_rejects_bad_configs_before_creating_anything() {
    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().join("run"));
    let mut config = RunConfig::defaults(Mode::Storage, 1);
    config.hosts = 0;
    assert!(supervisor::init(&root, &config).await.is_err());
    assert!(!root.root().exists());

    let mut config = RunConfig::defaults(Mode::Repository, 1);
    config.repo_update_retries.initial_backoff_ms = 0;
    assert!(supervisor::init(&root, &config).await.is_err());
    assert!(!root.root().exists());
}

#[tokio::test]
async fn repository_init_creates_a_repository_with_only_main() {
    let dir = tempfile::tempdir().unwrap();
    let root = RunRoot::new(dir.path().join("run"));
    let config = RunConfig::defaults(Mode::Repository, 1);
    supervisor::init(&root, &config).await.unwrap();
    assert_eq!(read_control(&root).unwrap(), Control::Wait);
    let repo = repo_mode::open_repo(&root, &config, None).await.unwrap();
    assert_eq!(
        repo.list_branches().await.unwrap(),
        BTreeSet::from([repo_mode::MAIN_BRANCH.to_owned()])
    );
    assert!(supervisor::init(&root, &config).await.is_err());
}
