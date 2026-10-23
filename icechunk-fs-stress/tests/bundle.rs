#![cfg(unix)]
#![expect(clippy::unwrap_used, clippy::panic, reason = "test helpers")]

use std::{
    collections::BTreeSet,
    fs,
    io::Read as _,
    path::{Path, PathBuf},
    process::Command,
};

use chrono::{DateTime, TimeZone as _, Utc};
use flate2::read::GzDecoder;
use icechunk_fs_stress::{
    bundle::{BUNDLE_JSON, Bundle, BundleInfo, STORAGE_LISTING, default_output_name},
    env_info::Build,
};

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 22, 9, 8, 7).unwrap()
}

fn write(path: &Path, contents: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn report(passed: bool) -> String {
    format!(r#"{{"passed": {passed}, "verdict": {{"violations": []}}}}"#)
}

/// A run root's own files, with a report if `passed` is given.
fn run_root(root: &Path, passed: Option<bool>) {
    write(&root.join("run.json"), "{}");
    write(&root.join("control"), "stop\n");
    write(&root.join("go"), "a\n");
    write(&root.join("hosts/a.json"), "{}");
    write(&root.join("done/a.json"), "{}");
    write(&root.join("logs/a.w0.0.log"), "{}\n");
    if let Some(passed) = passed {
        write(&root.join("report.json"), &report(passed));
        write(&root.join("summary.txt"), "PASS\n");
    }
}

fn storage_root(storage: &Path) {
    write(&storage.join("repo"), "repo info");
    write(&storage.join("repo.d/v1"), "version 1");
    write(&storage.join("repo.d/mirror/v0"), "version 0");
    for dir in ["chunks", "manifests", "snapshots", "transactions", "overwritten"] {
        write(&storage.join(dir).join("ABC"), "data");
    }
}

/// The archive's entries, directories ending in `/`, and its files' contents.
fn entries(archive: &Path) -> (BTreeSet<String>, Vec<(String, String)>) {
    let mut archive = tar::Archive::new(GzDecoder::new(fs::File::open(archive).unwrap()));
    let mut names = BTreeSet::new();
    let mut files = Vec::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let name = entry.path().unwrap().display().to_string();
        if entry.header().entry_type().is_dir() {
            names.insert(format!("{}/", name.trim_end_matches('/')));
        } else {
            let mut contents = String::new();
            entry.read_to_string(&mut contents).unwrap();
            assert!(names.insert(name.clone()), "{name} twice");
            files.push((name, contents));
        }
    }
    (names, files)
}

fn contents<'a>(files: &'a [(String, String)], name: &str) -> &'a str {
    &files.iter().find(|(n, _)| n == name).unwrap_or_else(|| panic!("no {name}")).1
}

fn info(files: &[(String, String)], top: &str) -> BundleInfo {
    serde_json::from_str(contents(files, &format!("{top}/{BUNDLE_JSON}"))).unwrap()
}

fn with_top(top: &str, names: &[&str]) -> BTreeSet<String> {
    std::iter::once(format!("{top}/"))
        .chain(names.iter().map(|name| format!("{top}/{name}")))
        .collect()
}

const RUN_FILES: [&str; 11] = [
    "control",
    "done/",
    "done/a.json",
    "go",
    "hosts/",
    "hosts/a.json",
    "logs/",
    "logs/a.w0.0.log",
    "report.json",
    "run.json",
    "summary.txt",
];

fn command_line() -> Vec<String> {
    ["fs-stress", "bundle", "--root", "r"].map(String::from).to_vec()
}

fn bundle(root: &Path, output: &Path) -> (BTreeSet<String>, Vec<(String, String)>) {
    let bundle = Bundle::collect(root).unwrap();
    let bundled = bundle.write(Some(output), &command_line(), now()).unwrap();
    assert_eq!(bundled.path, output);
    assert_eq!(bundled.size, fs::metadata(output).unwrap().len());
    entries(output)
}

#[test]
fn a_passed_run_is_bundled_without_its_storage() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("run");
    run_root(&root, Some(true));
    storage_root(&root.join("storage"));
    let output = dir.path().join("results.tar.gz");

    assert!(Bundle::collect(&root).unwrap().passed());
    let (names, files) = bundle(&root, &output);

    let mut expected = with_top("results", &RUN_FILES);
    expected.insert(format!("results/{BUNDLE_JSON}"));
    assert_eq!(names, expected);
    assert_eq!(contents(&files, "results/summary.txt"), "PASS\n");
    let info = info(&files, "results");
    assert_eq!(
        info,
        BundleInfo {
            build: Build::current(),
            created_at: now(),
            command_line: command_line(),
            passed: true,
            repo_info_from: vec![],
            skipped: vec![],
        }
    );
}

#[test]
fn a_failed_run_adds_its_repo_info_and_a_storage_listing() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("run");
    run_root(&root, Some(false));
    storage_root(&root.join("storage"));
    let output = dir.path().join("fs-stress-fail-x.tar.gz");

    assert!(!Bundle::collect(&root).unwrap().passed());
    let (names, files) = bundle(&root, &output);

    let top = "fs-stress-fail-x";
    let mut expected = with_top(top, &RUN_FILES);
    expected.extend(
        [
            BUNDLE_JSON,
            "storage/",
            "storage/repo",
            "storage/repo.d/",
            "storage/repo.d/mirror/",
            "storage/repo.d/mirror/v0",
            "storage/repo.d/v1",
            STORAGE_LISTING,
        ]
        .map(|name| format!("{top}/{name}")),
    );
    assert_eq!(names, expected);
    assert_eq!(contents(&files, &format!("{top}/storage/repo")), "repo info");
    assert_eq!(contents(&files, &format!("{top}/storage/repo.d/v1")), "version 1");
    let info = info(&files, top);
    assert!(!info.passed);
    assert_eq!(info.repo_info_from, ["storage"]);

    let listing = contents(&files, &format!("{top}/{STORAGE_LISTING}"));
    let lines: Vec<&str> = listing.lines().skip(1).collect();
    let paths: Vec<&str> =
        lines.iter().map(|line| line.rsplit(' ').next().unwrap()).collect();
    assert_eq!(
        paths,
        [
            "chunks/",
            "chunks/ABC",
            "manifests/",
            "manifests/ABC",
            "overwritten/",
            "overwritten/ABC",
            "repo",
            "repo.d/",
            "repo.d/mirror/",
            "repo.d/mirror/v0",
            "repo.d/v1",
            "snapshots/",
            "snapshots/ABC",
            "transactions/",
            "transactions/ABC",
        ]
    );
    let chunk = lines.iter().find(|line| line.ends_with(" chunks/ABC")).unwrap();
    let fields: Vec<&str> = chunk.split_whitespace().collect();
    assert_eq!(fields[0], "f", "{chunk}");
    assert_eq!(fields[1], "4", "{chunk}");
    assert!(
        DateTime::parse_from_rfc3339(fields[2]).unwrap().offset().local_minus_utc() == 0
    );
    assert!(fields[2].ends_with('Z'), "{chunk}");
    assert!(
        lines.iter().any(|line| line.starts_with("d ") && line.ends_with(" chunks/"))
    );
}

#[test]
fn a_run_without_a_report_fails() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("run");
    run_root(&root, None);
    storage_root(&root.join("storage"));
    assert!(!Bundle::collect(&root).unwrap().passed());
}

#[test]
fn an_unreadable_report_fails() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("run");
    run_root(&root, Some(true));
    write(&root.join("report.json"), "{ not json");
    assert!(!Bundle::collect(&root).unwrap().passed());
}

fn qualify_root(root: &Path, storage_mode: Option<bool>, repository_mode: Option<bool>) {
    write(&root.join("qualify.json"), "{}");
    write(&root.join("summary.txt"), "combined\n");
    for (dir, passed) in
        [("storage-mode", storage_mode), ("repository-mode", repository_mode)]
    {
        run_root(&root.join(dir), passed);
        storage_root(&root.join(dir).join("storage"));
    }
}

#[test]
fn a_qualification_passes_only_if_both_runs_passed() {
    let dir = tempfile::tempdir().unwrap();
    for (storage, repository, passed) in [
        (Some(true), Some(true), true),
        (Some(true), Some(false), false),
        (Some(false), Some(true), false),
        (Some(true), None, false),
    ] {
        let root = dir.path().join(format!("q-{storage:?}-{repository:?}"));
        qualify_root(&root, storage, repository);
        assert_eq!(
            Bundle::collect(&root).unwrap().passed(),
            passed,
            "{storage:?} {repository:?}"
        );
    }
}

#[test]
fn a_qualification_whose_second_run_never_started_fails() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("q");
    qualify_root(&root, Some(true), Some(true));
    fs::remove_file(root.join("repository-mode/report.json")).unwrap();
    fs::remove_file(root.join("repository-mode/run.json")).unwrap();
    write(&root.join("repository-mode/control"), "wait\n");
    assert!(!Bundle::collect(&root).unwrap().passed());
}

#[test]
fn a_failed_qualification_has_the_repo_info_of_both_runs() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("q");
    qualify_root(&root, Some(true), None);
    let output = dir.path().join("q.tar.gz");

    let (names, files) = bundle(&root, &output);

    for run in ["storage-mode", "repository-mode"] {
        for name in RUN_FILES.iter().filter(|name| {
            run == "storage-mode" || !["report.json", "summary.txt"].contains(name)
        }) {
            assert!(
                names.contains(&format!("q/{run}/{name}")),
                "{run}/{name}: {names:?}"
            );
        }
        for name in ["storage/repo", "storage/repo.d/v1", STORAGE_LISTING] {
            assert!(
                names.contains(&format!("q/{run}/{name}")),
                "{run}/{name}: {names:?}"
            );
        }
    }
    assert!(names.contains("q/qualify.json"));
    assert!(names.contains("q/summary.txt"));
    assert!(
        !names.iter().any(|name| [
            "chunks",
            "manifests",
            "snapshots",
            "transactions",
            "overwritten"
        ]
        .iter()
        .any(|dir| name.contains(&format!("/{dir}/")))),
        "{names:?}"
    );
    assert_eq!(
        info(&files, "q").repo_info_from,
        ["repository-mode/storage", "storage-mode/storage"]
    );
}

fn scenario_root(root: &Path, phase_2_passed: bool) {
    write(&root.join("scenario.json"), "{}");
    write(&root.join("report.json"), &report(phase_2_passed));
    write(&root.join("summary.txt"), "scenario\n");
    storage_root(&root.join("storage"));
    run_root(&root.join("phase-1"), Some(true));
    run_root(&root.join("phase-2"), Some(phase_2_passed));
    write(&root.join("phase-2/logs/scenario.from-standard.0.log"), "{}\n");
}

#[test]
fn a_scenario_shares_one_storage_root() {
    let dir = tempfile::tempdir().unwrap();
    let passed = dir.path().join("passed");
    scenario_root(&passed, true);
    assert!(Bundle::collect(&passed).unwrap().passed());
    let failed = dir.path().join("failed");
    scenario_root(&failed, false);
    let output = dir.path().join("s.tar.gz");

    let (names, files) = bundle(&failed, &output);

    for name in [
        "s/scenario.json",
        "s/report.json",
        "s/phase-1/run.json",
        "s/phase-1/report.json",
        "s/phase-2/logs/scenario.from-standard.0.log",
        "s/storage/repo",
        "s/storage/repo.d/v1",
    ] {
        assert!(names.contains(name), "{name}: {names:?}");
    }
    assert!(names.contains(&format!("s/{STORAGE_LISTING}")));
    assert!(!names.iter().any(|name| name.starts_with("s/storage/chunks")));
    assert_eq!(info(&files, "s").repo_info_from, ["storage"]);
}

#[test]
fn an_existing_output_is_not_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("run");
    run_root(&root, Some(true));
    let output = dir.path().join("out.tar.gz");
    fs::write(&output, "mine").unwrap();

    let err = Bundle::collect(&root)
        .unwrap()
        .write(Some(&output), &command_line(), now())
        .unwrap_err();

    assert!(format!("{err:#}").contains("already exists"), "{err:#}");
    assert_eq!(fs::read_to_string(&output).unwrap(), "mine");
}

#[test]
fn only_a_run_qualify_or_scenario_root_is_bundled() {
    let dir = tempfile::tempdir().unwrap();
    let other = dir.path().join("home");
    write(&other.join("notes.txt"), "private");
    let err = Bundle::collect(&other).unwrap_err();
    assert!(format!("{err:#}").contains("run.json"), "{err:#}");

    let err = Bundle::collect(&dir.path().join("missing")).unwrap_err();
    assert!(format!("{err:#}").contains("missing"), "{err:#}");
}

#[test]
fn files_that_vanish_or_are_not_regular_are_skipped_and_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("run");
    run_root(&root, Some(true));
    write(&dir.path().join("outside.txt"), "outside");
    std::os::unix::fs::symlink(dir.path().join("outside.txt"), root.join("logs/link"))
        .unwrap();
    let _socket = std::os::unix::net::UnixListener::bind(root.join("sock")).unwrap();
    let output = dir.path().join("out.tar.gz");

    let bundle = Bundle::collect(&root).unwrap();
    fs::remove_file(root.join("logs/a.w0.0.log")).unwrap();
    let bundled = bundle.write(Some(&output), &command_line(), now()).unwrap();
    assert_eq!(bundled.skipped, 3);

    let (names, files) = entries(&output);
    for gone in ["out/logs/link", "out/sock", "out/logs/a.w0.0.log"] {
        assert!(!names.contains(gone), "{gone}: {names:?}");
    }
    assert!(names.contains("out/logs/"));
    let skipped: Vec<(String, String)> = info(&files, "out")
        .skipped
        .into_iter()
        .map(|skipped| (skipped.path, skipped.reason))
        .collect();
    let paths: Vec<&str> = skipped.iter().map(|(path, _)| path.as_str()).collect();
    assert_eq!(paths, ["logs/link", "sock", "logs/a.w0.0.log"], "{skipped:?}");
    assert!(skipped[0].1.contains("symbolic link"), "{skipped:?}");
    assert!(skipped[1].1.contains("socket"), "{skipped:?}");
    assert!(!skipped[2].1.is_empty(), "{skipped:?}");
}

#[test]
fn an_output_inside_the_root_leaves_itself_out() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("run");
    run_root(&root, Some(true));
    let output = root.join("self.tar.gz");

    let (names, _) = bundle(&root, &output);

    assert!(!names.contains("self/self.tar.gz"), "{names:?}");
    assert!(names.contains("self/run.json"));
}

#[test]
fn the_default_name_says_pass_or_fail_and_when() {
    assert_eq!(
        default_output_name(true, now()),
        "fs-stress-pass-20261022T090807Z.tar.gz"
    );
    assert_eq!(
        default_output_name(false, now()),
        "fs-stress-fail-20261022T090807Z.tar.gz"
    );
}

#[test]
fn the_command_writes_to_the_current_directory_and_warns_about_host_names() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("run");
    run_root(&root, None);
    let cwd = dir.path().join("cwd");
    fs::create_dir_all(&cwd).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_fs-stress"))
        .args(["bundle", "--root"])
        .arg(&root)
        .current_dir(&cwd)
        .output()
        .unwrap();

    assert!(output.status.success(), "{output:?}");
    let written: Vec<PathBuf> =
        fs::read_dir(&cwd).unwrap().map(|entry| entry.unwrap().path()).collect();
    assert_eq!(written.len(), 1, "{written:?}");
    let name = written[0].file_name().unwrap().to_str().unwrap().to_owned();
    assert!(name.starts_with("fs-stress-fail-") && name.ends_with("Z.tar.gz"), "{name}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&name), "{stdout}");
    assert!(stdout.contains("host names and mount details"), "{stdout}");
    let top = name.trim_end_matches(".tar.gz");
    let (names, files) = entries(&written[0]);
    assert!(names.iter().all(|entry| entry.starts_with(&format!("{top}/"))), "{names:?}");
    let info = info(&files, top);
    assert_eq!(info.command_line[1..3], ["bundle", "--root"]);

    let again = Command::new(env!("CARGO_BIN_EXE_fs-stress"))
        .args(["bundle", "--root"])
        .arg(&root)
        .arg("--output")
        .arg(&written[0])
        .output()
        .unwrap();
    assert_eq!(again.status.code(), Some(3), "{again:?}");
}

#[test]
fn a_root_without_a_marker_is_refused_before_it_is_read() {
    use std::os::unix::fs::PermissionsExt as _;

    let dir = tempfile::tempdir().unwrap();
    let mount = dir.path().join("mount");
    write(&mount.join("a/b/c/d.txt"), "deep");
    // Walking it would fail: it can't be listed, only looked up in.
    fs::set_permissions(&mount, fs::Permissions::from_mode(0o100)).unwrap();

    let err = Bundle::collect(&mount).unwrap_err();

    fs::set_permissions(&mount, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(format!("{err:#}").contains("is not a run root"), "{err:#}");
}

#[test]
fn an_output_name_that_is_no_directory_name_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("run");
    run_root(&root, Some(true));
    let bundle = Bundle::collect(&root).unwrap();
    for name in ["..tar.gz", ".tar.gz", "..tgz"] {
        let output = dir.path().join(name);
        let err = bundle.write(Some(&output), &command_line(), now()).unwrap_err();
        assert!(
            format!("{err:#}").contains("can't be empty, . or .."),
            "{name}: {err:#}"
        );
        assert!(!output.exists(), "{name}");
    }
}

#[test]
fn a_report_that_is_a_symbolic_link_fails() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("run");
    run_root(&root, None);
    write(&dir.path().join("passed.json"), &report(true));
    std::os::unix::fs::symlink(dir.path().join("passed.json"), root.join("report.json"))
        .unwrap();
    assert!(!Bundle::collect(&root).unwrap().passed());
}

#[test]
fn earlier_bundles_in_the_root_are_left_out() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("run");
    run_root(&root, Some(true));
    write(&root.join("fs-stress-pass-20261021T000000Z.tar.gz"), "old");
    write(&root.join("fs-stress-fail-x.tgz"), "old");
    let output = dir.path().join("out.tar.gz");

    let (names, files) = bundle(&root, &output);

    assert!(!names.iter().any(|name| name.contains("fs-stress-")), "{names:?}");
    let skipped: Vec<_> =
        info(&files, "out").skipped.into_iter().map(|s| s.path).collect();
    assert_eq!(
        skipped,
        ["fs-stress-fail-x.tgz", "fs-stress-pass-20261021T000000Z.tar.gz"]
    );
}

#[test]
fn a_long_path_survives_the_archive() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("run");
    run_root(&root, Some(true));
    let long = format!("logs/{}/{}.log", "nested-directory".repeat(5), "w".repeat(80));
    assert!(long.len() > 100);
    write(&root.join(&long), "long");
    let output = dir.path().join("out.tar.gz");

    let (_, files) = bundle(&root, &output);

    assert_eq!(contents(&files, &format!("out/{long}")), "long");
}
