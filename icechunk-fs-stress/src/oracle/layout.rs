//! The writer feature and the current version, on the filesystem backend:
//! every version that was current lists the feature with a status the
//! implementation writes (L1), the first version's `N` is the creation time
//! (I1), no change lands while the status toggler holds `ReadOnly` (M4), and
//! the current version references only objects that exist (M7).

use std::{collections::HashSet, fs, io, path::Path};

use anyhow::Context as _;
use icechunk::{
    Repository,
    format::{
        MANIFESTS_FILE_PATH, ManifestId, SNAPSHOTS_FILE_PATH, SnapshotId,
        TRANSACTION_LOGS_FILE_PATH, layout_v1,
        repo_info::{RepoAvailability, RepoHeader, RepoInfo, RepoStatus},
        repo_info_file,
        writer_features::WriterFeature,
    },
    storage::{StorageContext, StorageErrorKind},
};

use crate::{
    config::RunConfig,
    layout::SUPERVISOR_SLOT,
    log::{LogName, Record},
    oracle::{
        History, Verdict, Violation, gc_cutoff::missing_after_gc_cutoff,
        storage::read_listed_backup,
    },
    storage_mode::{BACKUP_DIR, REPO_KEY, payload_etag, read_repo},
    toggler::TOGGLER_REASON,
};

/// The repository's own statuses a run writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatuses {
    Online,
    /// `Online`, and `ReadOnly` with [`TOGGLER_REASON`] while the status
    /// toggler holds it.
    Toggled,
}

impl RunStatuses {
    pub fn of(config: &RunConfig) -> Self {
        if config.readonly.is_some() { Self::Toggled } else { Self::Online }
    }

    fn allow(self, status: &RepoStatus) -> bool {
        match status.availability {
            RepoAvailability::Online => true,
            RepoAvailability::ReadOnly => {
                self == Self::Toggled
                    && status.limited_availability_reason.as_deref()
                        == Some(TOGGLER_REASON)
            }
        }
    }
}

/// L1: what keeps `info` from being a repo info of the filesystem layout
/// written by this run: it lists exactly `filesystem-layout-v1`, its status
/// is stored as the implementation writes it, and is one the run writes.
pub fn layout_defect(info: &RepoInfo, statuses: RunStatuses) -> Option<String> {
    let checked = || -> icechunk::format::IcechunkResult<Option<String>> {
        let features = info.raw_writer_features()?;
        let names: Vec<&str> = features.iter().map(|name| name.as_str()).collect();
        if names != [layout_v1::FEATURE] {
            return Ok(Some(format!(
                "it lists the writer features {names:?}, not [{:?}]",
                layout_v1::FEATURE
            )));
        }
        let status = info.status()?;
        let header = info.header()?;
        let written = RepoHeader::new(&[WriterFeature::FilesystemLayoutV1], &status);
        if header != written {
            return Ok(Some(format!(
                "its status {:?} isn't stored as the implementation writes it ({:?})",
                header.status(),
                written.status()
            )));
        }
        if !statuses.allow(&status) {
            return Ok(Some(format!(
                "its own status is {:?} ({:?}), not one the run sets",
                status.availability, status.limited_availability_reason
            )));
        }
        Ok(None)
    };
    checked().unwrap_or_else(|err| Some(format!("reading it: {}", err.kind())))
}

/// [`layout_defect`] of a repo info file.
pub fn file_layout_defect(bytes: &[u8], statuses: RunStatuses) -> Option<String> {
    match repo_info_file::decode(bytes) {
        Ok(info) => layout_defect(&info, statuses),
        Err(err) => Some(format!("it doesn't decode: {}", err.kind())),
    }
}

/// Repository mode: the hashes of the versions that were current, the
/// backups of the repo info in `overwritten/` and `current`, checking each
/// distinct one for L1, except the backups named in `earlier_eras`: those
/// the conversion scenario's earlier eras wrote, in the object store layout
/// among them. Every backup is a copy of a version that was current when it
/// was read: icechunk backs up the version it updates, conditionally on that
/// version, and so do the conversions.
pub fn versions_that_were_current(
    storage_root: &Path,
    current: &[u8],
    statuses: RunStatuses,
    earlier_eras: &HashSet<String>,
    verdict: &mut Verdict,
) -> anyhow::Result<HashSet<String>> {
    let mut was_current = HashSet::new();
    let mut add = |what: String, checked: bool, bytes: &[u8], verdict: &mut Verdict| {
        if was_current.insert(payload_etag(bytes))
            && checked
            && let Some(detail) = file_layout_defect(bytes, statuses)
        {
            verdict.violations.push(Violation::FeatureMissing { what, detail });
        }
    };
    add("the current version".to_owned(), true, current, verdict);
    let dir = storage_root.join(BACKUP_DIR);
    let context = || format!("listing {}", dir.display());
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(was_current),
        Err(err) => return Err(err).with_context(context),
    };
    for entry in entries {
        let path = entry.with_context(context)?.path();
        let name = path.file_name().map(|name| name.to_string_lossy().into_owned());
        let Some(name) = name.filter(|name| is_repo_info_backup(name)) else {
            continue;
        };
        let Some(bytes) = read_listed_backup(&path)? else {
            continue;
        };
        let checked = !earlier_eras.contains(&name);
        add(format!("the backup {BACKUP_DIR}/{name}"), checked, &bytes, verdict);
    }
    Ok(was_current)
}

/// A backup of the repo info, not of the configuration, and not the staging
/// file of an interrupted copy.
fn is_repo_info_backup(name: &str) -> bool {
    name.starts_with(&format!("{REPO_KEY}.")) && !name.contains(".staging.")
}

/// I1: `init` logged the first version's `N`, which lies within its
/// creation's times.
pub fn check_init_version(history: &History, verdict: &mut Verdict) {
    let inits: Vec<_> = history
        .logs
        .values()
        .flatten()
        .filter_map(|record| match record {
            Record::InitVersion { n, t_before_ms, t_after_ms } => {
                Some((*n, *t_before_ms, *t_after_ms))
            }
            _ => None,
        })
        .collect();
    let [(n, t_before_ms, t_after_ms)] = inits[..] else {
        verdict.violations.push(Violation::Harness {
            detail: format!("{} InitVersion records, expected one", inits.len()),
        });
        return;
    };
    let in_window =
        i64::try_from(n).is_ok_and(|n| (t_before_ms..=t_after_ms).contains(&n));
    if !in_window {
        verdict.violations.push(Violation::InitialVersion { n, t_before_ms, t_after_ms });
    }
}

/// A time by which the process of the worker log `name` had died, on its
/// host's clock: its supervisor's external kill of it, logged once it was
/// reaped, or the first start of a later incarnation of its slot, spawned
/// once it was. `None` if neither was logged, as for a straggler killed at
/// the end.
pub fn died_by_ms(history: &History, name: &LogName) -> Option<i64> {
    let incarnation = name.incarnation?;
    let supervisor = LogName {
        tag: name.tag.clone(),
        slot: SUPERVISOR_SLOT.to_owned(),
        incarnation: None,
    };
    let killed =
        history.logs.get(&supervisor).into_iter().flatten().find_map(
            |record| match record {
                Record::ExternalKill { t_ms, slot, incarnation: killed }
                    if *slot == name.slot && *killed == incarnation =>
                {
                    Some(*t_ms)
                }
                _ => None,
            },
        );
    killed.or_else(|| {
        let next =
            LogName { incarnation: Some(incarnation.checked_add(1)?), ..name.clone() };
        history
            .logs
            .range(next..)
            .take_while(|(later, _)| later.tag == name.tag && later.slot == name.slot)
            .filter_map(|(_, records)| match records.first()? {
                Record::Start { t_ms, .. } => Some(*t_ms),
                _ => None,
            })
            .min()
    })
}

/// M4: at the end of every hold of the status `ReadOnly`, the current
/// version is still the `ReadOnly` change's. A change that landed during the
/// hold, or right after the `ReadOnly` change, is a violation on any
/// filesystem: a newer current version is no stale read.
pub fn check_readonly_holds(history: &History, verdict: &mut Verdict) {
    for record in history.logs.values().flatten() {
        let Record::ReadOnlyHeld { version, current, .. } = record else {
            continue;
        };
        verdict.stats.readonly_holds += 1;
        let detail = match version.as_deref() {
            None => {
                "a change landed right after the ReadOnly change, before the toggler \
                     read its version"
                    .to_owned()
            }
            Some(version) if current != version => {
                format!("a change landed: the current version was {current}")
            }
            Some(_) => continue,
        };
        verdict.violations.push(Violation::ChangeDuringReadOnlyHold {
            version: version.clone(),
            detail,
        });
    }
}

/// M7: every snapshot file, transaction log and manifest the current
/// version's snapshots reference exists. The current version is read through
/// the repository's storage; one that doesn't decode is left to L1. The
/// missing snapshot file or transaction log of a commit that a GC's cutoff
/// came after is no violation: it goes to the commit's
/// [`Warning::GcCutoffAfterSessionStart`](crate::oracle::Warning::GcCutoffAfterSessionStart)
/// in `verdict`.
pub async fn check_current_objects(
    repo: &Repository,
    verdict: &mut Verdict,
) -> anyhow::Result<()> {
    let bytes = read_repo(repo.storage().as_ref())
        .await
        .context("reading the current version")?;
    let Ok(info) = repo_info_file::decode(&bytes) else { return Ok(()) };
    let mut dangling =
        |detail| verdict.violations.push(Violation::DanglingReference { detail });
    let snapshots = info
        .all_snapshots()
        .and_then(|snapshots| snapshots.collect::<Result<Vec<_>, _>>())
        .map_err(|err| {
            anyhow::anyhow!("reading the current version's snapshots: {}", err.kind())
        })?;
    let mut tx_logs: HashSet<SnapshotId> = HashSet::new();
    for snapshot in &snapshots {
        tx_logs.insert(snapshot.id.clone());
        tx_logs.extend(snapshot.pruned_ancestor_tx_logs.iter().cloned());
        let key = format!("{SNAPSHOTS_FILE_PATH}/{}", snapshot.id);
        if !object_exists(repo, &key).await? {
            let id = snapshot.id.to_string();
            if !missing_after_gc_cutoff(&mut verdict.warnings, &id, &key) {
                dangling(format!(
                    "the current version's snapshot {id} has no file {key}"
                ));
            }
            continue;
        }
        let manifests = match manifest_ids(repo, &snapshot.id).await {
            Ok(manifests) => manifests,
            Err(err) => {
                dangling(format!("reading the snapshot {}: {err:#}", snapshot.id));
                continue;
            }
        };
        for manifest in manifests {
            let key = format!("{MANIFESTS_FILE_PATH}/{manifest}");
            if !object_exists(repo, &key).await? {
                dangling(format!(
                    "the current version's snapshot {} references the missing {key}",
                    snapshot.id
                ));
            }
        }
    }
    for id in tx_logs {
        let key = format!("{TRANSACTION_LOGS_FILE_PATH}/{id}");
        if !object_exists(repo, &key).await?
            && !missing_after_gc_cutoff(&mut verdict.warnings, &id.to_string(), &key)
        {
            dangling(format!("the current version references the missing {key}"));
        }
    }
    Ok(())
}

async fn manifest_ids(
    repo: &Repository,
    snapshot: &SnapshotId,
) -> anyhow::Result<Vec<ManifestId>> {
    let snapshot = repo
        .asset_manager()
        .fetch_snapshot(snapshot)
        .await
        .map_err(|err| anyhow::anyhow!("{}", err.kind()))?;
    snapshot
        .manifest_files()
        .map(|info| {
            info.map(|info| info.id).map_err(|err| anyhow::anyhow!("{}", err.kind()))
        })
        .collect()
}

/// Whether the object exists, by a fresh open of its name: a directory
/// listing may be stale on a network filesystem.
pub async fn object_exists(repo: &Repository, key: &str) -> anyhow::Result<bool> {
    let ctx = StorageContext::unattributed(repo.storage_settings());
    // `object_store` rejects empty ranges; these files are never empty.
    match repo.storage().get_object_range(&ctx, key, Some(&(0..1))).await {
        Ok(_) => Ok(true),
        Err(err) if matches!(err.kind(), StorageErrorKind::ObjectNotFound) => Ok(false),
        Err(err) => Err(anyhow::anyhow!("opening {key}: {}", err.kind())),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Utc;

    use super::*;
    use crate::{
        log::OpResult,
        repo_mode::tests::{repo_root, repo_worker, who},
        storage_mode::initial_repo_info,
    };

    fn layout_repo_info() -> Vec<u8> {
        initial_repo_info(&[WriterFeature::FilesystemLayoutV1]).unwrap().to_vec()
    }

    fn history(records: Vec<Record>) -> History {
        let name =
            LogName { tag: "h".to_owned(), slot: "0".to_owned(), incarnation: Some(0) };
        History { logs: BTreeMap::from([(name, records)]), ..History::default() }
    }

    #[test]
    fn layout_defect_wants_the_feature_and_the_runs_status() {
        use RunStatuses::{Online, Toggled};
        let layout = repo_info_file::decode(&layout_repo_info()).unwrap();
        assert_eq!(layout_defect(&layout, Online), None);
        assert_eq!(layout_defect(&layout, Toggled), None);

        let object_store_info = initial_repo_info(&[]).unwrap();
        let detail = file_layout_defect(&object_store_info, Online).unwrap();
        assert!(detail.contains("lists the writer features []"), "{detail}");

        let spec_version = layout.spec_version().unwrap();
        let read_only = |reason: &str| {
            let status = RepoStatus {
                availability: RepoAvailability::ReadOnly,
                set_at: Utc::now(),
                limited_availability_reason: Some(reason.to_owned()),
            };
            layout.set_status(spec_version, &status, Some(("repo.1.X", 50))).unwrap()
        };
        let held = read_only("held");
        let toggled = read_only(TOGGLER_REASON);
        for (info, statuses) in [(&held, Online), (&held, Toggled), (&toggled, Online)] {
            let detail = layout_defect(info, statuses).unwrap();
            assert!(detail.contains("own status is ReadOnly"), "{statuses:?}: {detail}");
        }
        // The status toggler's `ReadOnly` versions are the run's own.
        assert_eq!(layout_defect(&toggled, Toggled), None);

        let detail = file_layout_defect(b"garbage", Online).unwrap();
        assert!(detail.contains("doesn't decode"), "{detail}");
    }

    /// L1 compares the stored status with the one the implementation writes,
    /// which catches availabilities it never writes.
    #[test]
    fn a_status_the_implementation_never_writes_is_a_layout_defect() {
        use icechunk::format::{
            flatbuffers::generated::RepoAvailability as Raw, repo_info::StoredRepoStatus,
            writer_features::FeatureName,
        };
        let layout = repo_info_file::decode(&layout_repo_info()).unwrap();
        let header = layout.header().unwrap();
        let set_at = layout.status().unwrap().set_at;
        let forged = |availability| {
            let feature = vec![FeatureName::new(layout_v1::FEATURE)];
            let status = StoredRepoStatus::for_tests(availability, set_at, None);
            layout.with_raw_header_for_tests(feature, status).unwrap()
        };

        // The written status, forged the same way, is no defect.
        let well_formed = forged(Raw::Online);
        assert_eq!(&well_formed.header().unwrap(), &header);
        assert_eq!(layout_defect(&well_formed, RunStatuses::Online), None);

        for availability in [Raw::Offline, Raw(9)] {
            let defect = layout_defect(&forged(availability), RunStatuses::Online);
            assert!(
                defect.as_deref().is_some_and(|defect| {
                    defect.contains("isn't stored as the implementation writes it")
                }),
                "{availability:?}: {defect:?}"
            );
        }
    }

    /// M4 on forged records.
    #[test]
    fn a_change_during_a_read_only_hold_is_a_violation() {
        let held = |version: Option<&str>, current: &str| Record::ReadOnlyHeld {
            t_ack_ms: 1000,
            version: version.map(str::to_owned),
            t_end_ms: 4000,
            current: current.to_owned(),
        };
        let check = |records| {
            let mut verdict = Verdict::default();
            check_readonly_holds(&history(records), &mut verdict);
            verdict
        };

        let fine = check(vec![held(Some("h"), "h"), held(Some("i"), "i")]);
        assert_eq!(fine.violations, []);
        assert_eq!(fine.stats.readonly_holds, 2);

        for (case, record, expected) in [
            (
                "during the hold",
                held(Some("h"), "later"),
                "the current version was later",
            ),
            ("right after the ReadOnly change", held(None, "later"), "right after"),
        ] {
            let verdict = check(vec![held(Some("h"), "h"), record]);
            assert!(
                matches!(&verdict.violations[..], [Violation::ChangeDuringReadOnlyHold {
                    detail, ..
                }] if detail.contains(expected)),
                "{case}: {verdict:?}"
            );
            assert_eq!(verdict.stats.readonly_holds, 2, "{case}");
        }
    }

    #[test]
    fn the_first_version_is_numbered_by_its_creation_time() {
        let init = |n, t_before_ms, t_after_ms| Record::InitVersion {
            n,
            t_before_ms,
            t_after_ms,
        };
        let check = |records| {
            let mut verdict = Verdict::default();
            check_init_version(&history(records), &mut verdict);
            verdict.violations
        };
        assert_eq!(check(vec![init(15, 10, 20)]), []);
        assert_eq!(check(vec![init(10, 10, 10)]), []);
        assert_eq!(
            check(vec![init(9, 10, 20)]),
            [Violation::InitialVersion { n: 9, t_before_ms: 10, t_after_ms: 20 }]
        );
        assert_eq!(
            check(vec![init(21, 10, 20)]),
            [Violation::InitialVersion { n: 21, t_before_ms: 10, t_after_ms: 20 }]
        );
        for records in [vec![], vec![init(15, 10, 20), init(15, 10, 20)]] {
            assert!(matches!(&check(records)[..], [Violation::Harness { detail }]
                    if detail.contains("InitVersion")),);
        }
    }

    #[test]
    fn a_process_died_by_its_kill_or_its_next_incarnations_start() {
        let log = |tag: &str, slot: &str, incarnation| LogName {
            tag: tag.to_owned(),
            slot: slot.to_owned(),
            incarnation,
        };
        let killed = |t_ms, slot: &str, incarnation| Record::ExternalKill {
            t_ms,
            slot: slot.to_owned(),
            incarnation,
        };
        let start = |t_ms| Record::Start {
            t_ms,
            host: "h".to_owned(),
            slot: "1".to_owned(),
            incarnation: 0,
            pid: 1,
        };
        // Other slots', incarnations' and hosts' kills don't count.
        let decoys = vec![killed(100, "2", 3), killed(110, "1", 2)];
        let died = |kills: Vec<Record>, later_starts: bool| {
            let mut logs = vec![
                (log("h", "1", Some(3)), vec![start(5)]),
                (log("h", SUPERVISOR_SLOT, None), kills),
                (log("g", SUPERVISOR_SLOT, None), vec![killed(120, "1", 3)]),
                (log("h", "2", Some(4)), vec![start(200)]),
            ];
            if later_starts {
                logs.extend([
                    (log("h", "1", Some(4)), vec![]),
                    (log("h", "1", Some(5)), vec![start(400)]),
                    (log("h", "1", Some(6)), vec![start(300)]),
                ]);
            }
            let history =
                History { logs: logs.into_iter().collect(), ..History::default() };
            died_by_ms(&history, &log("h", "1", Some(3)))
        };
        assert_eq!(died(decoys.clone(), true), Some(300));
        let kill = [decoys.clone(), vec![killed(250, "1", 3)]].concat();
        assert_eq!(died(kill, true), Some(250));
        assert_eq!(died(decoys, false), None);
    }

    #[test]
    fn versions_that_were_current_are_the_repo_info_backups_and_the_current_one() {
        let dir = tempfile::tempdir().unwrap();
        let overwritten = dir.path().join(BACKUP_DIR);
        fs::create_dir_all(&overwritten).unwrap();
        let layout = layout_repo_info();
        let object_store_info = initial_repo_info(&[]).unwrap().to_vec();
        fs::write(overwritten.join("repo.1.A"), &layout).unwrap();
        fs::write(overwritten.join("repo.2.B"), &layout).unwrap();
        fs::write(overwritten.join("repo.3.C"), &object_store_info).unwrap();
        fs::write(overwritten.join("repo.4.D.staging.x"), b"torn").unwrap();
        fs::write(overwritten.join("config.yaml.5.E"), b"config").unwrap();
        let current = b"current".to_vec();

        let mut verdict = Verdict::default();
        let was_current = versions_that_were_current(
            dir.path(),
            &current,
            RunStatuses::Online,
            &HashSet::new(),
            &mut verdict,
        )
        .unwrap();
        let expected: HashSet<String> = [&layout, &object_store_info, &current]
            .into_iter()
            .map(|b| payload_etag(b))
            .collect();
        assert_eq!(was_current, expected);
        let what: Vec<&str> = verdict
            .violations
            .iter()
            .map(|v| match v {
                Violation::FeatureMissing { what, .. } => what.as_str(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(what, ["the current version", "the backup overwritten/repo.3.C"]);

        // A backup of an earlier era is a version that was current, unchecked.
        let earlier = HashSet::from(["repo.3.C".to_owned()]);
        let mut verdict = Verdict::default();
        let with_earlier = versions_that_were_current(
            dir.path(),
            &layout,
            RunStatuses::Online,
            &earlier,
            &mut verdict,
        )
        .unwrap();
        assert_eq!(
            with_earlier,
            [&layout, &object_store_info].into_iter().map(|b| payload_etag(b)).collect()
        );
        assert_eq!(verdict.violations, []);

        let empty = tempfile::tempdir().unwrap();
        let mut verdict = Verdict::default();
        let layout_only = versions_that_were_current(
            empty.path(),
            &layout,
            RunStatuses::Online,
            &HashSet::new(),
            &mut verdict,
        )
        .unwrap();
        assert_eq!(layout_only, HashSet::from([payload_etag(&layout)]));
        assert_eq!(verdict, Verdict::default());
    }

    /// M7: a snapshot file, a transaction log or a manifest the current
    /// version's snapshots reference that doesn't exist.
    #[tokio::test]
    async fn objects_the_current_version_references_must_exist() {
        let (_dir, root, config) = repo_root().await;
        let mut worker = repo_worker(&root, &config, &who("h", "0", 0)).await;
        assert!(worker.ensure_branch().await.unwrap());
        for _ in 0..3 {
            assert!(matches!(
                worker.branch_commit().await.unwrap(),
                Some(OpResult::Ack { .. })
            ));
        }
        let repo = crate::repo_mode::open_repo(&root, &config, None).await.unwrap();
        let storage = root.storage();
        let first_file = |dir: &str| {
            let mut names: Vec<_> = fs::read_dir(storage.join(dir))
                .unwrap()
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .collect();
            names.sort();
            names.remove(0)
        };
        let check = || async {
            let mut verdict = Verdict::default();
            check_current_objects(&repo, &mut verdict).await.unwrap();
            verdict.violations
        };
        assert_eq!(check().await, []);
        for dir in [MANIFESTS_FILE_PATH, TRANSACTION_LOGS_FILE_PATH, SNAPSHOTS_FILE_PATH]
        {
            let name = first_file(dir);
            let path = storage.join(dir).join(&name);
            let bytes = fs::read(&path).unwrap();
            fs::remove_file(&path).unwrap();
            let violations = check().await;
            assert!(
                matches!(&violations[..], [Violation::DanglingReference { detail }]
                    if detail.contains(&format!("{dir}/{name}"))),
                "{dir}: {violations:?}"
            );
            fs::write(&path, bytes).unwrap();
        }
    }

    /// M7 after a GC whose cutoff came after a commit's start, as in a run on
    /// Lustre: that commit's missing snapshot file and transaction log are
    /// part of its warning; another snapshot's missing transaction log, or
    /// those of a commit that started after the cutoff, are violations.
    #[tokio::test]
    async fn objects_of_a_commit_a_gc_cutoff_came_after_are_warned() {
        use crate::oracle::{
            Warning,
            gc_cutoff::{
                check_gc_cutoffs,
                tests::{CUTOFF_MS, incident},
            },
        };
        let (_dir, root, config) = repo_root().await;
        let mut worker = repo_worker(&root, &config, &who("h", "0", 0)).await;
        assert!(worker.ensure_branch().await.unwrap());
        let mut snapshots = Vec::new();
        for _ in 0..2 {
            match worker.branch_commit().await.unwrap() {
                Some(OpResult::Ack { snapshot: Some(snapshot) }) => {
                    snapshots.push(snapshot);
                }
                other => panic!("expected an ack, got {other:?}"),
            }
        }
        let [first, second] = &snapshots[..] else { unreachable!() };
        let repo = crate::repo_mode::open_repo(&root, &config, None).await.unwrap();
        let storage = root.storage();
        let key = |dir: &str, id: &str| format!("{dir}/{id}");
        for missing in [
            key(SNAPSHOTS_FILE_PATH, first),
            key(TRANSACTION_LOGS_FILE_PATH, first),
            key(TRANSACTION_LOGS_FILE_PATH, second),
        ] {
            fs::remove_file(storage.join(missing)).unwrap();
        }
        let check = |intent_ms| {
            let (commit, gc) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
            let history = incident(commit, gc, first, intent_ms);
            let repo = &repo;
            async move {
                let mut verdict = Verdict::default();
                check_gc_cutoffs(&history, &mut verdict);
                check_current_objects(repo, &mut verdict).await.unwrap();
                verdict
            }
        };

        let verdict = check(973_998).await;
        let second_tx_log = key(TRANSACTION_LOGS_FILE_PATH, second);
        assert!(
            matches!(&verdict.violations[..], [Violation::DanglingReference { detail }]
                if detail.contains(&second_tx_log)),
            "{verdict:?}"
        );
        let excused =
            [key(SNAPSHOTS_FILE_PATH, first), key(TRANSACTION_LOGS_FILE_PATH, first)];
        assert!(
            matches!(&verdict.warnings[..], [Warning::GcCutoffAfterSessionStart { missing, .. }]
                if *missing == excused),
            "{verdict:?}"
        );

        let verdict = check(CUTOFF_MS + 1).await;
        assert_eq!(verdict.warnings, []);
        assert_eq!(verdict.violations.len(), 3, "{verdict:?}");
    }

    /// M7 through the workload: after a branch deletion or an expiration, GC
    /// deletes only what the current version no longer references.
    #[tokio::test]
    async fn gc_after_a_deletion_or_an_expiration_leaves_nothing_dangling() {
        for expiration in [false, true] {
            let (_dir, root, config) = repo_root().await;
            let mut worker = repo_worker(&root, &config, &who("h", "0", 0)).await;
            assert!(worker.ensure_branch().await.unwrap());
            for _ in 0..2 {
                assert!(matches!(
                    worker.branch_commit().await.unwrap(),
                    Some(OpResult::Ack { .. })
                ));
            }
            let repo = crate::repo_mode::open_repo(&root, &config, None).await.unwrap();
            if expiration {
                // Releases the first branch commit, which no ref points at.
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                let older_than_ms = crate::log::now_ms();
                crate::repo_mode::expire_snapshots(&repo, &config, older_than_ms)
                    .await
                    .unwrap();
            } else {
                assert!(matches!(
                    worker.delete_branch().await.unwrap(),
                    OpResult::Ack { .. }
                ));
            }
            let repo = crate::repo_mode::open_repo(&root, &config, None).await.unwrap();
            let snapshots =
                fs::read_dir(root.storage().join(SNAPSHOTS_FILE_PATH)).unwrap().count();
            crate::repo_mode::collect_garbage(&repo, &config, Utc::now()).await.unwrap();
            let left =
                fs::read_dir(root.storage().join(SNAPSHOTS_FILE_PATH)).unwrap().count();
            assert!(left < snapshots, "expiration: {expiration}: GC deleted nothing");
            let mut verdict = Verdict::default();
            check_current_objects(&repo, &mut verdict).await.unwrap();
            assert_eq!(verdict.violations, [], "expiration: {expiration}");
        }
    }
}
