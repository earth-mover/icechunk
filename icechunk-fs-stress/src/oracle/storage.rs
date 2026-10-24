//! Storage mode: rebuilds the chain of repo infos `repo` went through from
//! the backups, and checks every logged intent against it.

use std::{
    collections::{HashMap, HashSet},
    fs, io,
    path::Path,
};

use anyhow::Context as _;
use uuid::Uuid;

use crate::{
    log::{Op, OpResult},
    oracle::{
        History, Verdict, Violation, Warning,
        fork::unused_fork_backups,
        intents,
        mirror::{RunStatuses, layout_defect},
    },
    storage_mode::{BACKUP_DIR, Decoded, decode, payload_etag},
};

/// What the oracle keeps of a repo info: the payload but its padding, the
/// attempt and the L1 defect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    pub id: Uuid,
    /// `None` only for the initial repo info.
    pub parent: Option<String>,
    pub host: String,
    pub slot: String,
    pub incarnation: u32,
    pub seq: u64,
    /// The backup path its update named on its predecessor entry.
    pub attempt: Option<String>,
    /// Why it isn't a repo info of the filesystem layout, if it isn't.
    pub layout_defect: Option<String>,
}

impl From<Decoded> for Link {
    fn from(decoded: Decoded) -> Self {
        let layout_defect = layout_defect(&decoded.info, RunStatuses::Online);
        let attempt = decoded.attempt;
        match decoded.payload {
            Some(payload) => Self {
                id: payload.id,
                parent: Some(payload.parent),
                host: payload.host,
                slot: payload.slot,
                incarnation: payload.incarnation,
                seq: payload.seq,
                attempt,
                layout_defect,
            },
            None => Self {
                id: Uuid::nil(),
                parent: None,
                host: String::new(),
                slot: String::new(),
                incarnation: 0,
                seq: 0,
                attempt,
                layout_defect,
            },
        }
    }
}

/// The backups, without their bytes, so that memory scales with the number
/// of distinct backed-up versions rather than with the backups' size.
#[derive(Debug, Default)]
pub struct Backups {
    /// The decodable backups by etag, each decoded once: many attempts back
    /// up the same base.
    pub links: HashMap<String, Link>,
    /// The etag of every backup by its name in `overwritten/`.
    pub names: HashMap<String, String>,
    /// Each undecodable backup file's name, and why it doesn't decode.
    pub unparsable: Vec<(String, String)>,
}

impl Backups {
    fn add(&mut self, name: &str, bytes: &[u8]) {
        let etag = payload_etag(bytes);
        self.names.insert(name.to_owned(), etag.clone());
        if self.links.contains_key(&etag) {
            return;
        }
        match decode(bytes) {
            Ok(decoded) => {
                self.links.insert(etag, decoded.into());
            }
            Err(err) => {
                self.unparsable.push((name.to_owned(), format!("{err:#}")));
            }
        }
    }
}

/// Every backup under `storage_root`, read one file at a time, skipping the
/// staging files of interrupted copies.
pub fn read_backups(storage_root: &Path) -> anyhow::Result<Backups> {
    let dir = storage_root.join(BACKUP_DIR);
    let context = || format!("listing {}", dir.display());
    let mut backups = Backups::default();
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        // No attempt got as far as a backup.
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(backups),
        Err(err) => return Err(err).with_context(context),
    };
    for entry in entries {
        let entry = entry.with_context(context)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.contains(".staging.") {
            continue;
        }
        if let Some(bytes) = read_listed_backup(&entry.path())? {
            backups.add(&name, &bytes);
        }
    }
    Ok(backups)
}

/// A backup a listing of `overwritten/` returned, `None` if it is gone since:
/// a killed worker's late delete of its lost attempt's backup. A missing
/// backup that a landed version names still breaks the chain in `walk`.
pub(crate) fn read_listed_backup(path: &Path) -> anyhow::Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("reading {}", path.display())),
    }
}

/// Rebuilds the chain from `final_repo` back to the initial repo info and
/// checks every intent in `history` against it. `None` if `repo` couldn't be
/// read, which the caller reports. An undecodable backup breaks the history
/// unless it is a fork write's interrupted copy.
pub fn check_chain(
    final_repo: Option<&[u8]>,
    backups: &Backups,
    history: &History,
    network_fs: bool,
    verdict: &mut Verdict,
) {
    let partial = unused_fork_backups(history);
    for (name, error) in &backups.unparsable {
        if partial.contains(name.as_str()) {
            verdict.warnings.push(Warning::ForkActorPartialBackup {
                backup: name.clone(),
                detail: error.clone(),
            });
        } else {
            verdict.violations.push(Violation::BrokenHistory {
                detail: format!("backup {BACKUP_DIR}/{name}: {error}"),
            });
        }
    }
    let head = final_repo.and_then(|bytes| {
        decode(bytes)
            .map_err(|err| {
                verdict.violations.push(Violation::BrokenHistory {
                    detail: format!("final repo: {err:#}"),
                });
            })
            .ok()
            .map(|head| (payload_etag(bytes), Link::from(head)))
    });
    let chain = head
        .as_ref()
        .map(|(etag, head)| walk(etag.clone(), head, backups, verdict))
        .unwrap_or_default();
    let index: HashMap<&str, usize> = chain
        .links
        .iter()
        .enumerate()
        .map(|(position, (etag, _))| (etag.as_str(), position))
        .collect();
    let predecessor =
        |position: usize| chain.links.get(position + 1).map(|(etag, _)| etag);

    let mut claimed = HashSet::new();
    for (id, (op, result, _)) in intents(history, verdict) {
        let Op::Cas { base, new, .. } = op else {
            verdict.violations.push(Violation::Harness {
                detail: format!("storage-mode intent {id} is {op:?}"),
            });
            continue;
        };
        verdict.stats.intents += 1;
        let landed = index.get(new.as_str()).copied();
        match result {
            Some(result) if result.landed().is_some() => {
                verdict.stats.acks += 1;
                let detail = match landed.map(predecessor) {
                    Some(Some(before)) if *before == base => None,
                    Some(Some(before)) => Some(format!(
                        "{new} landed after {before}, not after its base {base}"
                    )),
                    // Beyond the break, or right before it.
                    None | Some(None) if !chain.complete => None,
                    Some(None) => Some(format!("{new} is the initial repo info")),
                    None => Some(format!("{new} is not on the chain")),
                };
                if let Some(detail) = detail {
                    verdict.violations.push(Violation::LostUpdate { id, detail });
                }
            }
            Some(OpResult::Conflict) => {
                verdict.stats.conflicts += 1;
                if landed.is_some() {
                    if network_fs {
                        verdict.warnings.push(Warning::LostReplyConflict { id });
                    } else {
                        verdict
                            .violations
                            .push(Violation::FalseConflict { id, landed_twice: false });
                    }
                }
            }
            Some(failed) => {
                if failed.is_refusal() {
                    verdict.stats.refused += 1;
                } else {
                    verdict.stats.errors += 1;
                }
                if landed.is_some() {
                    verdict.warnings.push(Warning::AmbiguousErrorLanded { id });
                }
            }
            None => verdict.stats.unresolved += 1,
        }
        claimed.insert(new);
    }

    let initial = chain.complete.then(|| chain.links.len() - 1);
    for (position, (etag, link)) in chain.links.iter().enumerate() {
        if Some(position) != initial && !claimed.contains(etag) {
            verdict.violations.push(Violation::Phantom {
                what: format!(
                    "chain element {etag} (id {}, written by {}.{}.{} as seq {})",
                    link.id, link.host, link.slot, link.incarnation, link.seq
                ),
            });
        }
    }
    verdict.stats.chain_len = chain.links.len() as u64;
}

/// L1 on the final repo info and every distinct backup: each is a version
/// that was current, since every backup is copied conditionally on the
/// version it backs up. Returns their etags, the versions that were current.
pub fn check_versions(
    final_repo: Option<&[u8]>,
    backups: &Backups,
    verdict: &mut Verdict,
) -> HashSet<String> {
    let head = final_repo.map(|bytes| {
        let defect = decode(bytes)
            .ok()
            .and_then(|decoded| layout_defect(&decoded.info, RunStatuses::Online));
        (payload_etag(bytes), defect)
    });
    let versions = backups
        .links
        .iter()
        .map(|(etag, link)| (etag.clone(), link.layout_defect.clone()))
        .chain(head);
    let mut was_current = HashSet::new();
    for (etag, defect) in versions {
        if let Some(detail) = defect {
            verdict.violations.push(Violation::FeatureMissing {
                what: format!("the version {etag}"),
                detail,
            });
        }
        was_current.insert(etag);
    }
    was_current
}

/// The payloads `repo` went through, the final one first.
#[derive(Debug, Default)]
struct Chain<'a> {
    links: Vec<(String, &'a Link)>,
    /// Whether the chain reaches the initial payload, which is then last.
    complete: bool,
}

/// Follows `parent` from `head` through `backups`, checking that each repo
/// info's attempt names a backup of its parent.
fn walk<'a>(
    head_etag: String,
    head: &'a Link,
    backups: &'a Backups,
    verdict: &mut Verdict,
) -> Chain<'a> {
    let mut broken =
        |detail| verdict.violations.push(Violation::BrokenHistory { detail });
    let mut seen = HashSet::from([head_etag.clone()]);
    let mut links = Vec::new();
    let (mut etag, mut link) = (head_etag, head);
    let complete = loop {
        let next = match &link.parent {
            None => {
                let initial = link.id == Uuid::nil();
                if !initial {
                    broken(format!(
                        "the chain ends at {etag} (id {}), not at the initial repo info",
                        link.id
                    ));
                }
                break initial;
            }
            Some(parent) if !seen.insert(parent.clone()) => {
                broken(format!("cycle: {etag} has the earlier {parent} as its parent"));
                break false;
            }
            Some(parent) => {
                let Some(next) = backups.links.get(parent) else {
                    broken(format!("no backup of {parent}, the parent of {etag}"));
                    break false;
                };
                let named =
                    link.attempt.as_ref().and_then(|name| backups.names.get(name));
                if named != Some(parent) {
                    broken(format!(
                        "{etag} names the backup {:?}, which holds {named:?}, not its \
                         parent {parent}",
                        link.attempt
                    ));
                }
                (parent.clone(), next)
            }
        };
        links.push((etag, link));
        (etag, link) = next;
    };
    links.push((etag, link));
    Chain { links, complete }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use chrono::Utc;
    use icechunk::format::{
        repo_info::{RepoAvailability, RepoStatus},
        repo_info_file,
        writer_features::WriterFeature,
    };
    use uuid::Uuid;

    use super::*;
    use crate::{
        fork::FORK_SLOT,
        log::{LogName, Op, OpResult, Record},
        oracle::{Violation, Warning},
        storage_mode::{Payload, initial_repo_info, next_repo_info, payload_etag},
    };

    fn initial() -> Vec<u8> {
        initial_repo_info(&[WriterFeature::FilesystemLayoutV1]).unwrap().to_vec()
    }

    /// A repo info `id` wrote on top of `parent`, and the backup path its
    /// attempt names.
    fn child(id: Uuid, parent: &[u8]) -> (Vec<u8>, String) {
        let backup = repo_info_file::new_backup_path();
        let payload = Payload {
            id,
            parent: payload_etag(parent),
            host: "h".to_owned(),
            slot: "0".to_owned(),
            incarnation: 0,
            seq: 0,
            padding: "xx".to_owned(),
        };
        let base = repo_info_file::decode(parent).unwrap();
        (next_repo_info(&base, &payload, &backup).unwrap().to_vec(), backup)
    }

    fn intent(id: Uuid, base: &[u8], new: &(Vec<u8>, String)) -> Record {
        let op = Op::Cas {
            base: payload_etag(base),
            new: payload_etag(&new.0),
            backup: new.1.clone(),
        };
        Record::Intent { t_ms: 0, id, op }
    }

    fn outcome(id: Uuid, result: OpResult) -> Record {
        Record::Outcome { t_ms: 0, id, result }
    }

    fn ack(id: Uuid) -> Record {
        outcome(id, OpResult::Ack { snapshot: None })
    }

    fn history(records: Vec<Record>) -> History {
        let name =
            LogName { tag: "h".to_owned(), slot: "0".to_owned(), incarnation: Some(0) };
        History { logs: BTreeMap::from([(name, records)]), ..History::default() }
    }

    fn indexed(backups: &[(&str, &[u8])]) -> Backups {
        let mut indexed = Backups::default();
        for (name, bytes) in backups {
            indexed.add(name, bytes);
        }
        indexed
    }

    fn check(
        final_repo: &[u8],
        backups: &[(&str, &[u8])],
        records: Vec<Record>,
        network_fs: bool,
    ) -> Verdict {
        check_history(final_repo, backups, &history(records), network_fs)
    }

    fn check_history(
        final_repo: &[u8],
        backups: &[(&str, &[u8])],
        history: &History,
        network_fs: bool,
    ) -> Verdict {
        let mut verdict = Verdict::default();
        check_chain(
            Some(final_repo),
            &indexed(backups),
            history,
            network_fs,
            &mut verdict,
        );
        verdict
    }

    /// `records`, and a fork actor's log announcing the backups `announced`
    /// and then writing forks that name the backups `written`.
    fn with_fork_actor(
        records: Vec<Record>,
        announced: &[&str],
        written: &[&str],
    ) -> History {
        let announcements = announced
            .iter()
            .map(|backup| Record::ForkBackup { t_ms: 0, backup: (*backup).to_owned() });
        let forks = written.iter().map(|backup| Record::ForkWritten {
            t_before_ms: 0,
            t_after_ms: 0,
            base_hash: "base".to_owned(),
            fork_hash: "fork".to_owned(),
            backup: (*backup).to_owned(),
            gc: None,
        });
        let mut history = history(records);
        let name = LogName {
            tag: "h".to_owned(),
            slot: FORK_SLOT.to_owned(),
            incarnation: Some(0),
        };
        history.logs.insert(name, announcements.chain(forks).collect());
        history
    }

    fn ids<const N: usize>() -> [Uuid; N] {
        std::array::from_fn(|_| Uuid::new_v4())
    }

    #[test]
    fn clean_three_update_chain_passes() {
        let [a, b, c, d] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let p2 = child(b, &p1.0);
        let p3 = child(c, &p2.0);
        let lost = child(d, &p2.0);
        let records = vec![
            intent(a, &p0, &p1),
            ack(a),
            intent(b, &p1.0, &p2),
            intent(d, &p2.0, &lost),
            ack(b),
            intent(c, &p2.0, &p3),
            outcome(d, OpResult::Conflict),
            ack(c),
        ];
        // Several attempts back up the same base.
        let backups: &[(&str, &[u8])] =
            &[(&p1.1, &p0), (&p2.1, &p1.0), (&p3.1, &p2.0), (&lost.1, &p2.0)];
        let verdict = check(&p3.0, backups, records, false);
        assert_eq!(verdict.violations, []);
        assert_eq!(verdict.warnings, []);
        let stats = &verdict.stats;
        assert_eq!(
            (stats.intents, stats.acks, stats.conflicts, stats.errors, stats.unresolved),
            (4, 3, 1, 0, 0)
        );
        assert_eq!(stats.chain_len, 4);

        let mut verdict = Verdict::default();
        let was_current = check_versions(Some(&p3.0), &indexed(backups), &mut verdict);
        assert_eq!(verdict, Verdict::default());
        let expected: HashSet<String> =
            [&p0, &p1.0, &p2.0, &p3.0].into_iter().map(|p| payload_etag(p)).collect();
        assert_eq!(was_current, expected);
    }

    /// A writer deletes the backup of an attempt it proved didn't land: the
    /// history is the same without it, since the change that replaced the
    /// base backed the base up too. A landed change's backup is still needed.
    #[test]
    fn a_conflicting_attempt_without_its_backup_passes() {
        let [a, b, d] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let p2 = child(b, &p1.0);
        let lost = child(d, &p1.0);
        let records = || {
            vec![
                intent(a, &p0, &p1),
                ack(a),
                intent(b, &p1.0, &p2),
                intent(d, &p1.0, &lost),
                ack(b),
                outcome(d, OpResult::Conflict),
            ]
        };
        let landed: &[(&str, &[u8])] = &[(&p1.1, &p0), (&p2.1, &p1.0)];
        let verdict = check(&p2.0, landed, records(), false);
        assert_eq!(verdict.violations, []);
        assert_eq!(verdict.warnings, []);
        assert_eq!((verdict.stats.acks, verdict.stats.conflicts), (2, 1));
        let mut verdict = Verdict::default();
        let was_current = check_versions(Some(&p2.0), &indexed(landed), &mut verdict);
        assert_eq!(verdict, Verdict::default());
        let expected: HashSet<String> =
            [&p0, &p1.0, &p2.0].into_iter().map(|p| payload_etag(p)).collect();
        assert_eq!(was_current, expected);

        let without_landed: &[(&str, &[u8])] = &[(&p1.1, &p0), (&lost.1, &p1.0)];
        let verdict = check(&p2.0, without_landed, records(), false);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                if detail.contains(&p2.1)),
            "{verdict:?}"
        );
    }

    #[test]
    fn versions_without_the_feature_or_the_runs_status_are_feature_missing() {
        let [a] = ids();
        let p0 = initial();
        let featureless = initial_repo_info(&[]).unwrap().to_vec();
        let base = repo_info_file::decode(&p0).unwrap();
        let spec_version = base.spec_version().unwrap();
        let read_only = RepoStatus {
            availability: RepoAvailability::ReadOnly,
            set_at: Utc::now(),
            limited_availability_reason: None,
        };
        let read_only = base.set_status(spec_version, &read_only, Some(("repo.2.X", 50)));
        let read_only =
            repo_info_file::encode(&read_only.unwrap(), spec_version, 3).unwrap();
        // A worker's update keeps the status it found.
        let head = child(a, &read_only);
        let backups: &[(&str, &[u8])] = &[("repo.1.X", &featureless), ("repo.2.X", &p0)];
        let mut verdict = Verdict::default();
        check_versions(Some(&head.0), &indexed(backups), &mut verdict);
        let mut missing: Vec<(String, String)> = verdict
            .violations
            .iter()
            .map(|v| match v {
                Violation::FeatureMissing { what, detail } => {
                    (what.clone(), detail.clone())
                }
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        missing.sort();
        let version = |bytes: &[u8]| format!("the version {}", payload_etag(bytes));
        let mut expected = vec![
            (version(&featureless), "lists the writer features []"),
            (version(&head.0), "own status is ReadOnly"),
        ];
        expected.sort();
        assert_eq!(missing.len(), 2, "{verdict:?}");
        for ((what, detail), (expected_what, expected_detail)) in
            missing.iter().zip(&expected)
        {
            assert_eq!(what, expected_what);
            assert!(detail.contains(expected_detail), "{detail}");
        }
    }

    #[test]
    fn fork_is_a_lost_update() {
        let [a, b] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let p2 = child(b, &p0);
        let records = vec![intent(a, &p0, &p1), intent(b, &p0, &p2), ack(a), ack(b)];
        let verdict = check(&p2.0, &[(&p1.1, &p0), (&p2.1, &p0)], records, false);
        assert!(
            matches!(&verdict.violations[..], [Violation::LostUpdate { id, .. }] if *id == a),
            "{verdict:?}"
        );
    }

    #[test]
    fn ack_missing_from_the_chain_is_a_lost_update() {
        let [a, x] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let px = child(x, &p1.0);
        let records = vec![intent(a, &p0, &p1), ack(a), intent(x, &p1.0, &px), ack(x)];
        let verdict = check(&p1.0, &[(&p1.1, &p0), (&px.1, &p1.0)], records, true);
        assert!(
            matches!(&verdict.violations[..], [Violation::LostUpdate { id, .. }] if *id == x),
            "{verdict:?}"
        );
    }

    #[test]
    fn ack_after_another_predecessor_is_a_lost_update() {
        let [a, b] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let p2 = child(b, &p1.0);
        let records = vec![intent(a, &p0, &p1), ack(a), intent(b, &p0, &p2), ack(b)];
        let verdict = check(&p2.0, &[(&p1.1, &p0), (&p2.1, &p1.0)], records, false);
        assert!(
            matches!(&verdict.violations[..], [Violation::LostUpdate { id, .. }] if *id == b),
            "{verdict:?}"
        );
    }

    #[test]
    fn chain_element_without_intent_is_a_phantom() {
        let [a, b] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let p2 = child(b, &p1.0);
        let records = vec![intent(b, &p1.0, &p2), ack(b)];
        let verdict = check(&p2.0, &[(&p1.1, &p0), (&p2.1, &p1.0)], records, false);
        assert!(
            matches!(&verdict.violations[..], [Violation::Phantom { what }]
                if what.contains(&payload_etag(&p1.0))),
            "{verdict:?}"
        );
    }

    #[test]
    fn conflict_that_landed_fails_only_on_a_local_fs() {
        let [a] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let records = || vec![intent(a, &p0, &p1), outcome(a, OpResult::Conflict)];

        let local = check(&p1.0, &[(&p1.1, &p0)], records(), false);
        assert_eq!(
            local.violations,
            [Violation::FalseConflict { id: a, landed_twice: false }]
        );
        assert_eq!(local.warnings, []);

        let network = check(&p1.0, &[(&p1.1, &p0)], records(), true);
        assert_eq!(network.violations, []);
        assert_eq!(network.warnings, [Warning::LostReplyConflict { id: a }]);
    }

    #[test]
    fn error_that_landed_is_a_warning() {
        let [a] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        for network_fs in [false, true] {
            let error = OpResult::Error { message: "timed out".to_owned() };
            let records = vec![intent(a, &p0, &p1), outcome(a, error)];
            let verdict = check(&p1.0, &[(&p1.1, &p0)], records, network_fs);
            assert_eq!(verdict.violations, []);
            assert_eq!(verdict.warnings, [Warning::AmbiguousErrorLanded { id: a }]);
            assert_eq!(verdict.stats.errors, 1);
        }
    }

    #[test]
    fn missing_backup_breaks_history() {
        let [a, b] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let p2 = child(b, &p1.0);
        let records = vec![intent(a, &p0, &p1), ack(a), intent(b, &p1.0, &p2), ack(b)];
        let verdict = check(&p2.0, &[(&p1.1, &p0)], records, false);
        // `a`'s ack is beyond the break, so it can't be checked.
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                if detail.contains(&payload_etag(&p1.0))),
            "{verdict:?}"
        );
    }

    /// The attempt a repo info names must be a backup of its parent: here
    /// `b`'s backup holds `p0`, though another attempt backed `p1` up.
    #[test]
    fn an_attempt_naming_another_backup_breaks_history() {
        let [a, b, x] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let p2 = child(b, &p1.0);
        let px = child(x, &p1.0);
        let records = vec![intent(a, &p0, &p1), ack(a), intent(b, &p1.0, &p2), ack(b)];
        let backups: &[(&str, &[u8])] = &[(&p1.1, &p0), (&p2.1, &p0), (&px.1, &p1.0)];
        let verdict = check(&p2.0, backups, records, false);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                if detail.contains(&p2.1) && detail.contains(&payload_etag(&p1.0))),
            "{verdict:?}"
        );
    }

    fn link(id: Uuid, parent: Option<&str>, attempt: Option<&str>) -> Link {
        Link {
            id,
            parent: parent.map(str::to_owned),
            host: "h".to_owned(),
            slot: "0".to_owned(),
            incarnation: 0,
            seq: 0,
            attempt: attempt.map(str::to_owned),
            layout_defect: None,
        }
    }

    /// Real repo infos can't end anywhere else: [`decode`] rejects one
    /// without stress metadata after the initial one.
    #[test]
    fn chain_must_end_at_the_initial_repo_info() {
        let [a] = ids();
        let mut verdict = Verdict::default();
        let head = link(a, None, None);
        let none = Backups::default();
        let chain = walk("e1".to_owned(), &head, &none, &mut verdict);
        assert!(!chain.complete);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                if detail.contains("e1")),
            "{verdict:?}"
        );
    }

    #[test]
    fn cycle_breaks_history() {
        // Real etags can't form a cycle, so the walk gets made-up ones.
        let [a, b] = ids();
        let backups = Backups {
            links: HashMap::from([("e2".to_owned(), link(b, Some("e1"), Some("b1")))]),
            names: HashMap::from([
                ("b1".to_owned(), "e1".to_owned()),
                ("b2".to_owned(), "e2".to_owned()),
            ]),
            unparsable: vec![],
        };
        let mut verdict = Verdict::default();
        let head = link(a, Some("e2"), Some("b2"));
        let chain = walk("e1".to_owned(), &head, &backups, &mut verdict);
        assert!(!chain.complete);
        assert_eq!(chain.links.len(), 2);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                if detail.contains("e1")),
            "{verdict:?}"
        );

        let mut verdict = Verdict::default();
        let head = link(a, Some("e1"), Some("b1"));
        let none = Backups::default();
        let chain = walk("e1".to_owned(), &head, &none, &mut verdict);
        assert!(!chain.complete);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { .. }]),
            "{verdict:?}"
        );
    }

    #[test]
    fn undecodable_repo_infos_break_history() {
        let [a] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let records = || vec![intent(a, &p0, &p1), ack(a)];
        let backups: &[(&str, &[u8])] = &[(&p1.1, &p0), ("repo.9.Y", b"garbage")];
        let verdict = check(&p1.0, backups, records(), false);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                if detail.contains("repo.9.Y")),
            "{verdict:?}"
        );

        let verdict = check(b"garbage", &[(&p1.1, &p0)], records(), false);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { .. }]),
            "{verdict:?}"
        );
    }

    /// The fork actor's copy of `repo` to its backup, interrupted as by
    /// `ESTALE` on NFS, leaves a partial backup that no ops log references.
    #[test]
    fn only_an_announced_partial_fork_backup_is_a_warning() {
        let [a] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let records = || vec![intent(a, &p0, &p1), ack(a)];
        let backups: &[(&str, &[u8])] = &[(&p1.1, &p0), ("repo.9.B", b"")];
        let check = |history| check_history(&p1.0, backups, &history, true);

        let verdict = check(with_fork_actor(records(), &["repo.8.A", "repo.9.B"], &[]));
        assert_eq!(verdict.violations, []);
        assert!(
            matches!(&verdict.warnings[..], [Warning::ForkActorPartialBackup { backup, detail }]
                if backup == "repo.9.B" && detail.contains("header")),
            "{verdict:?}"
        );
        assert_eq!(verdict.stats.chain_len, 2);

        let broken = |verdict: Verdict| {
            assert!(
                matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                    if detail.contains("repo.9.B")),
                "{verdict:?}"
            );
            assert_eq!(verdict.warnings, []);
        };
        // Not announced, or another backup announced.
        broken(check(with_fork_actor(records(), &[], &[])));
        broken(check(with_fork_actor(records(), &["repo.8.A"], &[])));
        // A fork was derived from it, so its copy was complete.
        broken(check(with_fork_actor(records(), &["repo.9.B"], &["repo.9.B"])));
    }

    #[test]
    fn unresolved_intents_are_fine_landed_or_not() {
        let [a, x] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let px = child(x, &p1.0);
        let records = vec![intent(a, &p0, &p1), intent(x, &p1.0, &px)];
        let verdict = check(&p1.0, &[(&p1.1, &p0)], records, false);
        assert_eq!(verdict.violations, []);
        assert_eq!(verdict.warnings, []);
        assert_eq!(verdict.stats.unresolved, 2);
    }

    #[test]
    fn non_storage_op_is_a_harness_error() {
        let p0 = initial();
        let op = Op::Gc { older_than_ms: None };
        let records = vec![Record::Intent { t_ms: 0, id: Uuid::new_v4(), op }];
        let verdict = check(&p0, &[], records, false);
        assert!(
            matches!(&verdict.violations[..], [Violation::Harness { .. }]),
            "{verdict:?}"
        );
    }

    #[test]
    fn a_listed_backup_deleted_before_its_read_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repo.1.X");
        assert_eq!(read_listed_backup(&path).unwrap(), None);
        fs::write(&path, b"bytes").unwrap();
        assert_eq!(read_listed_backup(&path).unwrap(), Some(b"bytes".to_vec()));
        // Any other error is still the harness's.
        assert!(read_listed_backup(dir.path()).is_err());
    }

    #[test]
    fn read_backups_keeps_only_links_and_skips_staging_files() {
        let dir = tempfile::tempdir().unwrap();
        let empty: Backups = read_backups(dir.path()).unwrap();
        assert!(empty.links.is_empty() && empty.unparsable.is_empty());

        let overwritten = dir.path().join("overwritten");
        fs::create_dir_all(&overwritten).unwrap();
        let [a] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        fs::write(overwritten.join(&p1.1), &p0).unwrap();
        fs::write(overwritten.join("repo.2.Y"), &p1.0).unwrap();
        fs::write(overwritten.join("repo.3.Z"), &p0).unwrap();
        fs::write(overwritten.join("repo.3.Z.staging.1"), b"torn").unwrap();
        fs::write(overwritten.join("repo.4.G"), b"garbage").unwrap();

        let Backups { links, names, unparsable } = read_backups(dir.path()).unwrap();
        // The type holds no bytes: links carry the chain fields only.
        let links: HashMap<String, Link> = links;
        let initial_link = Link { attempt: None, ..link(Uuid::nil(), None, None) };
        let initial_link =
            Link { host: String::new(), slot: String::new(), ..initial_link };
        let first = link(a, Some(&payload_etag(&p0)), Some(&p1.1));
        let expected = HashMap::from([
            (payload_etag(&p0), initial_link),
            (payload_etag(&p1.0), first),
        ]);
        assert_eq!(links, expected);
        let expected_names = HashMap::from([
            (p1.1.clone(), payload_etag(&p0)),
            ("repo.2.Y".to_owned(), payload_etag(&p1.0)),
            ("repo.3.Z".to_owned(), payload_etag(&p0)),
            ("repo.4.G".to_owned(), payload_etag(b"garbage")),
        ]);
        assert_eq!(names, expected_names);
        assert!(
            matches!(&unparsable[..], [(name, detail)]
                if name == "repo.4.G" && detail.contains("header")),
            "{unparsable:?}"
        );
    }
}
